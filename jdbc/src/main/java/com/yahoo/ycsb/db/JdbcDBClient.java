/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.                                                                                                                             
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you                                                                                                             
 * may not use this file except in compliance with the License. You                                                                                                                
 * may obtain a copy of the License at                                                                                                                                             
 *
 * http://www.apache.org/licenses/LICENSE-2.0                                                                                                                                      
 *
 * Unless required by applicable law or agreed to in writing, software                                                                                                             
 * distributed under the License is distributed on an "AS IS" BASIS,                                                                                                               
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or                                                                                                                 
 * implied. See the License for the specific language governing                                                                                                                    
 * permissions and limitations under the License. See accompanying                                                                                                                 
 * LICENSE file. 
 */

package com.yahoo.ycsb.db;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.StringByteIterator;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * A class that wraps a JDBC compliant database to allow it to be interfaced with YCSB.
 * This class extends {@link DB} and implements the database interface used by YCSB client.
 * <p/>
 * <br> Each client will have its own instance of this class. This client is
 * not thread safe.
 * <p/>
 * <br> This interface expects a schema <key> <field1> <field2> <field3> ...
 * All attributes are of type VARCHAR. All accesses are through the primary key. Therefore,
 * only one index on the primary key is needed.
 * <p/>
 * <p> The following options must be passed when using this database client.
 * <p/>
 * <ul>
 * <li><b>db.driver</b> The JDBC driver class to use.</li>
 * <li><b>db.url</b> The Database connection URL.</li>
 * <li><b>db.user</b> User name for the connection.</li>
 * <li><b>db.passwd</b> Password for the connection.</li>
 * </ul>
 *
 * @author sudipto
 */
public class JdbcDBClient extends DB implements JdbcDBClientConstants {

    private List<ConnectionDelegate> connectionDelegates;
    private boolean initialized = false;
    private Properties props;
    private static final String DEFAULT_PROP = "";
    private ConcurrentMap<StatementType, PreparedStatement> cachedStatements;

    /**
     * The statement type for the prepared statements.
     */
    private static class StatementType {

        enum Type {
            INSERT(1),
            DELETE(2),
            READ(3),
            UPDATE(4),
            SCAN(5),;
            int internalType;

            private Type(int type) {
                internalType = type;
            }

            int getHashCode() {
                final int prime = 31;
                int result = 1;
                result = prime * result + internalType;
                return result;
            }
        }

        Type type;
        int shardIndex;
        int numFields;
        String tableName;

        StatementType(Type type, String tableName, int numFields, int _shardIndex) {
            this.type = type;
            this.tableName = tableName;
            this.numFields = numFields;
            this.shardIndex = _shardIndex;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + numFields + 100 * shardIndex;
            result = prime * result
                    + ((tableName == null) ? 0 : tableName.hashCode());
            result = prime * result + ((type == null) ? 0 : type.getHashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            StatementType other = (StatementType) obj;
            if (numFields != other.numFields)
                return false;
            if (shardIndex != other.shardIndex)
                return false;
            if (tableName == null) {
                if (other.tableName != null)
                    return false;
            } else if (!tableName.equals(other.tableName))
                return false;
            if (type != other.type)
                return false;
            return true;
        }
    }

    class ConnectionDelegate {
        private final String url;
        private final String user;
        private final String password;

        private Connection connection;

        public ConnectionDelegate(String url, String user, String password) {
            this.url = url;
            this.user = user;
            this.password = password;
        }

        public Connection getConnection() {
            return connection;
        }

        public void connect() throws SQLException {
            DriverManager.setLoginTimeout(60);
            connection = DriverManager.getConnection(url, user, password);
            connection.setAutoCommit(true);
        }

        public void reconnect() throws SQLException {
            close();
            connect();
        }

        public void close() throws SQLException {
            connection.close();
            connection = null;
        }
    }

    protected abstract class TransactionTry {

        private final String key;
        private final int maxRetries;
        private final int returnCode;

        public TransactionTry(String key, int maxRetries, int returnCode) {
            this.key = key;
            this.maxRetries = maxRetries;
            this.returnCode = returnCode;
        }

        protected abstract int action() throws SQLException;

        /**
         * Ideally all drivers would distinguish non-transient from transient
         * exceptions, but they don't. So we introduce an exception filter here
         * to address this concern. As a fallback this does recognize the types,
         * but implementers can customize this by the exception message if need
         * be.
         *
         * @param exception the exception from an executed SQL operation.
         * @return true if the exception is a non-transient (critical, unrecoverable, issue).
         * @see java.sql.SQLNonTransientConnectionException
         */
        protected boolean exceptionRequiresReconnect(SQLException exception) {
            if (exception instanceof SQLTransientException) {
                return false;
            }
            final String matchesRequiringReconnect[] = {
                    "End of stream reached",
                    "Connection reset",
                    "Broken pipe"
            };
            for (String matchRequiringReconnect : matchesRequiringReconnect) {
                if (exception.getMessage().contains(matchRequiringReconnect)) {
                    return true;
                }
            }
            // be safe therefore assume a reconnect if there are other unforeseen failures
            return false;
        }

        protected abstract StatementType getStatementType();

        protected abstract void onError(SQLException exception);

        public int execute() {
            int retries = maxRetries;
            while (retries > 0) {
                try {
                    return action();
                } catch (SQLException e) {
                    onError(e);
                    if (exceptionRequiresReconnect(e)) {
                        cachedStatements.clear();
                        ConnectionDelegate connectionDelegate = connectionDelegates.get(getShardIndexByKey(key));
                        try {
                            connectionDelegate.reconnect();
                        } catch (SQLException e1) {
                            onError(e1);
                            throw new RuntimeException(e1);
                        }
                    }
                    retries--;
                }
            }
            return returnCode;
        }
    }

    /**
     * For the given key, returns what shard contains data for this key
     *
     * @param key Data key to do operation on
     * @return Shard index
     */
    private int getShardIndexByKey(String key) {
        int ret = Math.abs(key.hashCode()) % connectionDelegates.size();
        //System.out.println(connectionDelegates.size() + ": Shard instance for "+ key + " (hash  " + key.hashCode()+ " ) " + " is " + ret);
        return ret;
    }

    /**
     * For the given key, returns Connection object that holds connection
     * to the shard that contains this key
     *
     * @param key Data key to get information for
     * @return Connection object
     */
    private Connection getShardConnectionByKey(String key) {
        return connectionDelegates.get(getShardIndexByKey(key)).getConnection();
    }

    private void cleanupAllConnections() throws SQLException {
        for (ConnectionDelegate connectionDelegate : connectionDelegates) {
            connectionDelegate.close();
        }
    }

    /**
     * Initialize the database connection and set it up for sending requests to the database.
     * This must be called once per client.
     *
     * @throws DBException If it failed establishing a database connection.
     *                     See exception message text for details.
     */
    @Override
    public void init() throws DBException {
        if (initialized) {
            System.err.println("Client connection already initialized.");
            return;
        }
        props = getProperties();
        String urls = props.getProperty(CONNECTION_URL, DEFAULT_PROP);
        String user = props.getProperty(CONNECTION_USER, DEFAULT_PROP);
        String passwd = props.getProperty(CONNECTION_PASSWD, DEFAULT_PROP);
        String driver = props.getProperty(DRIVER_CLASS);

        try {
            if (driver != null) {
                Class.forName(driver);
            }
            int shardCount = 0;
            connectionDelegates = new ArrayList<ConnectionDelegate>(3);
            for (String url : urls.split(",")) {
                System.out.println("Adding shard node URL: " + url);
                createConnection(url, user, passwd);
                shardCount++;
            }

            System.out.println("Using " + shardCount + " shards");

            cachedStatements = new ConcurrentHashMap<StatementType, PreparedStatement>();
        } catch (ClassNotFoundException e) {
            System.err.println("Error in initializing the JDBC driver: " + e);
            throw new DBException(e);
        } catch (SQLException e) {
            System.err.println("Error in database operation: " + e);
            throw new DBException(e);
        } catch (NumberFormatException e) {
            System.err.println("Invalid value for fieldcount property. " + e);
            throw new DBException(e);
        }
        initialized = true;
    }

    private void createConnection(String url, String user, String password) throws SQLException {
        ConnectionDelegate connectionDelegate = new ConnectionDelegate(url, user, password);
        connectionDelegate.connect();
        connectionDelegates.add(connectionDelegate);
    }

    @Override
    public void cleanup() throws DBException {
        try {
            cleanupAllConnections();
        } catch (SQLException e) {
            System.err.println("Error in closing the connection. " + e);
            throw new DBException(e);
        }
    }

    private PreparedStatement createAndCacheInsertStatement(StatementType insertType, String key)
            throws SQLException {
        StringBuilder insert = new StringBuilder("INSERT INTO ");
        insert.append(insertType.tableName);
        insert.append(" VALUES(?");
        for (int i = 0; i < insertType.numFields; i++) {
            insert.append(",?");
        }
        insert.append(");");
        PreparedStatement insertStatement = getShardConnectionByKey(key).prepareStatement(insert.toString());
        PreparedStatement stmt = cachedStatements.putIfAbsent(insertType, insertStatement);
        if (stmt == null) return insertStatement;
        else return stmt;
    }

    private PreparedStatement createAndCacheReadStatement(StatementType readType, String key)
            throws SQLException {
        StringBuilder read = new StringBuilder("SELECT * FROM ");
        read.append(readType.tableName);
        read.append(" WHERE ");
        read.append(PRIMARY_KEY);
        read.append(" = ");
        read.append("?;");
        PreparedStatement readStatement = getShardConnectionByKey(key).prepareStatement(read.toString());
        PreparedStatement stmt = cachedStatements.putIfAbsent(readType, readStatement);
        if (stmt == null) return readStatement;
        else return stmt;
    }

    private PreparedStatement createAndCacheDeleteStatement(StatementType deleteType, String key)
            throws SQLException {
        StringBuilder delete = new StringBuilder("DELETE FROM ");
        delete.append(deleteType.tableName);
        delete.append(" WHERE ");
        delete.append(PRIMARY_KEY);
        delete.append(" = ?;");
        PreparedStatement deleteStatement = getShardConnectionByKey(key).prepareStatement(delete.toString());
        PreparedStatement stmt = cachedStatements.putIfAbsent(deleteType, deleteStatement);
        if (stmt == null) return deleteStatement;
        else return stmt;
    }

    private PreparedStatement createAndCacheUpdateStatement(StatementType updateType, String key)
            throws SQLException {
        StringBuilder update = new StringBuilder("UPDATE ");
        update.append(updateType.tableName);
        update.append(" SET ");
        for (int i = 1; i <= updateType.numFields; i++) {
            update.append(COLUMN_PREFIX);
            update.append(i);
            update.append("=?");
            if (i < updateType.numFields) update.append(", ");
        }
        update.append(" WHERE ");
        update.append(PRIMARY_KEY);
        update.append(" = ?;");
        PreparedStatement insertStatement = getShardConnectionByKey(key).prepareStatement(update.toString());
        PreparedStatement stmt = cachedStatements.putIfAbsent(updateType, insertStatement);
        if (stmt == null) return insertStatement;
        else return stmt;
    }

    private PreparedStatement createAndCacheScanStatement(StatementType scanType, String key)
            throws SQLException {
        StringBuilder select = new StringBuilder("SELECT * FROM ");
        select.append(scanType.tableName);
        select.append(" WHERE ");
        select.append(PRIMARY_KEY);
        select.append(" >= ");
        select.append("?;");
        PreparedStatement scanStatement = getShardConnectionByKey(key).prepareStatement(select.toString());
        PreparedStatement stmt = cachedStatements.putIfAbsent(scanType, scanStatement);
        if (stmt == null) return scanStatement;
        else return stmt;
    }

    @Override
    public int read(final String tableName, final String key, final Set<String> fields,
                    final HashMap<String, ByteIterator> result) {
        if (tableName == null) {
            return -1;
        }
        if (key == null) {
            return -1;
        }
        TransactionTry transactionTry = new TransactionTry(key, 3, -2) {
            @Override
            protected int action() throws SQLException {
                StatementType type = getStatementType();
                PreparedStatement readStatement = cachedStatements.get(type);
                if (readStatement == null) {
                    readStatement = createAndCacheReadStatement(type, key);
                }
                readStatement.setString(1, key);

                ResultSet resultSet = readStatement.executeQuery();
                if (!resultSet.next()) {
                    resultSet.close();
                    return 1;
                }
                if (result != null && fields != null) {
                    for (String field : fields) {
                        String value = resultSet.getString(field);
                        result.put(field, new StringByteIterator(value));
                    }
                }
                resultSet.close();

                return SUCCESS;
            }

            @Override
            protected StatementType getStatementType() {
                return new StatementType(StatementType.Type.READ, tableName, 1, getShardIndexByKey(key));
            }

            @Override
            protected void onError(SQLException e) {
                System.err.println("Error in processing read of table " + tableName + ": " + e);
            }
        };
        return transactionTry.execute();
    }

    @Override
    public int scan(final String tableName, final String key, final int recordcount,
                    final Set<String> fields, final Vector<HashMap<String, ByteIterator>> result) {
        if (tableName == null) {
            return -1;
        }
        if (key == null) {
            return -1;
        }
        TransactionTry transactionTry = new TransactionTry(key, 3, -2) {
            @Override
            protected int action() throws SQLException {
                StatementType type = getStatementType();
                PreparedStatement scanStatement = cachedStatements.get(type);
                if (scanStatement == null) {
                    scanStatement = createAndCacheScanStatement(type, key);
                }
                scanStatement.setString(1, key);
                ResultSet resultSet = scanStatement.executeQuery();
                for (int i = 0; i < recordcount && resultSet.next(); i++) {
                    if (result != null && fields != null) {
                        HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
                        for (String field : fields) {
                            String value = resultSet.getString(field);
                            values.put(field, new StringByteIterator(value));
                        }
                        result.add(values);
                    }
                }
                resultSet.close();
                return SUCCESS;
            }

            @Override
            protected StatementType getStatementType() {
                return new StatementType(StatementType.Type.SCAN, tableName, 1, getShardIndexByKey(key));
            }

            @Override
            protected void onError(SQLException e) {
                System.err.println("Error in processing scan of table: " + tableName + e);
            }
        };
        return transactionTry.execute();
    }

    @Override
    public int update(final String tableName, final String key, final HashMap<String, ByteIterator> values) {
        if (tableName == null) {
            return -1;
        }
        if (key == null) {
            return -1;
        }
        TransactionTry transactionTry = new TransactionTry(key, 3, -1) {
            @Override
            protected int action() throws SQLException {
                StatementType type = getStatementType();
                PreparedStatement updateStatement = cachedStatements.get(type);
                if (updateStatement == null) {
                    updateStatement = createAndCacheUpdateStatement(type, key);
                }
                int index = 1;
                for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
                    updateStatement.setString(index++, entry.getValue().toString());
                }
                updateStatement.setString(index, key);
                int result = updateStatement.executeUpdate();
                if (result == 1) return SUCCESS;
                else return 1;
            }

            @Override
            protected StatementType getStatementType() {
                return new StatementType(StatementType.Type.UPDATE, tableName, values.size(), getShardIndexByKey(key));
            }

            @Override
            protected void onError(SQLException e) {
                if (!e.getMessage().contains("update conflict in table")) {
                    System.err.println("Error in processing update to table: " + tableName + e);
                }
            }
        };
        return transactionTry.execute();
    }

    @Override
    public int insert(final String tableName, final String key, final HashMap<String, ByteIterator> values) {
        if (tableName == null) {
            return -1;
        }
        if (key == null) {
            return -1;
        }
        TransactionTry transactionTry = new TransactionTry(key, 3, -1) {
            @Override
            protected int action() throws SQLException {
                StatementType type = getStatementType();
                PreparedStatement insertStatement = cachedStatements.get(type);
                if (insertStatement == null) {
                    insertStatement = createAndCacheInsertStatement(type, key);
                }
                insertStatement.setString(1, key);
                int index = 2;
                for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
                    String field = entry.getValue().toString();
                    insertStatement.setString(index++, field);
                }
                int result = insertStatement.executeUpdate();
                if (result == 1) return SUCCESS;
                else return 1;
            }

            @Override
            protected StatementType getStatementType() {
                return new StatementType(StatementType.Type.INSERT, tableName, values.size(), getShardIndexByKey(key));
            }

            @Override
            protected void onError(SQLException e) {
                System.err.println("Error in processing insert to table: " + tableName + e);
            }
        };
        return transactionTry.execute();
    }

    @Override
    public int delete(final String tableName, final String key) {
        if (tableName == null) {
            return -1;
        }
        if (key == null) {
            return -1;
        }
        TransactionTry transactionTry = new TransactionTry(key, 3, -1) {
            @Override
            protected int action() throws SQLException {
                StatementType type = getStatementType();
                PreparedStatement deleteStatement = cachedStatements.get(type);
                if (deleteStatement == null) {
                    deleteStatement = createAndCacheDeleteStatement(type, key);
                }
                deleteStatement.setString(1, key);
                int result = deleteStatement.executeUpdate();
                if (result == 1) return SUCCESS;
                else return 1;
            }

            @Override
            protected StatementType getStatementType() {
                return new StatementType(StatementType.Type.DELETE, tableName, 1, getShardIndexByKey(key));
            }

            @Override
            protected void onError(SQLException e) {
                System.err.println("Error in processing delete to table: " + tableName + e);
            }
        };
        return transactionTry.execute();
    }
}
