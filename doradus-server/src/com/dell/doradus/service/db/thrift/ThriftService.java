/*
 * Copyright (C) 2014 Dell, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dell.doradus.service.db.thrift;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;

import com.dell.doradus.core.ServerConfig;
import com.dell.doradus.service.db.DBNotAvailableException;
import com.dell.doradus.service.db.DBService;
import com.dell.doradus.service.db.DBTransaction;
import com.dell.doradus.service.db.DColumn;
import com.dell.doradus.service.db.DRow;
import com.dell.doradus.service.db.StoreTemplate;

public class ThriftService extends DBService {
    private static final ThriftService INSTANCE = new ThriftService();
    private final Queue<DBConn> m_dbConnectionList = new ArrayDeque<DBConn>();
    
    private ThriftService() { }

    //----- Public Service methods

    /**
     * Return the singleton ThriftService service object.
     * 
     * @return  Static ThriftService object.
     */
    public static ThriftService instance() {return INSTANCE;}
    
    @Override
    public void initService() {
        ServerConfig config = ServerConfig.getInstance();
        m_logger.debug("Cassandra host list: {}", Arrays.toString(config.dbhost.split(",")));
        m_logger.debug("Cassandra port: {}", config.dbport);
        m_logger.debug("Cassandra keyspace: {}", config.keyspace);
    }   // initService

    @Override
    public void startService() {
        initializeDBConnections();
    }   // stopService

    @Override
    public void stopService() {
        synchronized (m_dbConnectionList) {
            Iterator<DBConn> iter = m_dbConnectionList.iterator();
            while (iter.hasNext()) {
                DBConn dbConn = iter.next();
                dbConn.close();
                iter.remove();
            }
        }
    }   // stopService

    //----- Public DBService methods: Store management
    
    @Override
    public void createStoreIfAbsent(StoreTemplate storeTemplate) {
        DBConn dbConn = getDBConnection();
        try {
            if (!dbConn.storeExists(storeTemplate.getName())) {
                dbConn.createStore(storeTemplate);
            }
        } finally {
            returnDBConnection(dbConn);
        }
    }   // createStoreIfAbsent
    
    @Override
    public void deleteStoreIfPresent(String storeName) {
        DBConn dbConn = getDBConnection();
        try {
            if (dbConn.storeExists(storeName)) {
                dbConn.deleteStore(storeName);
            }
        } finally {
            returnDBConnection(dbConn);
        }
    }   // deleteStoreIfPresent
    
    @Override
    public boolean storeExists(String storeName) {
        DBConn dbConn = getDBConnection();
        try {
            return dbConn.storeExists(storeName);
        } finally {
            returnDBConnection(dbConn);
        }
    }   // storeExists
    
    //----- Public DBService methods: Schema management
    
    /**
     * Get all properties of all registered applications. The result is a map of
     * application names to a map of application properties. Example application properties
     * are _application, _version, and _format, which define the application's schema
     * and the way it is stored in the database. The result is empty if there are no
     * applications defined.
     * 
     * @return  Map of application name -> property name -> property value for all
     *          known applications. Empty of there are no applications defined.
     */
    @Override
    public Map<String, Map<String, String>> getAllAppProperties() {
        DBConn dbConn = getDBConnection();
        try {
            return dbConn.getAllAppProperties();
        } finally {
            returnDBConnection(dbConn);
        }
    }   // getAllAppProperties

    /**
     * Get all properties of the application with the given name. The property names are
     * typically _application, _version, and _format, which define the application's
     * schema and the way it is stored in the database. The result is null if the given
     * application is not defined. 
     * 
     * @param appName   Name of application whose properties to get.
     * @return          Map of application properties as key/value pairs or null if no
     *                  such application is defined.
     */
    @Override
    public Map<String, String> getAppProperties(String appName) {
        DBConn dbConn = getDBConnection();
        try {
            return dbConn.getAppProperties(appName);
        } finally {
            returnDBConnection(dbConn);
        }
    }   // getAppProperties
    
    //----- Public DBService methods: Updates
    
    /**
     * Create a new {@link DBTransaction} object that holds updates that will be committed
     * together. The transactions can be by calling {@link #commit(DBTransaction)}.
     * 
     * @return  New {@link DBTransaction} with a timestamp of "now".
     */
    @Override
    public DBTransaction startTransaction() {
        return new CassandraTransaction();
    }   // startTransaction
    
    /**
     * Commit the updates in the given {@link DBTransaction}. An exception is thrown if
     * the updates cannot be committed after all retries. Regardless of whether the
     * updates are successful or not, the updates are cleared from the transaction object
     * before returning.
     * 
     * @param dbTran    {@link DBTransaction} containing updates to commit.
     */
    @Override
    public void commit(DBTransaction dbTran) {
        DBConn dbConn = getDBConnection();
        try {
            dbConn.commit(dbTran);
        } finally {
            returnDBConnection(dbConn);
        }
    }   // commit
    
    //----- Public DBService methods: Queries

    /**
     * Get all columns for the row with the given key in the given store. Columns are
     * returned as an Iterator of {@link DColumn}s. Null is returned if no row is
     * found with the given key.
     * 
     * @param storeName Name of store to query.
     * @param rowKey    Key of row to fetch.
     * @return          Iterator of {@link DColumn}s, or null if there is no such row.
     */
    @Override
    public Iterator<DColumn> getAllColumns(String storeName, String rowKey) {
        DBConn dbConn = getDBConnection();
        try {
            return dbConn.getAllColumns(storeName, rowKey);
        } finally {
            returnDBConnection(dbConn);
        }
    }   // getAllColumns

    /**
     * Get columns for the row with the given key in the given store. Columns range
     * is defined by an interval [startCol, endCol]. Columns are returned
     * as an Iterator of {@link DColumn}s. Empty iterator is returned if no row
     * is found with the given key or no columns in the given interval found.
     * 
     * @param storeName Name of store to query.
     * @param rowKey    Key of row to fetch.
     * @param startCol  First name in the column names interval.
     * @param endCol    Last name in the column names interval.
     * @param reversed  Flag: reverse iteration?
     * @return          Iterator of {@link DColumn}s, or null if there is no such row.
     */
    @Override
    public Iterator<DColumn> getColumnSlice(String storeName, String rowKey,
            String startCol, String endCol, boolean reversed) {
        DBConn dbConn = getDBConnection();
        try {
            return dbConn.getColumnSlice(storeName, rowKey, startCol, endCol, reversed);
        } finally {
            returnDBConnection(dbConn);
        }
    }   // getColumnSlice

    /**
     * Get columns for the row with the given key in the given store. Columns range
     * is defined by an interval [startCol, endCol]. Columns are returned
     * as an Iterator of {@link DColumn}s. Empty iterator is returned if no row
     * is found with the given key or no columns in the given interval found.
     * 
     * @param storeName Name of store to query.
     * @param rowKey    Key of row to fetch.
     * @param startCol  First name in the column names interval.
     * @param endCol    Last name in the column names interval.
     * @return          Iterator of {@link DColumn}s, or null if there is no such row.
     */
    @Override
    public Iterator<DColumn> getColumnSlice(String storeName, String rowKey,
            String startCol, String endCol) {
        return getColumnSlice(storeName, rowKey, startCol, endCol, false);
    }   // getColumnSlice

    /**
     * Get all rows of all columns in the given store. The results are returned as an
     * Iterator for {@link DRow} objects. If no rows are found, the iterator's hasNext()
     * method will immediately return false. If more rows are fetched than an internal
     * limit allows, an exception is thrown.
     * 
     * @param storeName Name of physical store to query.
     * @return          Iterator of {@link DRow} objects. May be empty but not null.
     */
    @Override
    public Iterator<DRow> getAllRowsAllColumns(String storeName) {
        DBConn dbConn = getDBConnection();
        try {
            return dbConn.getAllRowsAllColumns(storeName);
        } finally {
            returnDBConnection(dbConn);
        }
    }   // getAllRowsAllColumns

    /**
     * Get a single column for a single row in the given store. If the given row or column
     * is not found, null is returned. Otherwise, a {@link DColumn} containing the column
     * name and value is returned.
     * 
     * @param storeName Name of store to query.
     * @param rowKey    Key of row to read.
     * @param colName   Name of column to fetch.
     * @return          {@link DColumn} containing the column name and value or null if
     *                  the row or column was not found.
     */
    @Override
    public DColumn getColumn(String storeName, String rowKey, String colName) {
        DBConn dbConn = getDBConnection();
        try {
            return dbConn.getColumn(storeName, rowKey, colName);
        } finally {
            returnDBConnection(dbConn);
        }
    }   // getColumn

    /**
     * Get all columns for rows with a specific set of keys. Results are returned as an
     * Iterator of {@link DRow} objects. If any given row is not found, no entry will
     * with its key will be returned.
     * 
     * @param store     Name of store to query.
     * @param rowKeys   Collection of row keys to read.
     * @return          Iterator of {@link DRow} objects. May be empty but not null.
     */
    @Override
    public Iterator<DRow> getRowsAllColumns(String storeName, Collection<String> rowKeys) {
        DBConn dbConn = getDBConnection();
        try {
            return dbConn.getRowsAllColumns(storeName, rowKeys);
        } finally {
            returnDBConnection(dbConn);
        }
    }   // getRowsAllColumns

    /**
     * Get a specific set of columns for a specific set of rows. Results are returned as an
     * Iterator of {@link DRow} objects. If any given row is not found, no entry will exist
     * for it in iterator. If a row is found but none of the requested columns were found,
     * DRow object's column iterator will be empty.
     * 
     * @param store     Name of store to query.
     * @param rowKeys   Collection of row keys to read.
     * @param colNames  Collection of column names to read.
     * @return          Iterator for {@link DRow} objects. May be empty but not null.
     */
    @Override
    public Iterator<DRow> getRowsColumns(String             storeName,
                                         Collection<String> rowKeys,
                                         Collection<String> colNames) {
        DBConn dbConn = getDBConnection();
        try {
            return dbConn.getRowsColumns(storeName, rowKeys, colNames);
        } finally {
            returnDBConnection(dbConn);
        }
    }   // getRowsColumnSet
    
    @Override
    public Iterator<DRow> getRowsColumnSlice(String             storeName,
                                             Collection<String> rowKeys,
                                             String             startCol,
                                             String             endCol) {
        DBConn dbConn = getDBConnection();
        try {
            return dbConn.getRowsColumns(storeName, rowKeys, startCol, endCol);
        } finally {
            returnDBConnection(dbConn);
        }
    }   // getRowsColumnSlice

    @Override
    public Iterator<DRow> getRowsColumnSlice(String             storeName,
                                             Collection<String> rowKeys,
                                             String             startCol,
                                             String             endCol,
                                             boolean            reversed) {
        DBConn dbConn = getDBConnection();
        try {
            return dbConn.getRowsColumns(storeName, rowKeys, startCol, endCol, reversed);
        } finally {
            returnDBConnection(dbConn);
        }
    }   // getRowsColumnSlice

    //----- Package-private methods
    
    // Get an available database connection from the pool, creating a new one if needed.
    // This method will throw an error if the database is not currently available.
    DBConn getDBConnection() {
        DBConn dbConn = null;
        synchronized (m_dbConnectionList) {
            if (!getState().isRunning()) {
                throw new DBNotAvailableException("Initial Cassandra connection hasn't been established");
            }
            if (m_dbConnectionList.size() > 0) {
                dbConn = m_dbConnectionList.poll();
            } else {
                // In the future, we could create a different type based on configuration.
                dbConn = new DBConn(false);
            }
        }
        return dbConn;
    }   // getDBConnection
    
    // Return a database connection object to the pool. It is OK if the given connection
    // is null -- this reduces checking in "finally" blocks.
    void returnDBConnection(DBConn dbConn) {
        if (dbConn == null) {
            return;
        }
        
        if (!getState().isRunning()) {
            dbConn.close();     // from straggler thread; close and discard
        } else if (dbConn.isFailed()) {
            // Purge all connections in case a Cassandra node is now dead.
            dbConn.close();
            synchronized (m_dbConnectionList) {
                m_logger.info("Purging database connection pool");
                while (m_dbConnectionList.size() > 0) {
                    m_dbConnectionList.remove().close();
                }
            }
        } else {
            synchronized (m_dbConnectionList) {
                m_dbConnectionList.add((DBConn)dbConn);
            }
        }
    }   // returnDBConnection

    //----- Private methods
    
    // Initialize the DBConnection pool by creating the first connection to Cassandra.
    // Because Cassandra may be started at the same time we are, it may be 10 minutes or
    // more before Cassandra will talk to us. Hence, if we can't connect now, we just
    // sleep and keep trying. This is done in an asynchronous thread so we don't block
    // service requests. However, any request that requires the database will receive a
    // "waiting for Cassandra" exception until our thread successfully finishes.
    private void initializeDBConnections() {
        startFirstConnectThread();
        
        // If DoradusServer is being started in a test environment, the thread above may
        // not connect to Cassandra before a request is made, even though Cassandra is
        // running. To prevent this race condition, we wait up to 3 seconds, trying to
        // prevent unnecessary "service unavailable" errors. We check every 100 millis
        // so we proceed as soon as possible.
        for (int check = 0; !getState().isRunning() && check < 30; check++) {
            // Wait 100 millis, so 30 * 100 millis = 3 seconds.
            try {
                Thread.sleep(300);
            } catch (InterruptedException ex) {
                // ignore
            }
        }
    }   // initializeDBConnections

    // Start the asynchronous thread that connects to Cassandra, waiting indefinitely
    // until it succeeds. Once a connection is made, setRunning() is called.
    private void startFirstConnectThread() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                while (!getState().isRunning()) {
                    try {
                        DBConn firstDbConn = new DBConn(true);
                        synchronized (m_dbConnectionList) {
                            m_dbConnectionList.add(firstDbConn);
                            setRunning();
                        }
                    } catch (DBNotAvailableException ex) {
                        m_logger.info("Database is not reachable. Waiting to retry");
                        try {
                            Thread.sleep(ServerConfig.getInstance().db_connect_retry_wait_millis);
                        } catch (InterruptedException ex2) {
                            // ignore
                        }
                    } catch (Throwable ex) {
                        m_logger.error("!Fatal error trying to create first database connection -- shutting down", ex);
                        System.exit(1);
                    }
                }
            }   // run
        }).start();
    }   // startFirstConnectThread

}   // class ThriftService
