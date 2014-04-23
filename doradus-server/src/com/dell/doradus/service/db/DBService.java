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

package com.dell.doradus.service.db;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import com.dell.doradus.core.ServerConfig;
import com.dell.doradus.service.Service;

/**
 * Provides methods that access the physical database. This is currently Cassandra but
 * could be other physical stores. Database configuration options (host, port, keyspace,
 * etc.) are defined in doradus.yaml and loaded via {@link ServerConfig}.
 * 
 * <h1>Initialization</h1>
 * 
 * DBService is a singleton class, implemented as a Doradus {@link Service}. When
 * {@link #startService()} is called, it launches a thread to establish the first DB
 * connection asynchronously. If a connection cannot be established, the thread keeps
 * retrying until successful. This allows Cassandra to start at its own pace, which can
 * take a while if it needs to recover. Before the first connection is made, all DB access
 * methods throw a {@link DBNotAvailableException}. When DB connections become available,
 * the service's state will change to {@link Service.State#RUNNING}. Hence,
 * {@link Service#waitForFullService()} can be called to block until this happens.
 * <p>
 * Methods for specific DBService areas are described below.
 * 
 * <h1>Store management</h1>
 * 
 * Methods are provided to add, delete, and query physical <i>stores</i>. In Cassandra,
 * these are ColumnFamilies. A generic {@link StoreTemplate} is used to create new stores.
 * See these methods:
 * 
 * <pre>
 *      {@link #createStore(StoreTemplate)} - Create a new store.
 *      {@link #deleteStore(String)} - Delete an existing store.
 *      {@link #getAllStoreNames()} - Get the name of all stores present.
 * </pre>
 * 
 * <h1>Schema management</h1>
 * 
 * Methods are provided to add, update, and get application schemas. An application is
 * defines as a set of <i>properties</i>, one of which is its schema. See methods such as:
 * 
 * <pre>
 *      {@link #getAllAppProperties()} - Get properties for all applications.
 *      {@link #getAppProperties(String)} - Get properties for a specific application.
 * </pre>
 * 
 * <h1>Updates</h1>
 * 
 * Updates are handled by creating a {@link DBTransaction} object, adding column and row
 * updates to it, and then committing it. See these methods:
 * 
 * <pre>
 *      {@link #startTransaction()} - Create a new {@link DBTransaction}.
 *      {@link #commit(DBTransaction)} - Commit the changes in a {@link DBTransaction}
 * </pre>
 * 
 * <h1>Queries</h1>
 * 
 * Methods are provided to fetch data in various ways: single column of a single row, a
 * set of columns for a set of rows, all columns of all rows (careful!), etc. Columns are
 * returned as a {@link DColumn}; rows are returned as a {@link DRow}. Set of columns or
 * rows are returned as iterators of these objects. Iterators typically return a batch
 * from the database; additional batches are fetched, if needed, as the objects are
 * iterated. See methods such as these:
 * 
 * <pre>
 *      {@link #getAllColumns(String, String)} - Get all columns of a single row.
 *      {@link #getAllRowsAllColumns(String)} - Get all rows of all columns.
 *      {@link #getColumn(String, String, String)} - Get a single column from a single row.
 *      {@link #getRowsAllColumns(String, Set)} - Get all columns of a set of row keys.
 * </pre>
 */
public class DBService extends Service {
    private static final DBService INSTANCE = new DBService();

    private final Queue<DBConn> m_dbConnectionList = new ArrayDeque<DBConn>();
    
    /**
     * Get the singleton instance of this service. The service may or may not have been
     * initialized yet.
     * 
     * @return  The singleton instance of this service.
     */
    public static DBService instance() {
        return INSTANCE;
    }   // instance

    //----- Public Service methods
    
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
    
    /**
     * Create a new store using the given template.
     * 
     * @param storeTemplate {@link StoreTemplate} that describes new store to be created.
     *                      Must match the type of physical database we're using.
     */
    public void createStore(StoreTemplate storeTemplate) {
        DBConn dbConn = DBService.instance().getDBConnection();
        try {
            dbConn.createStore(storeTemplate);
        } finally {
            DBService.instance().returnDBConnection(dbConn);
        }
    }   // createStore
    
    /**
     * Delete the store with the given name.
     * 
     * @param storeName Name of store to delete.
     */
    public void deleteStore(String storeName) {
        DBConn dbConn = DBService.instance().getDBConnection();
        try {
            dbConn.deleteStore(storeName);
        } finally {
            DBService.instance().returnDBConnection(dbConn);
        }
    }   // getAllStoreNames
    
    /**
     * Create a new store using the given template if a store with the given name does not exist.
     * 
     * @param storeTemplate {@link StoreTemplate} that describes new store to be created.
     *                      Must match the type of physical database we're using.
     */
    public void createNewStore(StoreTemplate storeTemplate) {
        DBConn dbConn = DBService.instance().getDBConnection();
        try {
        	if (!dbConn.storeExists(storeTemplate.getStoreName())) {
        		dbConn.createStore(storeTemplate);
        	}
        } finally {
            DBService.instance().returnDBConnection(dbConn);
        }
    }	// createNewStore
    
    /**
     * Enumerate all current store names used by this Doradus instance.
     * 
     * @return  List of all store names.
     */
    public Collection<String> getAllStoreNames() {
        DBConn dbConn = DBService.instance().getDBConnection();
        try {
            return dbConn.getAllStoreNames();
        } finally {
            DBService.instance().returnDBConnection(dbConn);
        }
    }   // getAllStoreNames

    /**
     * Return true if the given store name exists in the database.
     * 
     * @param storeName Candidate store name.
     * @return          True if the given store name exists in the database.
     */
    public boolean storeExists(String storeName) {
        DBConn dbConn = DBService.instance().getDBConnection();
        try {
            return dbConn.storeExists(storeName);
        } finally {
            DBService.instance().returnDBConnection(dbConn);
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
    public Map<String, Map<String, String>> getAllAppProperties() {
        DBConn dbConn = DBService.instance().getDBConnection();
        try {
            return dbConn.getAllAppProperties();
        } finally {
            DBService.instance().returnDBConnection(dbConn);
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
    public Map<String, String> getAppProperties(String appName) {
        DBConn dbConn = DBService.instance().getDBConnection();
        try {
            return dbConn.getAppProperties(appName);
        } finally {
            DBService.instance().returnDBConnection(dbConn);
        }
    }   // getAppProperties
    
    /**
     * Get the database-level options, if any, from the "options" row. If there are no
     * options stored, an empty map is returned (but not null).
     * 
     * @return  Map of database-level options as key/value pairs. Empty if there are no
     *          options stored.
     */
    public Map<String, String> getDBOptions() {
        DBConn dbConn = DBService.instance().getDBConnection();
        try {
            return dbConn.getDBOptions();
        } finally {
            DBService.instance().returnDBConnection(dbConn);
        }
    }   // getDBOptions

    //----- Public DBService methods: Updates
    
    /**
     * Create a new {@link DBTransaction} object that holds updates that will be committed
     * together. The transactions can be by calling {@link #commit(DBTransaction)}.
     * 
     * @return  New {@link DBTransaction} with a timestamp of "now".
     */
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
     * @param startCol	First name in the column names interval.
     * @param endCol	Last name in the column names interval.
     * @param reversed	Flag: reverse iteration?
     * @return          Iterator of {@link DColumn}s, or null if there is no such row.
     */
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
     * @param startCol	First name in the column names interval.
     * @param endCol	Last name in the column names interval.
     * @return          Iterator of {@link DColumn}s, or null if there is no such row.
     */
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
    
    public Iterator<DRow> getRowsColumnSlice(String				storeName,
    										 Collection<String> rowKeys,
    										 String 			startCol,
    										 String				endCol) {
    	DBConn dbConn = getDBConnection();
    	try {
    		return dbConn.getRowsColumns(storeName, rowKeys, startCol, endCol);
    	} finally {
    		returnDBConnection(dbConn);
    	}
    }	// getRowsColumnSlice

    public Iterator<DRow> getRowsColumnSlice(String				storeName,
    										 Collection<String> rowKeys,
    										 String 			startCol,
    										 String				endCol,
    										 boolean			reversed) {
    	DBConn dbConn = getDBConnection();
    	try {
    		return dbConn.getRowsColumns(storeName, rowKeys, startCol, endCol, reversed);
    	} finally {
    		returnDBConnection(dbConn);
    	}
    }	// getRowsColumnSlice

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
                dbConn = new CassandraDBConn(false);
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
                m_dbConnectionList.add(dbConn);
            }
        }
    }   // returnDBConnection

    //----- Private methods

    // Singleton construction only
    private DBService() {}
    
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
                        DBConn firstDbConn = new CassandraDBConn(true);
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

}   // class DBService

