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

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.dell.doradus.core.ServerConfig;
import com.dell.doradus.service.Service;
import com.dell.doradus.service.db.cql.CQLService;
import com.dell.doradus.service.db.thrift.ThriftService;

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
 *      {@link #deleteStoreIfPresent(String)} - Delete an existing store.
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
public abstract class DBService extends Service {
    // Experimental: Choose service based on doradus.yaml setting
    private static final DBService INSTANCE =
        ServerConfig.getInstance().use_cql ? CQLService.instance() : ThriftService.instance();

    // Only subclasses can construct an object.
    protected DBService() {}
    
    /**
     * Get the singleton instance of this service. The service may or may not have been
     * initialized yet.
     * 
     * @return  The singleton instance of this service.
     */
    public static DBService instance() {
        return INSTANCE;
    }   // instance

    //----- Public Service methods: implemented by subclass
    
    //----- Public DBService methods: Store management
    
    /**
     * Create a new store using the given template if a store with the given name does not
     * exist. If the given store name already exists, this is a no-op.
     * 
     * @param storeTemplate {@link StoreTemplate} that describes new store to create.
     */
    public abstract void createStoreIfAbsent(StoreTemplate storeTemplate);
    
    /**
     * Delete the store with the given name if it exists. If the store does not exist,
     * this is a no-op.
     * 
     * @param storeName Name of store to delete.
     */
    public abstract void deleteStoreIfPresent(String storeName);
    
    /**
     * Return true if the given store name exists in the database.
     * 
     * @param storeName Candidate store name.
     * @return          True if the given store name exists in the database.
     */
    public abstract boolean storeExists(String storeName);
    
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
    public abstract Map<String, Map<String, String>> getAllAppProperties();

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
    public abstract Map<String, String> getAppProperties(String appName);
    
    //----- Public DBService methods: Updates
    
    /**
     * Create a new {@link DBTransaction} object that holds updates that will be committed
     * together. The transactions can be by calling {@link #commit(DBTransaction)}.
     * 
     * @return  New {@link DBTransaction} with a timestamp of "now".
     */
    public abstract DBTransaction startTransaction();
    
    /**
     * Commit the updates in the given {@link DBTransaction}. An exception is thrown if
     * the updates cannot be committed after all retries. Regardless of whether the
     * updates are successful or not, the updates are cleared from the transaction object
     * before returning.
     * 
     * @param dbTran    {@link DBTransaction} containing updates to commit.
     */
    public abstract void commit(DBTransaction dbTran);
    
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
    public abstract Iterator<DColumn> getAllColumns(String storeName, String rowKey);

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
    public abstract Iterator<DColumn> getColumnSlice(String storeName, String rowKey,
    		String startCol, String endCol, boolean reversed);

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
    public abstract Iterator<DColumn> getColumnSlice(String storeName, String rowKey,
    		String startCol, String endCol);

    /**
     * Get all rows of all columns in the given store. The results are returned as an
     * Iterator for {@link DRow} objects. If no rows are found, the iterator's hasNext()
     * method will immediately return false. If more rows are fetched than an internal
     * limit allows, an exception is thrown.
     * 
     * @param storeName Name of physical store to query.
     * @return          Iterator of {@link DRow} objects. May be empty but not null.
     */
    public abstract Iterator<DRow> getAllRowsAllColumns(String storeName);

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
    public abstract DColumn getColumn(String storeName, String rowKey, String colName);

    /**
     * Get all columns for rows with a specific set of keys. Results are returned as an
     * Iterator of {@link DRow} objects. If any given row is not found, no entry will
     * with its key will be returned.
     * 
     * @param store     Name of store to query.
     * @param rowKeys   Collection of row keys to read.
     * @return          Iterator of {@link DRow} objects. May be empty but not null.
     */
    public abstract Iterator<DRow> getRowsAllColumns(String storeName, Collection<String> rowKeys);

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
    public abstract Iterator<DRow> getRowsColumns(String             storeName,
                                                  Collection<String> rowKeys,
                                                  Collection<String> colNames);
    
    public abstract Iterator<DRow> getRowsColumnSlice(String             storeName,
                                                      Collection<String> rowKeys,
                                                      String             startCol,
                                                      String             endCol);

    public abstract Iterator<DRow> getRowsColumnSlice(String             storeName,
                                                      Collection<String> rowKeys,
                                                      String             startCol,
                                                      String             endCol,
                                                      boolean            reversed);

}   // class DBService

