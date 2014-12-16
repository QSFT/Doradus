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
import java.util.SortedMap;
import java.util.SortedSet;

import com.dell.doradus.core.ServerConfig;
import com.dell.doradus.service.Service;
import com.dell.doradus.service.db.cql.CQLService;
import com.dell.doradus.service.db.thrift.ThriftService;

/**
 * Provides methods that access the physical database. This is currently Cassandra but
 * could be other physical stores. Database configuration options (host, port, keyspace,
 * etc.) are defined in doradus.yaml and loaded via {@link ServerConfig}.
 */
public abstract class DBService extends Service {
    // ColumnFamily names shared by apps in the global keyspace
    public static final String APPS_STORE_NAME = "Applications";
    public static final String TASKS_STORE_NAME = "Tasks";

    // Choose service based on doradus.yaml setting
    private static final DBService INSTANCE =
        ServerConfig.getInstance().use_cql ? CQLService.instance() : ThriftService.instance();

    // Only subclasses can construct an object.
    protected DBService() {
        // Give up to 1 second after start() to allow startService() to succeed
        m_startDelayMillis = 1000;
    }
    
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
     * Create a new keyspace with the given name.
     * 
     * @param keyspace  Name of new keyspace.
     */
    public abstract void createKeyspace(String keyspace);

    /**
     * Drop the keyspace with the given name. Cassandra will take a snapshot of all CFs
     * so they can be recovered if needed.
     * 
     * @param keyspace  Name of keyspace to delete.
     */
    public abstract void dropKeyspace(String keyspace);
    
    /**
     * Return the keyspace in which the given application currently resides. Null is
     * returned if the application cannot be found amoung any active keyspaces.
     * 
     * @param appName   Application name.
     * @return          Keyspace name in which application resides or null if unknown.
     */
    public abstract String getKeyspaceForApp(String appName);
    
    /**
     * Create a new store using the given template if it does not yet exist. The template
     * defines the application, if any, to which the store belongs. If the given store
     * name already exists, this is a no-op.
     * 
     * TODO: params
     */
    public abstract void createStoreIfAbsent(String keyspace, String storeName, boolean bBinaryValues);
    
    /**
     * Delete the store with the given name belonging to the given application if it
     * exists. If the store does not exist, this is a no-op.
     * 
     * TODO: params
     */
    public abstract void deleteStoreIfPresent(String keyspace, String storeName);

    /**
     * Register the given application as a tenant of the given keyspace so that it will be
     * recognized even if the application's stores have not yet been created or its schema
     * has not yet been stored.
     * 
     * @param keyspace  Name of keyspace of which application is a tenant.
     * @param appName   Name of application to register.
     */
    public abstract void registerApplication(String keyspace, String appName);

    /**
     * Get the "tenant map", which maps each keyspace to the application names that it
     * contains.
     * 
     * @return  Map of keyspaces to application names.
     */
    public abstract SortedMap<String, SortedSet<String>> getTenantMap();        

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
     * Create a new {@link DBTransaction} object that holds updates for the given
     * application that will be committed together. The transactions can be by calling
     * {@link #commit(DBTransaction)}.
     * 
     * @param appName   Application name to which updates will be applied.
     * @return          New {@link DBTransaction} with a timestamp of "now".
     */
    public abstract DBTransaction startTransaction(String appName);
    
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
     * @param appName   Application that owns the given store. 
     * @param storeName Name of store to query.
     * @param rowKey    Key of row to fetch.
     * @return          Iterator of {@link DColumn}s, or null if there is no such row.
     */
    public abstract Iterator<DColumn> getAllColumns(String appName,
                                                    String storeName,
                                                    String rowKey);

    /**
     * Get columns for the row with the given key in the given store. Columns range
     * is defined by an interval [startCol, endCol]. Columns are returned
     * as an Iterator of {@link DColumn}s. Empty iterator is returned if no row
     * is found with the given key or no columns in the given interval found.
     * 
     * @param appName   Application that owns the given store. 
     * @param storeName Name of store to query.
     * @param rowKey    Key of row to fetch.
     * @param startCol	First name in the column names interval.
     * @param endCol	Last name in the column names interval.
     * @param reversed	Flag: reverse iteration?
     * @return          Iterator of {@link DColumn}s, or null if there is no such row.
     */
    public abstract Iterator<DColumn> getColumnSlice(String appName, String storeName,
            String rowKey, String startCol, String endCol, boolean reversed);

    /**
     * Get columns for the row with the given key in the given store. Columns range
     * is defined by an interval [startCol, endCol]. Columns are returned
     * as an Iterator of {@link DColumn}s. Empty iterator is returned if no row
     * is found with the given key or no columns in the given interval found.
     * 
     * @param appName   Application that owns the given store. 
     * @param storeName Name of store to query.
     * @param rowKey    Key of row to fetch.
     * @param startCol	First name in the column names interval.
     * @param endCol	Last name in the column names interval.
     * @return          Iterator of {@link DColumn}s, or null if there is no such row.
     */
    public abstract Iterator<DColumn> getColumnSlice(String appName, String storeName,
            String rowKey, String startCol, String endCol);

    /**
     * Get all rows of all columns in the given store. The results are returned as an
     * Iterator for {@link DRow} objects. If no rows are found, the iterator's hasNext()
     * method will immediately return false. If more rows are fetched than an internal
     * limit allows, an exception is thrown.
     * 
     * @param appName   Application that owns the given store. 
     * @param storeName Name of physical store to query.
     * @return          Iterator of {@link DRow} objects. May be empty but not null.
     */
    public abstract Iterator<DRow> getAllRowsAllColumns(String appName, String storeName);

    /**
     * Get a single column for a single row in the given store. If the given row or column
     * is not found, null is returned. Otherwise, a {@link DColumn} containing the column
     * name and value is returned.
     * 
     * @param appName   Application that owns the given store. 
     * @param storeName Name of store to query.
     * @param rowKey    Key of row to read.
     * @param colName   Name of column to fetch.
     * @return          {@link DColumn} containing the column name and value or null if
     *                  the row or column was not found.
     */
    public abstract DColumn getColumn(String appName, String storeName, String rowKey, String colName);

    /**
     * Get all columns for rows with a specific set of keys. Results are returned as an
     * Iterator of {@link DRow} objects. If any given row is not found, no entry will
     * with its key will be returned.
     * 
     * @param appName   Application that owns the given store. 
     * @param storeName Name of store to query.
     * @param rowKeys   Collection of row keys to read.
     * @return          Iterator of {@link DRow} objects. May be empty but not null.
     */
    public abstract Iterator<DRow> getRowsAllColumns(String appName, String storeName,
            Collection<String> rowKeys);

    /**
     * Get a specific set of columns for a specific set of rows. Results are returned as an
     * Iterator of {@link DRow} objects. If any given row is not found, no entry will exist
     * for it in iterator. If a row is found but none of the requested columns were found,
     * DRow object's column iterator will be empty.
     * 
     * @param appName   Application that owns the given store. 
     * @param storeName Name of store to query.
     * @param rowKeys   Collection of row keys to read.
     * @param colNames  Collection of column names to read.
     * @return          Iterator for {@link DRow} objects. May be empty but not null.
     */
    public abstract Iterator<DRow> getRowsColumns(String appName, String storeName,
            Collection<String> rowKeys, Collection<String> colNames);
    
    /**
     * Get a range of columns for a specific set of rows. Results are returned as an
     * Iterator of {@link DRow} objects. If any given row is not found, no entry will exist
     * for it in iterator. If a row is found but no columns were found in the requested
     * range, DRow object's column iterator will be empty.
     * 
     * @param appName   Application that owns the given store. 
     * @param storeName Name of store to query.
     * @param rowKeys   Collection of row keys to read.
     * @param startCol  Column names >= this name are returned.
     * @param endCol    Column names <= this name are returned.
     * @return          Iterator for {@link DRow} objects. May be empty but not null.
     */
    public abstract Iterator<DRow> getRowsColumnSlice(String appName, String storeName,
            Collection<String> rowKeys, String startCol, String endCol);

    //----- Public methods: Task queries
    
    /**
     * Get all rows and columns from the Tasks table in the given keyspace.
     * 
     * @param  keyspace Keyspace name whose Tasks table to query.
     * @return          Row iterator over all rows in the table.
     */
    public abstract Iterator<DRow> getAllTaskRows(String keyspace);

    //----- Protected methods
    
    /**
     * Throw a DBNotAvailableException if we're not running yet.
     */
    protected void checkState() {
        if (!getState().isRunning()) {
            throw new DBNotAvailableException("Cassandra connection has not been established");
        }
    }   // checkState

}   // class DBService

