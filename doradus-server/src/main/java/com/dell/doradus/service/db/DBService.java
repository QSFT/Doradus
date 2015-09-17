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

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.dell.doradus.common.UserDefinition;
import com.dell.doradus.common.Utils;
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
    // Choose service based on doradus.yaml setting
    private static final DBService INSTANCE = selectService();
    
    private static DBService selectService() {
        String dbServiceName = ServerConfig.getInstance().dbservice;
        if (!Utils.isEmpty(dbServiceName)) {
            try {
                @SuppressWarnings("unchecked")
                Class<DBService> serviceClass = (Class<DBService>) Class.forName(dbServiceName);
                Method instanceMethod = serviceClass.getMethod("instance", (Class<?>[])null);
                DBService instance = (DBService)instanceMethod.invoke(null, (Object[])null);
                return instance;
            } catch (Exception e) {
                throw new RuntimeException("Error initializing DBService: " + dbServiceName, e);
            }
        } else if (ServerConfig.getInstance().use_cql) {
            return CQLService.instance();
        } else {
            return ThriftService.instance();
        }
    }
    
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

    //----- Public Service methods

    // Implemented by subclasses
    
    //----- Public DBService methods: Tenant management
    
    /**
     * Create a new tenant with the given options.
     * 
     * @param tenant    {@link Tenant} that defines new tenant.
     * @param options   Optional map of options for new tenant.
     */
    public abstract void createTenant(Tenant tenant, Map<String, String> options);
    
    /**
     * Modify the given tenant with the given options.
     * 
     * @param tenant    {@link Tenant} to be modified.
     * @param options   Map of options to apply to tenant.
     */
    public abstract void modifyTenant(Tenant tenant, Map<String, String> options);
    
    /**
     * Drop the given tenant.
     * 
     * @param tenant    {@link Tenant} to drop.
     */
    public abstract void dropTenant(Tenant tenant);
    
    /**
     * Add the given list of users to the database with the defined permissions for the
     * given tenant.
     * 
     * @param tenant    {@link Tenant} to add users for.
     * @param users     List of {@link UserDefinition} to add.
     */
    public abstract void addUsers(Tenant tenant, Iterable<UserDefinition> users);
    
    /**
     * Modify the given list of users to for the given tenant. Currently, only passwords
     * can be modified.
     * 
     * @param tenant    {@link Tenant} whose users are to be modified.
     * @param users     List of {@link UserDefinition} to modify.
     */
    public abstract void modifyUsers(Tenant tenant, Iterable<UserDefinition> users);
    
    /**
     * Delete the given list of users for the given tenant.
     * 
     * @param tenant    {@link Tenant} to delete users for.
     * @param users     List of {@link UserDefinition} to delete.
     */
    public abstract void deleteUsers(Tenant tenant, Iterable<UserDefinition> users);
    
    /**
     * Get a list of all known {@link Tenant}s.
     * 
     * @return  Map of tenants to application names.
     */
    public abstract Collection<Tenant> getTenants();        

    //----- Public DBService methods: Store management
    
    /**
     * Create a new store in the given tenant. Columns hold string or binary values as
     * requested. If the given store already exists, this is a no-op.
     * 
     * @param tenant        {@link Tenant} that holds new store.
     * @param storeName     Name of new store to create.
     * @param bBinaryValues True to ensure that new store holds binary values. False means
     *                      columns hold string values.
     */
    public abstract void createStoreIfAbsent(Tenant tenant, String storeName, boolean bBinaryValues);
    
    /**
     * Delete the store with the given name belonging to the given tenant. If the store
     * does not exist, this is a no-op.
     * 
     * @param tenant    Tenant that owns store.
     * @param storeName Name of store to delete.
     */
    public abstract void deleteStoreIfPresent(Tenant tenant, String storeName);

    //----- Public DBService methods: Updates
    
    /**
     * Create a new {@link DBTransaction} object that holds updates for stores in the
     * given tenant that will be committed together. The transactions can be committed by
     * calling {@link #commit(DBTransaction)}.
     * 
     * @param tenant    {@link Tenant} in which updates will be made.
     * @return          New {@link DBTransaction}.
     */
    public DBTransaction startTransaction(Tenant tenant) {
        return startTransaction(tenant.getKeyspace());
    }
    
    /**
     * Create a new {@link DBTransaction} object that holds updates for stores in the
     * given namespace that will be committed together. The transactions can be committed by
     * calling {@link #commit(DBTransaction)}.
     * 
     * @param namespace    namespace in which updates will be made.
     * @return          New {@link DBTransaction}.
     */
    public DBTransaction startTransaction(String namespace) {
        return new DBTransaction(namespace);
    }
    
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
     * Get columns for the row with the given key in the given store, sorted by the column's name.
     * Columns range is defined by an interval [startCol, endCol). If there are more
     * than 'count' columns then only 'count' columns should be returned.
     * startColumn is included, endColumn is not included.
     * if startColumn is null the results should start from the first column in the row
     * if endcolumn is null then the results should end at the last column in the row
     * Empty array or null can be returned if no row
     * is found with the given key or no columns in the given interval found.
     * 
     * @param namespace namespace that owns the store. 
     * @param storeName Name of store to query.
     * @param rowKey    Key of row to fetch.
     * @param startCol  First name in the column names interval, inclusive, or null.
     * @param endCol    Upper bound on the column names interval, exclusive, or null.
     * @return          List of {@link DColumn}s or null. List should be sorted by the column's name
     */
    public abstract List<DColumn> getColumns(String namespace, String storeName, String rowKey,
                                         String startColumn, String endColumn, int count);
    

    /**
     * Get columns by the specified column names.
     * Empty array or null can be returned if no row is found
     * with the given key or no columns with the given column names are found.
     * 
     * @param namespace   Namespace that owns the store. 
     * @param storeName   Name of store to query.
     * @param rowKey      Key of row to fetch.
     * @param columnNames List of column names to return.
     * @return            List of {@link DColumn}s or null.
     */
    public abstract List<DColumn> getColumns(String namespace, String storeName, String rowKey, Collection<String> columnNames);

    /**
     * Use this method to get all the rows in the database.
     * First call should set continuationToken to null,
     * next calls should set it to the last row of the previous invocation,
     * so the chunk should start from the row next after continuationToken
     * Empty array or null can be returned if no rows in the given store are found.
     * 
     * @param namespace         Namespace that owns the store. 
     * @param storeName         Name of store to query.
     * @param continuationToken Last row of the previous invokation, or null.
     * @param count             Maximum number of rows returned.
     * @return                  List of row names or null.
     */
    public abstract List<String> getRows(String namespace, String storeName, String continuationToken, int count);
    
    /**
     * Get all columns for the row with the given key in the given store. Columns are
     * returned as an Iterator of {@link DColumn}s. If no row is found with the given key,
     * the iterator's hasNext() will be false.
     * 
     * @param tenant    {@link Tenant} that owns the store. 
     * @param storeName Name of store to query.
     * @param rowKey    Key of row to fetch.
     * @return          Iterator of {@link DColumn}s. If there is no such row, hasNext()
     *                  will be false.
     */
    public Iterable<DColumn> getAllColumns(Tenant tenant, String storeName, String rowKey) {
        return getColumnSlice(tenant, storeName, rowKey, null, null);
    }
    
    /**
     * Get columns for the row with the given key in the given store. Columns range
     * is defined by an interval [startCol, endCol]. Columns are returned
     * as an Iterator of {@link DColumn}s. Empty iterator is returned if no row
     * is found with the given key or no columns in the given interval found.
     * 
     * @param tenant    {@link Tenant} that owns the store. 
     * @param storeName Name of store to query.
     * @param rowKey    Key of row to fetch.
     * @param startCol	First name in the column names interval.
     * @param endCol	Last name in the column names interval.
     * @return          Iterator of {@link DColumn}s. If there is no such row, the
     *                  iterator's hasNext() will be false.
     */
    public Iterable<DColumn> getColumnSlice(Tenant tenant, String storeName,
            String rowKey, String startCol, String endCol) {
        DRow row = new DRow(tenant.getKeyspace(), storeName, rowKey);
        return row.getColumns(startCol, endCol, 1024);
    }

    /**
     * Get a single column for a single row in the given store. If the given row or column
     * is not found, null is returned. Otherwise, a {@link DColumn} containing the column
     * name and value is returned.
     * 
     * @param tenant    {@link Tenant} that owns the store. 
     * @param storeName Name of store to query.
     * @param rowKey    Key of row to read.
     * @param colName   Name of column to fetch.
     * @return          {@link DColumn} containing the column name and value or null if
     *                  the row or column was not found.
     */
    public DColumn getColumn(Tenant tenant, String storeName, String rowKey, String colName) {
        List<String> colNames = new ArrayList<String>(1);
        colNames.add(colName);
        List<DColumn> columns = getColumns(tenant.getKeyspace(), storeName, rowKey, colNames);
        if(columns.size() == 0) return null;
        else return columns.get(0);
    }

    public DColumn getLastColumn(Tenant tenant, String storeName, String rowKey, String startCol, String endCol) {
        DColumn lastCol = null;
        DRow row = new DRow(tenant.getKeyspace(), storeName, rowKey);
        for(DColumn c: row.getColumns(startCol, endCol, 1024)) {
            lastCol = c;
        }
        return lastCol;
    }
    
    
    /**
     * Get {@link DRow} object for the given row key.
     * the object will be returned even if the row has no columns or does not exist
     * 
     * @param tenant    {@link Tenant} that owns the store. 
     * @param storeName Name of physical store to query.
     * @return          {@link DRow} object. May be empty but not null.
     */
    public Iterable<DRow> getAllRows(Tenant tenant, String storeName) {
        return new SequenceIterable<DRow>(new RowSequence(tenant.getKeyspace(), storeName, 65536));
    }

    /**
     * Get iterator for rows with a specific set of keys. Results are returned as an
     * Iterator of {@link DRow} objects. If any given row is not found, no entry
     * with its key will be returned.
     * 
     * @param tenant    {@link Tenant} that owns the store. 
     * @param storeName Name of store to query.
     * @param rowKeys   Collection of row keys to read.
     * @return          Iterator of {@link DRow} objects. May be empty but not null.
     */
    public DRow getRow(Tenant tenant, String storeName, String rowKey) {
        return new DRow(tenant.getKeyspace(), storeName, rowKey);
    }
    
    /**
     * Get iterator for rows with a specific set of keys. Results are returned as an
     * Iterator of {@link DRow} objects. If any given row is not found, no entry
     * with its key will be returned.
     * 
     * @param tenant    {@link Tenant} that owns the store. 
     * @param storeName Name of store to query.
     * @param rowKeys   Collection of row keys to read.
     * @return          Iterator of {@link DRow} objects. May be empty but not null.
     */
    public Iterable<DRow> getRows(Tenant tenant, String storeName, Collection<String> rowKeys) {
        List<DRow> rows = new ArrayList<>(rowKeys.size());
        for(String rowKey: rowKeys) {
            rows.add(new DRow(tenant.getKeyspace(), storeName, rowKey));
        }
        return rows;
    }

    //----- Protected methods
    
    /**
     * Throw a DBNotAvailableException if we're not running yet.
     */
    protected void checkState() {
        if (!getState().isRunning()) {
            throw new DBNotAvailableException("Cassandra connection has not been established");
        }
    }

}

