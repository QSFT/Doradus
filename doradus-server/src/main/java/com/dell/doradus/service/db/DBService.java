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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.dell.doradus.common.Utils;
import com.dell.doradus.core.ServerParams;
import com.dell.doradus.service.Service;
import com.dell.doradus.service.tenant.TenantService;

/**
 * Provides methods that access a persistence instance (i.e., database). Each object
 * manages data for a specific Tenant. Concrete subclasses must implement the
 * following method to create instances for a specific tenant.
 * <pre>
 *      public static DBService createInstance(Tenant)
 * </pre>
 */
public abstract class DBService extends Service {
    // TODO: Make this an evaporative map in case tenants are deleted by another server.
    private static final Map<String, DBService> g_tenantServiceMap = new HashMap<>();
    
    // Choose service based on doradus.yaml setting
    private static final DBService INSTANCE = createDefaultDBInstance();

    private static DBService createDefaultDBInstance() {
        Tenant defaultTenant = TenantService.instance().getDefaultTenant();
        DBService dbservice = createTenantDBService(defaultTenant);
        g_tenantServiceMap.put(defaultTenant.getName(), dbservice);
        return dbservice;
    }
    
    // Tenant served by this DBObject:
    protected final Tenant m_tenant;
    
    // Only subclasses can construct an object.
    protected DBService(Tenant tenant) {
        m_tenant = tenant;
        addTenantDBParameters();
        
        // Give up to 1 second after start() to allow startService() to succeed
        m_startDelayMillis = 1000;
    }
    
    /**
     * Get the default {@link DBService} instance, which is the object that manages the
     * default database.
     * 
     * @return  The default DBService object.
     */
    public static DBService instance() {
        return INSTANCE;
    }   // instance

    /**
     * Get the {@link DBService} instance that manages data for the given tenant. If a
     * DBService instance does not yet exist for this tenant, a new one is created based
     * on the options defined by the tenant. This causes DB-specific parameters to be
     * copied to the DBService object.
     * 
     * @param tenant    Existing or potentially new tenant.
     * @return          {@link DBService} instances that can manage the tenant's data.
     */
    public static DBService instance(Tenant tenant) {
        synchronized (g_tenantServiceMap) {
            DBService dbservice = g_tenantServiceMap.get(tenant.getName());
            if (dbservice == null) {
                dbservice = createTenantDBService(tenant);
                // TODO: another way to do this?
                dbservice.initialize();
                dbservice.start();
                dbservice.waitForFullService();
                g_tenantServiceMap.put(tenant.getName(), dbservice);
            }
            return dbservice;
        }
    }

    //----- Public Service methods

    /**
     * Get the Tenant that this DBService manages data for.
     * 
     * @return  This DBService's {@link Tenant}.
     */
    public Tenant getTenant() {
        return m_tenant;
    }
    
    //----- Public DBService methods: Namespace management
    
    /**
     * Indicates if this DBService object supports namespaces. If this method returns
     * false, {@link #createNamespace(DBContext, String)} and {@link #dropNamespace(String)}
     * will probably throw an exception.
     * 
     * @return True if the database type represented by this DBService object supports
     *         namespaces.
     */
    public abstract boolean supportsNamespaces();
    
    /**
     * Create a new namespace for this DBService's Tenant. It is up to the concrete
     * DBService class to decide what, if anything, should be done to prepare the new
     * namespace. This method will throw if {@link #supportsNamespaces()} returns false
     * for this DBService.
     */
    public abstract void createNamespace();
    
    /**
     * Delete the namespace for this DBService's Tenant, including its applications and
     * data. This method will throw if {@link #supportsNamespaces()} returns false for
     * this DBService.
     */
    public abstract void dropNamespace();
    
    //----- Public DBService methods: Store management
    
    /**
     * Return true if the given store already exists for the tenant defined by this
     * DBService object.
     * 
     * @param storeName Store name.
     * @return          True if the store already exists.
     */
    public abstract boolean storeExists(String storeName);
    
    /**
     * Create a new store. Columns hold string or binary values as requested. If the
     * given store already exists, this is a no-op.
     * 
     * @param storeName     Name of new store to create.
     * @param bBinaryValues True to ensure that new store holds binary values. False means
     *                      columns hold string values.
     */
    public abstract void createStoreIfAbsent(String storeName, boolean bBinaryValues);
    
    /**
     * Delete the store with the given name. If the store does not exist, this is a no-op.
     * 
     * @param storeName Name of store to delete.
     */
    public abstract void deleteStoreIfPresent(String storeName);
    
    //----- Public DBService methods: Updates
    
    /**
     * Create a new {@link DBTransaction} object that holds updates that will be committed
     * together. The transactions can be committed by calling {@link #commit(DBTransaction)}.
     * 
     * @return  New {@link DBTransaction}.
     */
    public DBTransaction startTransaction() {
        return new DBTransaction(getTenant());
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
     * @param storeName Name of store to query.
     * @param rowKey    Key of row to fetch.
     * @param startCol  First name in the column names interval, inclusive, or null.
     * @param endCol    Upper bound on the column names interval, exclusive, or null.
     * @return          List of {@link DColumn}s or null. List should be sorted by the column's name
     */
    public abstract List<DColumn> getColumns(String storeName, String rowKey,
                                             String startColumn, String endColumn, int count);
    
    /**
     * Get columns by the specified column names.
     * Empty array or null can be returned if no row is found
     * with the given key or no columns with the given column names are found.
     * 
     * @param storeName   Name of store to query.
     * @param rowKey      Key of row to fetch.
     * @param columnNames List of column names to return.
     * @return            List of {@link DColumn}s or null.
     */
    public abstract List<DColumn> getColumns(String storeName, String rowKey, Collection<String> columnNames);
    
    /**
     * Use this method to get all the rows in the database.
     * First call should set continuationToken to null,
     * next calls should set it to the last row of the previous invocation,
     * so the chunk should start from the row next after continuationToken
     * Empty array or null can be returned if no rows in the given store are found.
     * 
     * @param storeName         Name of store to query.
     * @param continuationToken Last row of the previous invokation, or null.
     * @param count             Maximum number of rows returned.
     * @return                  List of row names or null.
     */
    public abstract List<String> getRows(String storeName, String continuationToken, int count);
    
    /**
     * Get all columns for the row with the given key in the given store. Columns are
     * returned as an Iterator of {@link DColumn}s. If no row is found with the given key,
     * the iterator's hasNext() will be false.
     * 
     * @param storeName Name of store to query.
     * @param rowKey    Key of row to fetch.
     * @return          Iterator of {@link DColumn}s. If there is no such row, hasNext()
     *                  will be false.
     */
    public Iterable<DColumn> getAllColumns(String storeName, String rowKey) {
        return getColumnSlice(storeName, rowKey, null, null);
    }

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
     * @return          Iterator of {@link DColumn}s. If there is no such row, the
     *                  iterator's hasNext() will be false.
     */
    public Iterable<DColumn> getColumnSlice(String storeName, String rowKey, String startCol, String endCol) {
        DRow row = new DRow(m_tenant, storeName, rowKey);
        return row.getColumns(startCol, endCol, 1024);
    }

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
        List<String> colNames = new ArrayList<String>(1);
        colNames.add(colName);
        List<DColumn> columns = getColumns(storeName, rowKey, colNames);
        if(columns.size() == 0) return null;
        else return columns.get(0);
    }

    public DColumn getLastColumn(String storeName, String rowKey, String startCol, String endCol) {
        DColumn lastCol = null;
        DRow row = new DRow(m_tenant, storeName, rowKey);
        for(DColumn c: row.getColumns(startCol, endCol, 1024)) {
            lastCol = c;
        }
        return lastCol;
    }
    
    /**
     * Get {@link DRow} object for the given row key.
     * the object will be returned even if the row has no columns or does not exist
     * 
     * @param storeName Name of physical store to query.
     * @return          {@link DRow} object. May be empty but not null.
     */
    public Iterable<DRow> getAllRows(String storeName) {
        return new SequenceIterable<DRow>(new RowSequence(m_tenant, storeName, 65536));
    }
    
    /**
     * Get iterator for rows with a specific set of keys. Results are returned as an
     * Iterator of {@link DRow} objects. If any given row is not found, no entry
     * with its key will be returned.
     * 
     * @param storeName Name of store to query.
     * @param rowKeys   Collection of row keys to read.
     * @return          Iterator of {@link DRow} objects. May be empty but not null.
     */
    public DRow getRow(String storeName, String rowKey) {
        return new DRow(m_tenant, storeName, rowKey);
    }
    
    /**
     * Get iterator for rows with a specific set of keys. Results are returned as an
     * Iterator of {@link DRow} objects. If any given row is not found, no entry
     * with its key will be returned.
     * 
     * @param storeName Name of store to query.
     * @param rowKeys   Collection of row keys to read.
     * @return          Iterator of {@link DRow} objects. May be empty but not null.
     */
    public Iterable<DRow> getRows(String storeName, Collection<String> rowKeys) {
        List<DRow> rows = new ArrayList<>(rowKeys.size());
        for(String rowKey: rowKeys) {
            rows.add(new DRow(m_tenant, storeName, rowKey));
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

    //----- Private methods
    
    // As a Service, this object should already have parameters defined for the concrete
    // DBService object from doradus.yaml. This method adds/overrides any parameters
    // defined in the TenantDefinition.
    private void addTenantDBParameters() {
        if (m_tenant.getDefinition().getOptionMap("DBService") != null) {
            addParams(m_tenant.getDBServiceParams());
        }
    }

    // Create a new DBService for the given tenant. This should only be called when a lock
    // is held on g_tenantServiceMap
    private static DBService createTenantDBService(Tenant tenant) {
        Map<String, Object> paramMap = tenant.getDefinition().getOptionMap("DBService");
        String dbServiceName = null;
        if (paramMap == null) {
            dbServiceName = ServerParams.instance().getModuleParamString("DBService", "dbservice");
            Utils.require(!Utils.isEmpty(dbServiceName), "DBService.dbservice parameter is not defined");
        } else {
            dbServiceName = paramMap.get("dbservice").toString();
            Utils.require(!Utils.isEmpty(dbServiceName),
                          "Tenant '%s' must define 'dbservice' within 'DBService' option", tenant.getName());
        }

        DBService dbservice = null;
        try {
            // Find and invoke static method: instance(Tenant).
            @SuppressWarnings("unchecked")
            Class<DBService> serviceClass = (Class<DBService>) Class.forName(dbServiceName);
            Method instanceMethod = serviceClass.getMethod("instance", new Class<?>[]{Tenant.class});
            dbservice = (DBService)instanceMethod.invoke(null, new Object[]{tenant});
        } catch (Throwable e) {
            throw new RuntimeException("Cannot load specified 'dbservice': " + dbServiceName, e);
        }
        return dbservice;
    }

    // Add/override parameters defined for this DBService with those in the given map.
    private void addParams(Map<String, Object> dbServiceParams) {
        if (dbServiceParams != null) {
            for (String paramName : dbServiceParams.keySet()) {
                m_serviceParamMap.put(paramName, dbServiceParams.get(paramName));
            }
        }
    }
    
}
