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

import com.dell.doradus.core.ServerConfig;
import com.dell.doradus.service.Service;
import com.dell.doradus.service.db.cql.CQLService;
import com.dell.doradus.service.db.thrift.ThriftService;
import com.dell.doradus.service.tenant.UserDefinition;

/**
 * Provides methods that access the physical database. This is currently Cassandra but
 * could be other physical stores. Database configuration options (host, port, keyspace,
 * etc.) are defined in doradus.yaml and loaded via {@link ServerConfig}.
 */
public abstract class DBService extends Service {
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
    public abstract DBTransaction startTransaction(Tenant tenant);
    
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
     * @param tenant    {@link Tenant} that owns the store. 
     * @param storeName Name of store to query.
     * @param rowKey    Key of row to fetch.
     * @return          Iterator of {@link DColumn}s, or null if there is no such row.
     */
    public abstract Iterator<DColumn> getAllColumns(Tenant tenant,
                                                    String storeName,
                                                    String rowKey);

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
     * @param reversed	Flag: reverse iteration?
     * @return          Iterator of {@link DColumn}s, or null if there is no such row.
     */
    public abstract Iterator<DColumn> getColumnSlice(Tenant tenant, String storeName,
            String rowKey, String startCol, String endCol, boolean reversed);

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
     * @return          Iterator of {@link DColumn}s, or null if there is no such row.
     */
    public abstract Iterator<DColumn> getColumnSlice(Tenant tenant, String storeName,
            String rowKey, String startCol, String endCol);

    /**
     * Get all rows of all columns in the given store. The results are returned as an
     * Iterator for {@link DRow} objects. If no rows are found, the iterator's hasNext()
     * method will immediately return false. If more rows are fetched than an internal
     * limit allows, an exception is thrown.
     * 
     * @param tenant    {@link Tenant} that owns the store. 
     * @param storeName Name of physical store to query.
     * @return          Iterator of {@link DRow} objects. May be empty but not null.
     */
    public abstract Iterator<DRow> getAllRowsAllColumns(Tenant tenant, String storeName);

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
    public abstract DColumn getColumn(Tenant tenant, String storeName, String rowKey, String colName);

    /**
     * Get all columns for rows with a specific set of keys. Results are returned as an
     * Iterator of {@link DRow} objects. If any given row is not found, no entry will
     * with its key will be returned.
     * 
     * @param tenant    {@link Tenant} that owns the store. 
     * @param storeName Name of store to query.
     * @param rowKeys   Collection of row keys to read.
     * @return          Iterator of {@link DRow} objects. May be empty but not null.
     */
    public abstract Iterator<DRow> getRowsAllColumns(Tenant tenant, String storeName,
            Collection<String> rowKeys);

    /**
     * Get a specific set of columns for a specific set of rows. Results are returned as an
     * Iterator of {@link DRow} objects. If any given row is not found, no entry will exist
     * for it in iterator. If a row is found but none of the requested columns were found,
     * DRow object's column iterator will be empty.
     * 
     * @param tenant    {@link Tenant} that owns the store. 
     * @param storeName Name of store to query.
     * @param rowKeys   Collection of row keys to read.
     * @param colNames  Collection of column names to read.
     * @return          Iterator for {@link DRow} objects. May be empty but not null.
     */
    public abstract Iterator<DRow> getRowsColumns(Tenant tenant, String storeName,
            Collection<String> rowKeys, Collection<String> colNames);
    
    /**
     * Get a range of columns for a specific set of rows. Results are returned as an
     * Iterator of {@link DRow} objects. If any given row is not found, no entry will exist
     * for it in iterator. If a row is found but no columns were found in the requested
     * range, DRow object's column iterator will be empty.
     * 
     * @param tenant    {@link Tenant} that owns the store. 
     * @param storeName Name of store to query.
     * @param rowKeys   Collection of row keys to read.
     * @param startCol  Column names >= this name are returned.
     * @param endCol    Column names <= this name are returned.
     * @return          Iterator for {@link DRow} objects. May be empty but not null.
     */
    public abstract Iterator<DRow> getRowsColumnSlice(Tenant tenant, String storeName,
            Collection<String> rowKeys, String startCol, String endCol);

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

