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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Represents a connection to the database and provides methods for fetching and updating
 * data. DBConn provides a columnar view of the database:
 * <ul>
 * <li>The database consists of named "stores". In Cassandra, these are mapped to
 *     ColumnFamilies.</li>
 * <li>Data lives in "rows", each of which contains a string-based key. In Cassandra, each
 *     row key is translated to binary via UTF-8.</li>
 * <li>A row consists of "columns", which have string names and values. In Cassandra,
 *     column names are always translated to binary via UTF-8; string values are mapped 
 *     the same way, but binary column values can also be written.</li>
 * </ul>
 * The idea is to hide underlying physical database details from other Doradus modules.
 */
public abstract class DBConn {
    // Logging interface:
    protected final Logger m_logger = LoggerFactory.getLogger(getClass().getSimpleName());

    //----- Connection management
    
    /**
     * Create a new connection object, connect to the database, and optionally check that
     * the database is initialized for use by Doradus.
     * 
     * @param bInitialize   If true, a check is made that the database is initialized for
     *                      use by Doradus.
     */
    public DBConn(boolean bInitialize) {
    }   // constructor
    
    /**
     * Close the database connection. This object cannot be reused after this method is
     * called.
     */
    public abstract void close();
    
    /**
     * Return true if the last operation for this connection, including all retries failed.
     * This may indicate that the corresponding Cassandra node is down.
     * 
     * @return  True if the last operation for this connection, including all retries failed.
     */
    public abstract boolean isFailed();

    //----- Store management
    
    /**
     * Create a new physical store based on the given template.
     * 
     * @param storeTemplate Template that holds properties used for the new store. 
     */
    public abstract void createStore(StoreTemplate storeTemplate);
    
    /**
     * Delete the physical store with the given name.
     * 
     * @param storeName Name of store to be deleted.
     */
    public abstract void deleteStore(String storeName);
    
    /**
     * Return true if the given store name currently exists.
     * 
     * @param storeName Candidate store name.
     * @return          True if the store exists in the database.
     */
    public abstract boolean storeExists(String storeName);
    
    /**
     * Get a list of all physical store names used by this Doradus instance.
     * 
     * @return  List of all physical store names used by this Doradus instance.
     */
    public abstract Collection<String> getAllStoreNames();

    //----- Schema requests
    
    /**
     * Get all properties of all registered applications. The resilt is a map of
     * application names to a map of application properties. An application's properties
     * are typically _application, _version, and _format, which define the application's
     * schema and the way it is stored in the database.
     * 
     * @return  Map of application name -> property name -> property value for all
     *          known applications.
     */
    public abstract Map<String, Map<String, String>> getAllAppProperties();

    /**
     * Get all properties of the application with the given name. The property names are
     * typically _application, _version, and _format, which define the application's
     * schema and the way it is stored in the database.
     * 
     * @param appName   Name of application whose properties to get.
     * @return          Map of application properties as key/value pairs.
     */
    public abstract Map<String, String> getAppProperties(String appName);
    
    /**
     * Get the database-level options, if any, from the "options" row. If there are no
     * options stored, an empty map is returned (but not null).
     * 
     * @return  Map of database-level options as key/value pairs. Empty if there are no
     *          options stored.
     */
    public abstract Map<String, String> getDBOptions();

    //----- Updates 
    
    /**
     * Commit the updates in the given {@link DBTransaction}.
     * 
     * @param dbTxn Batch of one or more updates to be committed.
     */
    public abstract void commit(DBTransaction dbTxn);

    //----- Queries

    /**
     * Get all columns for the row with the given key in the given store. Columns are
     * returned as string/byte[] key/value pairs.
     * 
     * @param storeName Name of store to query.
     * @param rowKey    Key of row to fetch.
     * @return          All columns for corresponding row, or null if there is no such row.
     */
    public abstract Iterator<DColumn> getAllColumns(String storeName, String rowKey);
    
    /**
     * Get a slice of columns for the row with the given key in the given store.
     * Columns are defined with a range of [startCol, endCol], and returned as
     * string/byte[] key/value pairs.
     * 
     * @param storeName Name of store to query.
     * @param rowKey    Key of row to fetch.
     * @param startCol	First column name of the interval
     * @param endCol	Last column name of the interval
     * @param reversed	Flag: reversed iteration?
     * @return
     */
    public abstract Iterator<DColumn> getColumnSlice(String storeName, String rowKey,
    		String startCol, String endCol, boolean reversed);
    
    /**
     * Get a slice of columns for the row with the given key in the given store.
     * Columns are defined with a range of [startCol, endCol], and returned as
     * string/byte[] key/value pairs.
     * 
     * @param storeName Name of store to query.
     * @param rowKey    Key of row to fetch.
     * @param startCol	First column name of the interval
     * @param endCol	Last column name of the interval
     * @return
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
     * is not found, null is returned. Otherwise, a {@link DColumn} is returned with the
     * column's name and value.
     * 
     * @param store     Name of store to query.
     * @param rowKey    Key of row to read.
     * @param colName   Name of column to fetch.
     * @return          {@link DColumn} containing the column name and value.
     */
    public abstract DColumn getColumn(String store, String rowKey, String colName);
    
    /**
     * Get all columns for rows with a specific set of keys. Results are returned as an
     * Iterator of {@link DRow} objects. If any given row is not found, no entry will
     * with its key will be returned.
     * 
     * @param storeName Name of store to query.
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
     * @param storeName Name of store to query.
     * @param rowKeys   Collection of row keys to read.
     * @param colNames  Collection of column names to read.
     * @return          Iterator for {@link DRow} objects. May be empty but not null.
     */
    public abstract Iterator<DRow> getRowsColumns(String             storeName,
                                                  Collection<String> rowKeys,
                                                  Collection<String> colNames);

    /**
     * Get a specific range of columns for a specific set of rows. Results are returned as an
     * Iterator of {@link DRow} objects. If any given row is not found, no entry will exist
     * for it in iterator. If a row is found but none of the requested columns were found,
     * DRow object's column iterator will be empty.
     * 
     * @param storeName Name of store to query.
     * @param rowKeys   Collection of row keys to read.
     * @param startCol  First column in a range.
     * @param endCol	Last column in a range
     * @return          Iterator for {@link DRow} objects. May be empty but not null.
     */
    public abstract Iterator<DRow> getRowsColumns(String             storeName,
                                                  Collection<String> rowKeys,
                                                  String			 startCol,
                                                  String			 endCol);

    /**
     * Get a specific range of columns for a specific set of rows. Results are returned as an
     * Iterator of {@link DRow} objects. If any given row is not found, no entry will exist
     * for it in iterator. If a row is found but none of the requested columns were found,
     * DRow object's column iterator will be empty.
     * 
     * @param storeName Name of store to query.
     * @param rowKeys   Collection of row keys to read.
     * @param startCol  First column in a range.
     * @param endCol	Last column in a range.
     * @param reversed	Flag: range in reversed order?
     * @return          Iterator for {@link DRow} objects. May be empty but not null.
     */
    public abstract Iterator<DRow> getRowsColumns(String             storeName,
                                                  Collection<String> rowKeys,
                                                  String			 startCol,
                                                  String			 endCol,
                                                  boolean 			 reversed);

}   // class DBConn
