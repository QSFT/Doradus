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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract class that encapsulates a set of updates that will be committed together.
 * Provides methods to post updates based on a tabular model: add/replace column, delete
 * column, and delete row. These methods operate on a "store", which is a ColumnFamily in
 * Cassandra.
 * <p>
 * Because the application schema model is central to Doradus, methods are also provided
 * to add columns to an application's definition row and to delete an application's
 * definition row. These operations are performed on the Applications ColumnFamily in
 * Cassandra.
 * <p>
 * The accumulated updates are commited by calling {@link DBService#commit(DBTransaction)}
 * which also clears the updates. Updates can also be cleared by calling {@link #clear()}.
 * The transaction object subsequently can be reused, causing the same timestamp to be
 * used for all new updates. To use a new timestamp, a new object must be created.
 */
public abstract class DBTransaction {
    // Protected logger available to concrete services:
    protected final Logger m_logger = LoggerFactory.getLogger(getClass().getSimpleName());
    
    //----- General methods
    
    /**
     * Clear all updates defined in this transaction.
     */
    public abstract void clear();

    /**
     * Get the total number of updates (column updates/deletes and row deletes) queued
     * in this transaction so far.
     * 
     * @return  Total number of updates queued in this transaction so far.
     */
    public abstract int getUpdateCount();
    
    //----- Application schema update methods
    
    /**
     * Add the given column to the application definition row for the given application.
     * 
     * @param appName   Name of application.
     * @param colName   Column name in string form.
     * @param colValue  Column value in string form.
     */
    public abstract void addAppColumn(String appName, String colName, String colValue);

    /**
     * Add an update that will delete the application definition row for the given
     * application name.
     * 
     * @param appName   Name of application.
     */
    public abstract void deleteAppRow(String appName);

    //----- Column/row update methods
    
    /**
     * Add or set a column to the given row in the given table with a null value.
     * 
     * @param storeName Name of store that owns row.
     * @param rowKey    Key of row that owns column.
     * @param colName   Name of column.
     * @param colValue  Column value as a string.
     */
    public abstract void addColumn(String storeName, String rowKey, String colName);
    
    /**
     * Add or set a column with the given string value.
     * 
     * @param storeName Name of store that owns row.
     * @param rowKey    Key of row that owns column.
     * @param colName   Name of column.
     * @param colValue  Column value as a string.
     */
    public abstract void addColumn(String storeName, String rowKey, String colName, String colValue);
    
    /**
     * Add or set a column with the given binary value.
     * 
     * @param storeName Name of store that owns row.
     * @param rowKey    Key of row that owns column.
     * @param colName   Name of column.
     * @param colValue  Column value in binary.
     */
    public abstract void addColumn(String storeName, String rowKey, String colName, byte[] colValue);
    
    /**
     * Add or set a column with the given long value. The column value is converted to
     * binary form using Long.toString(colValue), which is then converted to a String using
     * UTF-8. 
     * 
     * @param storeName Name of store that owns row.
     * @param rowKey    Key of row that owns column.
     * @param colName   Name of column.
     * @param colValue  Column value as a long.
     */
    public abstract void addColumn(String storeName, String rowKey, String colName, long colValue);
    
    /**
     * Add an update that will delete the row with the given row key from the given store.
     * 
     * @param storeName Name of store from which to delete an object row.
     * @param rowKey    Row key in string form.
     */
    public abstract void deleteRow(String storeName, String rowKey);
    
    /**
     * Add an update that will delete the column for the given store, row key, and column
     * name. If a column update exists for the same store/row/column, the results are
     * undefined when the transaction is committed.
     * 
     * @param storeName Name of store that owns row.
     * @param rowKey    Row key in string form.
     * @param colName   Column name in string form.
     */
    public abstract void deleteColumn(String storeName, String rowKey, String colName);

    /**
     * Add updates that will delete all given columns names for the given store name and
     * row key. If a column update exists for the same store/row/column, the results are
     * undefined when the transaction is committed.
     * 
     * @param storeName Name of store that owns row.
     * @param rowKey    Row key in string form.
     * @param colNames  Collection of column names in string form.
     */
    public abstract void deleteColumns(String storeName, String rowKey, Collection<String> colNames);

}   // DBTransaction
