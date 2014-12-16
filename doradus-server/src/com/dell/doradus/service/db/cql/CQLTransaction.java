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

package com.dell.doradus.service.db.cql;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.BatchStatement.Type;
import com.dell.doradus.common.Utils;
import com.dell.doradus.core.ServerConfig;
import com.dell.doradus.service.db.DBService;
import com.dell.doradus.service.db.DBTransaction;
import com.dell.doradus.service.db.cql.CQLStatementCache.Update;

/**
 * Concrete DBTransaction for use with the Cassandra CQL API. Updates are stored in
 * separate maps for column adds/updates and row/column deletions.
 */
public class CQLTransaction extends DBTransaction {
    // Map of table name -> row key -> CQLColumn list.
    private final Map<String, Map<String, List<CQLColumn>>> m_updateMap = new HashMap<>();
    
    // Map of table name -> row key -> list of column names. If a row's column name list
    // is null or empty, the whole row is to be deleted.
    private final Map<String, Map<String, List<String>>> m_deleteMap = new HashMap<>();
    
    private final String  m_keyspace;
    private final Session m_session;
    private int m_updates;
    private long m_timestamp;   // Used for async updates only

    /**
     * Start a new CQLTransaction.
     */
    public CQLTransaction(String appName) {
        // Ensure session is possible with app' keyspace.
        m_keyspace = CQLService.instance().getKeyspaceForApp(appName);
        m_session = CQLService.instance().getSession(m_keyspace);
    }

    //----- General methods
    
    @Override
    public void clear() {
        m_updateMap.clear();
        m_deleteMap.clear();
        m_updates = 0;
    }

    @Override
    public int getUpdateCount() {
        return m_updates;
    }

    //----- Application schema update methods
    
    @Override
    public void addAppColumn(String appName, String colName, String colValue) {
        addColumn(DBService.APPS_STORE_NAME, appName, colName, colValue);
    }   // addAppColumn

    @Override
    public void deleteAppRow(String appName) {
        deleteRow(DBService.APPS_STORE_NAME, appName);
    }

    //----- Column/row update methods
    
    // Null column value
    @Override
    public void addColumn(String storeName, String rowKey, String colName) {
        addColumn(storeName, rowKey, colName, "");
    }
    
    // String column value
    @Override
    public void addColumn(String storeName, String rowKey, String colName, String colValue) {
        String tableName = CQLService.storeToCQLName(storeName);
        List<CQLColumn> colList = getUpdateColList(tableName, rowKey);
        colList.add(new CQLColumn(colName, colValue));
        m_updates++;
    }
    
    // Binary column value
    @Override
    public void addColumn(String storeName, String rowKey, String colName, byte[] colValue) {
        String tableName = CQLService.storeToCQLName(storeName);
        List<CQLColumn> colList = getUpdateColList(tableName, rowKey);
        colList.add(new CQLColumn(colName, colValue));
        m_updates++;
    }

    // Long column value
    @Override
    public void addColumn(String storeName, String rowKey, String colName, long colValue) {
        addColumn(storeName, rowKey, colName, Long.toString(colValue));
    }

    // Add row key to list of deletes.
    @Override
    public void deleteRow(String storeName, String rowKey) {
        String tableName = CQLService.storeToCQLName(storeName);
        Map<String, List<String>> rowKeyMap = getDeleteColMap(tableName);
        rowKeyMap.put(rowKey, null);
        m_updates++;
    }   // deleteRow

    // Add a deletion for a single column
    @Override
    public void deleteColumn(String storeName, String rowKey, String colName) {
        String tableName = CQLService.storeToCQLName(storeName);
        Map<String, List<String>> rowKeyMap = getDeleteColMap(tableName);
        List<String> colList = null;
        if (rowKeyMap.containsKey(rowKey)) {
            colList = rowKeyMap.get(rowKey);
            if (colList == null) {
                // Row is being deleted. Warn about this for now.
                m_logger.warn("deleteColumn() called for row being deleted; ignored. " +
                              "table={}, row={}, column={}",
                              new Object[]{tableName, rowKey, colName});
                return;
            }
        }
        if (colList == null) {
            colList = new ArrayList<>();
            rowKeyMap.put(rowKey, colList);
        }
        colList.add(colName);
        m_updates++;
    }   // deleteColumn

    // Add a deletion for a collection of columns
    @Override
    public void deleteColumns(String storeName, String rowKey, Collection<String> colNames) {
        String tableName = CQLService.storeToCQLName(storeName);
        Map<String, List<String>> rowKeyMap = getDeleteColMap(tableName);
        List<String> colList = null;
        if (rowKeyMap.containsKey(rowKey)) {
            colList = rowKeyMap.get(rowKey);
            if (colList == null) {
                // Row is being deleted. Warn about this for now.
                m_logger.warn("deleteColumns() called for row being deleted; ignored. " +
                              "table={}, row={}, # of columns={}",
                              new Object[]{tableName, rowKey, colNames.size()});
                return;
            }
        }
        if (colList == null) {
            colList = new ArrayList<>();
            rowKeyMap.put(rowKey, colList);
        }
        colList.addAll(colNames);
        m_updates += colNames.size();
    }   // deleteColumns

    //----- Local public methods
    
    /**
     * Apply the updates accumulated in this transaction. The updates are cleared even if
     * the update fails.
     */
    public void commit() {
        try {
            m_timestamp = Utils.getTimeMicros();
            applyUpdates();
        } catch (Exception e) {
            m_logger.error("Updates failed", e);
            throw e;
        } finally {
            clear();
        }
    }   // commit
    
    //----- Private methods
    
    // Get the column list for the given table/row, adding the outer maps if needed.
    private List<CQLColumn> getUpdateColList(String tableName, String rowKey) {
        Map<String, List<CQLColumn>> rowMap = m_updateMap.get(tableName);
        if (rowMap == null) {
            rowMap = new HashMap<>();
            m_updateMap.put(tableName, rowMap);
        }
        List<CQLColumn> colList = rowMap.get(rowKey);
        if (colList == null) {
            colList = new ArrayList<>(1000);
            rowMap.put(rowKey, colList);
        }
        return colList;
    }   // getUpdateColList

    // Get the delete row/column map for the given table, adding the outer map if needed.
    private Map<String, List<String>> getDeleteColMap(String tableName) {
        Map<String, List<String>> rowMap = m_deleteMap.get(tableName);
        if (rowMap == null) {
            rowMap = new HashMap<>(1000);
            m_deleteMap.put(tableName, rowMap);
        }
        return rowMap;
    }   // getDeleteColMap
    
    // Execute a batch statement that applies all updates in this transaction.
    private void applyUpdates() {
        if (getUpdateCount() == 0) {
            m_logger.debug("Skipping commit with no updates");
        } else if (ServerConfig.getInstance().async_updates) {
            executeUpdatesAsynchronous();
        } else {
            executeUpdatesSynchronous();
        }
    }   // applyUpdates

    ///// Methods for asynchronous updates
    
     // Execute all updates asynchronously and wait for results.
    private void executeUpdatesAsynchronous() {
        List<ResultSetFuture> futureList = new ArrayList<>(1000);
        executeTableUpdatesAsynchronously(futureList);
        executeTableDeletesAsynchronously(futureList);
        m_logger.debug("Waiting for {} asynchronous futures", futureList.size());
        for (ResultSetFuture future : futureList) {
            future.getUninterruptibly();
        }
    }   // executeUpdatesAsynchronous

    // Execute all table updates asynchronously and add futures to the given list.
    private void executeTableUpdatesAsynchronously(List<ResultSetFuture> futureList) {
        for (String tableName : m_updateMap.keySet()) {
            executeTableUpdatesAsynchronous(tableName, futureList);
        }
    }   // executeTableUpdatesAsynchronously
    
    // Execute updates for the given table asynchronously and add futures to the given list.
    private void executeTableUpdatesAsynchronous(String tableName, List<ResultSetFuture> futureList) {
        boolean bBinary = CQLService.instance().columnValueIsBinary(m_keyspace, tableName);
        PreparedStatement prepState =
            CQLService.instance().getPreparedUpdate(m_keyspace, Update.INSERT_ROW_TS, tableName);
        Map<String, List<CQLColumn>> rowMap = m_updateMap.get(tableName);
        for (String key : rowMap.keySet()) {
            // All columns for the same row key are added to a single batch.
            BatchStatement batchState = new BatchStatement(Type.UNLOGGED);
            for (CQLColumn column : rowMap.get(key)) {
                BoundStatement boundState = prepState.bind();
                boundState.setString(0, key);
                boundState.setString(1, column.getName());
                if (bBinary) {
                    boundState.setBytes(2, ByteBuffer.wrap(column.getRawValue()));
                } else {
                    boundState.setString(2, column.getValue());
                }
                boundState.setLong(3, m_timestamp);
                batchState.add(boundState);
            }
            futureList.add(m_session.executeAsync(batchState));
        }
    }   // executeTableUpdatesAsynchronous

    // Execute all table deletes asynchronously and add futures to the given list.
    private void executeTableDeletesAsynchronously(List<ResultSetFuture> futureList) {
        for (String tableName : m_deleteMap.keySet()) {
            executeTableDeleteAsynchronous(tableName, futureList);
        }
    }   // executeTableDeletesAsynchronously
    
    // Execute deletes for the given table asynchronously and add futures to the given list.
    private void executeTableDeleteAsynchronous(String tableName, List<ResultSetFuture> futureList) {
        Map<String, List<String>> rowKeyMap = m_deleteMap.get(tableName);
        PreparedStatement deleteColPrepState = CQLService.instance().getPreparedUpdate(m_keyspace, Update.DELETE_COLUMN_TS, tableName);
        PreparedStatement deleteRowPrepState = CQLService.instance().getPreparedUpdate(m_keyspace, Update.DELETE_ROW_TS, tableName);
        for (String key : rowKeyMap.keySet()) {
            List<String> colList = rowKeyMap.get(key);
            if (colList != null && colList.size() > 0) {
                executeTableDeleteColumnsAsynchronous(key, colList, deleteColPrepState, futureList);
            } else {
                executeTableRowDeleteAsynchronously(key, deleteRowPrepState, futureList);
            }
        }
    }   // executeTableDeleteAsynchronous
    
    // Delete all columns for the given row in an asynchronous batch update and add
    // a future to the given list.
    private void executeTableDeleteColumnsAsynchronous(String                key,
                                                       List<String>          colList,
                                                       PreparedStatement     deleteColPrepState,
                                                       List<ResultSetFuture> futureList) {
        BatchStatement batchState = new BatchStatement(Type.UNLOGGED);
        for (String colName : colList) {
            BoundStatement boundState = deleteColPrepState.bind();
            boundState.setLong(0, m_timestamp);
            boundState.setString(1, key);
            boundState.setString(2, colName);
            batchState.add(boundState);
        }
        futureList.add(m_session.executeAsync(batchState));
    }   // executeTableDeleteColumnsAsynchronous
    
    // Execute a row delete asynchronously and add a future to the given list.
    private void executeTableRowDeleteAsynchronously(String                key,
                                                     PreparedStatement     deleteRowPrepState,
                                                     List<ResultSetFuture> futureList) {
        BoundStatement boundState = deleteRowPrepState.bind();
        boundState.setLong(0, m_timestamp);
        boundState.setString(1, key);
        futureList.add(m_session.executeAsync(boundState));
    }   // executeTableRowDeleteAsynchronously
    
    ///// Methods for synchronous updates

    // Execute all updates and deletes using synchronous statements.
    private void executeUpdatesSynchronous() {
        BatchStatement batchState = new BatchStatement(Type.UNLOGGED);
        addUpdates(batchState);
        addDeletes(batchState);
        executeBatch(batchState);
    }   // executeUpdatesSynchronous

    // Add row/column updates in the given transaction to the batch.
    private void addUpdates(BatchStatement batchState) {
        for (String tableName : m_updateMap.keySet()) {
            addTableUpdates(tableName, batchState);
        }
    }   // addUpdates
    
    // Add row/column update statements for the given table to the given batch.
    private void addTableUpdates(String tableName, BatchStatement batchState) {
        boolean valueIsBinary = CQLService.instance().columnValueIsBinary(m_keyspace, tableName);
        PreparedStatement prepState =
            CQLService.instance().getPreparedUpdate(m_keyspace, Update.INSERT_ROW, tableName);
        Map<String, List<CQLColumn>> rowMap = m_updateMap.get(tableName);
        for (String key : rowMap.keySet()) {
            List<CQLColumn> colList = rowMap.get(key);
            for (CQLColumn column : colList) {
                batchState.add(addColumnUpdate(prepState, valueIsBinary, key, column));
            }
        }
    }   // addTableUpdates
    
    // Create and return a BoundStatement for the given column update.
    private BoundStatement addColumnUpdate(PreparedStatement prepState,
                                           boolean           valueIsBinary,
                                           String            key,
                                           CQLColumn         column) {
        BoundStatement boundState = prepState.bind();
        boundState.setString(0, key);
        boundState.setString(1, column.getName());
        if (valueIsBinary) {
            boundState.setBytes(2, ByteBuffer.wrap(column.getRawValue()));
        } else {
            boundState.setString(2, column.getValue());
        }
        return boundState;
    }   // addColumnUpdate
    
    // Add row/column deletes in this transaction to the given batch.
    private void addDeletes(BatchStatement batchState) {
        for (String tableName : m_deleteMap.keySet()) {
            addTableDelete(batchState, tableName);
        }
    }   // addDeletes
    
    // Add row/column deletes for the given table to the given batch.
    private void addTableDelete(BatchStatement batchState, String tableName) {
        Map<String, List<String>> rowKeyMap = m_deleteMap.get(tableName);
        for (String key : rowKeyMap.keySet()) {
            List<String> colList = rowKeyMap.get(key);
            if (colList != null && colList.size() > 0) {
                // Unfortunately, we have to delete one column at a time.
                PreparedStatement prepState =
                    CQLService.instance().getPreparedUpdate(m_keyspace, Update.DELETE_COLUMN, tableName);
                for (String colName : colList) {
                    batchState.add(addColumnDelete(prepState, key, colName));
                }
            } else {
                PreparedStatement prepState =
                    CQLService.instance().getPreparedUpdate(m_keyspace, Update.DELETE_ROW, tableName);
                batchState.add(addRowDelete(prepState, key));
            }
        }
    }   // addTableDelete
    
    // Create and return a BoundStatement that deletes the given column.
    private BoundStatement addColumnDelete(PreparedStatement prepState,
                                           String            key,
                                           String            colName) {
        BoundStatement boundState = prepState.bind();
        boundState.setString(0, key);
        boundState.setString(1, colName);
        return boundState;
    }   // addColumnDelete
    
    // Create and return a BoundStatement that deletes the given row.
    private BoundStatement addRowDelete(PreparedStatement prepState,
                                        String            key) {
        BoundStatement boundState = prepState.bind();
        boundState.setString(0, key);
        return boundState;
    }   // addRowDelete
    
    // Execute and given update statement.
    private void executeBatch(BatchStatement batchState) {
        if (batchState.size() > 0) {
            m_logger.debug("Executing synchronous batch with {} statements", batchState.size());
            m_session.execute(batchState);
        }
    }   // executeBatch
    
}   // class CQLTransaction
