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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import com.dell.doradus.common.Utils;
import com.dell.doradus.core.Defs;
import com.dell.doradus.core.ServerConfig;
import com.dell.doradus.service.spider.ColFamTemplate;

/**
 * Wraps a connection to a Cassandra database and provides methods for accessing data
 * using that connection.
 */
public class CassandraDBConn extends DBConn {
    private static IDBAuthenticator g_dbAuthenticator = null;

    private Cassandra.Client m_client;
    private boolean m_bDBOpen;
    private boolean m_bFailed;
    private String m_host;

    //----- Public static constants and methods

    /**
     * Name of the Applications ColumnFamily.
     */
    public static final String COLUMN_FAMILY_APPS = "Applications";
    
    /**
     * ColumnParent for the Applications ColumnFamily.
     */
    public static final ColumnParent COLUMN_PARENT_APPS = new ColumnParent(COLUMN_FAMILY_APPS);
    
    //----- DBConn: Store management
    
    /**
     * Create a new ColumnFamily using the given definition.
     * 
     * @param cfTemplate    {@link ColFamTemplate} object that describes CF to create.
     */
    @Override
    public void createStore(StoreTemplate storeTemplate) {
        new CassandraSchemaMgr(m_client).createColumnFamily((ColFamTemplate) storeTemplate);
    }   // createStore
    
    /**
     * Delete the ColumnFamily with the given name.
     * 
     * @param cfName    Name of ColumnFamily to delete.
     */
    @Override
    public void deleteStore(String storeName) {
        new CassandraSchemaMgr(m_client).deleteColumnFamily(storeName);
    }   // deleteStore
    
    /**
     * Return true if the given store name currently exists.
     * 
     * @param storeName Candidate store name.
     * @return          True if the store exists in the database.
     */
    @Override
    public boolean storeExists(String storeName) {
        KsDef ksDef = null;
        try {
            ksDef = m_client.describe_keyspace(ServerConfig.getInstance().keyspace);
        } catch (Exception ex) {
            throw new RuntimeException("Failed to get keyspace definition for '" + ServerConfig.getInstance().keyspace + "'", ex);
        }
        
        List<CfDef> cfDefList = ksDef.getCf_defs();
        for (CfDef cfDef : cfDefList) {
            if (cfDef.getName().equals(storeName)) {
                return true;
            }
        }
        return false;
    }	// storeExists
    
    /**
     * Get the ColumnFamily names that exist within our configured keyspace.
     * 
     * @return  A collection of ColumnFamily names that exist within our configured keyspace.
     */
    @Override
    public Collection<String> getAllStoreNames() {
        return new CassandraSchemaMgr(m_client).getColumnFamilies();
    }
    
    //----- DBConn: Connection management

    /**
     * Create an object that is connected to one Cassandra node. If the first connection
     * attempt fails, an attempt is made to connect to another node in cluster. This is
     * repeated until a connection is made or all configured nodes have been tried. If no
     * connection was made, a {@link DBNotAvailableException} is thrown. If a connection
     * is successfully made, and bInitialize is true, the cluster is checked to see if
     * has all required schema components: keyspace, ColumnFamily, etc.
     * 
     * @param bInitialize   True to check the database's schema and perform any
     *                      initializations needed.
     */
    public CassandraDBConn(boolean bInitialize) {
        super(bInitialize);
        if (bInitialize) {
            checkAuthenticator();
        }
        connect();
        if (bInitialize) {
            initializeSchema(); 
            checkClusterQuorum();
        }
        try {
            m_client.set_keyspace(ServerConfig.getInstance().keyspace);
        } catch (Exception e) {
            m_logger.error("Cannot use Keyspace '" + ServerConfig.getInstance().keyspace + "'", e);
            throw new RuntimeException(e);
        }
    }   // constructor

    /**
     * Close the database connection. This object can be reused after the connection has
     * been closed by calling {@link #open()} again.
     */
    @Override
    public void close() {
        // Get the connection's protocol (TBinaryProtocol), and the protocol's transport
        // (TSocket) and close it.
        if (m_client != null) {
            TProtocol protocol = m_client.getInputProtocol();
            if (protocol != null) {
                TTransport transport = protocol.getTransport();
                if (transport != null) {
                    transport.close();
                }
            }
        }
        m_client = null;
        m_bFailed = true;   // Prevent reusing this connection until reconnected
        m_bDBOpen = false;
    }   // close

    /**
     * Return true if the last operation for this connection failed, indicating that the
     * Cassandra node may be dead.
     */
    @Override
    public boolean isFailed() {
        return m_bFailed;
    }   // isFailed

    //----- DBConn: Schema requests
    
    /**
     * Get all properties of all registered applications. The resilt is a map of
     * application names to a map of application properties. An application's properties
     * are typically _application, _version, and _format, which define the application's
     * schema and the way it is stored in the database.
     * 
     * @return  Map of application name -> property name -> property value for all
     *          known applications. Empty if there are no applications defined.
     */
    @Override
    public Map<String, Map<String, String>> getAllAppProperties() {
        Iterator<DRow> rowIter = fetchAllRows(COLUMN_PARENT_APPS);
        Map<String, Map<String, String>> appPropMap = new HashMap<>();
        while (rowIter.hasNext()) {
            // Skip system rows, whose row keys begin with "_".
            DRow row = rowIter.next();
            if (row.getKey().charAt(0) != '_') {
                Map<String, String> propMap = new HashMap<>();
                appPropMap.put(row.getKey(), propMap);
                Iterator<DColumn> colIter = row.getColumns();
                while (colIter.hasNext()) {
                    DColumn col = colIter.next();
                    propMap.put(col.getName(), col.getValue());
                }
            }
        }
        return appPropMap;
    }   // getAllAppProperties

    /**
     * Get all properties of the application with the given name. The property names are
     * typically _application, _version, and _format, which define the application's
     * schema and the way it is stored in the database. Null is returned if there is no
     * such application defined.
     * 
     * @param appName   Name of application whose properties to get.
     * @return          Map of application properties as key/value pairs or null if there
     *                  is no such application defined.
     */
    @Override
    public Map<String, String> getAppProperties(String appName) {
        Iterator<DColumn> colIter = fetchAllColumns(COLUMN_PARENT_APPS, Utils.toBytes(appName));
        if (colIter == null) {
            return null;
        }
        Map<String, String> propMap = new HashMap<>();
        while (colIter.hasNext()) {
            DColumn col = colIter.next();
            propMap.put(col.getName(), col.getValue());
        }
        return propMap;
    }   // getAppProperties
    
    /**
     * Get the database-level options, if any, from the "options" row. If there are no
     * options stored, an empty map is returned (but not null).
     * 
     * @return  Map of database-level options as key/value pairs. Empty if there are no
     *          options stored.
     */
    @Override
    public Map<String, String> getDBOptions() {
        Map<String, String> dbOpts = new HashMap<>();
        Iterator<DColumn> colIter = fetchAllColumns(COLUMN_PARENT_APPS, Utils.toBytes(Defs.OPTIONS_ROW_KEY));
        if (colIter != null) {
            while (colIter.hasNext()) {
                DColumn col = colIter.next();
                dbOpts.put(col.getName(), col.getValue());
            }
        }
        return dbOpts;
    }   // getDBOptions

    //----- DBConn: Updates 
    
    /**
     * Commit the updates in the given {@link DBTransaction}. The updates are cleared even
     * if all commit retries fail.
     * 
     * @param dbTran {@link DBTransaction} with transactions ready to commit.
     */
    @Override
    public void commit(DBTransaction dbTran) {
        // For extreme logging
        CassandraTransaction cassDBTran = (CassandraTransaction)dbTran;
        if (m_logger.isTraceEnabled()) {
            cassDBTran.traceMutations(m_logger);
        }
        try {
            commitMutations(cassDBTran);
            commitDeletes(cassDBTran);
        } finally {
            dbTran.clear();
        }
    }   // commit

    //----- DBConn: Queries

    // All columns for a single row.
    @Override
    public Iterator<DColumn> getAllColumns(String storeName, String rowKey) {
        return fetchAllColumns(CassandraDefs.columnParent(storeName), Utils.toBytes(rowKey));
    }   // getAllColumns
    
    @Override
    public Iterator<DColumn> getColumnSlice(String storeName, String rowKey, String startCol, String endCol, boolean reversed) {
    	return getColumnSlice(CassandraDefs.columnParent(storeName), Utils.toBytes(rowKey), Utils.toBytes(startCol), Utils.toBytes(endCol), reversed);
    }	// getColumnSlice

    @Override
    public Iterator<DColumn> getColumnSlice(String storeName, String rowKey, String startCol, String endCol) {
    	return getColumnSlice(CassandraDefs.columnParent(storeName), Utils.toBytes(rowKey), Utils.toBytes(startCol), Utils.toBytes(endCol));
    }	// getColumnSlice

    // Single column for a single row.
    @Override
    public DColumn getColumn(String storeName, String rowKey, String colName) {
        return fetchColumn(CassandraDefs.columnParent(storeName), Utils.toBytes(rowKey), Utils.toBytes(colName));
    }   // getColumn

    // All columns for all rows.
    @Override
    public Iterator<DRow> getAllRowsAllColumns(String storeName) {
        return fetchAllRows(CassandraDefs.columnParent(storeName));
    }   // getAllRowsAllColumns
    
    // All columns for specific row keys.
    @Override
    public Iterator<DRow> getRowsAllColumns(String storeName, Collection<String> rowKeys) {
        List<byte[]> rowKeyList = new ArrayList<>();
        for (String rowKey : rowKeys) {
            rowKeyList.add(Utils.toBytes(rowKey));
        }
        return fetchRowsAllColumns(CassandraDefs.columnParent(storeName), rowKeyList);
    }   // getRowsAllColumns

    // Specific columns for specific rows.
    @Override
    public Iterator<DRow> getRowsColumns(String             storeName,
                                         Collection<String> rowKeys,
                                         Collection<String> colNames) {
        List<byte[]> rowKeyList = new ArrayList<>();
        for (String rowKey : rowKeys) {
            rowKeyList.add(Utils.toBytes(rowKey));
        }
        List<byte[]> colNameList = new ArrayList<>();
        for (String colName : colNames) {
            colNameList.add(Utils.toBytes(colName));
        }
        return fetchRowsColumns(CassandraDefs.columnParent(storeName), rowKeyList, colNameList);
    }   // getRowsColumns
    
    @Override
    public Iterator<DRow> getRowsColumns(String             storeName,
            							 Collection<String> rowKeys,
            							 String			   	startCol,
            							 String			   	endCol) {
    	return getRowsColumns(storeName, rowKeys, startCol, endCol, false);
    }
    @Override
    public Iterator<DRow> getRowsColumns(String             storeName,
            							 Collection<String> rowKeys,
            							 String			   	startCol,
            							 String			   	endCol,
            							 boolean 			reversed) {
        List<byte[]> rowKeyList = new ArrayList<>();
        for (String rowKey : rowKeys) {
            rowKeyList.add(Utils.toBytes(rowKey));
        }
    	return fetchColumnSlice(CassandraDefs.columnParent(storeName), 
    			rowKeyList, Utils.toBytes(startCol), Utils.toBytes(endCol), reversed);
    }	// getRowsColumns
    
    //----- Package-private methods
    
    /**
     * Fetch a slice of columns for the record with the given key, starting with the given
     * column name, limited to the number of columns indicated. If there are no such
     * columns found, an empty array is returned but not full.
     * 
     * @param colPar        ColumnParent that owns row.
     * @param rowKey        Row key as a byte[].
     * @param startColName  Name of first column to slice as a byte[].
     * @return              Columns found, if any. May be empty but not null.
     */
    List<ColumnOrSuperColumn> fetchColumnSlice(ColumnParent colPar, byte[] rowKey, byte[] startColName) {
    	return getSlice(colPar,
    			CassandraDefs.slicePredicateStartCol(startColName),
    			ByteBuffer.wrap(rowKey));
    }   // fetchColumnSlice

    /**
     * Fetch a slice of columns for the record with the given key, starting with the given
     * column name, ending with the given column name, and limited to the number of columns
     * indicated. If there are no such columns found, an empty array is returned but not full.
     * 
     * @param colPar        ColumnParent that owns row.
     * @param rowKey        Row key as a byte[].
     * @param startColName  Name of first column to slice as a byte[].
     * @param endColName  	Name of last column to slice as a byte[].
     * @return              Columns found, if any. May be empty but not null.
     */
    List<ColumnOrSuperColumn> fetchColumnSlice(ColumnParent colPar, byte[] rowKey, byte[] startColName, byte[] endColName) {
    	return getSlice(colPar,
    			CassandraDefs.slicePredicateStartEndCol(startColName, endColName),
    			ByteBuffer.wrap(rowKey));
    }   // fetchColumnSlice

    Iterator<DRow> fetchColumnSlice(ColumnParent colPar, List<byte[]> rowKeys, byte[] startColName, byte[] endColName, boolean reversed) {
        
        List<DRow> rowList = new ArrayList<>();
        Map<ByteBuffer, List<ColumnOrSuperColumn>> keyMap = multigetSlice(
            		CassandraDefs.convertByteKeys(rowKeys), colPar,
            		CassandraDefs.slicePredicateStartEndCol(startColName, endColName, reversed));
        for (ByteBuffer rowKeyBB : keyMap.keySet()) {
            List<ColumnOrSuperColumn> coscList = keyMap.get(rowKeyBB);
            if (coscList.size() == 0) {
                continue;   // probably a tombstone.
            }
            
            byte[] rowKey = Utils.getBytes(rowKeyBB);   // destructive to ByteBuffer
            rowList.add(new CassandraRow(rowKey, new CassandraColumnBatch(colPar, rowKey, coscList)));
        }
        return rowList.iterator();
    }   // fetchColumnSlice

    /**
     * Perform a get_range_slices() request with the given parameters and retry the operation
     * if a database error occurs. Retries will attempt to get a new connection if an
     * error suggests that the current DB node or the Thrift connection has failed. If no rows
     * are found, an empty list is returned is returned.
     * 
     * @param colParent ColumnParent to query.
     * @param slicePred SlicePredicate defining columns to fetch.
     * @param keyRange  KeyRange defininig keys to fetch.
     * @return
     */
    List<KeySlice> getRangeSlices(ColumnParent   colParent,
                                  SlicePredicate slicePred, 
                                  KeyRange       keyRange) {
        m_logger.debug("Fetching {}.{} from {}",
                       new Object[]{toString(keyRange), toString(slicePred), toString(colParent)});
        List<KeySlice> keySliceList = null;
        boolean bSuccess = false;
        for (int attempts = 1; !bSuccess; attempts++) {
            try {
                // Attempt to retrieve a slice list.
                Date startDate = new Date();
                keySliceList = m_client.get_range_slices(colParent, slicePred, keyRange, ConsistencyLevel.ONE);
                timing("get_range_slices", startDate);
                if (attempts > 1) {
                    m_logger.info("get_range_slices() succeeded on attempt #{}", attempts);
                }
                bSuccess = true;
            } catch (Exception ex) {
                // Abort if all retries exceeded.
                if (attempts >= ServerConfig.getInstance().max_read_attempts) {
                    String errMsg = "All retries exceeded; abandoning get_range_slices() for table: " +
                                    colParent.getColumn_family();
                    m_bFailed = true;
                    m_logger.error(errMsg, ex);
                    throw new RuntimeException(errMsg, ex);
                }
                
                // Report retry as a warning.
                m_logger.warn("get_range_slices() attempt #{} failed: {}", attempts, ex);
                try {
                    Thread.sleep(attempts * ServerConfig.getInstance().retry_wait_millis);
                } catch (InterruptedException e1) {
                    // ignore
                }
                reconnect(ex);
            }
        }
        return keySliceList;
    }   // getRangeSlices

    
    
    
    List<ColumnOrSuperColumn> getSlice(ColumnParent colParent, SlicePredicate slicePred, ByteBuffer key) {
        m_logger.debug("Fetching {}.{} from {}", new Object[]{Utils.toString(key), toString(slicePred), toString(colParent)});
        List<ColumnOrSuperColumn> columnList = null;
        boolean bSuccess = false;
        for (int attempts = 1; !bSuccess; attempts++) {
            try {
                // Attempt to retrieve a slice list.
                Date startDate = new Date();
                columnList = m_client.get_slice(key, colParent, slicePred, ConsistencyLevel.ONE);
                timing("get_slice", startDate);
                if (attempts > 1) {
                    m_logger.info("get_slice() succeeded on attempt #{}", attempts);
                }
                bSuccess = true;
            } catch (Exception ex) {
                // Abort if all retries exceeded.
                if (attempts >= ServerConfig.getInstance().max_read_attempts) {
                    String errMsg = "All retries exceeded; abandoning get_slice() for table: " +
                                    colParent.getColumn_family();
                    m_bFailed = true;
                    m_logger.error(errMsg, ex);
                    throw new RuntimeException(errMsg, ex);
                }
                
                // Report retry as a warning.
                m_logger.warn("get_slice() attempt #{} failed: {}", attempts, ex);
                try {
                    Thread.sleep(attempts * ServerConfig.getInstance().retry_wait_millis);
                } catch (InterruptedException e1) {
                    // ignore
                }
                reconnect(ex);
            }
        }
        return columnList;
    }

    ColumnOrSuperColumn getColumn(ByteBuffer key, ColumnPath colPath) {
        m_logger.debug("Fetching {}.{}", new Object[]{Utils.toString(key), toString(colPath)});
        ColumnOrSuperColumn column = null;
        boolean bSuccess = false;
        for (int attempts = 1; !bSuccess; attempts++) {
            try {
                // Attempt to retrieve a slice list.
                Date startDate = new Date();
                column = m_client.get(key, colPath, ConsistencyLevel.ONE);
                timing("get", startDate);
                if (attempts > 1) {
                    m_logger.info("get() succeeded on attempt #{}", attempts);
                }
                bSuccess = true;
            } catch (Exception ex) {
                // Abort if all retries exceeded.
                if (attempts >= ServerConfig.getInstance().max_read_attempts) {
                    String errMsg = "All retries exceeded; abandoning get() for table: " +
                                    colPath.getColumn_family();
                    m_bFailed = true;
                    m_logger.error(errMsg, ex);
                    throw new RuntimeException(errMsg, ex);
                }
                
                // Report retry as a warning.
                m_logger.warn("get() attempt #{} failed: {}", attempts, ex);
                try {
                    Thread.sleep(attempts * ServerConfig.getInstance().retry_wait_millis);
                } catch (InterruptedException e1) {
                    // ignore
                }
                reconnect(ex);
            }
        }
        return column;
    }

    
    /**
     * Perform a multiget_slice() request with the given parameters and retry the
     * operation if a database error occurs. Retries will attempt to get a new connection
     * if an error suggests that the current DB node or the Thrift connection has failed.
     * If no rows are found, an empty map is returned.
     * 
     * @param rowKeyList    List of row keys to fetch.
     * @param colParent     ColumnParent to fetch rows from.
     * @param slicePred     SlicePredicate defining which columns to fetch.
     * @return              Map of row keys to column lists for each row found. A map
     *                      entry may have no columns if the row does not exist or it
     *                      is a tombstone.
     */
    Map<ByteBuffer, List<ColumnOrSuperColumn>> multigetSlice(List<ByteBuffer>  rowKeyList,
                                                             ColumnParent      colParent,
                                                             SlicePredicate    slicePred) {
        m_logger.debug("Fetching {} keys {} from {}",
                       new Object[]{rowKeyList.size(), toString(slicePred), toString(colParent)});
        Map<ByteBuffer, List<ColumnOrSuperColumn>> keyMap = null;
        boolean bSuccess = false;
        for (int attempts = 1; !bSuccess; attempts++) {
            try {
                Date startDate = new Date();
                keyMap = m_client.multiget_slice(rowKeyList, colParent, slicePred, ConsistencyLevel.ONE);
                timing("multiget_slice", startDate);
                if (attempts > 1) {
                    m_logger.info("multiget_slice() succeeded on attempt #{}", attempts);
                }
                bSuccess = true;
            } catch (Exception ex) {
                if (attempts >= ServerConfig.getInstance().max_read_attempts) {
                    String errMsg = "All retries exceeded; abandoning multiget_slice() for table: " +
                                    colParent.getColumn_family();
                    m_bFailed = true;
                    m_logger.error(errMsg, ex);
                    throw new RuntimeException(errMsg, ex);
                }
                
                m_logger.warn("multiget_slice() attempt #{} failed: {}", attempts, ex);
                try {
                    Thread.sleep(attempts * ServerConfig.getInstance().retry_wait_millis);
                } catch (InterruptedException ex2) {
                    // ignore
                }
                
                // Reconnect since the connection may be bad.
                reconnect(ex);
            }
        }
        return keyMap;
    }   // multigetSlice

    //----- Private methods
    
    // If an IDBAuthenticator is declared, make sure we can instantiate it.
    private void checkAuthenticator() {
        String className = ServerConfig.getInstance().dbauthenticator;
        if (Utils.isEmpty(className)) {
            return;
        }
        try {
            @SuppressWarnings("unchecked")
            Class<IDBAuthenticator> cls = (Class<IDBAuthenticator>)Class.forName(className);
            g_dbAuthenticator = cls.getConstructor().newInstance();
        } catch (Exception e) {
            g_dbAuthenticator = null;
            throw new RuntimeException("Unable to find IDBAuthenticator class: " + className, e);
        }
    }   // checkAuthenticator
    
    // Commit all row-deletions in the given MutationMap, if any, using the given timestamp.
    private void commitDeletes(CassandraTransaction cassDBTran) {
        Map<String, Set<ByteBuffer>> rowDeleteMap = cassDBTran.getRowDeletionMap();
        if (rowDeleteMap.size() == 0) {
            return;
        }
        
        // Iterate through all ColumnFamilies
        for (String colFamName : rowDeleteMap.keySet()) {
            // Delete each row in this key set.
            Set<ByteBuffer> rowKeySet = rowDeleteMap.get(colFamName);
            for (ByteBuffer rowKey : rowKeySet) {
                removeRow(cassDBTran.getTimestamp(), rowKey, new ColumnPath(colFamName));
            }
        }
    }   // commitDeletes

    // Commit the update mutations in the given MutationMap. Retry if needed up to the
    // configured maximum number of retries.
    private void commitMutations(CassandraTransaction cassDBTran) {
        Map<ByteBuffer, Map<String, List<Mutation>>> colMutMap = cassDBTran.getUpdateMap();
        if (colMutMap.size() == 0) {
            return;
        }
        m_logger.debug("Committing {} mutations", cassDBTran.totalColumnMutations());
        
        // The batch_mutate will be retried up to MAX_COMMIT_RETRIES times.
        boolean bSuccess = false;
        for (int attempts = 1; !bSuccess; attempts++) {
            try {
                // Attempt to commit all updates in the the current mutation map.
                Date startDate = new Date();
                m_client.batch_mutate(colMutMap, ConsistencyLevel.ONE);
                timing("commitMutations", startDate);
                if (attempts > 1) {
                    // Since we had a failure and warned about it, confirm which attempt succeeded.
                    m_logger.info("batch_mutate() succeeded on attempt #{}", attempts);
                }
                bSuccess = true;
            } catch (Exception ex) {
                // If we've reached the retry limit, we fail this commit.
                if (attempts >= ServerConfig.getInstance().max_commit_attempts) {
                    m_bFailed = true;
                    m_logger.error("All retries exceeded; abandoning batch_mutate()", ex);
                    throw new RuntimeException("All retries exceeded; abandoning batch_mutate()", ex);
                }
                
                // Report retry as a warning.
                m_logger.warn("batch_mutate() attempt #{} failed: {}", attempts, ex);
                try {
                    // We wait more with each failure.
                    Thread.sleep(attempts * ServerConfig.getInstance().retry_wait_millis);
                } catch (InterruptedException e1) {
                    // ignore
                }
                
                // Experience suggests that even for timeout exceptions, the connection
                // may be bad, so we attempt to reconnect. If this fails, it will throw
                // an IOException, which we pass to the caller.
                reconnect(ex);
            }
        }
    }   // commitMutations

    // Check that the Cassandra cluster, one node to which this connection is connected,
    // has enough nodes to warrant a quorum and therefore is safe to use. If not enough
    // nodes are available, this method throws.
    private void checkClusterQuorum() {
        // Prerequisites:
        assert m_bDBOpen;
        m_logger.debug("Checking for cluster quorum");
        
        // An easy way to check for a quorum is to perform a simple query on the
        // Applications table, which always exists.
        try {
            Date startDate = new Date();
            m_client.get_range_slices(COLUMN_PARENT_APPS,
                                      CassandraDefs.SLICE_PRED_ALL_COLS,
                                      CassandraDefs.KEY_RANGE_ALL_ROWS,
                                      ConsistencyLevel.QUORUM);
            timing("get_range_slices(quorum)", startDate);
        } catch (Exception e) {
            // Rethrow as an IOException.
            m_logger.info("Quorum check failed: " + e.toString());
            throw new RuntimeException("get_range_slices quorum check failed", e);
        }
    }   // checkClusterQuorum
    
    // Create a Thrift client interface and connect to a Cassandra database node. If this
    // connection has been connected before, we attempt to skip the same node in case it
    // has gone dead. If the DoradusCluster is configured with N nodes, we will attempt to
    // connect N times, once per node, until we get a successful connection. However,
    // there is no wait/retry logic as there is in reconnect().
    private void connect() {
        assert !m_bDBOpen;
        ServerConfig config = ServerConfig.getInstance();
        Exception lastException = null;
        String[] dbHosts = ServerConfig.getInstance().dbhost.split(",");
        for (int attempt = 1; attempt <= dbHosts.length; attempt++) {
            try {
                m_host = chooseHost(dbHosts);
                m_logger.info("Connecting to Cassandra node {}:{}", m_host, config.dbport);
                TSocket socket = new TSocket(m_host, config.dbport, config.db_timeout_millis);
                TTransport transport = new TFramedTransport(socket);
                TProtocol protocol = new TBinaryProtocol(transport);
                m_client = new Cassandra.Client(protocol);
                socket.open();
                
                // If an authenticator has been configured, login with it now, otherwise
                // the check-keyspace will fail.
                if (g_dbAuthenticator != null) {
                    m_client.login(g_dbAuthenticator.getAuthRequest());
                }
                
                // Don't set keyspace yet since it might not exist for first connection.
                m_bDBOpen = true;
                m_bFailed = false;
                return;
            } catch (Exception ex) {
                lastException = ex;
            }
        }
        
        // All connect attempts failed.
        m_bFailed = true;
        m_logger.error("Could not connect to Cassandra", lastException);
        throw new DBNotAvailableException("Error opening database", lastException);
    }   // connect

    // Choose the next Cassandra host name in the list or a random one.
    private String chooseHost(String[] dbHosts) {
        if (dbHosts.length == 1) {
            return dbHosts[0];
        }
        if (!Utils.isEmpty(m_host)) {
            for (int index = 0; index < dbHosts.length; index++) {
                if (dbHosts[index].equals(m_host)) {
                    return dbHosts[(++index) % dbHosts.length];
                }
            }
        }
        // No prior host: choose random host.
        return dbHosts[new Random().nextInt(dbHosts.length)];
    }   // chooseHost
    
    // Get a single column from the given row and CF.
    private DColumn fetchColumn(ColumnParent colPar, byte[] rowKey, byte[] colName) {
    	ColumnPath colPath = new ColumnPath();
    	colPath.setColumn_family(colPar.getColumn_family());
    	colPath.setColumn(colName);
    	Column col = getColumn(ByteBuffer.wrap(rowKey), colPath).getColumn();
        return new CassandraColumn(col.getName(), col.getValue());
    }   // fetchColumn

    // Fetch all columns of the rows with the given keys from the given ColumnFamily.
    private Iterator<DRow> fetchRowsAllColumns(ColumnParent colPar, Collection<byte[]> rowKeys) {
        List<DRow> rowList = new ArrayList<>();
        Map<ByteBuffer, List<ColumnOrSuperColumn>> keyMap =
            multigetSlice(CassandraDefs.convertByteKeys(rowKeys), colPar, CassandraDefs.SLICE_PRED_ALL_COLS);
        for (ByteBuffer rowKeyBB : keyMap.keySet()) {
            List<ColumnOrSuperColumn> coscList = keyMap.get(rowKeyBB);
            if (coscList.size() == 0) {
                continue;   // probably a tombstone.
            }
            
            byte[] rowKey = Utils.getBytes(rowKeyBB);   // destructive to ByteBuffer
            rowList.add(new CassandraRow(rowKey, new CassandraColumnBatch(colPar, rowKey, coscList)));
        }
        return rowList.iterator();
    }   // fetchRowsAllColumns
    
    // Return all columns of the row with the given key in the given ColumnFamily. If no
    // such row is found, null is returned.
    private Iterator<DColumn> fetchAllColumns(ColumnParent colPar, byte[] rowKey) {
        CassandraColumnBatch colBatch = new CassandraColumnBatch(this, colPar, rowKey);
        return colBatch.hasNext() ? colBatch : null;
    }   // fetchAllColumns
    
    /**
     * Return columns of the given column range in a row. If no columns found or
     * the row is empty an empty iterator is returned.
     * 
     * @param colPar
     * @param rowKey
     * @param startColName
     * @param endColName
     * @param reversed
     * @return
     */
    private Iterator<DColumn> getColumnSlice(ColumnParent colPar, byte[] rowKey,
    		byte[] startColName, byte[] endColName, boolean reversed) {
    	return new CassandraColumnBatch(this, colPar, rowKey, startColName, endColName, reversed);
    }	// getColumnSlice
    
    /**
     * Return columns of the given column range in a row. If no columns found or
     * the row is empty an empty iterator is returned.
     * 
     * @param colPar
     * @param rowKey
     * @param startColName
     * @param endColName
     * @return
     */
    private Iterator<DColumn> getColumnSlice(ColumnParent colPar, byte[] rowKey,
    		byte[] startColName, byte[] endColName) {
    	return new CassandraColumnBatch(this, colPar, rowKey, startColName, endColName);
    }	// getColumnSlice
    
    // Return all columns of all rows of the given ColumnFamily. Careful!
    private Iterator<DRow> fetchAllRows(ColumnParent colPar) {
        return new CassandraRowBatch(this, colPar);
    }   // fetchAllRows

    // Fetch the requested columns for the request rows from the requested ColumnFamily.
    private Iterator<DRow> fetchRowsColumns(ColumnParent       colPar,
                                            Collection<byte[]> rowKeys,
                                            Collection<byte[]> colNames) {
        List<DRow> rowList = new ArrayList<>();
        Map<ByteBuffer, List<ColumnOrSuperColumn>> keyMap =
            multigetSlice(CassandraDefs.convertByteKeys(rowKeys), colPar, CassandraDefs.slicePredicateColNames(colNames));
        for (ByteBuffer rowKeyBB : keyMap.keySet()) {
            List<ColumnOrSuperColumn> coscList = keyMap.get(rowKeyBB);
            if (coscList.size() == 0) {
                continue;   // probably a tombstone.
            }
            
            byte[] rowKey = Utils.getBytes(rowKeyBB);   // destructive to ByteBuffer
            rowList.add(new CassandraRow(rowKey, new CassandraColumnBatch(colPar, rowKey, coscList)));
        }
        return rowList.iterator();
    }   // fetchRowsColumns

    // Check that the Doradus keyspace exists and has everything we expect.
    private void initializeSchema() {
        CassandraSchemaMgr mgr = new CassandraSchemaMgr(m_client);
        mgr.initializeSchema();
    }   // initializeSchema
   
    // Attempt to reconnect this connection to Cassandra due to the given exception.
    // Because Cassandra could be very busy, if the reconnect fails, we will retry multiple
    // times, waiting a little longer between each attempt. If all retries fail, we throw
    // an IOException and leave the Thrift connection null.
    private void reconnect(Exception reconnectEx) {
        // Log the exception as a warning.
        m_logger.warn("Reconnecting to Cassandra due to error", reconnectEx);
        
        // Reconnect up to the configured number of times, waiting a little between each attempt.
        boolean bSuccess = false;
        for (int attempt = 1; !bSuccess; attempt++) {
            try {
                close();
                connect();
                m_client.set_keyspace(ServerConfig.getInstance().keyspace);
                m_logger.debug("Reconnected to Cassandra node: {}", m_host);
                assert m_client != null;
                assert m_bDBOpen;
                assert !m_bFailed;
                bSuccess = true;
            } catch (Exception ex) {
                // Abort if all retries failed.
                if (attempt >= ServerConfig.getInstance().max_reconnect_attempts) {
                    m_logger.error("All reconnect attempts failed; abandoning reconnect", ex);
                    throw new RuntimeException("All reconnect attempts failed; abandoning reconnect", ex);
                }
                m_logger.warn("Reconnect attempt #" + attempt + " failed", ex);
                try {
                    Thread.sleep(ServerConfig.getInstance().retry_wait_millis * attempt);
                } catch (InterruptedException e) {
                    // Ignore
                }
            }
        }
    }   // reconnect

    // Perform a row remove() update and retry if an error occurs.
    private void removeRow(long timestamp, ByteBuffer key, ColumnPath colPath) {
        // Prerequisites:
        assert key != null;
        assert colPath != null;
        m_logger.debug("Removing row {} from {}", Utils.toString(Utils.copyBytes(key)), toString(colPath));
        
        // The remove will be retried up to MAX_COMMIT_RETRIES times.
        boolean bSuccess = false;
        for (int attempts = 1; !bSuccess; attempts++) {
            try {
                // Attempt to remove the requested row.
                Date startDate = new Date();
                m_client.remove(key, colPath, timestamp, ConsistencyLevel.ONE);
                timing("remove", startDate);
                if (attempts > 1) {
                    // Since we had a failure and warned about it, confirm which commit succeeded.
                    m_logger.info("remove() succeeded on attempt #{}", attempts);
                }
                bSuccess = true;
            } catch (Exception ex) {
                // For a timeout exception, Cassandra may be very busy, so we retry up
                // to the configured limit.
                if (attempts >= ServerConfig.getInstance().max_commit_attempts) {
                    m_bFailed = true;
                    String errMsg = "All retries exceeded; abandoning remove() for table: " +
                                    colPath.getColumn_family();
                    m_logger.error(errMsg, ex);
                    throw new RuntimeException(errMsg, ex);
                }
                
                // Report retry as a warning.
                m_logger.warn("remove() attempt #{} failed: {}", attempts, ex);
                try {
                    // We wait more with each failure.
                    Thread.sleep(attempts * ServerConfig.getInstance().retry_wait_millis);
                } catch (InterruptedException e1) {
                    // ignore
                }
                
                // Reconnect since the connection may be bad. This throws an IOException
                // if unsuccessful.
                reconnect(ex);
            }
        }
    }   // removeRow
    
    // Timings output. If trace output is enabled, takes a snapshot of now via a new
    // Date(), subtracts it from the given timestamp, and displays the difference in
    // milliseconds using the given prefix as a label.
    private void timing(String metric, Date startDate) {
        m_logger.trace("Time for '{}': {}", metric,
                       ((new Date()).getTime() - startDate.getTime()) + " millis");
    }   // timing

    // Friendly toString() for KeyRange
    private static String toString(KeyRange keyRange) {
        ByteBuffer startKey = keyRange.start_key;
        String startKeyStr = "<null>";
        if (startKey != null) {
            startKeyStr = Utils.toString(startKey.array(), startKey.arrayOffset(), startKey.limit());
        }
        if (startKeyStr.length() == 0) {
            startKeyStr = "<first>";
        }
        ByteBuffer endKey = keyRange.end_key;
        String endKeyStr = "<null>";
        if (endKey != null) {
            endKeyStr = Utils.toString(endKey.array(), endKey.arrayOffset(), endKey.limit());
        }
        if (endKeyStr.length() == 0) {
            endKeyStr = "<last>";
        }
        StringBuilder buffer = new StringBuilder();
        if (startKeyStr.equals("<first>") && endKeyStr.equals("<last>")) {
            buffer.append("Keys(<all>)");
        } else if (startKeyStr.equals(endKeyStr)) {
            buffer.append("Key('");
            buffer.append(startKeyStr);
            buffer.append("')");
        } else {
            buffer.append("Keys('");
            buffer.append(startKeyStr);
            buffer.append("' to '");
            buffer.append(endKeyStr);
            buffer.append("')");
        }
        return buffer.toString();
    }   // toString(KeyRange)
    
    // Friendly toString() for a SlicePredicate
    private static String toString(SlicePredicate slicePred) {
        StringBuilder buffer = new StringBuilder();
        if (slicePred.isSetColumn_names()) {
            buffer.append("Columns(");
            buffer.append(slicePred.getColumn_names().size());
            buffer.append(" total)");
        } else if (slicePred.isSetSlice_range()) {
            SliceRange sliceRange = slicePred.getSlice_range();
            ByteBuffer startCol = sliceRange.start;
            String startColStr = "<null>";
            if (startCol != null) {
                startColStr = Utils.toString(startCol.array(), startCol.arrayOffset(), startCol.limit());
            }
            if (startColStr.length() == 0) {
                startColStr = "<first>";
            }
            ByteBuffer endCol = sliceRange.finish;
            String endColStr = "<null>";
            if (endCol != null) {
                endColStr = Utils.toString(endCol.array(), endCol.arrayOffset(), endCol.limit());
            }
            if (endColStr.length() == 0) {
                endColStr = "<last>";
            }
            if (startColStr.equals("<first>") && endColStr.equals("<last>")) {
                buffer.append("Slice(<all>)");
            } else {
                buffer.append("Slice('");
                buffer.append(startColStr);
                buffer.append("' to '");
                buffer.append(endColStr);
                buffer.append("')");
            }
        }
        return buffer.toString();
    }   // toString(KeyRange)
    
    // Friendly toString() for a ColumnParent
    private static String toString(ColumnParent colParent) {
        return "CF '" + colParent.getColumn_family() + "'";
    }   // toString(KeyRange)
    
    // Friendly toString() for a ColumnPath
    private static String toString(ColumnPath colPath) {
        return "CF '" + colPath.getColumn_family() + "'";
    }   // toString(KeyRange)
    
}   // class CassandraDBConn
