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

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BatchStatement.Type;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ProtocolOptions.Compression;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.dell.doradus.common.Utils;
import com.dell.doradus.core.ServerConfig;
import com.dell.doradus.service.db.DBService;
import com.dell.doradus.service.db.DBTransaction;
import com.dell.doradus.service.db.DColumn;
import com.dell.doradus.service.db.DRow;
import com.dell.doradus.service.db.StoreTemplate;
import com.dell.doradus.service.db.cql.CQLStatementCache.Query;
import com.dell.doradus.service.db.cql.CQLStatementCache.Update;

public class CQLService extends DBService {
    // Global CFs are quoted to enforce backwards compatibility/case-sensitivity:
    public static final String APPS_TABLE_NAME = "\"" + StoreTemplate.APPLICATIONS_STORE_NAME + "\"";
    public static final String TASKS_TABLE_NAME = "\"" + StoreTemplate.TASKS_STORE_NAME + "\"";

    // Authentication constants:
    private static final String PASSWD_FILENAME_PROPERTY    = "passwd.properties";
    private static final String USERNAME_KEY                = "dbusername";
    private static final String PASSWORD_KEY                = "dbpassword";
    private static final String DEFAULT_CONFIGFILE          = "config/doradus.properties";
    
    // Singleton instance:
    private static final CQLService INSTANCE = new CQLService();

    // Construction via singleton only
    private CQLService() { }

    // Members:
    private String m_keyspace;
    private Session m_session;
    private CQLStatementCache m_queryCache;
    private CQLSchemaManager m_schemaMgr;
    
    //----- Public Service methods

    /**
     * Return the singleton CQLService service object.
     * 
     * @return  Static CQLService object.
     */
    public static CQLService instance() {return INSTANCE;}
    
    @Override
    protected void initService() {
        ServerConfig config = ServerConfig.getInstance();
        m_keyspace = storeToCQLName(config.keyspace);   // Quoted for compatibility with Thrift
        m_logger.debug("Cassandra host list: {}", Arrays.toString(config.dbhost.split(",")));
        m_logger.debug("Cassandra port: {}", config.dbport);
        m_logger.debug("Cassandra keyspace: {}", m_keyspace);
    }

    @Override
    protected void startService() {
        initializeCQLSession();
    }   // startService

    @Override
    protected void stopService() {
        if (m_session != null && !m_session.isClosed()) {
            m_session.close();
        }
        m_session = null;
        m_queryCache = null;
        m_schemaMgr = null;
    }   // stopService

    //----- Public DBService methods: Store management
    
    @Override
    public void createStoreIfAbsent(StoreTemplate storeTemplate) {
        String tableName = storeToCQLName(storeTemplate.getName());
        if (!storeExists(tableName)) {
            m_schemaMgr.createCQLTable(storeTemplate);
        }
    }   // createStoreIfAbsent
    
    @Override
    public void deleteStoreIfPresent(String storeName) {
        m_schemaMgr.dropCQLTable(storeToCQLName(storeName));
    }   // deleteStoreIfPresent

    @Override
    public boolean storeExists(String storeName) {
        String tableName = storeToCQLName(storeName);
        return m_schemaMgr.tableExists(tableName);
    }   // storeExists

    //----- Public DBService methods: Schema management
    
    @Override
    public Map<String, Map<String, String>> getAllAppProperties() {
        Map<String, Map<String, String>> result = new HashMap<>();
        ResultSet rs = executeQuery(Query.SELECT_ALL_ROWS_ALL_COLUMNS, APPS_TABLE_NAME);
        String lastKey = "";
        Map<String, String> currentApp = null;
        for (Row row : rs) {
            String key = row.getString("key");
            if (key.charAt(0) == '_') {
                continue;
            }
            String colName = row.getString("column1");
            String colValue = row.getString("value");
            if (!key.equals(lastKey)) {
                currentApp = new HashMap<>();
                result.put(key, currentApp);
                lastKey = key;
            }
            currentApp.put(colName, colValue);
        }
        return result;
    }   // getAllAppProperties

    @Override
    public Map<String, String> getAppProperties(String appName) {
        Map<String, String> result = new HashMap<>();
        ResultSet rs = executeQuery(Query.SELECT_1_ROW_ALL_COLUMNS, APPS_TABLE_NAME, appName);
        for (Row row : rs) {
            String colName = row.getString("column1");
            String colValue = row.getString("value");
            result.put(colName, colValue);
        }
        return result;
    }

    //----- Public DBService methods: Updates
    
    @Override
    public DBTransaction startTransaction() {
        return new CQLTransaction();
    }

    @Override
    public void commit(DBTransaction dbTran) {
        try {
            applyUpdates((CQLTransaction)dbTran);
        } finally {
            dbTran.clear();
        }
    }   // commit

    //----- Public DBService methods: Queries

    @Override
    public Iterator<DColumn> getAllColumns(String storeName, String rowKey) {
        String tableName = storeToCQLName(storeName);
        return new CQLColumnIterator(executeQuery(Query.SELECT_1_ROW_ALL_COLUMNS, tableName, rowKey));
    }

    @Override
    public Iterator<DColumn> getColumnSlice(String  storeName,
                                            String  rowKey,
                                            String  startCol,
                                            String  endCol,
                                            boolean reversed) {
        String tableName = storeToCQLName(storeName);
        ResultSet rs = null;
        if (reversed) {
            // Swap start/end columns for CQL reversed queries
            rs = executeQuery(Query.SELECT_1_ROW_COLUMN_RANGE_DESC, tableName, rowKey, endCol, startCol);
        } else {
            rs = executeQuery(Query.SELECT_1_ROW_COLUMN_RANGE, tableName, rowKey, startCol, endCol);
        }
        return new CQLColumnIterator(rs);
    }

    @Override
    public Iterator<DColumn> getColumnSlice(String storeName,
                                            String rowKey,
                                            String startCol,
                                            String endCol) {
        String tableName = storeToCQLName(storeName);
        return new CQLColumnIterator(executeQuery(Query.SELECT_1_ROW_COLUMN_RANGE, tableName, rowKey, startCol, endCol));
    }

    @Override
    public Iterator<DRow> getAllRowsAllColumns(String storeName) {
        String tableName = storeToCQLName(storeName);
        return new CQLRowIterator(executeQuery(Query.SELECT_ALL_ROWS_ALL_COLUMNS, tableName));
    }

    @Override
    public DColumn getColumn(String storeName, String rowKey, String colName) {
        String tableName = storeToCQLName(storeName);
        CQLColumnIterator colIter = new CQLColumnIterator(executeQuery(Query.SELECT_1_ROW_1_COLUMN, tableName, rowKey, colName));
        if (!colIter.hasNext()) {
            return null;
        }
        return colIter.next();
    }   // getColumn

    @Override
    public Iterator<DRow> getRowsAllColumns(String storeName, Collection<String> rowKeys) {
        String tableName = storeToCQLName(storeName);
        return new CQLRowIterator(executeQuery(Query.SELECT_ROW_SET_ALL_COLUMNS,
                                               tableName,
                                               new ArrayList<String>(rowKeys)));
    }

    @Override
    public Iterator<DRow> getRowsColumns(String             storeName,
                                         Collection<String> rowKeys,
                                         Collection<String> colNames) {
        String tableName = storeToCQLName(storeName);
        return new CQLRowIterator(executeQuery(Query.SELECT_ROW_SET_COLUMN_SET,
                                               tableName,
                                               new ArrayList<String>(rowKeys),
                                               new ArrayList<String>(colNames)));
    }

    @Override
    public Iterator<DRow> getRowsColumnSlice(String             storeName,
                                             Collection<String> rowKeys,
                                             String             startCol,
                                             String             endCol) {
        String tableName = storeToCQLName(storeName);
        return new CQLRowIterator(executeQuery(Query.SELECT_ROW_SET_COLUMN_RANGE,
                                               tableName,
                                               new ArrayList<String>(rowKeys),
                                               startCol,
                                               endCol));
    }

    @Override
    public Iterator<DRow> getRowsColumnSlice(String             storeName,
                                             Collection<String> rowKeys,
                                             String             startCol,
                                             String             endCol,
                                             boolean            reversed) {
        String tableName = storeToCQLName(storeName);
        ResultSet rs = null;
        if (reversed) {
            // Swap start/end columns for CQL reversed queries
            rs = executeQuery(Query.SELECT_ROW_SET_COLUMN_RANGE_DESC, tableName, new ArrayList<String>(rowKeys), endCol, startCol);
        } else {
            rs = executeQuery(Query.SELECT_ROW_SET_COLUMN_RANGE, tableName, new ArrayList<String>(rowKeys), startCol, endCol);
        }
        return new CQLRowIterator(rs);
    }

    //----- CQLService-specific public methods
    
    /**
     * Convert the given store name into a quoted CQL name if it isn't already quoted.
     * 
     * @param storeName Store name, possibly unquoted.
     * @return          Same name surrounded by double quotes.
     */
    public static String storeToCQLName(String storeName) {
        if (storeName.charAt(0) == '"') {
            return storeName;
        } else {
            return "\"" + storeName + "\"";
        }
    }   // storeToCQLName
    
    //----- Private methods
    
    // Execute the given query for the given table using the given values.
    private ResultSet executeQuery(Query query, String tableName, Object... values) {
        m_logger.debug("Executing statement {} on table {}; total params={}",
                       new Object[]{query, tableName, values.length});
        PreparedStatement prepState = m_queryCache.getPreparedQuery(query, tableName);
        BoundStatement boundState = prepState.bind(values);
        return m_session.execute(boundState);
    }   // executeQuery
    
    // Execute and given update statement.
    private ResultSet executeUpdate(CQLTransaction dbTran, Statement state) {
        m_logger.debug("Executing batch with {} updates", dbTran.getUpdateCount());
        try {
            return m_session.execute(state);
        } catch (Exception e) {
            m_logger.error("Batch statement failed", e);
            throw e;
        }
    }   // executeUpdate
    
    private void applyUpdates(CQLTransaction dbTran) {
        if (dbTran.getUpdateCount() == 0) {
            m_logger.debug("Skipping commit with no updates");
            return;
        }

        // TODO: how do we set the timestamp? Don't bother?
        BatchStatement batchState = new BatchStatement(Type.UNLOGGED);
        addUpdates(dbTran, batchState);
        addDeletes(dbTran, batchState);
        executeUpdate(dbTran, batchState);
    }   // applyUpdates

    // Add row/column updates in the given transaction to the batch.
    private void addUpdates(CQLTransaction dbTran, BatchStatement batchState) {
        Map<String, Map<String, List<CQLColumn>>> updateMap = dbTran.getUpdateMap();
        for (String tableName : updateMap.keySet()) {
            boolean bBinaryValues = tableValuesAreBinary(tableName);
            PreparedStatement prepState = m_queryCache.getPreparedUpdate(Update.INSERT_ROW, tableName);
            Map<String, List<CQLColumn>> rowMap = updateMap.get(tableName);
            for (String key : rowMap.keySet()) {
                List<CQLColumn> colList = rowMap.get(key);
                for (CQLColumn column : colList) {
                    BoundStatement boundState = prepState.bind();
                    boundState.setString(0, key);
                    boundState.setString(1, column.getName());
                    if (bBinaryValues) {
                        boundState.setBytes(2, ByteBuffer.wrap(column.getRawValue()));
                    } else {
                        boundState.setString(2, column.getValue());
                    }
                    batchState.add(boundState);
                }
            }
        }
    }   // addUpdates
    
    // Add row/column deletes in the given transaction to the batch.
    private void addDeletes(CQLTransaction dbTran, BatchStatement batchState) {
        Map<String, Map<String, List<String>>> deleteMap = dbTran.getDeleteMap();
        for (String tableName : deleteMap.keySet()) {
            Map<String, List<String>> rowKeyMap = deleteMap.get(tableName);
            for (String key : rowKeyMap.keySet()) {
                List<String> colList = rowKeyMap.get(key);
                if (colList != null && colList.size() > 0) {
                    for (String colName : colList) {
                        // Unfortunately, we have to delete one column at a time.
                        PreparedStatement prepState = m_queryCache.getPreparedUpdate(Update.DELETE_COLUMN, tableName);
                        BoundStatement boundState = prepState.bind(key, colName);
                        batchState.add(boundState);
                    }
                } else {
                    PreparedStatement prepState = m_queryCache.getPreparedUpdate(Update.DELETE_ROW, tableName);
                    BoundStatement boundState = prepState.bind(key);
                    batchState.add(boundState);
                }
            }
        }
    }   // addDeletes
    
    private boolean tableValuesAreBinary(String tableName) {
        Metadata metadata = m_session.getCluster().getMetadata();
        KeyspaceMetadata ksMetadata = metadata.getKeyspace(m_keyspace);
        TableMetadata tableMetadata = ksMetadata.getTable(tableName);
        ColumnMetadata colMetadata = tableMetadata.getColumn("value");
        return colMetadata.getType().equals(DataType.blob());
    }   // tableValuesAreBinary

    private void initializeCQLSession() {
        startCreateSessionThread();
    }   // initializeCQLSession

    private void startCreateSessionThread() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                Cluster cluster = buildClusterSpecs();
                while (!getState().isRunning()) {
                    try {
                        createKeyspaceSession(cluster);
                        setRunning();
                    } catch (Exception e) {
                        // for now
                        m_logger.info("Database is not reachable: {}. Waiting to retry", e);
                        try {
                            Thread.sleep(ServerConfig.getInstance().db_connect_retry_wait_millis);
                        } catch (InterruptedException ex2) {
                            // ignore
                        }
                    }
                }
            }
        }).start();
    }   // startNewSessionThread
    
    // Build Cluster object from ServerConfig settings.
    private Cluster buildClusterSpecs() {
        ServerConfig config = ServerConfig.getInstance();
        Cluster.Builder builder = Cluster.builder();
        
        // dbhost
        String[] nodeAddresses = config.dbhost.split(",");
        for (String address : nodeAddresses) {
            builder.addContactPoint(address);
        }
        
        // dbport
        builder.withPort(ServerConfig.getInstance().dbport);
        
        // db_timeout_millis and db_connect_retry_wait_millis
        SocketOptions socketOpts = new SocketOptions();
        socketOpts.setReadTimeoutMillis(config.db_timeout_millis);
        socketOpts.setConnectTimeoutMillis(config.db_connect_retry_wait_millis);
        builder.withSocketOptions(socketOpts);
        
        // dbauthenticator
        String authenticator = config.dbauthenticator;
        if (!Utils.isEmpty(authenticator) && authenticator.endsWith("PasswordAuthenticator")) {
            setClusterCredentials(builder);
        }
        
        // Experimental: Always use compression
        builder.withCompression(Compression.SNAPPY);
        
        // TODO: Allow SSL connections
        return builder.build();
    }   // buildClusterSpecs

    // Set the cluster credentials for password authentication.
    private void setClusterCredentials(Builder builder) {
        String filename = System.getProperty(PASSWD_FILENAME_PROPERTY);
        if (filename == null) {
            filename = DEFAULT_CONFIGFILE;
        }
        try (InputStream in = new BufferedInputStream(new FileInputStream(filename))) {
            Properties props = new Properties();
            props.load(in);
            String username = props.getProperty(USERNAME_KEY);
            String password = props.getProperty(PASSWORD_KEY);
            builder.withCredentials(username, password);
        } catch (IOException e) {
            throw new RuntimeException("Could not access passwd.properties file", e);
        }       
    }   // setClusterCredentials
    
    // Attempt to connect to the given cluster and set m_session to the right keyspace.
    private void createKeyspaceSession(Cluster cluster) {
        cluster.init();     // force connection and throw if unavailable
        displayClusterInfo(cluster);
        checkKeyspace(cluster);
        connectToKeyspace(cluster);
        checkGlobalTables();
    }   // createKeyspaceSession

    // Ensure the keyspace and initial CFs exist.
    private void checkKeyspace(Cluster cluster) {
        Metadata metadata = cluster.getMetadata();
        KeyspaceMetadata ksMetadata = metadata.getKeyspace(m_keyspace);
        if (ksMetadata == null) {
            m_logger.info("Creating keyspace {}", m_keyspace);
            CQLSchemaManager.createKeySpace(cluster, m_keyspace);
        }
    }   // checkKeyspace
    
    // Set m_session to a keyspace-specific session and initialize query cache and schema
    // manager.
    private void connectToKeyspace(Cluster cluster) {
        m_session = cluster.connect(m_keyspace);
        m_queryCache = new CQLStatementCache(m_session);
        m_schemaMgr = new CQLSchemaManager(m_session, m_keyspace);
    }   // connectToKeyspace
    
    // Check that the global required CFs exist, creating them if needed. 
    private void checkGlobalTables() {
        Cluster cluster = m_session.getCluster();
        Metadata metadata = cluster.getMetadata();
        KeyspaceMetadata ksMetadata = metadata.getKeyspace(m_keyspace);
        
        TableMetadata tableMetadata = ksMetadata.getTable(APPS_TABLE_NAME);
        if (tableMetadata == null) {
            createApplicationsTable();
        }
        tableMetadata = ksMetadata.getTable(TASKS_TABLE_NAME);
        if (tableMetadata == null) {
            createTasksTable();
        }
    }   // checkGlobalTables

    // Create the global Applications table.
    private void createApplicationsTable() {
        StoreTemplate template = new StoreTemplate(APPS_TABLE_NAME, false);
        m_schemaMgr.createCQLTable(template);
    }   // createApplicationsColumnFamily

    // Create the global Tasks table.
    private void createTasksTable() {
        StoreTemplate template = new StoreTemplate(TASKS_TABLE_NAME, false);
        m_schemaMgr.createCQLTable(template);
    }   // createTasksColumnFamily
    
    private void displayClusterInfo(Cluster cluster) {
        Metadata metadata = cluster.getMetadata();
        m_logger.info("Connected to cluster with topography:");
        RoundRobinPolicy policy = new RoundRobinPolicy();
        for (Host host : metadata.getAllHosts()) {
            m_logger.info("   Host {}: datacenter: {}, rack: {}, distance: {}",
                          new Object[]{host.getAddress(), host.getDatacenter(), 
                host.getRack(), policy.distance(host)});
        }
    }   // displayClusterInfo
    
}   // class CQLService
