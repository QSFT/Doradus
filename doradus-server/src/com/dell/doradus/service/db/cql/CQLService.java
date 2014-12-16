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

import java.io.FileInputStream;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ProtocolOptions.Compression;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.SSLOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.dell.doradus.core.ServerConfig;
import com.dell.doradus.service.db.DBService;
import com.dell.doradus.service.db.DBTransaction;
import com.dell.doradus.service.db.DColumn;
import com.dell.doradus.service.db.DRow;
import com.dell.doradus.service.db.cql.CQLStatementCache.Query;
import com.dell.doradus.service.db.cql.CQLStatementCache.Update;

/**
 * Implements the DBService interface using the CQL API to communicate with Cassandra.
 * Note on ServerConfig.application_keyspaces: Currently, if this option is false, all
 * applications are accessed from the global keyspace, defined by ServerConfig.keyspace.
 * If application_keyspaces is true, then each application is accessed in its own
 * keyspace where the application name == the keyspace name. Two instances of the server
 * can be run at the same time, each with differing application_keyspaces values, so that
 * a mix of applications can be used in the same cluster.
 */
public class CQLService extends DBService {
    // Compare to store names, table names are quoted 
    private static final String APPS_TABLE_NAME = "\"" + DBService.APPS_STORE_NAME + "\"";
    private static final String TASKS_TABLE_NAME = "\"" + DBService.TASKS_STORE_NAME + "\"";
    
    // Singleton instance:
    private static final CQLService INSTANCE = new CQLService();

    // Construction via singleton only
    private CQLService() { }

    // Members:
    private Cluster m_cluster;
    private final Map<String, String> m_appKeyspaceMap = new HashMap<String, String>();
    private final Map<String, Session> m_ksSessionMap = new HashMap<>();
    private final Map<String, CQLStatementCache> m_ksStatementCacheMap = new HashMap<>();
    
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
        m_logger.info("Using CQL API");
        m_logger.debug("Cassandra host list: {}", Arrays.toString(config.dbhost.split(",")));
        m_logger.debug("Cassandra port: {}", config.dbport);
        m_logger.debug("Default application keyspace: {}", config.keyspace);
    }

    @Override
    protected void startService() {
        initializeCluster();
    }   // startService

    @Override
    protected void stopService() {
        synchronized (m_ksSessionMap) {
            for (Session session : m_ksSessionMap.values()) {
                session.close();
            }
            m_ksSessionMap.clear();
            m_ksStatementCacheMap.clear();
        }
    }   // stopService

    //----- Public DBService methods: Store management

    @Override
    public void createKeyspace(String keyspace) {
        String cqlKeyspace = storeToCQLName(keyspace);
        KeyspaceMetadata ksMetadata = m_cluster.getMetadata().getKeyspace(cqlKeyspace);
        if (ksMetadata == null) {
            try (Session session = m_cluster.connect()) {   // no-keyspace session
                CQLSchemaManager schemaMgr = new CQLSchemaManager(session, null);
                schemaMgr.createKeyspace(cqlKeyspace);
            }
        }
    }   // createKeyspace

    @Override
    public void dropKeyspace(String keyspace) {
        String cqlKeyspace = storeToCQLName(keyspace);
        synchronized (m_ksSessionMap) {
            Session session = m_ksSessionMap.get(cqlKeyspace);
            if (session != null) {
                session.close();
                m_ksSessionMap.remove(cqlKeyspace);
                m_ksStatementCacheMap.remove(cqlKeyspace);
            }
            try (Session noKSSession = m_cluster.connect()) {   // no-keyspace session
                CQLSchemaManager schemaMgr = new CQLSchemaManager(noKSSession, null);
                schemaMgr.dropKeyspace(cqlKeyspace);
            }
        }
    }   // dropKeyspace
    
    @Override
    public String getKeyspaceForApp(String appName) {
        synchronized (m_appKeyspaceMap) {
            if (!m_appKeyspaceMap.containsKey(appName)) {
                refreshAppKeyspaceMap();
            }
            return m_appKeyspaceMap.get(appName);
        }
    }   // getKeyspaceForApp
    
    @Override
    public void createStoreIfAbsent(String keyspace, String storeName, boolean bBinaryValues) {
        checkState();
        String cqlKeyspace = storeToCQLName(keyspace);
        String tableName = storeToCQLName(storeName);
        if (!storeExists(cqlKeyspace, tableName)) {
            Session session = getOrCreateKeyspaceSession(cqlKeyspace);
            CQLSchemaManager schemaMgr = new CQLSchemaManager(session, cqlKeyspace);
            schemaMgr.createCQLTable(storeName, bBinaryValues);
        }
    }   // createStoreIfAbsent
    
    @Override
    public void deleteStoreIfPresent(String keyspace, String storeName) {
        checkState();
        String cqlKeyspace = storeToCQLName(keyspace);
        String tableName = storeToCQLName(storeName);
        if (storeExists(cqlKeyspace, tableName)) {
            Session session = getOrCreateKeyspaceSession(cqlKeyspace);
            CQLSchemaManager schemaMgr = new CQLSchemaManager(session, cqlKeyspace);
            schemaMgr.dropCQLTable(tableName);
        }
    }   // deleteStoreIfPresent

    @Override
    public void registerApplication(String keyspace, String appName) {
        synchronized (m_appKeyspaceMap) {
            m_appKeyspaceMap.put(appName, storeToCQLName(keyspace));
        }
    }   // registerApplication

    @Override
    public SortedMap<String, SortedSet<String>> getTenantMap() {
        checkState();
        refreshAppKeyspaceMap();
        SortedMap<String, SortedSet<String>> result = new TreeMap<>();
        synchronized (m_appKeyspaceMap) {
            for (String appName : m_appKeyspaceMap.keySet()) {
                String keyspace = m_appKeyspaceMap.get(appName);
                SortedSet<String> appNameSet = result.get(keyspace);
                if (appNameSet == null) {
                    appNameSet = new TreeSet<>();
                    result.put(keyspace, appNameSet);
                }
                appNameSet.add(appName);
            }
        }
        return result;
    }   // getTenantMap
    
    /**
     * Return true if column values for the given keyspace/table name are binary.
     * 
     * @param keyspace  Quoted keyspace name.
     * @param tableName Quoted columnfamily name.
     * @return          True if the given table's column values are binary.
     */
    public boolean columnValueIsBinary(String keyspace, String tableName) {
        assert keyspace.charAt(0) == '"';
        assert tableName.charAt(0) == '"';
        KeyspaceMetadata ksMetadata = m_cluster.getMetadata().getKeyspace(keyspace);
        TableMetadata tableMetadata = ksMetadata.getTable(tableName);
        ColumnMetadata colMetadata = tableMetadata.getColumn("value");
        return colMetadata.getType().equals(DataType.blob());
    }   // columnValueIsBinary

    //----- Public DBService methods: Schema management
    
    @Override
    public Map<String, Map<String, String>> getAllAppProperties() {
        checkState();
        refreshAppKeyspaceMap();
        Map<String, Map<String, String>> result = new HashMap<>();
        synchronized (m_appKeyspaceMap) {
            for (String appName : m_appKeyspaceMap.keySet()) {
                Map<String, String> appProps = getAppProperties(appName);
                if (appProps != null) { // watch for just-deleted apps
                    result.put(appName, appProps);
                }
            }
        }
        return result;
    }   // getAllAppProperties

    @Override
    public Map<String, String> getAppProperties(String appName) {
        checkState();
        String cqlKeyspace = getKeyspaceForApp(appName);
        if (cqlKeyspace == null) {
            return null;
        }
        return getAppProperties(cqlKeyspace, appName);
    }	// getAppProperties

    //----- Public DBService methods: Updates

    @Override
    public DBTransaction startTransaction(String appName) {
        checkState();
        return new CQLTransaction(appName);
    }

    @Override
    public void commit(DBTransaction dbTran) {
        checkState();
        ((CQLTransaction)dbTran).commit();
    }   // commit

    //----- Public DBService methods: Queries

    @Override
    public Iterator<DColumn> getAllColumns(String appName, String storeName, String rowKey) {
        checkState();
        String keyspace = getKeyspaceForApp(appName);
        String tableName = storeToCQLName(storeName);
        return new CQLColumnIterator(executeQuery(Query.SELECT_1_ROW_ALL_COLUMNS, keyspace, tableName, rowKey));
    }

    @Override
    public Iterator<DColumn> getColumnSlice(String appName, String storeName, String rowKey,
                                            String startCol, String  endCol, boolean reversed) {
        checkState();
        String keyspace = getKeyspaceForApp(appName);
        String tableName = storeToCQLName(storeName);
        ResultSet rs = null;
        if (reversed) {
            // Swap start/end columns for CQL reversed queries
            rs = executeQuery(Query.SELECT_1_ROW_COLUMN_RANGE_DESC, keyspace, tableName, rowKey, endCol, startCol);
        } else {
            rs = executeQuery(Query.SELECT_1_ROW_COLUMN_RANGE, keyspace, tableName, rowKey, startCol, endCol);
        }
        return new CQLColumnIterator(rs);
    }

    @Override
    public Iterator<DColumn> getColumnSlice(String appName, String storeName, String rowKey,
                                            String startCol, String endCol) {
        checkState();
        String keyspace = getKeyspaceForApp(appName);
        String tableName = storeToCQLName(storeName);
        return new CQLColumnIterator(executeQuery(Query.SELECT_1_ROW_COLUMN_RANGE,
                                                  keyspace,
                                                  tableName,
                                                  rowKey,
                                                  startCol,
                                                  endCol));
    }

    @Override
    public Iterator<DRow> getAllRowsAllColumns(String appName, String storeName) {
        checkState();
        String keyspace = getKeyspaceForApp(appName);
        String tableName = storeToCQLName(storeName);
        return new CQLRowIterator(executeQuery(Query.SELECT_ALL_ROWS_ALL_COLUMNS, keyspace, tableName));
    }

    @Override
    public DColumn getColumn(String appName, String storeName, String rowKey, String colName) {
        checkState();
        String keyspace = getKeyspaceForApp(appName);
        String tableName = storeToCQLName(storeName);
        CQLColumnIterator colIter =
            new CQLColumnIterator(executeQuery(Query.SELECT_1_ROW_1_COLUMN, keyspace, tableName, rowKey, colName));
        if (!colIter.hasNext()) {
            return null;
        }
        return colIter.next();
    }   // getColumn

    @Override
    public Iterator<DRow> getRowsAllColumns(String appName, String storeName, Collection<String> rowKeys) {
        checkState();
        String keyspace = getKeyspaceForApp(appName);
        String tableName = storeToCQLName(storeName);
        return new CQLRowIterator(executeQuery(Query.SELECT_ROW_SET_ALL_COLUMNS,
                                               keyspace,
                                               tableName,
                                               new ArrayList<String>(rowKeys)));
    }   // getRowsAllColumns

    @Override
    public Iterator<DRow> getRowsColumns(String             appName,
                                         String             storeName,
                                         Collection<String> rowKeys,
                                         Collection<String> colNames) {
        checkState();
        String keyspace = getKeyspaceForApp(appName);
        String tableName = storeToCQLName(storeName);
        return new CQLRowIterator(executeQuery(Query.SELECT_ROW_SET_COLUMN_SET,
                                               keyspace,
                                               tableName,
                                               new ArrayList<String>(rowKeys),
                                               new ArrayList<String>(colNames)));
    }   // getRowsColumns

    @Override
    public Iterator<DRow> getRowsColumnSlice(String             appName,
                                             String             storeName,
                                             Collection<String> rowKeys,
                                             String             startCol,
                                             String             endCol) {
        checkState();
        String keyspace = getKeyspaceForApp(appName);
        String tableName = storeToCQLName(storeName);
        return new CQLRowIterator(executeQuery(Query.SELECT_ROW_SET_COLUMN_RANGE,
                                               keyspace,
                                               tableName,
                                               new ArrayList<String>(rowKeys),
                                               startCol,
                                               endCol));
    }   // getRowsColumnSlice

    //----- Public methods: Task queries
    
    public Iterator<DRow> getAllTaskRows(String keyspace) {
        checkState();
        String cqlKeyspace = storeToCQLName(keyspace);
        String tableName = storeToCQLName(TASKS_TABLE_NAME);
        return new CQLRowIterator(executeQuery(Query.SELECT_ALL_ROWS_ALL_COLUMNS, cqlKeyspace, tableName));
    }   // getAllTaskRow


    //----- CQLService-specific public methods

    /**
     * Get the {@link PreparedStatement} for the given {@link CQLStatementCache.Query} to
     * the given table name, residing in the given keyspace. If needed, the query
     * statement is compiled and cached.
     * 
     * @param keyspace      Keyspace name.
     * @param query         Query statement type.
     * @param tableName     Quoted table name.
     * @return              PreparedStatement for requested keyspace/table/update.
     */
    public PreparedStatement getPreparedQuery(String keyspace, Query query, String tableName) {
        String cqlKeysapce = storeToCQLName(keyspace);
        CQLStatementCache statementCache = m_ksStatementCacheMap.get(cqlKeysapce);
        assert statementCache != null;
        return statementCache.getPreparedQuery(query, tableName);
    }   // getPreparedQuery
    
    /**
     * Get the {@link PreparedStatement} for the given {@link CQLStatementCache.Update} to
     * the given table name, residing in the given keyspace. If needed, the update
     * statement is compiled and cached.
     * 
     * @param keyspace      Keyspace name.
     * @param update        Update statement type.
     * @param tableName     Quoted table name.
     * @return              PreparedStatement for requested keyspace/table/update.
     */
	public PreparedStatement getPreparedUpdate(String keyspace, Update update, String tableName) {
        String cqlKeyspace = storeToCQLName(keyspace);
	    CQLStatementCache statementCache = m_ksStatementCacheMap.get(cqlKeyspace);
	    assert statementCache != null;
	    return statementCache.getPreparedUpdate(update, tableName);
	}  // getPreparedUpdate
	
	/**
	 * Get the CQL session being used by this CQL service.
	 * 
	 * @param keyspace Keyspace to which session applies.
	 * @return         The CQL Session object connected to the appropriate keyspace.
	 */
	public Session getSession(String keyspace) {
		return getOrCreateKeyspaceSession(keyspace);
	}	// getSession
	
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

    // Rebuild the m_appKeyspaceMap from all current keyspaces/Applications CFs
    private void refreshAppKeyspaceMap() {
        List<KeyspaceMetadata> ksMetadataList = m_cluster.getMetadata().getKeyspaces();
        synchronized (m_appKeyspaceMap) {
            m_appKeyspaceMap.clear();
            for (KeyspaceMetadata ksMetadata : ksMetadataList) {
                String keyspace = storeToCQLName(ksMetadata.getName());
                TableMetadata tableMeta = ksMetadata.getTable(APPS_TABLE_NAME);
                if (!keyspace.startsWith("system") && tableMeta != null) {
                    Map<String, Map<String, String>> allAppProps = getAllAppProperties(keyspace);
                    for (String appName : allAppProps.keySet()) {
                        String existingKeyspace = m_appKeyspaceMap.get(appName);
                        if (existingKeyspace == null) {
                            m_appKeyspaceMap.put(appName, keyspace);
                        } else {
                            m_logger.warn("Application {} found in multiple keyspaces: " +
                                          "instance in keyspace {} will be used; " +
                                          "instance in keyspace {} will be ignored.",
                                          new Object[]{appName, existingKeyspace, keyspace});
                        }
                    }
                }
            }
        }
    }   // refreshAppKeyspaceMap

    // Return true if the given table exists in the given keyspace.
    private boolean storeExists(String keyspace, String tableName) {
        KeyspaceMetadata ksMetadata = m_cluster.getMetadata().getKeyspace(keyspace);
        return (ksMetadata != null) && (ksMetadata.getTable(tableName) != null);
    }   // storeExists

    // Get the properties of all applications defined in the Applications CF in the given keyspace.
    // Return empty map if none found.
    private Map<String, Map<String, String>> getAllAppProperties(String cqlKeyspace) {
        Map<String, Map<String, String>> result = new HashMap<>();
        ResultSet rs = executeQuery(Query.SELECT_ALL_ROWS_ALL_COLUMNS, cqlKeyspace, APPS_TABLE_NAME);
        CQLRowIterator rowIter = new CQLRowIterator(rs);
        while (rowIter.hasNext()) {
            DRow row = rowIter.next();
            String appName = row.getKey();
            result.put(appName, getAllRowColumns(row));
        }
        return result;
    }   // getAppProperties

    // Get all properties for the given application defined in the given keyspace.
    // Return null if not found.
    private Map<String, String> getAppProperties(String cqlKeyspace, String appName) {
        ResultSet rs = executeQuery(Query.SELECT_1_ROW_ALL_COLUMNS, cqlKeyspace, APPS_TABLE_NAME, appName);
        CQLRowIterator rowIter = new CQLRowIterator(rs);
        if (!rowIter.hasNext()) {
            return null;
        }
        DRow row = rowIter.next();
        return getAllRowColumns(row);
    }   // getAppProperties
    
    // Get all column values of the given row as name/value string pairs.
    private Map<String, String> getAllRowColumns(DRow row) {
        Map<String, String> rowProps = new HashMap<>();
        Iterator<DColumn> colIter = row.getColumns();
        while (colIter.hasNext()) {
            DColumn column = colIter.next();
            rowProps.put(column.getName(), column.getValue());
        }
        return rowProps;
    }   // getAllRowColumns
    
    // Execute the given query for the given table using the given values.
    private ResultSet executeQuery(Query query, String cqlKeyspace, String tableName, Object... values) {
        m_logger.debug("Executing statement {} on table {}/{}; total params={}",
                       new Object[]{query, cqlKeyspace, tableName, values.length});
        Session session = getOrCreateKeyspaceSession(cqlKeyspace);
        PreparedStatement prepState = getPreparedQuery(cqlKeyspace, query, tableName);
        BoundStatement boundState = prepState.bind(values);
        return session.execute(boundState);
    }   // executeQuery
    
    // Establish the CQL session
    private void initializeCluster() {
        while (true) {
            try {
                m_cluster = buildClusterSpecs();
                connectToCluster();
                break;
            } catch (Exception e) {
                m_cluster = null;
                m_logger.info("Database is not reachable: {}. Waiting to retry", e);
                try {
                    Thread.sleep(ServerConfig.getInstance().db_connect_retry_wait_millis);
                } catch (InterruptedException ex2) {
                    // ignore
                }
            }
        }
    }   // initializeCluster

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
        
        // compression
        builder.withCompression(Compression.SNAPPY);
        
        // TLS/SSL
        if (config.dbtls) {
            builder.withSSL(getSSLOptions());
        }
        
        return builder.build();
    }   // buildClusterSpecs

    // Build SSLOptions from SSL/TLS configuration options. 
    private SSLOptions getSSLOptions() {
        ServerConfig config = ServerConfig.getInstance();
        SSLContext sslContext = null;
        try {
            sslContext = getSSLContext(config.truststore,
                                       config.truststorepassword,
                                       config.keystore,
                                       config.keystorepassword);
        } catch (Exception e) {
            throw new RuntimeException("Unable to build SSLContext", e);
        }
        List<String> cipherSuites = config.dbtls_cipher_suites;
        if (cipherSuites == null) {
            cipherSuites = new ArrayList<>();
        }
        return new SSLOptions(sslContext, cipherSuites.toArray(new String[]{}));
    }   // getSSLOptions
    
    // Build an SSLContext from the given truststore and keystore parameters.
    private SSLContext getSSLContext(String truststorePath,
                                     String truststorePassword,
                                     String keystorePath, 
                                     String keystorePassword) throws Exception {

        FileInputStream tsf = new FileInputStream(truststorePath);
        KeyStore ts = KeyStore.getInstance("JKS");
        ts.load(tsf, truststorePassword.toCharArray());
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(ts);

        FileInputStream ksf = new FileInputStream(keystorePath);
        KeyStore ks = KeyStore.getInstance("JKS");
        ks.load(ksf, keystorePassword.toCharArray());
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(ks, keystorePassword.toCharArray());

        SSLContext ctx = SSLContext.getInstance("SSL");
        ctx.init(kmf.getKeyManagers(), tmf.getTrustManagers(), new SecureRandom());
        return ctx;
    }   // getSSLContext
    
    // Attempt to connect to the given cluster and throw if it is unavailable.
    private void connectToCluster() {
        assert m_cluster != null;
        m_cluster.init();     // force connection and throw if unavailable
        displayClusterInfo();
    }   // connectToCluster

    // Get or create a session to the given keyspace.
    private Session getOrCreateKeyspaceSession(String keyspace) {
        String cqlKeyspace = storeToCQLName(keyspace);
        Session session = m_ksSessionMap.get(cqlKeyspace);
        if (session == null) {
            synchronized (m_ksSessionMap) {
                // Watch for race conditions outside of synchronized block
                session = m_ksSessionMap.get(cqlKeyspace);
                if (session == null) {
                    session = m_cluster.connect(cqlKeyspace);
                    m_ksSessionMap.put(cqlKeyspace, session);
                    m_ksStatementCacheMap.put(cqlKeyspace, new CQLStatementCache(session));
                }
            }
        }
        return session;
    }   // getOrCreateKeyspaceSession
    
    // Display configuration information for the given cluster.
    private void displayClusterInfo() {
        Metadata metadata = m_cluster.getMetadata();
        m_logger.info("Connected to cluster with topography:");
        RoundRobinPolicy policy = new RoundRobinPolicy();
        for (Host host : metadata.getAllHosts()) {
            m_logger.info("   Host {}: datacenter: {}, rack: {}, distance: {}",
                          new Object[]{host.getAddress(), host.getDatacenter(), 
                host.getRack(), policy.distance(host)});
        }
        m_logger.info("Current keyspaces:");
        List<KeyspaceMetadata> keyspaces = metadata.getKeyspaces();
        if (keyspaces.isEmpty()) {
            m_logger.info("   <none>");
        }
        for (KeyspaceMetadata keyspace : keyspaces) {
            Collection<TableMetadata> tables = keyspace.getTables();
            m_logger.info("   {}: contains {} ColumnFamilies", keyspace.getName(), tables.size());
        }
    }   // displayClusterInfo

}   // class CQLService
