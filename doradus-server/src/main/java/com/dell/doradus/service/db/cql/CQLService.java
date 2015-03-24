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
import com.dell.doradus.common.Utils;
import com.dell.doradus.core.ServerConfig;
import com.dell.doradus.service.db.DBService;
import com.dell.doradus.service.db.DBTransaction;
import com.dell.doradus.service.db.DColumn;
import com.dell.doradus.service.db.DRow;
import com.dell.doradus.service.db.Tenant;
import com.dell.doradus.service.db.cql.CQLStatementCache.Query;
import com.dell.doradus.service.db.cql.CQLStatementCache.Update;
import com.dell.doradus.service.schema.SchemaService;

/**
 * Implements the DBService interface using the CQL API to communicate with Cassandra. A
 * note on keyspace and store names: Doradus uses mixed uppercase/lowercase names, which
 * must be quoted in CQL calls. Hence, public methods that accept a keyspace or store name
 * call {@link #storeToCQLName(String)} to add quotes, if needed. Private methods assume
 * this has already been done.
 */
public class CQLService extends DBService {
    // Quoted version of Applications CF name:
    private static final String APPS_CQL_NAME = "\"" + SchemaService.APPS_STORE_NAME + "\"";
    
    // Singleton instance:
    private static final CQLService INSTANCE = new CQLService();

    // Construction via singleton only
    private CQLService() { }

    // Members:
    private Cluster m_cluster;
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

    //----- Public DBService methods: Tenant management
    
    @Override
    public void createTenant(Tenant tenant, Map<String, String> options) {
        checkState();
        String cqlKeyspace = storeToCQLName(tenant.getKeyspace());
        KeyspaceMetadata ksMetadata = m_cluster.getMetadata().getKeyspace(cqlKeyspace);
        if (ksMetadata == null) {
            try (Session session = m_cluster.connect()) {   // no-keyspace session
                CQLSchemaManager schemaMgr = new CQLSchemaManager(session, null);
                schemaMgr.createKeyspace(cqlKeyspace, options);
            }
        }
    }   // createKeyspace

    @Override
    public void dropTenant(Tenant tenant) {
        checkState();
        String cqlKeyspace = storeToCQLName(tenant.getKeyspace());
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
    public void addUsers(Tenant tenant, Map<String, String> users) {
        checkState();
        String cqlKeyspace = storeToCQLName(tenant.getKeyspace());
        Session session = getOrCreateKeyspaceSession(cqlKeyspace);
        StringBuilder cql = new StringBuilder();
        for (String userid : users.keySet()) {
            m_logger.debug("Adding new user '{}' for keyspace {}", userid, cqlKeyspace);
            // CREATE USER Foo WITH PASSWORD 'bar' NOSUPERUSER;
            cql.setLength(0);
            String password = users.get(userid);
            cql.append("CREATE USER ");
            cql.append(userid);
            cql.append(" WITH PASSWORD '");
            cql.append(password);   // TODO: escape embedded 's?
            cql.append("' NOSUPERUSER;");
            session.execute(cql.toString());
            
            // GRANT ALL PERMISSIONS ON KEYSPACE "Casey1" TO Foo;
            cql.setLength(0);
            cql.append("GRANT ALL PERMISSIONS ON KEYSPACE ");
            cql.append(cqlKeyspace);
            cql.append(" TO ");
            cql.append(userid);
            cql.append(";");
            session.execute(cql.toString());
        }
    }   // addUsers
    
    @Override
    public Collection<Tenant> getTenants() {
        checkState();
        List<Tenant> tenants = new ArrayList<>();
        try (Session session = m_cluster.connect()) {
            List<KeyspaceMetadata> keyspaceList = m_cluster.getMetadata().getKeyspaces();
            for (KeyspaceMetadata ksMetadata : keyspaceList) {
                if (ksMetadata.getTable(APPS_CQL_NAME) != null) {
                    tenants.add(new Tenant(ksMetadata.getName()));
                }
            }
        }
        return tenants;
    }   // getTenantMap
    
    //----- Public DBService methods: Store management

    @Override
    public void createStoreIfAbsent(Tenant tenant, String storeName, boolean bBinaryValues) {
        checkState();
        String cqlKeyspace = storeToCQLName(tenant.getKeyspace());
        String tableName = storeToCQLName(storeName);
        if (!storeExists(cqlKeyspace, tableName)) {
            Session session = getOrCreateKeyspaceSession(cqlKeyspace);
            CQLSchemaManager schemaMgr = new CQLSchemaManager(session, cqlKeyspace);
            schemaMgr.createCQLTable(storeName, bBinaryValues);
        }
    }   // createStoreIfAbsent
    
    @Override
    public void deleteStoreIfPresent(Tenant tenant, String storeName) {
        checkState();
        String cqlKeyspace = storeToCQLName(tenant.getKeyspace());
        String tableName = storeToCQLName(storeName);
        if (storeExists(cqlKeyspace, tableName)) {
            Session session = getOrCreateKeyspaceSession(cqlKeyspace);
            CQLSchemaManager schemaMgr = new CQLSchemaManager(session, cqlKeyspace);
            schemaMgr.dropCQLTable(tableName);
        }
    }   // deleteStoreIfPresent

    /**
     * Return true if column values for the given keyspace/table name are binary.
     * 
     * @param keyspace  Keyspace name.
     * @param storeName Store (ColumnFamily) name.
     * @return          True if the given table's column values are binary.
     */
    public boolean columnValueIsBinary(String keyspace, String storeName) {
        String cqlKeyspace = storeToCQLName(keyspace);
        String tableName = storeToCQLName(storeName);
        KeyspaceMetadata ksMetadata = m_cluster.getMetadata().getKeyspace(cqlKeyspace);
        TableMetadata tableMetadata = ksMetadata.getTable(tableName);
        ColumnMetadata colMetadata = tableMetadata.getColumn("value");
        return colMetadata.getType().equals(DataType.blob());
    }   // columnValueIsBinary

    //----- Public DBService methods: Updates

    @Override
    public DBTransaction startTransaction(Tenant tenant) {
        checkState();
        return new CQLTransaction(tenant);
    }

    @Override
    public void commit(DBTransaction dbTran) {
        checkState();
        ((CQLTransaction)dbTran).commit();
    }   // commit

    //----- Public DBService methods: Queries

    @Override
    public Iterator<DColumn> getAllColumns(Tenant tenant, String storeName, String rowKey) {
        checkState();
        String keyspace = storeToCQLName(tenant.getKeyspace());
        String tableName = storeToCQLName(storeName);
        return new CQLColumnIterator(executeQuery(Query.SELECT_1_ROW_ALL_COLUMNS, keyspace, tableName, rowKey));
    }

    @Override
    public Iterator<DColumn> getColumnSlice(Tenant tenant, String storeName, String rowKey,
                                            String startCol, String  endCol, boolean reversed) {
        checkState();
        String keyspace = storeToCQLName(tenant.getKeyspace());
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
    public Iterator<DColumn> getColumnSlice(Tenant tenant, String storeName, String rowKey,
                                            String startCol, String endCol) {
        checkState();
        String keyspace = storeToCQLName(tenant.getKeyspace());
        String tableName = storeToCQLName(storeName);
        return new CQLColumnIterator(executeQuery(Query.SELECT_1_ROW_COLUMN_RANGE,
                                                  keyspace,
                                                  tableName,
                                                  rowKey,
                                                  startCol,
                                                  endCol));
    }

    @Override
    public Iterator<DRow> getAllRowsAllColumns(Tenant tenant, String storeName) {
        checkState();
        String keyspace = storeToCQLName(tenant.getKeyspace());
        String tableName = storeToCQLName(storeName);
        return new CQLRowIterator(executeQuery(Query.SELECT_ALL_ROWS_ALL_COLUMNS, keyspace, tableName));
    }

    @Override
    public DColumn getColumn(Tenant tenant, String storeName, String rowKey, String colName) {
        checkState();
        String keyspace = storeToCQLName(tenant.getKeyspace());
        String tableName = storeToCQLName(storeName);
        CQLColumnIterator colIter =
            new CQLColumnIterator(executeQuery(Query.SELECT_1_ROW_1_COLUMN, keyspace, tableName, rowKey, colName));
        if (!colIter.hasNext()) {
            return null;
        }
        return colIter.next();
    }   // getColumn

    @Override
    public Iterator<DRow> getRowsAllColumns(Tenant tenant, String storeName, Collection<String> rowKeys) {
        checkState();
        String keyspace = storeToCQLName(tenant.getKeyspace());
        String tableName = storeToCQLName(storeName);
        return new CQLRowIterator(executeQuery(Query.SELECT_ROW_SET_ALL_COLUMNS,
                                               keyspace,
                                               tableName,
                                               new ArrayList<String>(rowKeys)));
    }   // getRowsAllColumns

    @Override
    public Iterator<DRow> getRowsColumns(Tenant             tenant,
                                         String             storeName,
                                         Collection<String> rowKeys,
                                         Collection<String> colNames) {
        checkState();
        String keyspace = storeToCQLName(tenant.getKeyspace());
        String tableName = storeToCQLName(storeName);
        return new CQLRowIterator(executeQuery(Query.SELECT_ROW_SET_COLUMN_SET,
                                               keyspace,
                                               tableName,
                                               new ArrayList<String>(rowKeys),
                                               new ArrayList<String>(colNames)));
    }   // getRowsColumns

    @Override
    public Iterator<DRow> getRowsColumnSlice(Tenant             tenant,
                                             String             storeName,
                                             Collection<String> rowKeys,
                                             String             startCol,
                                             String             endCol) {
        checkState();
        String keyspace = storeToCQLName(tenant.getKeyspace());
        String tableName = storeToCQLName(storeName);
        return new CQLRowIterator(executeQuery(Query.SELECT_ROW_SET_COLUMN_RANGE,
                                               keyspace,
                                               tableName,
                                               new ArrayList<String>(rowKeys),
                                               startCol,
                                               endCol));
    }   // getRowsColumnSlice

    //----- CQLService-specific public methods

    /**
     * Get the {@link PreparedStatement} for the given {@link CQLStatementCache.Query} to
     * the given table name, residing in the given keyspace. If needed, the query
     * statement is compiled and cached.
     * 
     * @param keyspace      Keyspace name.
     * @param query         Query statement type.
     * @param storeName     Store (ColumnFamily) name.
     * @return              PreparedStatement for requested keyspace/table/update.
     */
    public PreparedStatement getPreparedQuery(String keyspace, Query query, String storeName) {
        String cqlKeysapce = storeToCQLName(keyspace);
        String tableName = storeToCQLName(storeName);
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
     * @param storeName     Store (ColumnFamily) name.
     * @return              PreparedStatement for requested keyspace/table/update.
     */
	public PreparedStatement getPreparedUpdate(String keyspace, Update update, String storeName) {
        String cqlKeyspace = storeToCQLName(keyspace);
        String tableName = storeToCQLName(storeName);
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
        String cqlKeyspace = storeToCQLName(keyspace);
		return getOrCreateKeyspaceSession(cqlKeyspace);
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

    // Return true if the given table exists in the given keyspace.
    private boolean storeExists(String cqlKeyspace, String tableName) {
        assert cqlKeyspace.charAt(0) == '"';
        KeyspaceMetadata ksMetadata = m_cluster.getMetadata().getKeyspace(cqlKeyspace);
        return (ksMetadata != null) && (ksMetadata.getTable(tableName) != null);
    }   // storeExists

    // Execute the given query for the given table using the given values.
    private ResultSet executeQuery(Query query, String cqlKeyspace, String tableName, Object... values) {
        assert cqlKeyspace.charAt(0) == '"';
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
        
        // dbuser/dbpassword
        if (!Utils.isEmpty(config.dbuser)) {
            builder.withCredentials(config.dbuser, config.dbpassword);
        }
        
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
    private Session getOrCreateKeyspaceSession(String cqlKeyspace) {
        assert cqlKeyspace.charAt(0) == '"';
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
