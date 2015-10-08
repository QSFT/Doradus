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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ProtocolOptions.Compression;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.SSLOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.dell.doradus.common.Utils;
import com.dell.doradus.service.db.CassandraService;
import com.dell.doradus.service.db.DBService;
import com.dell.doradus.service.db.DBTransaction;
import com.dell.doradus.service.db.DColumn;
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
public class CQLService extends CassandraService {
    // Quoted version of Applications CF name:
    private static final String APPS_CQL_NAME = "\"" + SchemaService.APPS_STORE_NAME + "\"";
    
    public CQLService(Tenant tenant) {
        super(tenant);
        m_statementCache = new CQLStatementCache(tenant);
        if (Utils.isEmpty(tenant.getNamespace())) {
            m_keyspace = storeToCQLName(tenant.getName());
        } else {
            m_keyspace = storeToCQLName(tenant.getNamespace());
        }
        m_cluster = buildClusterSpecs();
        connectToCluster();
    }

    // Members:
    private Cluster m_cluster;
    private Session m_session;
    private final CQLStatementCache m_statementCache;
    private final String m_keyspace;
    
    //----- Public Service methods

    @Override
    protected void stopService() {
        if (m_session != null) {
            m_session.close();
        }
    }   // stopService

    //----- Public DBService methods: Namespace management

    @Override
    public void createNamespace() {
        KeyspaceMetadata ksMetadata = m_cluster.getMetadata().getKeyspace(m_keyspace);
        if (ksMetadata == null) {
            Map<String, String> options = getKeyspaceOptions(getTenant());
            new CQLSchemaManager(this).createKeyspace(options);
        }
    }
    
    private Map<String, String> getKeyspaceOptions(Tenant tenant) {
        // TODO: Get keyspace options from tenant definiton; inherit from default tenant if needed.
        return null;
    }

    @Override
    public void dropNamespace() {
        m_statementCache.clear();
        new CQLSchemaManager(this).dropKeyspace();
    }
    
    public List<String> getDoradusKeyspaces() {
        List<String> keyspaces = new ArrayList<>();
        List<KeyspaceMetadata> keyspaceList = m_cluster.getMetadata().getKeyspaces();
        for (KeyspaceMetadata ksMetadata : keyspaceList) {
            if (ksMetadata.getTable(APPS_CQL_NAME) != null) {
                keyspaces.add(ksMetadata.getName());
            }
        }
        return keyspaces;
    }

    //----- Public DBService methods: Store management

    @Override
    public void createStoreIfAbsent(String storeName, boolean bBinaryValues) {
        String tableName = storeToCQLName(storeName);
        if (!storeExists(tableName)) {
            new CQLSchemaManager(this).createCQLTable(storeName, bBinaryValues);
        }
    }   // createStoreIfAbsent
    
    @Override
    public void deleteStoreIfPresent(String storeName) {
        String tableName = storeToCQLName(storeName);
        if (storeExists(tableName)) {
            new CQLSchemaManager(this).dropCQLTable(tableName);
        }
    }   // deleteStoreIfPresent

    /**
     * Return true if column values for the given store name are binary.
     * 
     * @param storeName Store (ColumnFamily) name.
     * @return          True if the given table's column values are binary.
     */
    public boolean columnValueIsBinary(String storeName) {
        return !DBService.isSystemTable(storeName);
    }

    //----- Public DBService methods: Updates

    @Override
    public void commit(DBTransaction dbTran) {
        new CQLTransaction(this).commit(dbTran);
    }   // commit

    //----- Public DBService methods: Queries

    @Override
    public List<DColumn> getColumns(String storeName, String rowKey, String startColumn, String endColumn, int count) {
        String tableName = storeToCQLName(storeName);
        ResultSet rs = null;
        if (startColumn == null) {
            if (endColumn == null) {
                rs = executeQuery(Query.SELECT_1_ROW_ALL_COLUMNS, tableName, rowKey, new Integer(count));
            } else {
                rs = executeQuery(Query.SELECT_1_ROW_LOWER_COLUMNS, tableName, rowKey, endColumn, new Integer(count));
            }
        } else if (endColumn == null) {
            rs = executeQuery(Query.SELECT_1_ROW_UPPER_COLUMNS, tableName, rowKey, startColumn, new Integer(count));
        } else {
            rs = executeQuery(Query.SELECT_1_ROW_COLUMN_RANGE, tableName, rowKey, startColumn, endColumn, new Integer(count));
        }
        List<Row> list = rs.all();
        List<DColumn> result = new ArrayList<>(list.size());
        boolean isValueBinary = columnValueIsBinary(storeName);
        for(Row r: list) {
            DColumn cqlCol = null;
            if (isValueBinary) {
                cqlCol = new DColumn(r.getString("column1"), r.getBytes("value"));
            } else {
                cqlCol = new DColumn(r.getString("column1"), r.getString("value"));
            }
            
            result.add(cqlCol);
        }
        return result;
    }

    @Override
    public List<DColumn> getColumns(String storeName, String rowKey, Collection<String> columnNames) {
        String tableName = storeToCQLName(storeName);
        boolean isValueBinary = columnValueIsBinary(storeName);
        ResultSet rs = executeQuery(Query.SELECT_1_ROW_COLUMN_SET,
                                    tableName,
                                    rowKey,
                                    new ArrayList<String>(columnNames));    // must be a List
        List<Row> list = rs.all();
        List<DColumn> result = new ArrayList<>(list.size());
        for(Row r: list) {
            DColumn cqlCol = null;
            if (isValueBinary) {
                cqlCol = new DColumn(r.getString("column1"), r.getBytes("value"));
            } else {
                cqlCol = new DColumn(r.getString("column1"), r.getString("value"));
            }
            
            result.add(cqlCol);
        }
        return result;
    }

    @Override
    public List<String> getRows(String storeName, String continuationToken, int count) {
        String tableName = storeToCQLName(storeName);
        Set<String> rows = new HashSet<String>();
        //unfortunately I don't know how to get one record per row in CQL so we'll read everything
        //and find out the rows
        ResultSet rs = executeQuery(Query.SELECT_ROWS_RANGE, tableName);
        while(true) {
            Row r = rs.one();
            if(r == null) break;
            String key = r.getString("key");
            if(continuationToken != null && continuationToken.compareTo(key) >= 0) {
                continue;
            }
            rows.add(key);
        }
        List<String> result = new ArrayList<>(rows);
        Collections.sort(result);
        if(result.size() > count) {
            result = new ArrayList<>(result.subList(0,  count));
        }
        return result;
    }
    
    
    //----- CQLService-specific public methods

    /**
     * Get the {@link PreparedStatement} for the given {@link CQLStatementCache.Query} to
     * the given table name. If needed, the query statement is compiled and cached.
     * 
     * @param query         Query statement type.
     * @param storeName     Store (ColumnFamily) name.
     * @return              PreparedStatement for requested table/query.
     */
    public PreparedStatement getPreparedQuery(Query query, String storeName) {
        String tableName = storeToCQLName(storeName);
        return m_statementCache.getPreparedQuery(tableName, query);
    }   // getPreparedQuery
    
	/**
	 * Get the {@link PreparedStatement} for the given {@link CQLStatementCache.Update} to
	 * the given table name. If needed, the update statement is compiled and cached.
	 * 
	 * @param update        Update statement type.
	 * @param storeName     Store (ColumnFamily) name.
	 * @return              PreparedStatement for requested table/update.
	 */
	public PreparedStatement getPreparedUpdate(Update update, String storeName) {
	    String tableName = storeToCQLName(storeName);
	    return m_statementCache.getPreparedUpdate(tableName, update);
	}  // getPreparedUpdate
	
    /**
     * Get the CQL session being used by this CQL service.
     * 
     * @return A CQL Session object.
     */
    public Session getSession() {
        return m_session;
    }   // getSession
    
    /**
     * Get the keyspace name used by this DBSevice's tenant. The name is quoted for use in
     * CQL statements.
     * 
     * @return  Quoted keyspace name used by this DBSevice's tenant.
     */
    public String getKeyspace() {
        return m_keyspace;
    }
    
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
    private boolean storeExists(String tableName) {
        KeyspaceMetadata ksMetadata = m_cluster.getMetadata().getKeyspace(m_keyspace);
        return (ksMetadata != null) && (ksMetadata.getTable(tableName) != null);
    }   // storeExists

    // Execute the given query for the given table using the given values.
    private ResultSet executeQuery(Query query, String tableName, Object... values) {
        m_logger.debug("Executing statement {} on table {}.{}; total params={}",
                       new Object[]{query, m_keyspace, tableName, values.length});
        try {
            PreparedStatement prepState = getPreparedQuery(query, tableName);
            BoundStatement boundState = prepState.bind(values);
            return m_session.execute(boundState);
        } catch (Exception e) {
            String params = "[" + Utils.concatenate(Arrays.asList(values), ",") + "]";
            m_logger.error("Query failed: query={}, keyspace={}, table={}, params={}; error: {}",
                           query, m_keyspace, tableName, params, e);
            throw e;
        }
    }   // executeQuery
    
    // Build Cluster object from ServerConfig settings.
    private Cluster buildClusterSpecs() {
        Cluster.Builder builder = Cluster.builder();
        
        // dbhost
        String dbhost = getParamString("dbhost");
        String[] nodeAddresses = dbhost.split(",");
        for (String address : nodeAddresses) {
            builder.addContactPoint(address);
        }
        
        // dbport
        builder.withPort(getParamInt("dbport", 9042));
        
        // db_timeout_millis and db_connect_retry_wait_millis
        SocketOptions socketOpts = new SocketOptions();
        socketOpts.setReadTimeoutMillis(getParamInt("db_timeout_millis", 10000));
        socketOpts.setConnectTimeoutMillis(getParamInt("db_connect_retry_wait_millis", 5000));
        builder.withSocketOptions(socketOpts);
        
        // dbuser/dbpassword
        String dbuser = getParamString("dbuser");
        if (!Utils.isEmpty(dbuser)) {
            builder.withCredentials(dbuser, getParamString("dbpassword"));
        }
        
        // compression
        builder.withCompression(Compression.SNAPPY);
        
        // TLS/SSL
        if (getParamBoolean("dbtls")) {
            builder.withSSL(getSSLOptions());
        }
        
        return builder.build();
    }   // buildClusterSpecs

    // Build SSLOptions from SSL/TLS configuration options. 
    private SSLOptions getSSLOptions() {
        SSLContext sslContext = null;
        try {
            sslContext = getSSLContext(getParamString("truststore"),
                                       getParamString("truststorepassword"),
                                       getParamString("keystore"),
                                       getParamString("keystorepassword"));
        } catch (Exception e) {
            throw new RuntimeException("Unable to build SSLContext", e);
        }
        List<String> cipherSuites = getParamList("dbtls_cipher_suites");
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
        m_session = m_cluster.connect();
        displayClusterInfo();
    }   // connectToCluster

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
        m_logger.info("Database contains {} keyspaces", metadata.getKeyspaces().size());
    }   // displayClusterInfo

}   // class CQLService
