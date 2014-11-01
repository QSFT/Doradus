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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

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
import com.dell.doradus.service.db.StoreTemplate;
import com.dell.doradus.service.db.cql.CQLStatementCache.Query;

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
        checkState();
        String tableName = storeToCQLName(storeTemplate.getName());
        if (!storeExists(tableName)) {
            m_schemaMgr.createCQLTable(storeTemplate);
        }
    }   // createStoreIfAbsent
    
    @Override
    public void deleteStoreIfPresent(String storeName) {
        checkState();
        m_schemaMgr.dropCQLTable(storeToCQLName(storeName));
    }   // deleteStoreIfPresent

    @Override
    public boolean storeExists(String storeName) {
        checkState();
        String tableName = storeToCQLName(storeName);
        return m_schemaMgr.tableExists(tableName);
    }   // storeExists

    //----- Public DBService methods: Schema management
    
    @Override
    public Map<String, Map<String, String>> getAllAppProperties() {
        checkState();
        Map<String, Map<String, String>> result = new HashMap<>();
        ResultSet rs = executeQuery(Query.SELECT_ALL_ROWS_ALL_COLUMNS, APPS_TABLE_NAME);
        CQLRowIterator rowIter = new CQLRowIterator(rs);
        while (rowIter.hasNext()) {
        	DRow row = rowIter.next();
            String appName = row.getKey();
            if (appName.charAt(0) == '_') {
                continue;
            }
            Map<String, String> appProps = new HashMap<>();
            result.put(appName, appProps);
            Iterator<DColumn> colIter = row.getColumns();
            while (colIter.hasNext()) {
            	DColumn column = colIter.next();
            	appProps.put(column.getName(), column.getValue());
            }
        }
        return result;
    }   // getAllAppProperties

    @Override
    public Map<String, String> getAppProperties(String appName) {
        checkState();
        ResultSet rs = executeQuery(Query.SELECT_1_ROW_ALL_COLUMNS, APPS_TABLE_NAME, appName);
        CQLRowIterator rowIter = new CQLRowIterator(rs);
        if (!rowIter.hasNext()) {
        	return null;
        }
        Map<String, String> result = new HashMap<>();
    	DRow row = rowIter.next();
        Iterator<DColumn> colIter = row.getColumns();
        while (colIter.hasNext()) {
        	DColumn column = colIter.next();
        	result.put(column.getName(), column.getValue());
        }
        return result;
    }	// getAppProperties

    //----- Public DBService methods: Updates
    
    @Override
    public DBTransaction startTransaction() {
        checkState();
        return new CQLTransaction();
    }

    @Override
    public void commit(DBTransaction dbTran) {
        checkState();
        ((CQLTransaction)dbTran).commit();
    }   // commit

    //----- Public DBService methods: Queries

    @Override
    public Iterator<DColumn> getAllColumns(String storeName, String rowKey) {
        checkState();
        String tableName = storeToCQLName(storeName);
        return new CQLColumnIterator(executeQuery(Query.SELECT_1_ROW_ALL_COLUMNS, tableName, rowKey));
    }

    @Override
    public Iterator<DColumn> getColumnSlice(String  storeName,
                                            String  rowKey,
                                            String  startCol,
                                            String  endCol,
                                            boolean reversed) {
        checkState();
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
        checkState();
        String tableName = storeToCQLName(storeName);
        return new CQLColumnIterator(executeQuery(Query.SELECT_1_ROW_COLUMN_RANGE, tableName, rowKey, startCol, endCol));
    }

    @Override
    public Iterator<DRow> getAllRowsAllColumns(String storeName) {
        checkState();
        String tableName = storeToCQLName(storeName);
        return new CQLRowIterator(executeQuery(Query.SELECT_ALL_ROWS_ALL_COLUMNS, tableName));
    }

    @Override
    public DColumn getColumn(String storeName, String rowKey, String colName) {
        checkState();
        String tableName = storeToCQLName(storeName);
        CQLColumnIterator colIter = new CQLColumnIterator(executeQuery(Query.SELECT_1_ROW_1_COLUMN, tableName, rowKey, colName));
        if (!colIter.hasNext()) {
            return null;
        }
        return colIter.next();
    }   // getColumn

    @Override
    public Iterator<DRow> getRowsAllColumns(String storeName, Collection<String> rowKeys) {
        checkState();
        String tableName = storeToCQLName(storeName);
        return new CQLRowIterator(executeQuery(Query.SELECT_ROW_SET_ALL_COLUMNS,
                                               tableName,
                                               new ArrayList<String>(rowKeys)));
    }

    @Override
    public Iterator<DRow> getRowsColumns(String             storeName,
                                         Collection<String> rowKeys,
                                         Collection<String> colNames) {
        checkState();
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
        checkState();
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
        checkState();
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
     * Get the keyspace name being used by this CQL service in the form needed for CQL
     * methods. In this form, it is quoted to invoke case sensitivity.
     * 
     * @return	Quoted keyspace name used by this CQL service.
     */
    public String getKeyspace() {
    	return m_keyspace;
    }	// getKeyspace
    
    /**
     * Get the {@link CQLStatementCache} object used by this CQL Service to cache prepared
     * statements.
     * 
     * @return	Prepared statement query cache object.
     */
	public CQLStatementCache getQueryCache() {
		return m_queryCache;
	}	// getQueryCache
 
	/**
	 * Get the CQL session being used by this CQL service.
	 * 
	 * @return The CQL Session object connected to the appropriate keyspace.
	 */
	public Session getSession() {
		return m_session;
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
    
    // Execute the given query for the given table using the given values.
    private ResultSet executeQuery(Query query, String tableName, Object... values) {
        m_logger.debug("Executing statement {} on table {}; total params={}",
                       new Object[]{query, tableName, values.length});
        PreparedStatement prepState = m_queryCache.getPreparedQuery(query, tableName);
        BoundStatement boundState = prepState.bind(values);
        return m_session.execute(boundState);
    }   // executeQuery
    
    // Establish the CQL session
    private void initializeCQLSession() {
        while (true) {
            try {
                Cluster cluster = buildClusterSpecs();
                createKeyspaceSession(cluster);
                break;
            } catch (Exception e) {
                m_logger.info("Database is not reachable: {}. Waiting to retry", e);
                try {
                    Thread.sleep(ServerConfig.getInstance().db_connect_retry_wait_millis);
                } catch (InterruptedException ex2) {
                    // ignore
                }
            }
        }
    }   // initializeCQLSession

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
        
        // compression
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
        checkColumnFamilyCompatibility();
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
    
    // Check that all existing CFs have key and column1 types of text.
    private void checkColumnFamilyCompatibility() {
        Metadata metadata = m_session.getCluster().getMetadata();
        KeyspaceMetadata ksMetadata = metadata.getKeyspace(CQLService.instance().getKeyspace());
        for (TableMetadata tableMetadata : ksMetadata.getTables()) {
            String errMsg = null;
            ColumnMetadata colMetadata = tableMetadata.getColumn("key");
            if (colMetadata == null) {
                errMsg = "does not have a column named 'key'";
            }
            if (errMsg == null && !colMetadata.getType().equals(DataType.text())) {
                errMsg = "the column 'key' must have type 'text' but is '" +
                         colMetadata.getType().toString() + "'";
            }
            colMetadata = tableMetadata.getColumn("column1");
            if (errMsg == null && colMetadata == null) {
                errMsg = "does not have a column named 'column1'";
            }
            if (errMsg == null && !colMetadata.getType().equals(DataType.text())) {
                errMsg = "the column 'column1' must have type 'text' but is '" +
                         colMetadata.getType().toString() + "'";
            }
            colMetadata = tableMetadata.getColumn("value");
            if (errMsg == null && colMetadata == null) {
                errMsg = "does not have a column named 'value'";
            }
            if (errMsg != null) {
                // Throw an Error to force server shutdown.
                String throwMsg =
                    String.format("This database is not compatible with the CQL API. " +
                                  "ColumnFamily '%s': %s. Please create a new database " +
                                  "or use the Thrift API.",
                                  tableMetadata.getName(), errMsg);
                throw new Error(throwMsg);
            }
        }
    }   // checkColumnFamilyCompatibility
    
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
    
    // Display configuration information for the given cluster.
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
