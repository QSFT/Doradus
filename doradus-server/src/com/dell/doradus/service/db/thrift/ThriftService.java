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

package com.dell.doradus.service.db.thrift;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import com.dell.doradus.common.Utils;
import com.dell.doradus.core.ServerConfig;
import com.dell.doradus.service.db.DBNotAvailableException;
import com.dell.doradus.service.db.DBService;
import com.dell.doradus.service.db.DBTransaction;
import com.dell.doradus.service.db.DColumn;
import com.dell.doradus.service.db.DRow;

public class ThriftService extends DBService {
    private static final ThriftService INSTANCE = new ThriftService();

    // Known app names to keyspace map:
    private final Map<String, String> m_appKeyspaceMap = new HashMap<String, String>();

    // Keyspace names to DBConn queue:
    private final Map<String, Queue<DBConn>> m_dbKeyspaceDBConns = new HashMap<>();
    
    private ThriftService() { }

    //----- Public Service methods

    /**
     * Return the singleton ThriftService service object.
     * 
     * @return  Static ThriftService object.
     */
    public static ThriftService instance() {return INSTANCE;}
    
    @Override
    public void initService() {
        ServerConfig config = ServerConfig.getInstance();
        m_logger.info("Using Thrift API");
        m_logger.debug("Cassandra host list: {}", Arrays.toString(config.dbhost.split(",")));
        m_logger.debug("Cassandra port: {}", config.dbport);
        m_logger.debug("Default application keyspace: {}", config.keyspace);
    }   // initService

    @Override
    public void startService() {
        initializeDBConnections();
    }   // stopService

    @Override
    public void stopService() {
        purgeAllConnections();
    }   // stopService

    //----- Public DBService methods: Store management

    @Override
    public void createKeyspace(String keyspace) {
        checkState();
        // Use a temporary, no-keyspace session
        try (DBConn dbConn = new DBConn(null)) {
            CassandraSchemaMgr schemaMgr = new CassandraSchemaMgr(dbConn.getClientSession());
            if (!schemaMgr.keyspaceExists(keyspace)) {
                schemaMgr.createKeyspace(keyspace);
            }
        }
    }   // createKeyspace

    @Override
    public void dropKeyspace(String keyspace) {
        checkState();
        // Use a temporary, no-keyspace session
        try (DBConn dbConn = new DBConn(null)) {
            CassandraSchemaMgr schemaMgr = new CassandraSchemaMgr(dbConn.getClientSession());
            if (!schemaMgr.keyspaceExists(keyspace)) {
                schemaMgr.dropKeyspace(keyspace);
            }
        }
    }   // dropKeyspace
    
    @Override
    public String getKeyspaceForApp(String appName) {
        checkState();
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
        DBConn dbConn = getDBConnection(keyspace);
        try {   
            CassandraSchemaMgr schemaMgr = new CassandraSchemaMgr(dbConn.getClientSession());
            if (!schemaMgr.columnFamilyExists(keyspace, storeName)) {
                schemaMgr.createColumnFamily(keyspace, storeName, bBinaryValues);
            }
        } finally {
            returnDBConnection(dbConn);
        }
    }   // createStoreIfAbsent
    
    @Override
    public void deleteStoreIfPresent(String keyspace, String storeName) {
        checkState();
        DBConn dbConn = getDBConnection(keyspace);
        try {
            CassandraSchemaMgr schemaMgr = new CassandraSchemaMgr(dbConn.getClientSession());
            if (schemaMgr.columnFamilyExists(keyspace, storeName)) {
                schemaMgr.deleteColumnFamily(storeName);
            }
        } finally {
            returnDBConnection(dbConn);
        }
    }   // deleteStoreIfPresent
    
    @Override
    public void registerApplication(String keyspace, String appName) {
        checkState();
        synchronized (m_appKeyspaceMap) {
            m_appKeyspaceMap.put(appName, keyspace);
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
    
    //----- Public DBService methods: Schema management
    
    /**
     * Get all properties of all registered applications. The result is a map of
     * application names to a map of application properties. Example application properties
     * are _application, _version, and _format, which define the application's schema
     * and the way it is stored in the database. The result is empty if there are no
     * applications defined.
     * 
     * @return  Map of application name -> property name -> property value for all
     *          known applications. Empty of there are no applications defined.
     */
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

    /**
     * Get all properties of the application with the given name. The property names are
     * typically _application, _version, and _format, which define the application's
     * schema and the way it is stored in the database. The result is null if the given
     * application is not defined. 
     * 
     * @param appName   Name of application whose properties to get.
     * @return          Map of application properties as key/value pairs or null if no
     *                  such application is defined.
     */
    @Override
    public Map<String, String> getAppProperties(String appName) {
        checkState();
        String keyspace = getKeyspaceForApp(appName);
        if (keyspace == null) {
            return null;
        }
        DBConn dbConn = getDBConnection(keyspace);
        try {
            return dbConn.getAppProperties(appName);
        } finally {
            returnDBConnection(dbConn);
        }
    }   // getAppProperties
    
    //----- Public DBService methods: Updates
    
    @Override
    public DBTransaction startTransaction(String appName) {
        checkState();
        return new CassandraTransaction(appName);
    }   // startTransaction
    
    @Override
    public void commit(DBTransaction dbTran) {
        checkState();
        CassandraTransaction cassDBTran = (CassandraTransaction)dbTran;
        DBConn dbConn = getDBConnection(cassDBTran.getKeyspace());
        try {
            dbConn.commit(dbTran);
        } finally {
            returnDBConnection(dbConn);
        }
    }   // commit
    
    //----- Public DBService methods: Queries

    @Override
    public Iterator<DColumn> getAllColumns(String appName, String storeName, String rowKey) {
        checkState();
        String keyspace = getKeyspaceForApp(appName);
        DBConn dbConn = getDBConnection(keyspace);
        try {
            return dbConn.getAllColumns(storeName, rowKey);
        } finally {
            returnDBConnection(dbConn);
        }
    }   // getAllColumns

    @Override
    public Iterator<DColumn> getColumnSlice(String appName, String storeName, String rowKey,
                                            String startCol, String endCol, boolean reversed) {
        checkState();
        String keyspace = getKeyspaceForApp(appName);
        DBConn dbConn = getDBConnection(keyspace);
        try {
            return dbConn.getColumnSlice(storeName, rowKey, startCol, endCol, reversed);
        } finally {
            returnDBConnection(dbConn);
        }
    }   // getColumnSlice

    @Override
    public Iterator<DColumn> getColumnSlice(String appName, String storeName, String rowKey,
                                            String startCol, String endCol) {
        checkState();
        return getColumnSlice(appName, storeName, rowKey, startCol, endCol, false);
    }   // getColumnSlice

    @Override
    public Iterator<DRow> getAllRowsAllColumns(String appName, String storeName) {
        checkState();
        String keyspace = getKeyspaceForApp(appName);
        DBConn dbConn = getDBConnection(keyspace);
        try {
            return dbConn.getAllRowsAllColumns(storeName);
        } finally {
            returnDBConnection(dbConn);
        }
    }   // getAllRowsAllColumns

    @Override
    public DColumn getColumn(String appName, String storeName, String rowKey, String colName) {
        checkState();
        String keyspace = getKeyspaceForApp(appName);
        DBConn dbConn = getDBConnection(keyspace);
        try {
            return dbConn.getColumn(storeName, rowKey, colName);
        } finally {
            returnDBConnection(dbConn);
        }
    }   // getColumn

    @Override
    public Iterator<DRow> getRowsAllColumns(String appName, String storeName, Collection<String> rowKeys) {
        checkState();
        String keyspace = getKeyspaceForApp(appName);
        DBConn dbConn = getDBConnection(keyspace);
        try {
            return dbConn.getRowsAllColumns(storeName, rowKeys);
        } finally {
            returnDBConnection(dbConn);
        }
    }   // getRowsAllColumns

    @Override
    public Iterator<DRow> getRowsColumns(String             appName,
                                         String             storeName,
                                         Collection<String> rowKeys,
                                         Collection<String> colNames) {
        checkState();
        String keyspace = getKeyspaceForApp(appName);
        DBConn dbConn = getDBConnection(keyspace);
        try {
            return dbConn.getRowsColumns(storeName, rowKeys, colNames);
        } finally {
            returnDBConnection(dbConn);
        }
    }   // getRowsColumnSet
    
    @Override
    public Iterator<DRow> getRowsColumnSlice(String             appName,
                                             String             storeName,
                                             Collection<String> rowKeys,
                                             String             startCol,
                                             String             endCol) {
        checkState();
        String keyspace = getKeyspaceForApp(appName);
        DBConn dbConn = getDBConnection(keyspace);
        try {
            return dbConn.getRowsColumns(storeName, rowKeys, startCol, endCol);
        } finally {
            returnDBConnection(dbConn);
        }
    }   // getRowsColumnSlice

    //----- Public methods: Task queries
    
    @Override
    public Iterator<DRow> getAllTaskRows(String keyspace) {
        checkState();
        DBConn dbConn = getDBConnection(keyspace);
        try {
            return dbConn.getAllRowsAllColumns(TASKS_STORE_NAME);
        } finally {
            returnDBConnection(dbConn);
        }
    }   // getAllTaskRows

    //----- Package-private methods
    
    // Get an available database connection from the pool for the given keyspace, creating
    // a new one if needed.
    DBConn getDBConnection(String keyspace) {
        DBConn dbConn = null;
        synchronized (m_dbKeyspaceDBConns) {
            Queue<DBConn> dbQueue = m_dbKeyspaceDBConns.get(keyspace);
            if (dbQueue == null) {
                dbQueue = new ArrayDeque<>();
                m_dbKeyspaceDBConns.put(keyspace, dbQueue);
            }
            if (dbQueue.size() > 0) {
                dbConn = dbQueue.poll();
            } else {
                dbConn = new DBConn(keyspace);
            }
        }
        return dbConn;
    }   // getDBConnection
    
    // Return a database connection object to the pool. It is OK if the given connection
    // is null -- this reduces checking in "finally" blocks.
    void returnDBConnection(DBConn dbConn) {
        if (dbConn == null) {
            return;
        }
        
        if (!getState().isRunning()) {
            dbConn.close();     // from straggler thread; close and discard
        } else if (dbConn.isFailed()) {
            // Purge all connections in case a Cassandra node is now dead.
            dbConn.close();
            m_logger.info("Purging database connection pool");
            purgeAllConnections();
        } else {
            returnGoodConnection(dbConn);
        }
    }   // returnDBConnection

    //----- Private methods
    
    // Add/return the given DBConnection to the keyspace/connection list map.
    private void returnGoodConnection(DBConn dbConn) {
        String keyspace = dbConn.getKeyspace();
        assert !Utils.isEmpty(keyspace);
        synchronized (m_dbKeyspaceDBConns) {
            Queue<DBConn> connQueue = m_dbKeyspaceDBConns.get(keyspace);
            if (connQueue == null) {
                connQueue = new ArrayDeque<>();
                m_dbKeyspaceDBConns.put(keyspace, connQueue);
            }
            connQueue.add(dbConn);
        }
    }   // returnGoodConnection

    // Get all properties for applications that live in the given keyspace. 
    private Map<String, Map<String, String>> getAllAppProperties(String keyspace) {
        DBConn dbConn = getDBConnection(keyspace);
        try {
            return dbConn.getAllAppProperties();
        } finally {
            returnDBConnection(dbConn);
        }
    }   // getAllAppProperties
    
    // Initialize the DBConnection pool by creating the first connection to Cassandra.
    private void initializeDBConnections() {
        while (true) {
            try {
                // Create and toss a no-keyspace connection.
                DBConn dbConn = new DBConn(null);
                dbConn.close();
                break;
            } catch (DBNotAvailableException ex) {
                m_logger.info("Database is not reachable. Waiting to retry");
                try {
                    Thread.sleep(ServerConfig.getInstance().db_connect_retry_wait_millis);
                } catch (InterruptedException ex2) {
                    // ignore
                }
            }
        }
    }   // initializeDBConnections

    // Close and delete all db connections, e.g., 'cause we're shutting down.
    private void purgeAllConnections() {
        synchronized (m_dbKeyspaceDBConns) {
            for (String keyspace : m_dbKeyspaceDBConns.keySet()) {
                Iterator<DBConn> iter = m_dbKeyspaceDBConns.get(keyspace).iterator();
                while (iter.hasNext()) {
                    DBConn dbConn = iter.next();
                    dbConn.close();
                    iter.remove();
                }
            }
            m_dbKeyspaceDBConns.clear();
        }
    }   // purgeAllConnections
    
    // Rebuild the m_appKeyspaceMap from all current keyspaces/Applications CFs
    private void refreshAppKeyspaceMap() {
        synchronized (m_appKeyspaceMap) {
            m_appKeyspaceMap.clear();
            // Use a temporary, no-keyspace session
            try (DBConn dbConn = new DBConn(null)) {
                CassandraSchemaMgr schemaMgr = new CassandraSchemaMgr(dbConn.getClientSession());
                Collection<String> keyspaceList = schemaMgr.getKeyspaces();
                for (String keyspace : keyspaceList) {
                    if (schemaMgr.columnFamilyExists(keyspace, APPS_STORE_NAME)) {
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
        }
    }   // refreshAppKeyspaceMap

}   // class ThriftService
