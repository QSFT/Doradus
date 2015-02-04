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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import com.dell.doradus.common.Utils;
import com.dell.doradus.core.ServerConfig;
import com.dell.doradus.service.db.DBNotAvailableException;
import com.dell.doradus.service.db.DBService;
import com.dell.doradus.service.db.DBTransaction;
import com.dell.doradus.service.db.DColumn;
import com.dell.doradus.service.db.DRow;
import com.dell.doradus.service.db.Tenant;
import com.dell.doradus.service.schema.SchemaService;

public class ThriftService extends DBService {
    private static final ThriftService INSTANCE = new ThriftService();

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
        m_logger.info("Cassandra host list: {}", Arrays.toString(config.dbhost.split(",")));
        m_logger.info("Cassandra port: {}", config.dbport);
        m_logger.info("Default application keyspace: {}", config.keyspace);
    }   // initService

    @Override
    public void startService() {
        initializeDBConnections();
    }   // stopService

    @Override
    public void stopService() {
        purgeAllConnections();
    }   // stopService

    //----- Public DBService methods: Tenant management
    
    @Override
    public void createTenant(Tenant tenant, Map<String, String> options) {
        checkState();
        // Use a temporary, no-keyspace session
        String keyspace = tenant.getKeyspace();
        try (DBConn dbConn = new DBConn(null)) {
            CassandraSchemaMgr schemaMgr = new CassandraSchemaMgr(dbConn.getClientSession());
            if (!schemaMgr.keyspaceExists(keyspace)) {
                schemaMgr.createKeyspace(keyspace, options);
            }
        }
    }   // createTenant

    @Override
    public void dropTenant(Tenant tenant) {
        checkState();
        // Use a temporary, no-keyspace session
        String keyspace = tenant.getKeyspace();
        try (DBConn dbConn = new DBConn(null)) {
            CassandraSchemaMgr schemaMgr = new CassandraSchemaMgr(dbConn.getClientSession());
            if (!schemaMgr.keyspaceExists(keyspace)) {
                schemaMgr.dropKeyspace(keyspace);
            }
        }
    }   // dropTenant
    
    @Override
    public void addUsers(Tenant tenant, Map<String, String> users) {
        throw new RuntimeException("This method is not supported for the Thrift API");
    }
    
    @Override
    public Collection<Tenant> getTenants() {
        checkState();
        List<Tenant> tenantList = new ArrayList<>();
        // Use a temporary, no-keyspace session
        try (DBConn dbConn = new DBConn(null)) {
            CassandraSchemaMgr schemaMgr = new CassandraSchemaMgr(dbConn.getClientSession());
            Collection<String> keyspaceList = schemaMgr.getKeyspaces();
            for (String keyspace : keyspaceList) {
                if (schemaMgr.columnFamilyExists(keyspace, SchemaService.APPS_STORE_NAME)) {
                    tenantList.add(new Tenant(keyspace));
                }
            }
        }
        return tenantList;
    }   // getTenantMap
    
    //----- Public DBService methods: Store management

    @Override
    public void createStoreIfAbsent(Tenant tenant, String storeName, boolean bBinaryValues) {
        checkState();
        String keyspace = tenant.getKeyspace();
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
    public void deleteStoreIfPresent(Tenant tenant, String storeName) {
        checkState();
        String keyspace = tenant.getKeyspace();
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
    
    //----- Public DBService methods: Updates
    
    @Override
    public DBTransaction startTransaction(Tenant tenant) {
        checkState();
        return new CassandraTransaction(tenant);
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
    public Iterator<DColumn> getAllColumns(Tenant tenant, String storeName, String rowKey) {
        checkState();
        String keyspace = tenant.getKeyspace();
        DBConn dbConn = getDBConnection(keyspace);
        try {
            return dbConn.getAllColumns(storeName, rowKey);
        } finally {
            returnDBConnection(dbConn);
        }
    }   // getAllColumns

    @Override
    public Iterator<DColumn> getColumnSlice(Tenant tenant, String storeName, String rowKey,
                                            String startCol, String endCol, boolean reversed) {
        checkState();
        String keyspace = tenant.getKeyspace();
        DBConn dbConn = getDBConnection(keyspace);
        try {
            return dbConn.getColumnSlice(storeName, rowKey, startCol, endCol, reversed);
        } finally {
            returnDBConnection(dbConn);
        }
    }   // getColumnSlice

    @Override
    public Iterator<DColumn> getColumnSlice(Tenant tenant, String storeName, String rowKey,
                                            String startCol, String endCol) {
        checkState();
        return getColumnSlice(tenant, storeName, rowKey, startCol, endCol, false);
    }   // getColumnSlice

    @Override
    public Iterator<DRow> getAllRowsAllColumns(Tenant tenant, String storeName) {
        checkState();
        String keyspace = tenant.getKeyspace();
        DBConn dbConn = getDBConnection(keyspace);
        try {
            return dbConn.getAllRowsAllColumns(storeName);
        } finally {
            returnDBConnection(dbConn);
        }
    }   // getAllRowsAllColumns

    @Override
    public DColumn getColumn(Tenant tenant, String storeName, String rowKey, String colName) {
        checkState();
        String keyspace = tenant.getKeyspace();
        DBConn dbConn = getDBConnection(keyspace);
        try {
            return dbConn.getColumn(storeName, rowKey, colName);
        } finally {
            returnDBConnection(dbConn);
        }
    }   // getColumn

    @Override
    public Iterator<DRow> getRowsAllColumns(Tenant tenant, String storeName, Collection<String> rowKeys) {
        checkState();
        String keyspace = tenant.getKeyspace();
        DBConn dbConn = getDBConnection(keyspace);
        try {
            return dbConn.getRowsAllColumns(storeName, rowKeys);
        } finally {
            returnDBConnection(dbConn);
        }
    }   // getRowsAllColumns

    @Override
    public Iterator<DRow> getRowsColumns(Tenant             tenant,
                                         String             storeName,
                                         Collection<String> rowKeys,
                                         Collection<String> colNames) {
        checkState();
        String keyspace = tenant.getKeyspace();
        DBConn dbConn = getDBConnection(keyspace);
        try {
            return dbConn.getRowsColumns(storeName, rowKeys, colNames);
        } finally {
            returnDBConnection(dbConn);
        }
    }   // getRowsColumnSet
    
    @Override
    public Iterator<DRow> getRowsColumnSlice(Tenant             tenant,
                                             String             storeName,
                                             Collection<String> rowKeys,
                                             String             startCol,
                                             String             endCol) {
        checkState();
        String keyspace = tenant.getKeyspace();
        DBConn dbConn = getDBConnection(keyspace);
        try {
            return dbConn.getRowsColumns(storeName, rowKeys, startCol, endCol);
        } finally {
            returnDBConnection(dbConn);
        }
    }   // getRowsColumnSlice

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

    // Initialize the DBConnection pool by creating the first connection to Cassandra.
    private void initializeDBConnections() {
        boolean bSuccess = false;
        while (!bSuccess) {
            // Create a no-keyspace connection and fetch all keyspaces to prove that the
            // cluster is really ready.
            try (DBConn dbConn = new DBConn(null)) {
                new CassandraSchemaMgr(dbConn.getClientSession()).getKeyspaces();
                bSuccess = true;
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

}   // class ThriftService
