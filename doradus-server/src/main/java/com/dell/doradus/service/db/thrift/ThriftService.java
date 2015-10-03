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
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;

import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.KeySlice;

import com.dell.doradus.common.Utils;
import com.dell.doradus.service.db.CassandraService;
import com.dell.doradus.service.db.DBNotAvailableException;
import com.dell.doradus.service.db.DBTransaction;
import com.dell.doradus.service.db.DColumn;
import com.dell.doradus.service.db.Tenant;
import com.dell.doradus.service.schema.SchemaService;

public class ThriftService extends CassandraService {
    // Members to handle rotation host selection:
    private final Object m_lastHostLock = new Object();
    private String       m_lastHost;
    private boolean      m_bUseSecondaryHosts;
    private long         m_lastPrimaryHostCheckTimeMillis;
    
    // DBConn queue:
    private final Queue<DBConn> m_dbConns = new ArrayDeque<>();
    
    // Single-thread all schema access for our tenant:
    private final Object m_schemaLock = new Object();
    private final CassandraSchemaMgr m_schemaMgr;

    private ThriftService(Tenant tenant) {
        super(tenant);
        m_schemaMgr = new CassandraSchemaMgr(this);
    }

    //----- Public Service methods

    /**
     * Return the singleton ThriftService service object.
     * 
     * @return  Static ThriftService object.
     */
    public static ThriftService instance(Tenant tenant) {
        return new ThriftService(tenant);
    }
    
    @Override
    public void initService() { }

    @Override
    public void startService() {
        initializeDBConnections();
    }   // stopService

    @Override
    public void stopService() {
        purgeAllConnections();
    }   // stopService

    //----- Public DBService methods: Namespace management
    
    @Override
    public boolean supportsNamespaces() {
        return true;
    }
    
    @Override
    public void createNamespace() {
        checkState();
        // Use a temporary, no-keyspace session
        try (DBConn dbConn = createAndConnectConn(null)) {
            synchronized (m_schemaLock) {
                String keyspace = getTenant().getName();
                if (!m_schemaMgr.keyspaceExists(dbConn, keyspace)) {
                    Map<String, String> options = getKeyspaceOptions(getTenant());
                    m_schemaMgr.createKeyspace(dbConn, keyspace, options);
                }
            }
        }
    }   // createTenant

    private Map<String, String> getKeyspaceOptions(Tenant tenant) {
        // TODO: Get keyspace options from tenant definiton; inherit from default tenant if needed.
        return null;
    }

    @Override
    public void dropNamespace() {
        checkState();
        // Use a temporary, no-keyspace session
        try (DBConn dbConn = createAndConnectConn(null)) {
            synchronized (m_schemaLock) {
                String keyspace = getTenant().getName();
                if (m_schemaMgr.keyspaceExists(dbConn, keyspace)) {
                    m_schemaMgr.dropKeyspace(dbConn, keyspace);
                }
            }
        }
    }   // dropTenant
    
    public List<String> getDoradusKeyspaces() {
        checkState();
        List<String> keyspaces = new ArrayList<>();
        // Use a temporary, no-keyspace session
        try (DBConn dbConn = createAndConnectConn(null)) {
            synchronized (m_schemaLock) {
                Collection<String> keyspaceList = m_schemaMgr.getKeyspaces(dbConn);
                for (String keyspace : keyspaceList) {
                    if (m_schemaMgr.columnFamilyExists(dbConn, keyspace, SchemaService.APPS_STORE_NAME)) {
                        keyspaces.add(keyspace);
                    }
                }
            }
        }
        return keyspaces;
    }   // getTenantMap
    
    //----- Public DBService methods: Store management

    @Override
    public void createStoreIfAbsent(String storeName, boolean bBinaryValues) {
        checkState();
        String keyspace = getTenant().getName();
        DBConn dbConn = getDBConnection();
        try {
            synchronized (m_schemaLock) {
                if (!m_schemaMgr.columnFamilyExists(dbConn, keyspace, storeName)) {
                    m_schemaMgr.createColumnFamily(dbConn, keyspace, storeName, bBinaryValues);
                }
            }
        } finally {
            returnDBConnection(dbConn);
        }
    }   // createStoreIfAbsent
    
    @Override
    public void deleteStoreIfPresent(String storeName) {
        checkState();
        String keyspace = getTenant().getName();
        DBConn dbConn = getDBConnection();
        try {
            synchronized (m_schemaLock) {
                if (m_schemaMgr.columnFamilyExists(dbConn, keyspace, storeName)) {
                    m_schemaMgr.deleteColumnFamily(dbConn, storeName);
                }
            }
        } finally {
            returnDBConnection(dbConn);
        }
    }   // deleteStoreIfPresent
    
    //----- Public DBService methods: Updates
    
    @Override
    public void commit(DBTransaction dbTran) {
        checkState();
        assert dbTran.getTenant().getName().equals(this.getTenant().getName());
        DBConn dbConn = getDBConnection();
        try {
            dbConn.commit(dbTran);
        } finally {
            returnDBConnection(dbConn);
        }
    }   // commit
    
    //----- Public DBService methods: Queries

    @Override
    public List<DColumn> getColumns(String storeName, String rowKey, String startColumn, String endColumn, int count) {
        checkState();
        DBConn dbConn = getDBConnection();
        try {
            List<ColumnOrSuperColumn> columns = dbConn.getSlice(
                    CassandraDefs.columnParent(storeName),
                    CassandraDefs.slicePredicateStartEndCol(Utils.toBytes(startColumn), Utils.toBytes(endColumn), count),
                    Utils.toByteBuffer(rowKey));
            List<DColumn> result = new ArrayList<>(columns.size());
            for(ColumnOrSuperColumn column: columns) {
                result.add(new DColumn(column.getColumn().getName(), column.getColumn().getValue()));
            }
            return result;
        } finally {
            returnDBConnection(dbConn);
        }
    }

    @Override
    public List<DColumn> getColumns(String storeName, String rowKey, Collection<String> columnNames) {
        checkState();
        DBConn dbConn = getDBConnection();
        try {
            List<byte[]> colNameList = new ArrayList<>(columnNames.size());
            for (String colName : columnNames) {
                colNameList.add(Utils.toBytes(colName));
            }

            List<ColumnOrSuperColumn> columns = dbConn.getSlice(
                    CassandraDefs.columnParent(storeName),
                    CassandraDefs.slicePredicateColNames(colNameList),
                    Utils.toByteBuffer(rowKey));
            List<DColumn> result = new ArrayList<>(columns.size());
            for(ColumnOrSuperColumn column: columns) {
                result.add(new DColumn(column.getColumn().getName(), column.getColumn().getValue()));
            }
            return result;
        } finally {
            returnDBConnection(dbConn);
        }
    }

    @Override
    public List<String> getRows(String storeName, String continuationToken, int count) {
        checkState();
        DBConn dbConn = getDBConnection();
        try {
            List<KeySlice> keys = dbConn.getRangeSlices(
                    CassandraDefs.columnParent(storeName), 
                    CassandraDefs.slicePredicateStartEndCol(null, null, 1),
                    CassandraDefs.keyRangeStartRow(Utils.toBytes(continuationToken), count));
            List<String> result = new ArrayList<>(keys.size());
            for(KeySlice key: keys) {
                result.add(Utils.toString(key.getKey()));
            }
            return result;
        } finally {
            returnDBConnection(dbConn);
        }
    }

    //----- Package-private methods

    // Get an available database connection from the pool, creating a new one if needed.
    DBConn getDBConnection() {
        DBConn dbConn = null;
        synchronized (m_dbConns) {
            if (m_dbConns.size() > 0) {
                dbConn = m_dbConns.poll();
            } else {
                dbConn = createAndConnectConn(getTenant().getName());
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

    // Connect the given DBConn to an available Cassandra node. Initially, primary hosts
    // configured in "dbhost" are tried. As long as one of them is available, new calls
    // to this method cycle through the primary hosts. If no primary hosts are reachable
    // and secondary hosts are configured in "secondary_dbhost", those are tried. If a
    // secondary host is reached, subsequent calls to this method rotate through those
    // hosts until primary_host_recheck_millis is reached, at which time we try primary
    // hosts again. If neither any primary nor secondary hosts can be reached, a
    // DBNotAvailableException is thrown. If a connection is made but the DBConn's
    // configured keyspace is not available, a RuntimeException is thrown.
    void connectDBConn(DBConn dbConn) throws DBNotAvailableException, RuntimeException {
        // If we're using failover hosts, see if it's time to try primary hosts again.
        if (m_bUseSecondaryHosts &&
            (System.currentTimeMillis() - m_lastPrimaryHostCheckTimeMillis) > getParamInt("primary_host_recheck_millis", 60000)) {
            m_bUseSecondaryHosts = false;
        }
        
        // Try all primary hosts first if possible.
        DBNotAvailableException lastException = null;
        if (!m_bUseSecondaryHosts) {
            String[] dbHosts = getParamString("dbhost").split(",");
            for (int attempt = 1; !dbConn.isOpen() && attempt <= dbHosts.length; attempt++) {
                try {
                    dbConn.connect(chooseHost(dbHosts));
                } catch (DBNotAvailableException ex) {
                    lastException = ex;
                } catch (RuntimeException ex) {
                    throw ex;   // bad keyspace; abort connection attempts
                }
            }
            m_lastPrimaryHostCheckTimeMillis = System.currentTimeMillis();
        }
        
        // Try secondary hosts if needed and if configured.
        if (!dbConn.isOpen() && !Utils.isEmpty(getParamString("secondary_dbhost"))) {
            if (!m_bUseSecondaryHosts) {
                m_logger.info("All connections to 'dbhost' failed; trying 'secondary_dbhost'");
            }
            String[] dbHosts = getParamString("secondary_dbhost").split(",");
            for (int attempt = 1; !dbConn.isOpen() && attempt <= dbHosts.length; attempt++) {
                try {
                    dbConn.connect(chooseHost(dbHosts));
                } catch (DBNotAvailableException e) {
                    lastException = e;
                } catch (RuntimeException ex) {
                    throw ex;   // bad keyspace; abort connection attempts
                }
            }
            if (dbConn.isOpen()) {
                m_bUseSecondaryHosts = true; // stick with secondary hosts for now.
            }
        }
        
        if (!dbConn.isOpen()) {
            m_logger.error("All Thrift connection attempts failed.", lastException);
            throw lastException;
        }
    }   // connectDBConn
    
    //----- Private methods
    
    // Create a new DBConn object and connect it to an available Cassandra node. If
    // keyspace is non-null, create a session to the given keyspace. Throw a
    // DBNotAvailableException if no connection is possible. Throw a RuntimeException ifa
    // the given keyspace does not exist.
    private DBConn createAndConnectConn(String keyspace) throws DBNotAvailableException, RuntimeException {
        DBConn dbConn = new DBConn(this, keyspace);
        connectDBConn(dbConn);
        return dbConn;
    }   // createAndConnectConn
    
    // Choose the next Cassandra host name in the list or a random one.
    private String chooseHost(String[] dbHosts) {
        String host = null;
        synchronized (m_lastHostLock) {
            if (dbHosts.length == 1) {
                host = dbHosts[0];
            } else if (!Utils.isEmpty(m_lastHost)) {
                for (int index = 0; host == null && index < dbHosts.length; index++) {
                    if (dbHosts[index].equals(m_lastHost)) {
                        host = dbHosts[(++index) % dbHosts.length];
                    }
                }
            }
            if (host == null) {
                host = dbHosts[new Random().nextInt(dbHosts.length)];
            }
            m_lastHost = host;
        }
        return host;
    }   // chooseHost
    
    // Add/return the given DBConnection to the connection queue.
    private void returnGoodConnection(DBConn dbConn) {
        synchronized (m_dbConns) {
            m_dbConns.add(dbConn);
        }
    }   // returnGoodConnection

    // Initialize the DBConnection pool by creating the first connection to Cassandra.
    private void initializeDBConnections() {
        boolean bSuccess = false;
        while (!bSuccess) {
            // Create a no-keyspace connection and fetch all keyspaces to prove that the
            // cluster is really ready.
            try (DBConn dbConn = createAndConnectConn(null)) {
                m_schemaMgr.getKeyspaces(dbConn);
                bSuccess = true;
            } catch (DBNotAvailableException ex) {
                m_logger.info("Database is not reachable. Waiting to retry");
                try {
                    Thread.sleep(getParamInt("db_connect_retry_wait_millis", 5000));
                } catch (InterruptedException ex2) {
                    // ignore
                }
            }
        }
    }   // initializeDBConnections

    // Close and delete all db connections, e.g., 'cause we're shutting down.
    private void purgeAllConnections() {
        synchronized (m_dbConns) {
            Iterator<DBConn> iter = m_dbConns.iterator();
            while (iter.hasNext()) {
                DBConn dbConn = iter.next();
                dbConn.close();
                iter.remove();
            }
        }
    }   // purgeAllConnections

}   // class ThriftService
