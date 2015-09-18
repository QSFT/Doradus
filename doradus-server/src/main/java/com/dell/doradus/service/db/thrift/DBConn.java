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

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.thrift.AuthenticationRequest;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSSLTransportFactory;
import org.apache.thrift.transport.TSSLTransportFactory.TSSLTransportParameters;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dell.doradus.common.Utils;
import com.dell.doradus.core.ServerConfig;
import com.dell.doradus.service.db.DBNotAvailableException;
import com.dell.doradus.service.db.DBService;
import com.dell.doradus.service.db.DBTransaction;

/**
 * Represents a Thrift connection to a Cassandra database and provides methods for
 * fetching and updating data. DBConn uses method and variable names that match the
 * {@link DBService} class's terms such as "stores" and "rows".
 */
public class DBConn implements AutoCloseable {
    // Logging interface:
    protected final Logger m_logger = LoggerFactory.getLogger(getClass().getSimpleName());
    
    private Cassandra.Client m_client;
    private boolean m_bDBOpen;
    private boolean m_bFailed;
    private String m_keyspace;

    //----- Public static constants and methods

    /**
     * Name of the Applications ColumnFamily.
     */
    public static final String COLUMN_FAMILY_APPS = "Applications";
    
    /**
     * ColumnParent for the Applications ColumnFamily.
     */
    public static final ColumnParent COLUMN_PARENT_APPS = new ColumnParent(COLUMN_FAMILY_APPS);
    
    //----- DBConn: Connection management

    /**
     * Create a new DBConn that will connect to the given keyspace. No connection is
     * attempted until {@link #connect(String)} is called. If the given keyspace is null
     * or empty, this DBConn will create a no-keyspace session.
     * 
     * @param keyspace  Name of keyspace to set connection to or null to for a
     *                  no-keyspace session.
     */
    public DBConn(String keyspace) {
        m_keyspace = keyspace;
    }   // constructor

    /**
     * Attempt to connect to the given Cassandra host name or IP address. If a connection
     * cannot be made, a {@link DBNotAvailableException} is thrown that wraps the
     * Cassandra exception. If a connection is made and a keyspace was configured, the
     * session is set to the configured keyspace. However, if the keyspace cannot be used,
     * a RuntimeException is thrown.
     * 
     * @param  dbhost                   Name or address of Cassandra host to connect to.
     * @throws DBNotAvailableException  If the given host is not reachable.
     * @throws RuntimeException         If a keyspace was configured but cannot be used.
     */
    public void connect(String dbhost) throws DBNotAvailableException, RuntimeException {
        assert !m_bDBOpen;
        ServerConfig config = ServerConfig.getInstance();
        int bufferSize = config.thrift_buffer_size_mb * 1024 * 1024;
        
        // Attempt to open the requested dbhost.
        try {
            TSocket socket = null;
            if (config.dbtls) {
                m_logger.debug("Connecting to Cassandra node {}:{} using TLS", dbhost, config.dbport);
                socket = createTLSSocket(dbhost);
            } else {
                m_logger.debug("Connecting to Cassandra node {}:{}", dbhost, config.dbport);
                socket = new TSocket(dbhost, config.dbport, config.db_timeout_millis);
                socket.open();
            }
            TTransport transport = new TFramedTransport(socket, bufferSize);
            TProtocol protocol = new TBinaryProtocol(transport);
            m_client = new Cassandra.Client(protocol);
        } catch (Exception e) {
            throw new DBNotAvailableException(e);
        }
        
        // Set keyspace if requested.
        if (!Utils.isEmpty(m_keyspace)) {
            try {
                m_client.set_keyspace(m_keyspace);
            } catch (Exception e) {
                // This can't be retried, so we throw a RuntimeException
                m_logger.error("Cannot use Keyspace '" + m_keyspace + "'", e);
                throw new RuntimeException(e);
            }
        }
        
        // Set credentials if requested.
        if (!Utils.isEmpty(config.dbuser)) {
            try {
                Map<String, String> credentials = new HashMap<>();
                credentials.put("username", config.dbuser);
                credentials.put("password", config.dbpassword);
                AuthenticationRequest auth_request = new AuthenticationRequest(credentials);
                m_client.login(auth_request);
            } catch (Exception e) {
                // This can't be retried, so we throw a RuntimeException
                m_logger.error("Could not authenticate with dbuser '" + config.dbuser + "'", e);
                throw new RuntimeException(e);
            }
        }

        m_bDBOpen = true;
        m_bFailed = false;
    }   // connect

    /**
     * Get the keyspace with which this connected was created. This will be null for a
     * no-keyspace session.
     * 
     * @return  Keyspace to which this connection is attached, null if none.
     */
    public String getKeyspace() {
        return m_keyspace;
    }   // getKeyspace
    
    /**
     * Get this DBConn's Cassandra.Client session. This should only be used temporarily
     * since it is shared with this DBConn object.
     * 
     * @return  This DBConn's Cassandra.Client session, which will be a keyspace or a
     *          no-keyspace session depending on how this object was constructed.
     */
    public Cassandra.Client getClientSession() {
        return m_client;
    }   // getClientSession
    
    /**
     * Close the database connection. This object can be reused after the connection has
     * been closed by calling {@link #open()} again.
     */
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
    public boolean isFailed() {
        return m_bFailed;
    }   // isFailed

    /**
     * Indicate if this connection is open, meaning a successful connection to Cassandra
     * was made.
     * 
     * @return  True if this connection is open.
     */
    public boolean isOpen() {
        return m_bDBOpen;
    }
    //----- DBConn: Updates 
    
    /**
     * Commit the updates in the given {@link DBTransaction}. The updates are cleared even
     * if all commit retries fail.
     * 
     * @param dbTran {@link DBTransaction} with transactions ready to commit.
     */
    public void commit(DBTransaction dbTran) {
        // For extreme logging
        if (m_logger.isTraceEnabled()) {
            dbTran.traceMutations(m_logger);
        }
        try {
            long timestamp = Utils.getTimeMicros();
            commitMutations(dbTran, timestamp);
            commitDeletes(dbTran, timestamp);
        } finally {
            dbTran.clear();
        }
    }   // commit

    //----- DBConn: Queries

    //----- Package-private methods
    
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
            } catch (InvalidRequestException ex) {
                // No point in retrying this one.
                String errMsg = "get_range_slices() failed for table: " + colParent.getColumn_family();
                m_bFailed = true;
                m_logger.error(errMsg, ex);
                throw new RuntimeException(errMsg, ex);
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

    /**
     * Perform a get_slice() request with the given parameters and retry the operation
     * if a database error occurs. Retries will attempt to get a new connection if an
     * error suggests that the current DB node or the Thrift connection has failed. If no
     * rows are found, an empty list is returned is returned.
     * 
     * @param colParent ColumnParent to query.
     * @param slicePred SlicePredicate defining columns to fetch.
     * @param key       Row key to fetch.
     * @return          List of columns found; empty if the row doesn't exist or doesn't
     *                  have any columns satisfying the slice predicate.
     */
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
            } catch (InvalidRequestException ex) {
                // No point in retrying this one.
                String errMsg = "get_slice() failed for table: " + colParent.getColumn_family();
                m_bFailed = true;
                m_logger.error(errMsg, ex);
                throw new RuntimeException(errMsg, ex);
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
    }   // getSlice

    //----- Private methods

    // Fetch a single column.
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
            } catch (NotFoundException ex) {
                return null;
            } catch (InvalidRequestException ex) {
                // No point in retrying this one.
                String errMsg = "get() failed for table: " + colPath.getColumn_family();
                m_bFailed = true;
                m_logger.error(errMsg, ex);
                throw new RuntimeException(errMsg, ex);
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
    }   // getColumn


    // Commit all row-deletions in the given MutationMap, if any, using the given timestamp.
    private void commitDeletes(DBTransaction dbTran, long timestamp) {
        Map<String, Set<ByteBuffer>> rowDeleteMap = CassandraTransaction.getRowDeletionMap(dbTran);
        if (rowDeleteMap.size() == 0) {
            return;
        }
        
        // Iterate through all ColumnFamilies
        for (String colFamName : rowDeleteMap.keySet()) {
            // Delete each row in this key set.
            Set<ByteBuffer> rowKeySet = rowDeleteMap.get(colFamName);
            for (ByteBuffer rowKey : rowKeySet) {
                removeRow(timestamp, rowKey, new ColumnPath(colFamName));
            }
        }
    }   // commitDeletes

    // Commit the update mutations in the given MutationMap. Retry if needed up to the
    // configured maximum number of retries.
    private void commitMutations(DBTransaction dbTran, long timestamp) {
        Map<ByteBuffer, Map<String, List<Mutation>>> colMutMap = CassandraTransaction.getUpdateMap(dbTran, timestamp);
        if (colMutMap.size() == 0) {
            return;
        }
        m_logger.debug("Committing {} mutations", CassandraTransaction.totalColumnMutations(dbTran));
        
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
            } catch (InvalidRequestException ex) {
                // No point in retrying this one.
                m_bFailed = true;
                m_logger.error("batch_mutate() failed", ex);
                throw new RuntimeException("batch_mutate() failed", ex);
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
                // an DBNotAvailableException, which we pass to the caller.
                reconnect(ex);
            }
        }
    }   // commitMutations

    // Create a TSocket using configured TLS/SSL options. 
    private TSocket createTLSSocket(String host) throws TTransportException {
        ServerConfig config = ServerConfig.getInstance();
        String[] cipherSuites = config.dbtls_cipher_suites.toArray(new String[]{});
        TSSLTransportParameters sslParams = new TSSLTransportParameters("SSL", cipherSuites);
        if (!Utils.isEmpty(config.keystore)) {
            sslParams.setKeyStore(config.keystore, config.keystorepassword);
        }
        if (!Utils.isEmpty(config.truststore)) {
            sslParams.setTrustStore(config.truststore, config.truststorepassword);
        }
        return TSSLTransportFactory.getClientSocket(host, config.dbport, config.db_timeout_millis, sslParams);
    }   // createTLSSocket

    // Attempt to reconnect this connection to Cassandra due to the given exception.
    // Because Cassandra could be very busy, if the reconnect fails, we will retry multiple
    // times, waiting a little longer between each attempt. If all retries fail, we throw
    // an DBNotAvailableException and leave the Thrift connection null.
    private void reconnect(Exception reconnectEx) {
        // Log the exception as a warning.
        m_logger.warn("Reconnecting to Cassandra due to error", reconnectEx);
        
        // Reconnect up to the configured number of times, waiting a little between each attempt.
        boolean bSuccess = false;
        for (int attempt = 1; !bSuccess; attempt++) {
            try {
                close();
                ThriftService.instance().connectDBConn(this);
                m_logger.debug("Reconnected successful");
                bSuccess = true;
            } catch (Exception ex) {
                // Abort if all retries failed.
                if (attempt >= ServerConfig.getInstance().max_reconnect_attempts) {
                    m_logger.error("All reconnect attempts failed; abandoning reconnect", ex);
                    throw new DBNotAvailableException("All reconnect attempts failed", ex);
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
            } catch (InvalidRequestException ex) {
                // No point in retrying this one.
                String errMsg = "remove() failed for table: " + colPath.getColumn_family(); 
                m_bFailed = true;
                m_logger.error(errMsg, ex);
                throw new RuntimeException(errMsg, ex);
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
                
                // Reconnect since the connection may be bad. This throws an DBNotAvailableException
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

}   // class DBConn
