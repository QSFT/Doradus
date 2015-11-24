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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.KsDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides methods for accessing and managing Cassandra schemas: creating keyspaces,
 * creating and deleting ColumnFamilies, single- and multi-gets, updates, etc. Each
 * instance is created for a specific ThriftService instance.
 */
public class CassandraSchemaMgr {
    private static final String DEFAULT_KS_STRATEGY_CLASS = "SimpleStrategy";
    private static final String DEFAULT_KS_REPLICATION_FACTOR = "1";
    private static final List<CfDef> DEFAULT_KS_CF_DEFS = new ArrayList<CfDef>();
    private static final boolean DEFAULT_KS_DURABLE_WRITES = true;
    
    private static final Logger m_logger = LoggerFactory.getLogger(CassandraSchemaMgr.class.getSimpleName());

    private final ThriftService m_service;
    
    // Static methods only
    public CassandraSchemaMgr(ThriftService service) {
        m_service = service;
    }

    //----- Keyspace methods

    /**
     * Get a list of all known keyspaces. This method can be used with any DB connection.
     * 
     * @param  dbConn   Database connection to use.
     * @return          List of all known keyspaces, empty if none.
     */
    public Collection<String> getKeyspaces(DBConn dbConn) {
        List<String> result = new ArrayList<>();
        try {
            for (KsDef ksDef : dbConn.getClientSession().describe_keyspaces()) {
                result.add(ksDef.getName());
            }
        } catch (Exception e) {
            String errMsg = "Failed to get keyspace description";
            m_logger.error(errMsg, e);
            throw new RuntimeException(errMsg, e);
        }
        return result;
    }   // getKeyspaces

    /**
     * Create a new keyspace with the given name. The keyspace is created with parameters
     * defined for our DBService instance, if any. This method should be used with a
     * no-keyspace DB connection.
     * 
     * @param dbConn    Database connection to use.
     * @param keyspace  Name of new keyspace.
     */
    public void createKeyspace(DBConn dbConn, String keyspace) {
        m_logger.info("Creating Keyspace '{}'", keyspace);
        try {
            KsDef ksDef = setKeySpaceOptions(keyspace);
            dbConn.getClientSession().system_add_keyspace(ksDef);
            waitForSchemaPropagation(dbConn);
            Thread.sleep(1000);  // wait for gossip to other Cassandra nodes
        } catch (Exception ex) {
            String errMsg = "Failed to create Keyspace '" + keyspace + "'"; 
            m_logger.error(errMsg, ex);
            throw new RuntimeException(errMsg, ex);
        }
    }   // createKeyspace
    
    
    /**
     * Return true if a keyspace with the given name exists. This method can be used with
     * any DB connection.
     * 
     * @param dbConn    Database connection to use.
     * @param keyspace  Keyspace name.
     * @return          True if it exists.
     */
    public boolean keyspaceExists(DBConn dbConn, String keyspace) {
        try {
            dbConn.getClientSession().describe_keyspace(keyspace);
            return true;
        } catch (Exception e) {
            return false;   // Notfound
        }
    }   // keyspaceExists

    /**
     * Delete the keyspace with the given name. This method can use any DB connection.
     * 
     * @param dbConn    Database connection to use.
     * @param keyspace  Name of keyspace to drop.
     */
    public void dropKeyspace(DBConn dbConn, String keyspace) {
        m_logger.info("Deleting Keyspace '{}'", keyspace);
        try {
            dbConn.getClientSession().system_drop_keyspace(keyspace);
            waitForSchemaPropagation(dbConn);
        } catch (Exception ex) {
            String errMsg = "Failed to delete Keyspace '" + keyspace + "'"; 
            m_logger.error(errMsg, ex);
            throw new RuntimeException(errMsg, ex);
        }
    }   // dropKeyspace
    
    //----- ColumnFamily methods
    
    /**
     * Create a new ColumnFamily with the given parameters. If the new CF is
     * created successfully, wait for all nodes in the cluster to receive the schema
     * change before returning. An exception is thrown if the CF create fails. The
     * current DB connection can be connected to any keyspace.
     * 
     * @param dbConn        Database connection to use.
     * @param keyspace      Keyspace that owns new CF.
     * @param cfName        name of new CF.
     * @param bBinaryValues True if column values shoud be binary.
     */
    public void createColumnFamily(DBConn dbConn, String keyspace, String cfName, boolean bBinaryValues) {
        m_logger.info("Creating ColumnFamily: {}:{}", keyspace, cfName);
        
        CfDef cfDef = new CfDef();
        cfDef.setKeyspace(keyspace);
        cfDef.setName(cfName);
        cfDef.setColumn_type("Standard");
        cfDef.setComparator_type("UTF8Type");
        cfDef.setKey_validation_class("UTF8Type");
        if (bBinaryValues) {
            cfDef.setDefault_validation_class("BytesType");
        } else {
            cfDef.setDefault_validation_class("UTF8Type");
        }
        
        Map<String, Object> cfOptions = m_service.getParamMap("cf_defaults");
        if (cfOptions != null) {
            for (String optName : cfOptions.keySet()) {
                Object optValue = cfOptions.get(optName);
                CfDef._Fields fieldEnum = CfDef._Fields.findByName(optName);
                if (fieldEnum == null) {
                    m_logger.warn("Unknown ColumnFamily option: {}", optName);
                    continue;
                }
                try {
                    cfDef.setFieldValue(fieldEnum, optValue);
                } catch (Exception e) {
                    m_logger.warn("Error setting ColumnFamily option '" + optName +
                                  "' to '" + optValue + "' -- ignoring", e);
                }
            }
        }
        
        // In a multi-node startup, multiple nodes may be trying to create the same CF.
        for (int attempt = 1; !columnFamilyExists(dbConn, keyspace, cfName); attempt++) {
            try {
                dbConn.getClientSession().system_add_column_family(cfDef);
                waitForSchemaPropagation(dbConn);
            } catch (Exception ex) {
                if (attempt > m_service.getParamInt("max_commit_attempts", 10)) {
                    String msg = String.format("%d attempts to create ColumnFamily %s:%s failed",
                                               attempt, keyspace, cfName);
                    throw new RuntimeException(msg, ex);
                }
                try { Thread.sleep(1000); } catch (InterruptedException e) { }
            }
        }
    }   // createColumnFamily
    
    /**
     * Return true if the given store name currently exists in the given keyspace. This
     * method can be used with a connection connected to any keyspace.
     * 
     * @param dbConn    Database connection to use.
     * @param cfName    Candidate ColumnFamily name.
     * @return          True if the CF exists in the database.
     */
    public boolean columnFamilyExists(DBConn dbConn, String keyspace, String cfName) {
        KsDef ksDef = null;
        try {
            ksDef = dbConn.getClientSession().describe_keyspace(keyspace);
        } catch (Exception ex) {
            throw new RuntimeException("Failed to get keyspace definition for '" + keyspace + "'", ex);
        }
        
        List<CfDef> cfDefList = ksDef.getCf_defs();
        for (CfDef cfDef : cfDefList) {
            if (cfDef.getName().equals(cfName)) {
                return true;
            }
        }
        return false;
    }   // columnFamilyExists
    
    /**
     * Delete the column family with the given name. The given DB connection must be
     * connected to the keyspace in which the CF is to be deleted.
     * 
     * @param dbConn    Database connection to use.
     * @param cfName    Name of CF to delete.
     */
    public void deleteColumnFamily(DBConn dbConn, String cfName) {
        m_logger.info("Deleting ColumnFamily: {}", cfName);
        try {
            dbConn.getClientSession().system_drop_column_family(cfName);
            waitForSchemaPropagation(dbConn);
        } catch (Exception ex) {
            throw new RuntimeException("drop_column_family failed", ex);
        }
    }   // deleteColumnFamily

    //----- Private methods
    
    // Build KsDef from doradus.yaml and DBService-specific options.
    private KsDef setKeySpaceOptions(String keyspace) {
        KsDef ksDef = new KsDef();
        ksDef.setName(keyspace);
        Map<String, Object> ksDefs = m_service.getParamMap("ks_defaults");
        if (ksDefs != null) {
            for (String name : ksDefs.keySet()) {
                Object value = ksDefs.get(name);
                try {
                    KsDef._Fields field = KsDef._Fields.findByName(name);
                    if (field == null) {
                        m_logger.warn("Unknown KeySpace option: {} -- ignoring", name);
                    } else {
                        ksDef.setFieldValue(field, value);
                    }
                } catch (Exception e) {
                    m_logger.warn("Error setting Keyspace option '" + name + "' to '" + value + "' -- ignoring", e);
                }
            }
        }

        // Legacy support: ReplicationFactor -> replication_factor
        if (m_service.getParam("ReplicationFactor") != null) {
            Map<String, String> stratOpts = new HashMap<>();
            stratOpts.put("replication_factor", m_service.getParamString("ReplicationFactor"));
            ksDef.setStrategy_options(stratOpts);
        }
        
        // required: strategy_class, strategy_options, replication_factor, cf_defs, durable_writes
        if (!ksDef.isSetStrategy_class()) {
            ksDef.setStrategy_class(DEFAULT_KS_STRATEGY_CLASS);
        }
        if (!ksDef.isSetStrategy_options()) {
            Map<String, String> stratOpts = new HashMap<>();
            stratOpts.put("replication_factor", DEFAULT_KS_REPLICATION_FACTOR);
            ksDef.setStrategy_options(stratOpts);
        }
        if (!ksDef.isSetCf_defs()) {
            ksDef.setCf_defs(DEFAULT_KS_CF_DEFS);
        }
        if (!ksDef.isSetDurable_writes()) {
            ksDef.setDurable_writes(DEFAULT_KS_DURABLE_WRITES);
        }
        return ksDef;
    }   // setKeySpaceOptions

    
    // in a multi-node cluster, updating schema does not wait for it to be propagated to other nodes.
    // This method waits until all the nodes have the same schema
    private void waitForSchemaPropagation(DBConn dbConn) {
    	for(int i = 0; i < 5; i++) {
    		try {
	    		Map<String, List<String>> versions = dbConn.getClientSession().describe_schema_versions();
	    		if(versions.size() <= 1) return;
	    		m_logger.info("Schema versions are not synchronized yet. Retrying");
	            Thread.sleep(500 + 1000 * i);
    		}catch(Exception ex) {
    			m_logger.warn("Error waiting for schema propagation: {}", ex.getMessage());
    		}
    		m_logger.error("Schema versions have not been synchronized");
    	}
    }
    
    
}   // class CassandraSchemaMgr
