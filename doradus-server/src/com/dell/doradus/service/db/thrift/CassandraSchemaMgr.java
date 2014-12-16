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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.KsDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dell.doradus.core.ServerConfig;

/**
 * Provides methods for accessing and managing Cassandra schemas: creating keyspaces,
 * creating and deleting ColumnFamilies, single- and multi-gets, updates, etc. This class
 * should be used in the following sequence:
 * <ul>
 * <li>Create a {@link CassandraSchemaMgr} object with a Cassandra.Client connection open
 *     to the database.</li>
 * <li>Perform the desired schema operations, taking care not to use the Cassandra.Client
 *     for anything else.</li>
 * <li>Discard the {@link CassandraSchemaMgr} object so the Cassandra.Client can be used
 *     for other purposes.</li>
 * </ul>
 */
public class CassandraSchemaMgr {
    private static final String DEFAULT_KS_STRATEGY_CLASS = "SimpleStrategy";
    private static final String DEFAULT_KS_REPLICATION_FACTOR = "1";
    private static final List<CfDef> DEFAULT_KS_CF_DEFS = new ArrayList<CfDef>();
    private static final boolean DEFAULT_KS_DURABLE_WRITES = true;
    
    private final Cassandra.Client m_client;
    private final Logger m_logger = LoggerFactory.getLogger(getClass().getSimpleName());

    /**
     * Create an object uses the given connection to perform schema operations. The given
     * client connection should not be used for anything else until this object is
     * discarded.
     * 
     * @param client    Client session connected to the Cassandra database.
     */
    public CassandraSchemaMgr(Cassandra.Client client) {
        m_client = client;
    }   // constructor

    //----- Keyspace methods

    /**
     * Get a list of all known keyspaces. This method can be used with any client session.
     * 
     * @return  List of all known keyspaces, empty if none.
     */
    public Collection<String> getKeyspaces() {
        List<String> result = new ArrayList<>();
        try {
            for (KsDef ksDef : m_client.describe_keyspaces()) {
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
     * Create a new keyspace with the given name. This method should be passed a
     * no-keyspace client session.
     * 
     * @param keyspace  Name of new keyspace.
     */
    public void createKeyspace(String keyspace) {
        m_logger.info("Creating Keyspace '{}'", keyspace);
        try {
            KsDef ksDef = setKeySpaceOptions(keyspace);
            m_client.system_add_keyspace(ksDef);
        } catch (Exception ex) {
            String errMsg = "Failed to create Keyspace '" + keyspace + "'"; 
            m_logger.error(errMsg, ex);
            throw new RuntimeException(errMsg, ex);
        }
    }   // createKeyspace
    
    /**
     * Return true if a keyspace with the given name exists. This method can be used with
     * any client session.
     * 
     * @param keyspace  Keyspace name.
     * @return          True if it exists.
     */
    public boolean keyspaceExists(String keyspace) {
        try {
            m_client.describe_keyspace(keyspace);
            return true;
        } catch (Exception e) {
            return false;   // Notfound
        }
    }   // keyspaceExists

    /**
     * Delete the keyspace with the given name. This method can use any client session.
     * 
     * @param keyspace  Name of keyspace to drop.
     */
    public void dropKeyspace(String keyspace) {
        m_logger.info("Deleting Keyspace '{}'", keyspace);
        try {
            m_client.system_drop_keyspace(keyspace);
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
     * current client session can be connected to any keyspace.
     * 
     * @param keyspace      Keyspace that owns new CF.
     * @param cfName        name of new CF.
     * @param bBinaryValues True if column values shoud be binary.
     */
    public void createColumnFamily(String keyspace, String cfName, boolean bBinaryValues) {
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
        
        // A little kludgey for now
        Map<String, Object> cfOptions = cfName.startsWith("OLAP")
                                      ? ServerConfig.getInstance().olap_cf_defaults
                                      : ServerConfig.getInstance().cf_defaults;
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
        
        // Create the column familiy and wait for all nodes to agree.
        try {
            validateSchema(m_client.system_add_column_family(cfDef));
        } catch (Exception ex) {
            throw new RuntimeException("ColumnFamily creation failed", ex);
        }
    }   // createColumnFamily
    
    /**
     * Return true if the given store name currently exists in the given keyspace. This
     * method can be used with a connection connected to any keyspace.
     * 
     * @param cfName    Candidate ColumnFamily name.
     * @return          True if the CF exists in the database.
     */
    public boolean columnFamilyExists(String keyspace, String cfName) {
        KsDef ksDef = null;
        try {
            ksDef = m_client.describe_keyspace(keyspace);
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
     * Delete the column family with the given name. The current client session must
     * be connected to the keyspace in which the CF is to be deleted.
     * 
     * @param cfName    Name of CF to delete.
     */
    public void deleteColumnFamily(String cfName) {
        m_logger.info("Deleting ColumnFamily: {}", cfName);
        try {
            m_client.system_drop_column_family(cfName);
        } catch (Exception ex) {
            throw new RuntimeException("drop_column_family failed", ex);
        }
    }   // deleteColumnFamily

    /**
     * Get the ColumnFamily names that exist within the given keyspace. This method can
     * be used with a client session connected to any keyspace.
     * 
     * @return  A collection of ColumnFamily belonging to the given keyspace.
     */
    public Collection<String> getColumnFamilies(String keyspace) {
        KsDef ksDef = null;
        try {
            ksDef = m_client.describe_keyspace(keyspace);
        } catch (Exception ex) {
            throw new RuntimeException("Failed to get keyspace definition for '" + keyspace + "'", ex);
        }
        
        Set<String> result = new HashSet<>();
        for (CfDef cfDef : ksDef.getCf_defs()) {
            result.add(cfDef.getName());
        }
        return result;
    }   // getColumnFamilies
    
    //----- Private methods
    
    // Build KsDef from configuration options and defaults.
    private KsDef setKeySpaceOptions(String keyspace) {
        KsDef ksDef = new KsDef();
        ksDef.setName(keyspace);
        Map<String, Object> ksOptions = ServerConfig.getInstance().ks_defaults;
        if (ksOptions != null) {
            for (String name : ksOptions.keySet()) {
                Object value = ksOptions.get(name);
                if (name.equals("name") && !keyspace.equals(value)) {
                    m_logger.warn("ks_defaults.name: Keyspace name should be set through 'keyspace' option -- ignored");
                    continue;
                }
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
    
    // Check that all Cassandra nodes are in agreement on the latest schema change.
    private void validateSchema(String currentVersionId) {
        Map<String, List<String>> versions = null;

        long limit = System.currentTimeMillis() + 5000;
        boolean inAgreement = false;
        do {
            try {
                versions = m_client.describe_schema_versions(); // getting schema version for nodes of the ring
            } catch (Exception e) {
                continue;
            }

            for (String version : versions.keySet()) {
                if (version.equals(currentVersionId)) {
                    inAgreement = true;
                    break;
                }
            }
        } while (limit - System.currentTimeMillis() >= 0 && !inAgreement);

        if (!inAgreement) {
            throw new RuntimeException("Cannot get agreement on Cassandra schema versions");
        }
    }   // validateSchema

}   // class CassandraSchemaMgr
