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

package com.dell.doradus.service.db;

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
import org.apache.cassandra.thrift.NotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dell.doradus.core.ServerConfig;
import com.dell.doradus.service.spider.ColFamTemplate;

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
     * @param client    Cassandra.Client object open to the database.
     */
    public CassandraSchemaMgr(Cassandra.Client client) {
        m_client = client;
    }   // constructor

    /**
     * Check that the Doradus keyspace and its Applications ColumnFamily exist and have
     * the right options. If necessary, create and/or modify them.
     */
    public void initializeSchema() {
        String keySpace = ServerConfig.getInstance().keyspace;
        KsDef ksDef = null;
        try {
            ksDef = m_client.describe_keyspace(keySpace);
        } catch (NotFoundException ex) {
            // ignore NotFound but propagate other exceptions
        } catch (Exception ex) {
            throw new RuntimeException("Error getting keyspace description", ex);
        }
        if (ksDef == null) {
            createKeyspace();
        }
        
        // Keyspace should exist so map into connection
        String keyspace = ServerConfig.getInstance().keyspace;
        try {
            m_client.set_keyspace(keyspace);
        } catch (Exception ex) {
            String errMsg = "Client connection cannot use Keyspace '" + keyspace + "'"; 
            m_logger.error(errMsg, ex);
            throw new RuntimeException(errMsg, ex);
        }
        
        checkGlobalColumnFamilies();
    }   // initializeSchema

    /**
     * Create a new ColumnFamily from the given ColFamTemplate object. If the new CF is
     * created successfully, wait for all nodes in the cluster to receive the schema
     * change before returning. An exception is thrown if the CF create fails.
     * 
     * @param cfTemplate    {@link ColFamTemplate} that describes new CF to create.
     */
    public void createColumnFamily(ColFamTemplate cfTemplate) {
        String keySpace = ServerConfig.getInstance().keyspace;
        m_logger.info("Creating ColumnFamily: {}", cfTemplate);
        
        CfDef cfDef = new CfDef();
        cfDef.setKeyspace(keySpace);
        cfDef.setName(cfTemplate.getCFName());
        cfDef.setColumn_type(cfTemplate.getColumnType());
        cfDef.setComparator_type(cfTemplate.getComparatorType());
        cfDef.setKey_validation_class(cfTemplate.getKeyValidator());
        cfDef.setDefault_validation_class(cfTemplate.getDefaultColValueValidator());
        
        Map<String, Object> cfOptions = cfTemplate.getOptions();
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
     * Delete the column family with the given name.
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
     * Get the ColumnFamily names that exist within our configured keyspace.
     * 
     * @return  A collection of ColumnFamily names that exist within our configured keyspace.
     */
    public Collection<String> getColumnFamilies() {
        String keySpace = ServerConfig.getInstance().keyspace;
        KsDef ksDef = null;
        try {
            ksDef = m_client.describe_keyspace(keySpace);
        } catch (Exception ex) {
            throw new RuntimeException("Failed to get keyspace definition for '" + keySpace + "'", ex);
        }
        
        Set<String> result = new HashSet<>();
        for (CfDef cfDef : ksDef.getCf_defs()) {
            result.add(cfDef.getName());
        }
        return result;
    }   // getColumnFamilies
    
    //----- Private methods
    
    // Create the Doradus keyspace.
    private void createKeyspace() {
        String keyspace = ServerConfig.getInstance().keyspace;
        m_logger.info("Creating Keyspace '{}'", keyspace);
        
        try {
            KsDef ksDef = setKeySpaceOptions();
            m_client.system_add_keyspace(ksDef);
        } catch (Exception ex) {
            String errMsg = "Failed to create Keyspace '" + keyspace + "'"; 
            m_logger.error(errMsg, ex);
            throw new RuntimeException(errMsg, ex);
        }
    }   // createKeySpace
    
    // Build KsDef from configuration options and defaults.
    private KsDef setKeySpaceOptions() {
        String keyspace = ServerConfig.getInstance().keyspace;
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
            stratOpts.put(KsDef._Fields.REPLICATION_FACTOR.getFieldName(), DEFAULT_KS_REPLICATION_FACTOR);
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
    
    // Check that the schema CF ("Applications") exists and create it if necessary. 
    private void checkGlobalColumnFamilies() {
        Collection<String> cfNames = getColumnFamilies();
        if (!cfNames.contains(ColFamTemplate.APPLICATIONS_CF_NAME)) {
            createApplicationsColumnFamily();
        }
        if (!cfNames.contains(ColFamTemplate.TASKS_CF_NAME)) {
        	createTasksColumnFamily();
        }
    }   // checkGlobalColumnFamilies
    
    // Create the schema table, which is called "Applications", which is need only once
    // per Doradus instance.
    private void createApplicationsColumnFamily() {
        // Set the keyspace name in case we are initializing and haven't done so yet.
        String keySpace = ServerConfig.getInstance().keyspace;
        try {
            m_client.set_keyspace(keySpace);
        } catch (Exception e) {
            String errMsg = "Cannot set connection keyspace to '" + keySpace + "'";
            m_logger.error(errMsg, e);
            throw new RuntimeException(errMsg, e);
        }
        ColFamTemplate appCF = ColFamTemplate.applicationCFTemplate();
        createColumnFamily(appCF);
    }   // createApplicationsColumnFamily
    
    // Create the schema table, which is called "Tasks", which is need only once
    // per Doradus instance.
    private void createTasksColumnFamily() {
        // Set the keyspace name in case we are initializing and haven't done so yet.
        String keySpace = ServerConfig.getInstance().keyspace;
        try {
            m_client.set_keyspace(keySpace);
        } catch (Exception e) {
            String errMsg = "Cannot set connection keyspace to '" + keySpace + "'";
            m_logger.error(errMsg, e);
            throw new RuntimeException(errMsg, e);
        }
        ColFamTemplate tasksCF = ColFamTemplate.tasksCFTemplate();
        createColumnFamily(tasksCF);
    }   // createTasksColumnFamily
    
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
