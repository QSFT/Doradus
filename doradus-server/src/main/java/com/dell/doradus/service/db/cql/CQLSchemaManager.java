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

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ResultSet;
import com.dell.doradus.core.ServerConfig;
import com.dell.doradus.service.db.DBService;
import com.dell.doradus.service.db.Tenant;

/**
 * Performs schema management operations using the CQL API. All methods are static and
 * call the {@link CQLService} to get a session object when needed.
 */
public class CQLSchemaManager {
    // Members:
    private static final Logger m_logger = LoggerFactory.getLogger(CQLSchemaManager.class.getSimpleName());

    private CQLSchemaManager() { }
    
    //----- Public methods
    
    /**
     * Create a new keyspace with the given name and optional options. The keyspace is
     * created with the following CQL command:
     * <pre>
     *      CREATE KEYSPACE "<i>keyspace</i>" WITH <i>prop</i>=<i>value</i> AND ...;
     * </pre>
     * Where the list of <i>prop</i> properties come from {@link ServerConfig#ks_defaults}.
     * 
     * @param tenant    {@link Tenant} that will use new keyspace.
     * @param options   Options to use for new keyspace.
     */
    public static void createKeyspace(Tenant tenant, Map<String, String> options) {
        String cqlKeyspace = CQLService.storeToCQLName(tenant.getName());
        m_logger.info("Creating new keyspace: {}", cqlKeyspace);
        StringBuilder cql = new StringBuilder();
        cql.append("CREATE KEYSPACE ");
        cql.append(cqlKeyspace);
        cql.append(keyspaceDefaultsToCQLString(options));
        cql.append(";");
        executeCQL(tenant, cql.toString());
    }   // createKeyspace
    
    /**
     * Modify the keyspace with the given name with the given options. The only option
     * that can be modified is ReplicationFactor.  If it is present, the keyspace is
     * altered with the following CQL command:
     * <pre>
     *      ALTER KEYSPACE "<i>keyspace</i>" WITH REPLICATION = {'class':'SimpleStrategy',
     *        'replication_factor' : <i>replication_factor</i> };
     * </pre>
     * 
     * @param tenant    {@link Tenant} that uses keyspace.
     * @param options   Modified options to use for keyspace. Only the option
     *                  "replication_factor" is examined.
     */
    public static void modifyKeyspace(Tenant tenant, Map<String, String> options) {
        if (!options.containsKey("ReplicationFactor")) {
            return;
        }
        String cqlKeyspace = CQLService.storeToCQLName(tenant.getName());
        m_logger.info("Modifying keyspace: {}", cqlKeyspace);
        StringBuilder cql = new StringBuilder();
        cql.append("ALTER KEYSPACE ");
        cql.append(cqlKeyspace);
        cql.append(" WITH REPLICATION = {'class':'");
        String strategyClass = "SimpleStrategy";
        Map<String, Object> ksDefs = ServerConfig.getInstance().ks_defaults;
        if (ksDefs != null && ksDefs.containsKey("strategy_class")) {
            strategyClass = ksDefs.get("strategy_class").toString();
        }
        cql.append(strategyClass);
        cql.append("','replication_factor':");
        cql.append(options.get("ReplicationFactor"));
        cql.append("};");
        executeCQL(tenant, cql.toString());
    }   // createKeyspace
    
    /**
     * Drop the keyspace for the given tenant. The keyspace is dropped with the following
     * CQL command:
     * <pre>
     *      DROP KEYSPACE "<i>keyspace</i>";
     * </pre>
     * 
     * @param tenant    {@link Tenant} that owns keyspace to drop.
     */
    public static void dropKeyspace(Tenant tenant) {
        String cqlKeyspace = CQLService.storeToCQLName(tenant.getName());
        m_logger.info("Dropping keyspace: {}", cqlKeyspace);
        StringBuilder cql = new StringBuilder();
        cql.append("DROP KEYSPACE ");
        cql.append(cqlKeyspace);
        cql.append(";");
        executeCQL(tenant, cql.toString());
    }   // dropKeyspace
    
    /**
     * Create a CQL table with the given name. For backward compatibility with the
     * Thrift API, all tables look like this:
     * <pre>
     *      CREATE TABLE "<i>keyspace</i>"."<i>name</i>" (
     *          key     text,
     *          column1 text,
     *          value   text,   // or blob based on bBinaryValues
     *          PRIMARY KEY(key, column1)
     *      ) WITH COMPACT STORAGE [WITH <i>prop</i> AND <i>prop</i> AND ...]
     * </pre>
     * Where the WITH <i>prop</i> clauses come from {@link ServerConfig#olap_cf_defaults}
     * or {@link ServerConfig#cf_defaults}.
     * 
     * @param tenant        {@link Tenant} that owns keyspace.
     * @param storeName     Name of table (unquoted) to create.
     * @param bBinaryValues True if store's values will be binary.
     */
    public static void createCQLTable(Tenant tenant, String storeName, boolean bBinaryValues) {
        String cqlKeyspace = CQLService.storeToCQLName(tenant.getName());
        String tableName = CQLService.storeToCQLName(storeName);
        m_logger.info("Creating CQL table {}", tableName);
        
        StringBuffer cql = new StringBuffer();
        cql.append("CREATE TABLE ");
        cql.append(qualifiedTableName(cqlKeyspace, tableName));
        cql.append("(key text,column1 text,value ");
        if (bBinaryValues) {
            cql.append("blob,");
        } else {
            cql.append("text,");
        }
        cql.append("PRIMARY KEY(key,column1)) WITH COMPACT STORAGE ");
        cql.append(tablePropertiesToCQLString(storeName));
        cql.append(";");
        executeCQL(tenant, cql.toString());
    }   // createCQLTable
    
    /**
     * Drop the table with the given name owned by the given tenant. The CQL executed is:
     * <pre>
     *      DROP TABLE "<i>keyspace</i>"."<i>name</i>";
     * </pre>
     * 
     * @param tenant    {@link Tenant} that owns table.
     * @param storeName Name of table to be dropped.
     */
    public static void dropCQLTable(Tenant tenant, String storeName) {
        String cqlKeyspace = CQLService.storeToCQLName(tenant.getName());
        String tableName = CQLService.storeToCQLName(storeName);
        m_logger.info("Dropping table: {}", tableName);
        String cql = "DROP TABLE " + qualifiedTableName(cqlKeyspace, tableName) + ";";
        executeCQL(tenant, cql);
    }   // dropCQLTable
    
    //----- Private methods
    
    // Find doradus.yaml options for the given template and turn into a CQL string:
    //      "AND <prop>=<value> AND..."
    @SuppressWarnings("unchecked")
    private static String tablePropertiesToCQLString(String storeName) {
        StringBuilder buffer = new StringBuilder();
        
        // A little kludgey for now
        Map<String, Object> cfOptions = storeName.startsWith("OLAP")
                                      ? ServerConfig.getInstance().olap_cf_defaults
                                      : ServerConfig.getInstance().cf_defaults;
        if (cfOptions != null) {
            for (String optName : cfOptions.keySet()) {
                buffer.append(" AND ");
                if (optName.equals("compression_options")) {
                    buffer.append("compression");
                } else {
                    buffer.append(optName);
                }
                buffer.append("=");
                Object optValue = cfOptions.get(optName);
                if (optValue instanceof Map) {
                    buffer.append(mapToCQLString((Map<String, Object>) optValue));
                } else {
                    buffer.append(optValue.toString());
                }
            }
        }
        return buffer.toString();
    }   // tablePropertiesToCQLString

    // Turn keyspace options into the CQL string:
    //      "WITH DURABLE_WRITES=true AND
    //            REPLICATION={'class':'SimpleStrategy', 'replication_factor':'1'}"
    // Allow RF to be overridden with ReplicationFactor in the given options.
    @SuppressWarnings("unchecked")
    private static String keyspaceDefaultsToCQLString(Map<String, String> options) {
        // Defaults:
        boolean durable_writes = true;
        Map<String, Object> replication = new HashMap<String, Object>();
        replication.put("class", "SimpleStrategy");
        replication.put("replication_factor", "1");

        // Override defaults if configured
        Map<String, Object> ksDefs = ServerConfig.getInstance().ks_defaults;
        if (ksDefs != null) {
            if (ksDefs.containsKey("durable_writes")) {
                durable_writes = Boolean.parseBoolean(ksDefs.get("durable_writes").toString());
            }
            if (ksDefs.containsKey("strategy_class")) {
                replication.put("class", ksDefs.get("strategy_class").toString());
            }
            if (ksDefs.containsKey("strategy_options")) {
                Object value = ksDefs.get("strategy_options");
                if (value instanceof Map) {
                    Map<String, Object> replOpts = (Map<String, Object>)value;
                    if (replOpts.containsKey("replication_factor")) {
                        replication.put("replication_factor", replOpts.get("replication_factor").toString());
                    }
                }
            }
        }
        
        // Override replication_factor if requested.
        if (options != null && options.containsKey("ReplicationFactor")) {
            replication.put("replication_factor", options.get("ReplicationFactor"));
        }
        
        StringBuilder buffer = new StringBuilder();
        buffer.append(" WITH DURABLE_WRITES=");
        buffer.append(durable_writes);
        buffer.append(" AND REPLICATION=");
        buffer.append(mapToCQLString(replication));
        return buffer.toString();
    }   // keyspaceDefaultsToCQLString
    
    // Turn the map into "{'<key1>':<value1>, '<key2>':<value2>, ...}".
    // Values must be quoted if they aren't literals.
    private static String mapToCQLString(Map<String, Object> valueMap) {
        StringBuffer buffer = new StringBuffer();
        buffer.append("{");
        boolean bFirst = true;
        for (String name : valueMap.keySet()) {
            if (bFirst) {
                bFirst = false;
            } else {
                buffer.append(",");
            }
            buffer.append("'");
            buffer.append(name);
            buffer.append("':");
            Object value = valueMap.get(name);
            if (value instanceof String) {
                buffer.append("'");
                buffer.append(value.toString());
                buffer.append("'");
            } else {
                buffer.append(value.toString());
            }
        }
        buffer.append("}");
        return buffer.toString();
    }   // mapToCQLString

    // Execute and optionally log the given CQL statement.
    private static ResultSet executeCQL(Tenant tenant, String cql) {
        m_logger.trace("Executing CQL: {}", cql);
        try {
            return ((CQLService)DBService.instance(tenant)).getSession().execute(cql);
        } catch (Exception e) {
            m_logger.error("CQL query failed", e);
            m_logger.info("   Query={}", cql);
            throw e;
        }
    }   // executeCQL
    
    private static String qualifiedTableName(String keyspace, String tableName) {
        assert keyspace.charAt(0) == '"';
        return keyspace + "." + tableName;
    }

}   // class CQLSchemaManager
