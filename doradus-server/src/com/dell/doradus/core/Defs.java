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

package com.dell.doradus.core;

/**
 * Common definitions used by Doradus, corralled into a single location. Only static
 * final members are used; no instances or member variables exist.
 */
final public class Defs {
    // All statics -- no objects allowed.
    private Defs() {
        throw new AssertionError();
    }
    
    //---------- Application store definitions ----------
    
    //----- Application definition row (row key is the application name)
    
    /**
     * Column name in an application definition row that holds each application's schema.
     */
    public static final String COLNAME_APP_SCHEMA = "_application";
    
    /**
     * Column name in an application definiiton row that holds the content-type of the
     * schema definition (e.g., "text/xml").
     */
    public static final String COLNAME_APP_SCHEMA_FORMAT = "_format";
    
    /**
     * Column name in the an application definition row that holds the schema format
     * version number.
     */
    public static final String COLNAME_APP_SCHEMA_VERSION = "_version";
    
    /**
     * The column name suffix where we keep each table's last aging check timestamp. The
     * full column name is "{table}/LastAgingCheck"; there may be up to one column per
     * table that uses data aging.
     */
    public static final String LAST_AGING_CHECK = "LastAgingCheck";
    
    //----- Options row (_options)
    
    /**
     * Key of "global options" row in the Applications table. 
     */
    public static final String OPTIONS_ROW_KEY = "_options";
    
    /**
     * Column name of database-level schema version in the global options row.
     */
    public static final String DB_SCHEMA_VERSION = "DBSchemaVersion";

    //----- Nodes row (_nodes)
    
    /**
     * Key of the node registration row in Applications table:
     */
    public static final String NODES_ROW_KEY = "_nodes";

    //---------- Terms store definitions ----------
    
    /**
     * The key of the "all objects" record that resides in each Terms table.
     */
    public static final String ALL_OBJECTS_ROW_KEY = "_";
    
    /**
     * Key of "current shards" row in sharded table Terms tables.
     */
    public static final String SHARDS_ROW_KEY = "_shards";

    /**
     * Row key for system marks in Terms table:
     */
    public static final String TABLE_CHECKS_ROW_KEY = "_table-checks";
    
    /**
     * Row key for the "field registry" row in the Terms table.
     */
    public static final String FIELD_REGISTRY_ROW_KEY = "_fields";
    
    /**
     * Row key prefix for "term registry" rows in the Terms table.
     */
    public static final String TERMS_REGISTRY_ROW_PREFIX = "_terms";
    
    //---------- Tasks store definitions ----------
    
    /**
     * Row key prefix for a "claim" row in the tasks table.
     */
    public static final String TASK_CLAIM_ROW_PREFIX = "_claim";

}   // class Defs
