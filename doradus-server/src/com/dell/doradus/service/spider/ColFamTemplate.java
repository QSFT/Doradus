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

package com.dell.doradus.service.spider;

import java.util.Map;

import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.core.ServerConfig;
import com.dell.doradus.service.db.StoreTemplate;

/**
 * Provides a ColumnFamily template for a specific type of Doradus table. For example, an
 * ColFamTemplate can represent the global Application CF, an application-specific Counters
 * CF, or a table-specific objects CF. Each object holds the CF column type ("Standard" or
 * "Super"), CF name, key and column validation classes, and column comparator for a
 * specific CF role. Note that "keyspace" is not stored in this object since it is applied
 * when the CF is actually created.
 */
public class ColFamTemplate extends StoreTemplate {
    // Global CF names:
    public static final String APPLICATIONS_CF_NAME = "Applications";
    public static final String TASKS_CF_NAME = "Tasks";
    
    // Properties of a column family:
    private String m_cfName;                        // Column Family name
    private String m_colType;                       // CF type (Standard or Super)
    private String m_comparatorType;                // Column name comparator (e.g., UTF8Type)
    private String m_keyValidationClass;            // Key validater (e.g., UTF8Type)
    private String m_defaultValidationClass;        // Default column value validator (e.g., BytesType)
    
    // Optional CF options:
    private Map<String, Object> m_cfOptions;

    //----- Static Public methods
    
    /**
     * Return the name of the "Object" CF for the given table, which is
     * "{application}_{table}".
     * 
     * @param tableDef  {@link TableDefinition} of a table.
     * @return          Name of the Object CF for the given table.
     */
    public static String objectCFName(TableDefinition tableDef) {
        return tableDef.getAppDef().getAppName() + "_" + tableDef.getTableName();
    }   // objectCFName
    
    /**
     * Return the name of the "Statistics" CF for the given application, which is
     * "{application}_Statistics".
     * 
     * @param appDef    {@link ApplicationDefinition} of an application.
     * @return          Name of given application's Statistics CF.
     */
    public static String statsCFName(ApplicationDefinition appDef) {
        return appDef.getAppName() + "_Statistics";
    }   // statsCFName
    
    /**
     * Return the name of the the "Terms" table for the given table, which is
     * "{application}_{table}_Terms".
     * 
     * @param tableDef  {@link TableDefinition} of a table.
     * @return          Name of the given table's Terms CF.
     */
    public static String termsCFName(TableDefinition tableDef) {
        return tableDef.getAppDef().getAppName() + "_" + tableDef.getTableName() + "_Terms";
    }   // termsCFName
        
    //----- Public methods
    
    @Override
    public String getStoreName() {
        return getCFName();
    }   // getStoreName
    
    /**
     * Get this object's ColumnFamily name (i.e., the Cassandra CF name, which is unique
     * within the keyspace).
     * 
     * @return  This object's ColumnFamily name.
     */
    public String getCFName() {
        return m_cfName;
    }   // getCFName
    
    /**
     * Get this object's column type ("Standard" or "Super").
     * 
     * @return  This object's column type ("Standard" or "Super").
     */
    public String getColumnType() {
        return m_colType;
    }   // getColumnType
    
    /**
     * Get this object's column name comparator type (e.g., "UTF8Type").
     * 
     * @return  This object's column name comparator type (e.g., "UTF8Type").
     */
    public String getComparatorType() {
        return m_comparatorType;
    }   // getComparatorType
    
    /**
     * Get this object's key value comparator type (e.g., "UTF8Type").
     * 
     * @return  This object's key value comparator type (e.g., "UTF8Type").
     */
    public String getKeyValidator() {
        return m_keyValidationClass;
    }   // getKeyValidator

    /**
     * Get this object's default column value validator (e.g., "BytesType").
     * 
     * @return  This object's default column value validator (e.g., "BytesType").
     */
    public String getDefaultColValueValidator() {
        return m_defaultValidationClass;
    }   // getDefaultColValueValidator
    
    /**
     * Get the optional CF options for this template. 
     * 
     * @return  CF options as a String/Object map, if any, otherwise null.
     */
    public Map<String, Object> getOptions() {
        return m_cfOptions;
    }   // getOptions
    
    /**
     * Get a simple string representing this object in the form: "<name> (<type>)".
     * 
     * @return  A simple string representing this object in the form: "<name> (<type>)".
     */
    @Override
    public String toString() {
        return m_cfName + " (" + m_colType + ")";
    }   // toString
    
    public static ColFamTemplate applicationCFTemplate() {
        return standardCFTemplate(APPLICATIONS_CF_NAME,                 // CF name
                                  "UTF8Type",                           // comparator type
                                  "UTF8Type",                           // key validator
                                  "BytesType");                         // default col value validator
    }   // applicationCFTemplate

    public static ColFamTemplate tasksCFTemplate() {
        return standardCFTemplate(TASKS_CF_NAME,                        // CF name
                                  "UTF8Type",                           // comparator type
                                  "UTF8Type",                           // key validator
                                  "UTF8Type");                          // default col value validator
    }   // tasksCFTemplate
    
    // To use in the future
    public static ColFamTemplate olapRootTemplate() {
    	return olapCFTemplate("OLAP");
    }

    /**
     * Get the application-level {@link ColFamTemplate}s needed for an application with
     * the given {@link ApplicationDefinition}. The objects are returned as an array.
     *  
     * @param appDef    {@link ApplicationDefinition} of an application.
     * @return          Array of {@link ColFamTemplate} objects that define the CFs needed
     *                  for the given application. This array will *not* contain any
     *                  table-level CFs (see {@link #tableCFTemplates(TableDefinition)}).
     */
    public static ColFamTemplate[] applicationCFTemplates(ApplicationDefinition appDef) {
        return new ColFamTemplate[] {
            statisticsCFTemplate(appDef)
        };
    }   // applicationCFTemplates
    
    /**
     * Get the table-level {@link ColFamTemplate}s needed for the given
     * {@link TableDefinition}. The objects are returned as an array.
     * 
     * @param tableDef  {@link TableDefinition} of a table.
     * @return          Array if {@link ColFamTemplate} needed to hold data for the given
     *                  table.
     */
    public static ColFamTemplate[] tableCFTemplates(TableDefinition tableDef) {
        return new ColFamTemplate[] {
            objectsCFTemplate(tableDef),
            termsCFTemplate(tableDef)
        };
    }   // tableCFTemplates
    
    ////////// Private methods
    
    // Create a ColFamTemplate that represents a Standard CF with the given parameters.
    private static ColFamTemplate standardCFTemplate(
        String cfName,
        String comparatorType,
        String keyValidator, 
        String defaultColValueValidator
        ) {
        ColFamTemplate cfTemplate = new ColFamTemplate();
        cfTemplate.m_cfName = cfName;
        cfTemplate.m_colType = "Standard";
        cfTemplate.m_comparatorType = comparatorType;
        cfTemplate.m_keyValidationClass = keyValidator;
        cfTemplate.m_defaultValidationClass = defaultColValueValidator;
        cfTemplate.m_cfOptions = ServerConfig.getInstance().cf_defaults;
        return cfTemplate;
    }   // standardCFTemplate
    
    public static ColFamTemplate olapCFTemplate(String cfName) {
        ColFamTemplate cfTemplate = new ColFamTemplate();
        cfTemplate.m_cfName = cfName;
        cfTemplate.m_colType = "Standard";
        cfTemplate.m_comparatorType = "BytesType";
        cfTemplate.m_keyValidationClass = "BytesType";
        cfTemplate.m_defaultValidationClass = "BytesType";
        cfTemplate.m_cfOptions = ServerConfig.getInstance().olap_cf_defaults;
        return cfTemplate;
    }
    
    ///// Application-specific ColumnFamilies
    
    // Create a ColFamTemplate that represents the given application's Statistics CF. This
    // is a simple standard table.
    private static ColFamTemplate statisticsCFTemplate(ApplicationDefinition appDef) {
        return standardCFTemplate(statsCFName(appDef),          // CF name
                                  "UTF8Type",                   // comparator type
                                  "UTF8Type",                   // key validator
                                  "BytesType");                 // default col value validator
    }   // statisticsCFTemplate
    
    
    ///// Table-specific ColumnFamilies

    // Create a ColFamTemplate that represents the given table's objects CF. This is a
    // standard table. Keys are binary object IDs, so the key validator must be BytesType.
    // Columns with link values also have binary values, so the comparator type must also
    // be BytesType.
    private static ColFamTemplate objectsCFTemplate(TableDefinition tableDef) {
        return standardCFTemplate(objectCFName(tableDef),       // CF name
                                  "BytesType",                  // comparator type
                                  "BytesType",                  // key validator
                                  "BytesType");                 // default col value validator
    }   // objectsCFTemplate
    
    // Create a ColFamTemplate that represents the given table's terms CF. This is a
    // standard table. Since we may index link fields, whose values are binary object IDs,
    // key validator must be BytesType. Column values are binary object IDs, so the
    // comparator type must also be BytesType.
    private static ColFamTemplate termsCFTemplate(TableDefinition tableDef) {
        return standardCFTemplate(termsCFName(tableDef),        // CF name
                                  "BytesType",                  // comparator type
                                  "BytesType",                  // key validator
                                  "BytesType");                 // default col value validator
    }   // termsCFTemplate
    
}   // class ColFamTemplate
