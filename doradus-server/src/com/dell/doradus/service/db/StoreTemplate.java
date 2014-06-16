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

/**
 * Holds properties for a "store", which is a named physical storage container such as a
 * ColumnFamily. Currently, only the store name and whether it holds binary values are
 * kept in this class. 
 */
public class StoreTemplate {
    // Global stores that exist in all Doradus databases:
    public static final String APPLICATIONS_STORE_NAME = "Applications";
    public static final String TASKS_STORE_NAME = "Tasks";
    
    private final String    m_storeName;
    private final boolean   m_binaryValues;
    
    /**
     * Create a StoreTemplate with the given name and binary value option.
     * 
     * @param storeName     Name of store.
     * @param bBinaryValues True if the store will hold binary values. If false, all
     *                      values are presumed to be UTF8 text strings.
     */
    public StoreTemplate(String storeName, boolean bBinaryValues) {
        m_storeName = storeName;
        m_binaryValues = bBinaryValues;
    }   // constructor
    
    /**
     * Get this store's name.
     * 
     * @return  This store's name.
     */
    public String getName() {
        return m_storeName;
    }

    /**
     * Get this store's binary value option. If true, the store is expected to hold binary
     * values, otherwise all values are UTF8 text strings.
     * 
     * @return  This store's binary value option.
     */
    public boolean valuesAreBinary() {
        return m_binaryValues;
    }   // valuesAreBinary
    
    @Override
    public String toString() {
        return "Store: " + m_storeName;
    }   // toString
    
}   // StoreTemplate
