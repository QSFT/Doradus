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

import java.nio.ByteBuffer;

import com.dell.doradus.common.Utils;
import com.dell.doradus.service.db.DColumn;

/**
 * Implementation of DColumn that holds values returned from CQL. Holds the column name as
 * a String and the value in binary. The name is usually the value of the Thrift column
 * called "column1" and the value comes from the Thrift column called "value".
 */
public class CQLColumn implements DColumn {
    private final String m_colName;
    private final byte[] m_binaryValue;

    /**
     * Create a CQLColumn with the given column name and text value.
     * 
     * @param colName   Name of column extracted.
     * @param colValue  Column value as a string. Converted to binary via UTF-8.
     */
    public CQLColumn(String colName, String colValue) {
        m_colName = colName;
        m_binaryValue = Utils.toBytes(colValue);
    }
    
    /**
     * Create a CQLColumn with the given column name and binary value.
     * 
     * @param colName   Name of column extracted.
     * @param colValue  Column value in binary. The value is copied.
     */
    public CQLColumn(String colName, byte[] colValue) {
        m_colName = colName;
        m_binaryValue = colValue.clone();
    }
    
    /**
     * Create a CQLColumn with the given column name and ByteBuffer value.
     * 
     * @param colName   Name of column extracted.
     * @param colValue  Column value as a ByteBuffer. The value is extracted and copied.
     */
    public CQLColumn(String colName, ByteBuffer colValue) {
        m_colName = colName;
        m_binaryValue = Utils.getBytes(colValue);
    }
    
    @Override
    public String getName() {
        return m_colName;
    }

    @Override
    public byte[] getRawValue() {
        return m_binaryValue;
    }

    @Override
    public String getValue() {
        return Utils.toString(m_binaryValue);
    }

}   // class CQLColumn
