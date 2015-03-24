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

import java.nio.ByteBuffer;

import com.dell.doradus.common.Utils;

/**
 * Represents a Doradus column, which is a named value within a row. A column has a name
 * and a value.
 */
public class DColumn {
    private final String m_name;
    private final byte[] m_value;

    /**
     * Create a DColumn with the given binary column name and value.
     * 
     * @param name   Binary column name. It is converted to a String using UTF8. 
     * @param value  Binary column value. The value is *not* copied. If the given value
     *               is null, a byte[0] is stored.
     */
    public DColumn(byte[] name, byte[] value) {
        m_name = Utils.toString(name);
        m_value = value == null ? new byte[0] : value;
    }   // constructor
    
    /**
     * Create a DColumn with the given String column name and binary value.
     * 
     * @param name   String column name. 
     * @param value  Binary column value. The value is *not* copied. If the given value
     *               is null, a byte[0] is stored.
     */
    public DColumn(String name, byte[] value) {
        m_name = name;
        m_value = value == null ? new byte[0] : value;
    }   // constructor
    
    /**
     * Create a DColumn with the given String column name and ByteBuffer value.
     * 
     * @param name   String column name.
     * @param value  Column value as a ByteBuffer. The value is extracted and copied.
     */
    public DColumn(String name, ByteBuffer value) {
        m_name = name;
        m_value = Utils.getBytes(value);
    }   // constructor
    
    /**
     * Create a DColumn with the given String column name and value.
     * 
     * @param name   String column name.
     * @param value  String column value. It is converted to binary using UTF8.
     */
    public DColumn(String name, String value) {
        m_name = name;
        m_value = Utils.toBytes(value);
    }   // constructor
    
    /**
     * Get this column's Name as a String.
     * 
     * @return  This column's Name as a String.
     */
    public String getName() {
        return m_name;
    }   // getName
    
    /**
     * Get this column's binary value.
     * 
     * @return  This column's binary value as a byte[].
     */
    public byte[] getRawValue() {
        return m_value;
    }   // getRawValue

    /**
     * Get this column's value as a String via UTF-8 conversion. If the column's value
     * is not stored in UTF-8, the results are undefined.
     * 
     * @return  This column's value as a String via UTF-8 conversion.
     */
    public String getValue() {
        return Utils.toString(m_value);
    }   // getValue
    
}   // interface DColumn
