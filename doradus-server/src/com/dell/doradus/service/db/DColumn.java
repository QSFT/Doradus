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
 * Represents a Doradus column, which is a named value within a row. A column has a name
 * and a value.
 */
public interface DColumn {
    /**
     * Get this column's Name as a String.
     * 
     * @return  This column's Name as a String.
     */
    String getName();
    
    /**
     * Get this column's binary value.
     * 
     * @return  This column's binary value as a byte[].
     */
    byte[] getRawValue();
    
    /**
     * Get this column's value as a String via UTF-8 conversion. If the column's value
     * is not stored in UTF-8, the results are undefined.
     * 
     * @return  This column's value as a String via UTF-8 conversion.
     */
    String getValue();
    
}   // interface DColumn
