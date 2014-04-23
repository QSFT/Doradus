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

import java.util.Iterator;

/**
 * Represents a row, which is a record stored in a Doradus store. A row has a key and a
 * collection of columns. The Row object will contain only those columns, if any, that 
 * were retrieved from the database.
 */
public interface DRow {

    /**
     * Get this row's key as a String.
     * 
     * @return  The row's key as a String.
     */
    String getKey();
    
    /**
     * Get the columns owned by this row as an Iterator. Only the columns retrieved from
     * the database for this row will be returned. If no columns were retrieved, the
     * Iterator's hasNext() method will be immediately false. Currently, the iterator does
     * not allow values to be removed, so an exception is thrown if remove() is called.
     *  
     * @return  The columns owned by this row as an Iterator.
     */
    Iterator<DColumn> getColumns();
    
}   // interface DRow
