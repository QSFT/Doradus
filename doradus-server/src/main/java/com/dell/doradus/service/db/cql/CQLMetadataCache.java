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

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.TableMetadata;

/**
 * Currently caches whether a store has binary or string values
 */
public class CQLMetadataCache {
    private Cluster m_cluster;
    private Map<String, Map<String, Boolean>> m_columnValueIsBinary = new HashMap<>();
    
    public CQLMetadataCache(Cluster cluster) { m_cluster = cluster; }


    /**
     * Return true if column values for the given namespace/store name are binary.
     * 
     * @param namespace Namespace (Keyspace) name.
     * @param storeName Store (ColumnFamily) name.
     * @return          True if the given table's column values are binary.
     */
    public boolean columnValueIsBinary(String namespace, String storeName) {
        Boolean cachedValue = getCachedValueIsBinary(namespace, storeName);
        if(cachedValue != null) return cachedValue.booleanValue();
        
        String cqlKeyspace = CQLService.storeToCQLName(namespace);
        String tableName = CQLService.storeToCQLName(storeName);
        KeyspaceMetadata ksMetadata = m_cluster.getMetadata().getKeyspace(cqlKeyspace);
        TableMetadata tableMetadata = ksMetadata.getTable(tableName);
        ColumnMetadata colMetadata = tableMetadata.getColumn("value");
        boolean isBinary = colMetadata.getType().equals(DataType.blob());
        
        putCachedValueIsBinary(namespace, storeName, isBinary);
        return isBinary;
    }

    public Boolean getCachedValueIsBinary(String namespace, String storeName) {
        Map<String, Boolean> map = m_columnValueIsBinary.get(namespace);
        if(map == null) return null;
        else return map.get(storeName);
    }

    public void putCachedValueIsBinary(String namespace, String storeName, boolean isBinary) {
        Map<String, Boolean> map = m_columnValueIsBinary.get(namespace);
        if(map == null) {
            map = new HashMap<>();
            m_columnValueIsBinary.put(namespace, map);
        }
        map.put(storeName, new Boolean(isBinary));
    }
    
}
