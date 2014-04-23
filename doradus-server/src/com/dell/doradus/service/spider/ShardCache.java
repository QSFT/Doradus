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

import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.common.Utils;
import com.dell.doradus.core.Defs;
import com.dell.doradus.service.db.DBService;
import com.dell.doradus.service.db.DColumn;

/**
 * This class caches sharding information for sharded tables. For each sharded table, a
 * map is maintained of shard numbers to shard start dates. The cache is intended
 * primarily for update code to quickly determine if an object update will start a new
 * shard. The cache is refreshed occasionally to compensate for data aging and other
 * background activities.
 */
public class ShardCache {
    // The cache expiration time:
    private static final long MAX_CACHE_TIME_MILLIS = 1000 * 60;  // 1 minute
    
    // Cache key-to-cached date map. Key is <application name>/<table name>, and the value
    // is the cache timestamp.
    private final Map<String, Date> m_cacheMap = new ConcurrentHashMap<String, Date>();
    
    // Logging interface:
    private final Logger m_logger = LoggerFactory.getLogger(getClass().getSimpleName());
    
    // This map stores app name -> table name -> shard number -> sharding-start date 
    private final Map<String, Map<String, Map<Integer, Date>>> m_appShardMap =
        new HashMap<String, Map<String, Map<Integer, Date>>>();
    
    /**
     * Clear all cached shard information.
     */
    public synchronized void clearAll() {
        m_appShardMap.clear();
        m_cacheMap.clear();
    }   // clearAll
    
    /**
     * Clear all cached shard information for the given application.
     * 
     * @param appDef    {@link ApplicationDefinition} of an application.
     */
    public synchronized void clear(ApplicationDefinition appDef) {
        String appName = appDef.getAppName();
        for (TableDefinition tableDef : appDef.getTableDefinitions().values()) {
            m_cacheMap.remove(appName + "/" + tableDef.getTableName()); // might have no entry
        }
        m_appShardMap.remove(appName);
    }   // clear

    /**
     * Get the shard number-to-date map for a sharded table. The given
     * {@link TableDefinition} must be sharded. If the given table has no registered
     * shards, an empty map is returned. The map is copied so the caller is free
     * to modify.
     *   
     * @param tableDef  {@link TableDefinition} of a sharded table.
     * @return          Map of shard numbers-to-start dates. The map will be empty
     *                  (but not null) if the given table currently has no non-default shards.
     */
    public synchronized Map<Integer, Date> getShardMap(TableDefinition tableDef) {
        assert tableDef != null;
        assert tableDef.isSharded();
        
        String appName = tableDef.getAppDef().getAppName();
        String tableName = tableDef.getTableName();
        String cacheKey = appName + "/" + tableName;
        Date cacheDate = m_cacheMap.get(cacheKey);
        if (cacheDate == null || isTooOld(cacheDate)) {
            loadShardCache(tableDef);
        }
        
        Map<Integer, Date> result = new HashMap<Integer, Date>();
        Map<String, Map<Integer, Date>> tableMap = m_appShardMap.get(appName);
        if (tableMap != null) {
            Map<Integer, Date> shardMap = tableMap.get(tableDef.getTableName());
            if (shardMap != null) {
                result.putAll(shardMap);
            }
        }
        return result;
    }   // getShardMap
    
    /**
     * Verify that the shard with the given number has been registered for the given table.
     * If it hasn't, the shard's starting date is computed, the shard is registered in the
     * _shards row, and the start date is cached.
     * 
     * @param tableDef      TableDefinition of a sharded table.
     * @param shardNumber   Shard number (must be > 0).
     */
    public synchronized void verifyShard(TableDefinition tableDef, int shardNumber) {
        assert tableDef != null;
        assert tableDef.isSharded();
        assert shardNumber > 0;
        
        Map<String, Map<Integer, Date>> tableMap = m_appShardMap.get(tableDef.getAppDef().getAppName());
        if (tableMap != null) {
            Map<Integer, Date> shardMap = tableMap.get(tableDef.getTableName());
            if (shardMap != null) {
                if (shardMap.containsKey(shardNumber)) {
                    return;
                }
            }
        }
        
        // Unknown app/table/shard number, so start it.
        Date shardDate = tableDef.computeShardStart(shardNumber);
        addShardStart(tableDef, shardNumber, shardDate);
    }   // verifyShard
    
    ///// Private methods
    
    // Create a local transaction to add the register the given shard, then cache it.
    private void addShardStart(TableDefinition tableDef, int shardNumber, Date shardDate) {
        SpiderTransaction dbTran = new SpiderTransaction();
        dbTran.addShardStart(tableDef, shardNumber, shardDate);
        dbTran.commit();
        synchronized (this) {
            cacheShardValue(tableDef, shardNumber, shardDate);
        }
    }   // addShardStart

    // Cache the given shard.
    private void cacheShardValue(TableDefinition tableDef,
                                 Integer         shardNumber,
                                 Date            shardStart) {
        // Get or create the app name -> table map
        String appName = tableDef.getAppDef().getAppName();
        Map<String, Map<Integer, Date>> tableShardNumberMap = m_appShardMap.get(appName);
        if (tableShardNumberMap == null) {
            tableShardNumberMap = new HashMap<String, Map<Integer,Date>>();
            m_appShardMap.put(appName, tableShardNumberMap);
        }
        
        // Get or create the table name -> shard number map
        String tableName = tableDef.getTableName();
        Map<Integer, Date> shardNumberMap = tableShardNumberMap.get(tableName);
        if (shardNumberMap == null) {
            shardNumberMap = new HashMap<Integer, Date>();
            tableShardNumberMap.put(tableName, shardNumberMap);
        }
        
        // Add the shard number -> start date
        shardNumberMap.put(shardNumber, shardStart);
        m_logger.debug("Sharding date for {}.{} shard #{} set to: {} ({})",
                       new Object[]{appName, tableName, shardNumber, shardStart.getTime(), Utils.formatDateUTC(shardStart)});
    }   // cacheShardValue

    // Load and cache/replace sharding information for the given table. Caller must
    // worry about concurrency.
    private void loadShardCache(TableDefinition tableDef) {
        String appName = tableDef.getAppDef().getAppName();
        String tableName = tableDef.getTableName();
        m_logger.debug("Loading shard cache for {}.{}", appName, tableName);
        
        Date cacheDate = new Date();
        String cacheKey = appName + "/" + tableName;
        m_cacheMap.put(cacheKey, cacheDate);
        
        Map<String, Map<Integer, Date>> tableMap = m_appShardMap.get(appName);
        if (tableMap == null) {
            tableMap = new HashMap<>();
            m_appShardMap.put(appName, tableMap);
        }
        
        Map<Integer, Date> shardMap = tableMap.get(tableName);
        if (shardMap == null) {
            shardMap = new HashMap<>();
            tableMap.put(tableName, shardMap);
        }
        
        Iterator<DColumn> colIter =
            DBService.instance().getAllColumns(SpiderService.termsStoreName(tableDef), Defs.SHARDS_ROW_KEY);
        if (colIter != null) {
            while (colIter.hasNext()) {
                DColumn col = colIter.next();
                Integer shardNum = Integer.parseInt(col.getName());
                Date shardDate = new Date(Long.parseLong(col.getValue()));
                shardMap.put(shardNum, shardDate);
            }
        }
    }   // loadShardCache
    
    // Return true if given date has exceeded MAX_CACHE_TIME_MILLIS time.
    private boolean isTooOld(Date cacheDate) {
        Date now = new Date();
        return now.getTime() - cacheDate.getTime() > MAX_CACHE_TIME_MILLIS;
    }   // isTooOld

}   // class ShardCache
