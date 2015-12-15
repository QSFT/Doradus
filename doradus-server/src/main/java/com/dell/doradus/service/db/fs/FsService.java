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

package com.dell.doradus.service.db.fs;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dell.doradus.service.db.DBService;
import com.dell.doradus.service.db.DBTransaction;
import com.dell.doradus.service.db.DColumn;
import com.dell.doradus.service.db.Tenant;

public class FsService extends DBService {
    private String ROOT; 
    protected final Logger m_logger = LoggerFactory.getLogger(getClass());
    
    private Map<String, FsStore> m_stores = new HashMap<>();
    
    private final Object m_sync = new Object();
    
    public FsService(Tenant tenant) {
        super(tenant);
        
        ROOT = getParamString("db-path");
        if(ROOT == null) throw new RuntimeException("FsService: db-path not defined");
        m_logger.info("Using FS API");
        File root = new File(ROOT);
        if(!root.exists()) root.mkdirs();
    }

    @Override public void stopService() {
        for(FsStore store: m_stores.values()) store.close();
    }
    
    @Override public void createNamespace() {
        synchronized (m_sync) {
            File namespaceDir = new File(ROOT + "/" + getTenant().getName());
            if(!namespaceDir.exists())namespaceDir.mkdir();
        }
    }

    @Override public void dropNamespace() {
        synchronized(m_sync) {
            File namespaceDir = new File(ROOT + "/" + getTenant().getName());
            FileUtils.deleteDirectory(namespaceDir);
        }
    }
    
    @Override public void createStoreIfAbsent(String storeName, boolean bBinaryValues) {
        synchronized(m_sync) {
            File storeDir = new File(ROOT + "/" + getTenant().getName() + "/" + storeName);
            if(!storeDir.exists()) storeDir.mkdir();
        }
    }
    
    @Override public void deleteStoreIfPresent(String storeName) {
        synchronized(m_sync) {
        	closeStore(getTenant().getName(), storeName);
            File storeDir = new File(ROOT + "/" + getTenant().getName() + "/" + storeName);
            if(storeDir.exists()) FileUtils.deleteDirectory(storeDir);
        }
    }
    
    @Override public void commit(DBTransaction dbTran) {
        synchronized(m_sync) {
        	String keyspace = dbTran.getTenant().getName();
        	Set<String> stores = new HashSet<String>();
            Map<String, Map<String, List<DColumn>>> columnUpdates = dbTran.getColumnUpdatesMap();
            Map<String, Map<String, List<String>>> columnDeletes = dbTran.getColumnDeletesMap();
            Map<String, List<String>> rowDeletes = dbTran.getRowDeletesMap();
            stores.addAll(columnUpdates.keySet());
            stores.addAll(columnDeletes.keySet());
            stores.addAll(rowDeletes.keySet());
            
            for(String storeName: stores) {
                FsStore store = getStore(keyspace, storeName);
                store.addMutations(columnUpdates.get(storeName), columnDeletes.get(storeName), rowDeletes.get(storeName));
            }
        }
    }
    
    @Override public List<DColumn> getColumns(String storeName, String rowKey, String startColumn, String endColumn, int count) {
        synchronized(m_sync) {
            FsStore store = getStore(getTenant().getName(), storeName);
            return store.getColumns(rowKey, startColumn, endColumn, count);
        }
    }

    @Override
    public List<DColumn> getColumns(String storeName, String rowKey, Collection<String> columnNames) {
        synchronized(m_sync) {
            FsStore store = getStore(getTenant().getName(), storeName);
            return store.getColumns(rowKey, columnNames);
        }
    }

    @Override public List<String> getRows(String storeName, String continuationToken, int count) {
        synchronized (m_sync) {
            List<String> list = new ArrayList<>();
            File storeDir = new File(ROOT + "/" + getTenant().getName() + "/" + storeName);
            if(!storeDir.exists()) return list;
            for(File rowFile: storeDir.listFiles()) {
                String row = FileUtils.decode(rowFile.getName());
                if(continuationToken != null && continuationToken.compareTo(row) >= 0) continue;
                list.add(row);
                if(list.size() >= count) break;
            }
            Collections.sort(list);
            return list;
        }
    }


    private FsStore getStore(String namespace, String storeName) {
        String path = ROOT + "/" + namespace + "/" + storeName;
        FsStore store = m_stores.get(path);
        if(store == null) {
            File tenantDir = new File(ROOT + "/" + namespace);
            if(!tenantDir.exists()) tenantDir.mkdir();
            store = new FsStore(tenantDir, storeName);
            m_stores.put(path, store);
        }
        return store;
    }
    
    private void closeStore(String namespace, String storeName) {
        String path = ROOT + "/" + namespace + "/" + storeName;
        FsStore store = m_stores.get(path);
        if(store == null) return;
        store.close();
        m_stores.remove(path);
    }
    
}
