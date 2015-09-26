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
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dell.doradus.core.ServerConfig;
import com.dell.doradus.service.db.ColumnDelete;
import com.dell.doradus.service.db.ColumnUpdate;
import com.dell.doradus.service.db.DBService;
import com.dell.doradus.service.db.DBTransaction;
import com.dell.doradus.service.db.DColumn;
import com.dell.doradus.service.db.RowDelete;
import com.dell.doradus.service.db.Tenant;

public class FsService extends DBService {
    public static final String ROOT = "c:/temp/FS"; 
    protected final Logger m_logger = LoggerFactory.getLogger(getClass().getSimpleName());
    private static final FsService INSTANCE = new FsService();
    private final Object m_sync = new Object();
    
    private FsService() { }

    public static FsService instance() { return INSTANCE; }
    
    @Override public void initService() {
        ServerConfig config = ServerConfig.getInstance();
        m_logger.info("Using FS API");
        m_logger.debug("Default application keyspace: {}", config.keyspace);
    }

    @Override public void startService() {
        File root = new File(ROOT);
        if(!root.exists()) root.mkdirs();
    }
    
    @Override public void stopService() { }
    
    @Override public boolean supportsNamespaces() {
        return true;
    }

    @Override public void createNamespace(Tenant tenant) {
        synchronized (m_sync) {
            File namespaceDir = new File(ROOT + "/" + tenant.getName());
            if(!namespaceDir.exists())namespaceDir.mkdir();
        }
    }

    @Override public void dropNamespace(Tenant tenant) {
        synchronized(m_sync) {
            File namespaceDir = new File(ROOT + "/" + tenant.getName());
            deleteDirectory(namespaceDir);
        }
    }
    
    private void deleteDirectory(File dir) {
        for(File file: dir.listFiles()) {
            if(file.isDirectory()) deleteDirectory(file);
            else file.delete();
        }
        dir.delete();
    }
    
    public List<String> getNamespaces() {
        List<String> namespaces = new ArrayList<>();
        synchronized (m_sync) {
            File root = new File(ROOT);
            for(String namespace: root.list()) {
                namespaces.add(namespace);
            }
        }
        return namespaces;
    }

    @Override public void createStoreIfAbsent(Tenant tenant, String storeName, boolean bBinaryValues) {
        synchronized(m_sync) {
            File storeDir = new File(ROOT + "/" + tenant.getName() + "/" + storeName);
            if(!storeDir.exists()) storeDir.mkdir();
        }
    }
    
    @Override public void deleteStoreIfPresent(Tenant tenant, String storeName) {
        synchronized(m_sync) {
            File storeDir = new File(ROOT + "/" + tenant.getName() + "/" + storeName);
            if(storeDir.exists()) deleteDirectory(storeDir);
        }
    }
    
    @Override public DBTransaction startTransaction(Tenant tenant) {
   		return new DBTransaction(tenant.getName());
    }
    
    public String encode(String name) {
        StringBuilder sb = new StringBuilder();
        for(int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            if(c != '_' && Character.isLetterOrDigit(c)) sb.append(c);
            else {
                String esc = String.format("_%02x", (int)c);
                sb.append(esc);
            }
        }
        return sb.toString();
    }
    
    public String decode(String name) {
        StringBuilder sb = new StringBuilder();
        for(int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            if(c != '_') sb.append(c);
            else {
                c = (char)Integer.parseInt(name.substring(i + 1, i + 3), 16);
                sb.append(c);
                i += 2;
            }
        }
        return sb.toString();
    }
    
    @Override public void commit(DBTransaction dbTran) {
        synchronized(m_sync) {
        	String keyspace = dbTran.getNamespace();
        	//1. update
        	for(ColumnUpdate mutation: dbTran.getColumnUpdates()) {
        	    String store = mutation.getStoreName();
        	    String row = encode(mutation.getRowKey());
                String rowPath = ROOT + "/" + keyspace + "/" + store + "/" + row;
                File rowFile = new File(rowPath);
                if(!rowFile.exists()) rowFile.mkdir();
                DColumn c = mutation.getColumn();
                String column = encode(c.getName());
                byte[] value = c.getRawValue();
                try {
                    FileOutputStream stream = new FileOutputStream(rowPath + "/" + column);
                    stream.write(value);
                    stream.close();
                } catch (IOException ex) {
                    m_logger.warn("Error", ex);
                }
        	}
    		//2. delete columns
            for(ColumnDelete mutation: dbTran.getColumnDeletes()) {
    			String store = mutation.getStoreName();
                String row = encode(mutation.getRowKey());
                String column = encode(mutation.getColumnName());
                String path = ROOT + "/" + keyspace + "/" + store + "/" + row + "/" + column;
                File columnFile = new File(path);
                if(columnFile.exists()) columnFile.delete();
    		}
            //3. delete rows
            for(RowDelete mutation: dbTran.getRowDeletes()) {
                String store = mutation.getStoreName();
                String row = encode(mutation.getRowKey());
                String path = ROOT + "/" + keyspace + "/" + store + "/" + row;
                File rowFile = new File(path);
                if(rowFile.exists()) deleteDirectory(rowFile);
            }
        }
    }
    
    @Override public List<DColumn> getColumns(String namespace, String storeName,
            String rowKey, String startColumn, String endColumn, int count) {
        synchronized(m_sync) {
            rowKey = encode(rowKey);
            try {
                ArrayList<DColumn> list = new ArrayList<>();
                File rowFile = new File(ROOT + "/" + namespace + "/" + storeName + "/" + rowKey);
                if(!rowFile.exists()) return list;
                for(File columnFile: rowFile.listFiles()) {
                    String fileName = columnFile.getName();
                    String colName = decode(fileName);
                    if(startColumn != null && colName.compareTo(startColumn) < 0) continue;
                    if(endColumn != null && colName.compareTo(endColumn) >= 0) continue;
                    FileInputStream fis = new FileInputStream(columnFile);
                    byte[] data = new byte[(int)columnFile.length()];
                    fis.read(data);
                    fis.close();
                    list.add(new DColumn(colName, data));
                    if(list.size() >= count) break;
                }
                Collections.sort(list);
                return list;
            }catch(IOException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    @Override
    public List<DColumn> getColumns(String namespace, String storeName,
            String rowKey, Collection<String> columnNames) {
        synchronized(m_sync) {
            rowKey = encode(rowKey);
            try {
                ArrayList<DColumn> list = new ArrayList<>();
                File rowFile = new File(ROOT + "/" + namespace + "/" + storeName + "/" + rowKey);
                if(!rowFile.exists()) return list;
                for(String columnName: columnNames) {
                    File columnFile = new File(rowFile.getPath() + "/" + columnName);
                    if(!columnFile.exists()) continue;
                    FileInputStream fis = new FileInputStream(columnFile);
                    byte[] data = new byte[(int)columnFile.length()];
                    fis.read(data);
                    fis.close();
                    list.add(new DColumn(columnName, data));
                }
                Collections.sort(list);
                return list;
            }catch(IOException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    @Override public List<String> getRows(String namespace, String storeName, String continuationToken, int count) {
        synchronized (m_sync) {
            List<String> list = new ArrayList<>();
            File storeDir = new File(ROOT + "/" + namespace + "/" + storeName);
            if(!storeDir.exists()) return list;
            for(File rowFile: storeDir.listFiles()) {
                String row = decode(rowFile.getName());
                if(continuationToken != null && continuationToken.compareTo(row) >= 0) continue;
                list.add(row);
                if(list.size() >= count) break;
            }
            Collections.sort(list);
            return list;
        }
    }


}
