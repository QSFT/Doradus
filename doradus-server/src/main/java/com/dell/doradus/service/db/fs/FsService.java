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
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dell.doradus.common.UserDefinition;
import com.dell.doradus.core.ServerConfig;
import com.dell.doradus.service.db.DBService;
import com.dell.doradus.service.db.DBTransaction;
import com.dell.doradus.service.db.DColumn;
import com.dell.doradus.service.db.DRow;
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
    
    @Override public void createTenant(Tenant tenant, Map<String, String> options) {
        synchronized (m_sync) {
            String keyspace = tenant.getKeyspace();
            File keyspaceDir = new File(ROOT + "/" + keyspace);
            if(!keyspaceDir.exists())keyspaceDir.mkdir();
        }
    }

    @Override public void modifyTenant(Tenant tenant, Map<String, String> options) {
        throw new RuntimeException("Not implemented");
    }
    
    @Override public void dropTenant(Tenant tenant) {
        synchronized(m_sync) {
            String keyspace = tenant.getKeyspace();
            File keyspaceDir = new File(ROOT + "/" + keyspace);
            deleteDirectory(keyspaceDir);
        }
    }
    
    private void deleteDirectory(File dir) {
        for(File file: dir.listFiles()) {
            if(file.isDirectory()) deleteDirectory(file);
            else file.delete();
        }
        dir.delete();
    }
    
    @Override public void addUsers(Tenant tenant, Iterable<UserDefinition> users) {
        throw new RuntimeException("This method is not supported");
    }
    
    @Override public void modifyUsers(Tenant tenant, Iterable<UserDefinition> users) {
        throw new RuntimeException("This method is not supported");
    }
    
    @Override public void deleteUsers(Tenant tenant, Iterable<UserDefinition> users) {
        throw new RuntimeException("This method is not supported");
    }
    
    @Override public Collection<Tenant> getTenants() {
        List<Tenant> tenants = new ArrayList<>();
        synchronized (m_sync) {
            File root = new File(ROOT);
            for(String keyspace: root.list()) {
                tenants.add(new Tenant(keyspace));
            }
        }
        return tenants;
    }

    @Override public void createStoreIfAbsent(Tenant tenant, String storeName, boolean bBinaryValues) {
        synchronized(m_sync) {
            File storeDir = new File(ROOT + "/" + tenant.getKeyspace() + "/" + storeName);
            if(!storeDir.exists()) storeDir.mkdir();
        }
    }
    
    @Override public void deleteStoreIfPresent(Tenant tenant, String storeName) {
        synchronized(m_sync) {
            File storeDir = new File(ROOT + "/" + tenant.getKeyspace() + "/" + storeName);
            if(storeDir.exists()) deleteDirectory(storeDir);
        }
    }
    
    @Override public DBTransaction startTransaction(Tenant tenant) {
   		return new FsTransaction(tenant.getKeyspace());
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
        	FsTransaction t = (FsTransaction)dbTran;
        	String keyspace = t.getKeyspace();
        	//1. update
    		for(Map.Entry<String, Map<String, List<DColumn>>> e: t.getUpdateMap().entrySet()) {
    		    String store = e.getKey();
    			Map<String, List<DColumn>> rows = e.getValue();
    			for(Map.Entry<String, List<DColumn>> r: rows.entrySet()) {
    				String row = r.getKey();
    	            row = encode(row);
    				String rowPath = ROOT + "/" + keyspace + "/" + store + "/" + row;
    				File rowFile = new File(rowPath);
    				if(!rowFile.exists()) rowFile.mkdir();
    				List<DColumn> columns = r.getValue();
    				for(DColumn c: columns) {
    				    String column = c.getName();
    				    column = encode(column);
    				    byte[] value = c.getRawValue();
                        try {
                            FileOutputStream stream = new FileOutputStream(rowPath + "/" + column);
                            stream.write(value);
                            stream.close();
                        } catch (IOException ex) {
                            m_logger.warn("Error", ex);
                        }
    				}
    			}
    		}
    		//2. delete
    		for(Map.Entry<String, Map<String, List<String>>> e: t.getDeleteMap().entrySet()) {
    			String store = e.getKey();
    			Map<String, List<String>> rows = e.getValue();
    			for(Map.Entry<String, List<String>> r: rows.entrySet()) {
                    String row = r.getKey();
                    row = encode(row);
                    String rowPath = ROOT + "/" + keyspace + "/" + store + "/" + row;
    				List<String> columns = r.getValue();
                    File rowFile = new File(rowPath);
                    if(!rowFile.exists()) continue;
    				if(columns == null) {
    				    deleteDirectory(rowFile);
    				} else {
        				for(String c: columns) {
        				    File columnFile = new File(rowPath + "/" + encode(c));
        				    if(columnFile.exists()) columnFile.delete();
        				}
    				}
    			}
    		}
        }
    }
    
    @Override public Iterator<DColumn> getAllColumns(Tenant tenant, String storeName, String rowKey) {
        synchronized(m_sync) {
            rowKey = encode(rowKey);
            try {
                ArrayList<DColumn> list = new ArrayList<>();
                File rowFile = new File(ROOT + "/" + tenant.getKeyspace() + "/" + storeName + "/" + rowKey);
                if(!rowFile.exists()) return list.iterator();
                for(File columnFile: rowFile.listFiles()) {
                    FileInputStream fis = new FileInputStream(columnFile);
                    byte[] data = new byte[(int)columnFile.length()];
                    fis.read(data);
                    fis.close();
                    list.add(new DColumn(decode(columnFile.getName()), data));
                }
                Collections.sort(list);
                return list.iterator();
            }catch(IOException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    
    
    @Override public Iterator<DColumn> getColumnSlice(Tenant tenant, String storeName, String rowKey,
                                            String startCol, String endCol, boolean reversed) {
        synchronized(m_sync) {
            rowKey = encode(rowKey);
            try {
                ArrayList<DColumn> list = new ArrayList<>();
                File rowFile = new File(ROOT + "/" + tenant.getKeyspace() + "/" + storeName + "/" + rowKey);
                if(!rowFile.exists()) return list.iterator();
                for(File columnFile: rowFile.listFiles()) {
                    String fileName = columnFile.getName();
                    String colName = decode(fileName);
                    if(colName.compareTo(startCol) < 0) continue;
                    if(colName.compareTo(endCol) >= 0) continue;
                    FileInputStream fis = new FileInputStream(columnFile);
                    byte[] data = new byte[(int)columnFile.length()];
                    fis.read(data);
                    fis.close();
                    list.add(new DColumn(colName, data));
                }
                Collections.sort(list);
                if(reversed)Collections.reverse(list);
                return list.iterator();
            }catch(IOException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    @Override public Iterator<DColumn> getColumnSlice(Tenant tenant, String storeName, String rowKey,
                                            String startCol, String endCol) {
        return getColumnSlice(tenant, storeName, rowKey, startCol, endCol, false);
    }

    @Override public DColumn getColumn(Tenant tenant, String storeName, String rowKey, String colName) {
        synchronized(m_sync) {
            rowKey = encode(rowKey);
            try {
                File colFile = new File(ROOT + "/" + tenant.getKeyspace() + "/" + storeName + "/" + rowKey + "/" + encode(colName));
                if(!colFile.exists()) return null;
                FileInputStream fis = new FileInputStream(colFile);
                byte[] data = new byte[(int)colFile.length()];
                fis.read(data);
                fis.close();
                return new DColumn(colName, data);
            }catch(IOException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    @Override public Iterator<DRow> getAllRowsAllColumns(Tenant tenant, String storeName) {
        synchronized (m_sync) {
            List<DRow> rows = new ArrayList<>();
            File storeDir = new File(ROOT + "/" + tenant.getKeyspace() + "/" + storeName);
            if(!storeDir.exists()) return rows.iterator();
            for(File rowFile: storeDir.listFiles()) {
                String row = rowFile.getName();
                RowIter rowIter = new RowIter(decode(row), getAllColumns(tenant, storeName, row));
                rows.add(rowIter);
            }
            return rows.iterator();
        }
    }
    
    @Override public Iterator<DRow> getRowsAllColumns(Tenant tenant, String storeName, Collection<String> rowKeys) {
    	throw new RuntimeException("Not supported");
    }

    @Override public Iterator<DRow> getRowsColumns(Tenant   tenant,
                                         String             storeName,
                                         Collection<String> rowKeys,
                                         Collection<String> colNames) {
        synchronized (m_sync) {
            List<DRow> rows = new ArrayList<>();
            for(String rowKey: rowKeys) {
                List<DColumn> cols = new ArrayList<>();
                for(String col: colNames) {
                    DColumn c = getColumn(tenant, storeName, rowKey, col); 
                    if(c != null) cols.add(c);
                }
                rows.add(new RowIter(rowKey, cols.iterator()));
            }
            return rows.iterator();
        }
    }
    
    @Override public Iterator<DRow> getRowsColumnSlice(Tenant   tenant,
                                             String             storeName,
                                             Collection<String> rowKeys,
                                             String             startCol,
                                             String             endCol) {
    	throw new RuntimeException("Not supported");
    }


}
