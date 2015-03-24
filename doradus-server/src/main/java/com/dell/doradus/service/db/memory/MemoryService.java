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

package com.dell.doradus.service.db.memory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import com.dell.doradus.core.ServerConfig;
import com.dell.doradus.service.db.DBService;
import com.dell.doradus.service.db.DBTransaction;
import com.dell.doradus.service.db.DColumn;
import com.dell.doradus.service.db.DRow;
import com.dell.doradus.service.db.Tenant;

public class MemoryService extends DBService {
	public static class Row implements DRow {
		public String name;
		public SortedMap<String, byte[]> columns = new TreeMap<>();
		
		public Row(String name) {
			this.name = name;
		}

		@Override public String getKey() { return name; }

		@Override public Iterator<DColumn> getColumns() {
			List<DColumn> result = new ArrayList<DColumn>();
			for(Map.Entry<String, byte[]> e: columns.entrySet()) {
				result.add(new DColumn(e.getKey(), e.getValue()));
			}
			return result.iterator();
		}
	}
	public static class Store {
		public String name;
		public Map<String, Row> rows = new HashMap<>();
		
		public Store(String name) {
			this.name = name;
		}
	}
	public static class Keyspace {
		public String name;
		public Map<String, Store> stores = new HashMap<>();
		
		public Keyspace(String name) {
			this.name = name;
		}
	}
	
    private static final MemoryService INSTANCE = new MemoryService();
    private final Object m_sync = new Object(); 

    // holds ALL data
    private final Map<String, Keyspace> m_Keyspaces = new HashMap<>();
    
    private MemoryService() { }

    public static MemoryService instance() {return INSTANCE;}
    
    @Override public void initService() {
        ServerConfig config = ServerConfig.getInstance();
        m_logger.info("Using MEMORY API");
        m_logger.warn("Memory API is not persistent and can be used ONLY for testing purposes!");
        m_logger.debug("Default application keyspace: {}", config.keyspace);
    }

    @Override public void startService() { }
    @Override public void stopService() { }

    @Override public void createTenant(Tenant tenant, Map<String, String> options) {
    	synchronized (m_sync) {
    	    String keyspace = tenant.getKeyspace();
    		if(m_Keyspaces.get(keyspace) != null) return;
    		Keyspace ks = new Keyspace(keyspace);
    		m_Keyspaces.put(keyspace, ks);
		}
    }

    @Override public void dropTenant(Tenant tenant) {
    	synchronized (m_sync) {
            String keyspace = tenant.getKeyspace();
    		if(m_Keyspaces.get(keyspace) == null) return;
    		m_Keyspaces.remove(keyspace);
		}
    }
    
    @Override public void addUsers(Tenant tenant, Map<String, String> users) {
        throw new RuntimeException("This method is not supported for the Memory API");
    }
    
    @Override public Collection<Tenant> getTenants() {
        List<Tenant> tenants = new ArrayList<>();
        synchronized (m_sync) {
            for (String keyspace : m_Keyspaces.keySet()) {
                tenants.add(new Tenant(keyspace));
            }
        }
        return tenants;
    }

    @Override public void createStoreIfAbsent(Tenant tenant, String storeName, boolean bBinaryValues) {
    	synchronized (m_sync) {
    		Keyspace ks = m_Keyspaces.get(tenant.getKeyspace());
    		if(ks == null) throw new RuntimeException("Keyspace does not exist");
    		Store st = ks.stores.get(storeName);
    		if(st == null) ks.stores.put(storeName, new Store(storeName));
		}
    }
    
    @Override public void deleteStoreIfPresent(Tenant tenant, String storeName) {
    	synchronized (m_sync) {
    		Keyspace ks = m_Keyspaces.get(tenant.getKeyspace());
    		if(ks == null) throw new RuntimeException("Keyspace does not exist");
    		Store st = ks.stores.get(storeName);
    		if(st != null) ks.stores.remove(storeName);
		}
    }
    
    @Override public DBTransaction startTransaction(Tenant tenant) {
    	synchronized (m_sync) {
    		Keyspace ks = getKeyspace(tenant);
    		return new MemoryTransaction(ks.name);
		}
    }
    
    @Override public void commit(DBTransaction dbTran) {
    	MemoryTransaction t = (MemoryTransaction)dbTran;
    	synchronized (m_sync) {
        	Keyspace ks = m_Keyspaces.get(t.getKeyspace());
    		if(ks == null) throw new RuntimeException("Keyspace does not exist");
    		for(Map.Entry<String, Map<String, List<DColumn>>> e: t.getUpdateMap().entrySet()) {
    			String table = e.getKey();
    			Map<String, List<DColumn>> rows = e.getValue();
    			Store store = ks.stores.get(table);
    			if(store == null) {
    				store = new Store(table);
    				ks.stores.put(table, store);
    			}
    			for(Map.Entry<String, List<DColumn>> r: rows.entrySet()) {
    				String row = r.getKey();
    				List<DColumn> columns = r.getValue();
    				Row mrow = store.rows.get(row);
    				if(mrow == null) {
    					mrow = new Row(row);
    					store.rows.put(row, mrow);
    				}
    				for(DColumn c: columns) {
    					String col = c.getName();
    					byte[] val = c.getRawValue();
    					mrow.columns.put(col, val);
    				}
    			}
    		}
    		for(Map.Entry<String, Map<String, List<String>>> e: t.getDeleteMap().entrySet()) {
    			String table = e.getKey();
    			Map<String, List<String>> rows = e.getValue();
    			Store store = ks.stores.get(table);
    			if(store == null) continue;
    			for(Map.Entry<String, List<String>> r: rows.entrySet()) {
    				String row = r.getKey();
    				List<String> columns = r.getValue();
    				Row mrow = store.rows.get(row);
    				if(mrow == null) continue;
    				if(columns == null) {
    					store.rows.remove(row);
    				} else {
	    				for(String c: columns) {
	    					mrow.columns.remove(c);
	    				}
    				}
    			}
    		}
		}
    }
    
    private Keyspace getKeyspace(Tenant tenant) {
		String keyspace = tenant.getKeyspace();
		Keyspace ks = m_Keyspaces.get(keyspace);
		if(ks == null) {
			ks = new Keyspace(keyspace);
			m_Keyspaces.put(keyspace, ks);
		}
		return ks;
    }
    
    @Override public Iterator<DColumn> getAllColumns(Tenant tenant, String storeName, String rowKey) {
    	synchronized (m_sync) {
    		Keyspace ks = getKeyspace(tenant);
    		Store st = ks.stores.get(storeName);
    		if(st == null) return new ArrayList<DColumn>(0).iterator();
    		Row r = st.rows.get(rowKey);
    		if(r == null) return new ArrayList<DColumn>(0).iterator();
    		List<DColumn> list = new ArrayList<>();
    		for(Map.Entry<String, byte[]> e: r.columns.entrySet()) {
    			list.add(new DColumn(e.getKey(), e.getValue()));
    		}
    		return list.iterator();
		}
    }

    @Override public Iterator<DColumn> getColumnSlice(Tenant tenant, String storeName, String rowKey,
                                            String startCol, String endCol, boolean reversed) {
    	if(reversed) throw new RuntimeException("Not supported");
    	synchronized (m_sync) {
    		Keyspace ks = getKeyspace(tenant);
    		Store st = ks.stores.get(storeName);
    		if(st == null) throw new RuntimeException("Store does not exist");
    		Row r = st.rows.get(rowKey);
    		if(r == null) return new ArrayList<DColumn>(0).iterator();
    		List<DColumn> list = new ArrayList<>();
    		for(Map.Entry<String, byte[]> e: r.columns.entrySet()) {
    			if(startCol.compareTo(e.getKey()) > 0) continue;
    			if(endCol.compareTo(e.getKey()) <= 0) continue;
    			list.add(new DColumn(e.getKey(), e.getValue()));
    		}
    		return list.iterator();
		}
    }

    @Override public Iterator<DColumn> getColumnSlice(Tenant tenant, String storeName, String rowKey,
                                            String startCol, String endCol) {
        return getColumnSlice(tenant, storeName, rowKey, startCol, endCol, false);
    }

    @Override public DColumn getColumn(Tenant tenant, String storeName, String rowKey, String colName) {
    	synchronized (m_sync) {
    		Keyspace ks = getKeyspace(tenant);
    		Store st = ks.stores.get(storeName);
    		if(st == null) throw new RuntimeException("Store does not exist");
    		Row r = st.rows.get(rowKey);
    		if(r == null) return null;
    		byte[] val = r.columns.get(colName);
    		if(val == null) return null;
    		return new DColumn(colName, val);
		}
    }

    @Override public Iterator<DRow> getAllRowsAllColumns(Tenant tenant, String storeName) {
    	synchronized (m_sync) {
    		Keyspace ks = getKeyspace(tenant);
    		Store st = ks.stores.get(storeName);
    		if(st == null) return new ArrayList<DRow>(0).iterator();
    		List<DRow> list = new ArrayList<>();
    		for(Row row: st.rows.values()) {
    			list.add(row);
    		}
    		return list.iterator();
		}
    }
    
    @Override public Iterator<DRow> getRowsAllColumns(Tenant tenant, String storeName, Collection<String> rowKeys) {
    	throw new RuntimeException("Not supported");
    }

    @Override public Iterator<DRow> getRowsColumns(Tenant   tenant,
                                         String             storeName,
                                         Collection<String> rowKeys,
                                         Collection<String> colNames) {
    	throw new RuntimeException("Not supported");
    }
    
    @Override public Iterator<DRow> getRowsColumnSlice(Tenant   tenant,
                                             String             storeName,
                                             Collection<String> rowKeys,
                                             String             startCol,
                                             String             endCol) {
    	throw new RuntimeException("Not supported");
    }


}
