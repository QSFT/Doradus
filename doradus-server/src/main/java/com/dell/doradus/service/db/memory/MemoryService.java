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
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import com.dell.doradus.service.db.ColumnDelete;
import com.dell.doradus.service.db.ColumnUpdate;
import com.dell.doradus.service.db.DBService;
import com.dell.doradus.service.db.DBTransaction;
import com.dell.doradus.service.db.DColumn;
import com.dell.doradus.service.db.RowDelete;
import com.dell.doradus.service.db.Tenant;

public class MemoryService extends DBService {
	public static class Row {
		public String name;
		public SortedMap<String, byte[]> columns = new TreeMap<>();
		
		public Row(String name) {
			this.name = name;
		}

		public String getKey() { return name; }

		public List<DColumn> getColumns() {
			List<DColumn> result = new ArrayList<DColumn>();
			for(Map.Entry<String, byte[]> e: columns.entrySet()) {
				result.add(new DColumn(e.getKey(), e.getValue()));
			}
			return result;
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
	
    private final Object m_sync = new Object(); 

    // holds ALL data
    private final Map<String, Keyspace> m_Keyspaces = new HashMap<>();
    
    public MemoryService(Tenant tenant) {
        super(tenant);

        m_logger.info("Using MEMORY API");
        m_logger.warn("Memory API is not persistent and can be used ONLY for testing purposes!");
    }

    @Override public void stopService() { }

    @Override public void createNamespace() {
    	synchronized (m_sync) {
    	    String namespace = getTenant().getName();
    		if(m_Keyspaces.get(namespace) != null) return;
    		Keyspace ks = new Keyspace(namespace);
    		m_Keyspaces.put(namespace, ks);
		}
    }

    @Override public void dropNamespace() {
    	synchronized (m_sync) {
            String namespace = getTenant().getName();
    		if(m_Keyspaces.get(namespace) == null) return;
    		m_Keyspaces.remove(namespace);
		}
    }
    
    public List<String> getNamespaces() {
        List<String> namespaces = new ArrayList<>();
        synchronized (m_sync) {
            for (String keyspace : m_Keyspaces.keySet()) {
                namespaces.add(keyspace);
            }
        }
        return namespaces;
    }

    @Override public void createStoreIfAbsent(String storeName, boolean bBinaryValues) {
    	synchronized (m_sync) {
    		Keyspace ks = m_Keyspaces.get(getTenant().getName());
    		if(ks == null) throw new RuntimeException("Keyspace does not exist");
    		Store st = ks.stores.get(storeName);
    		if(st == null) ks.stores.put(storeName, new Store(storeName));
		}
    }
    
    @Override public void deleteStoreIfPresent(String storeName) {
    	synchronized (m_sync) {
    		Keyspace ks = m_Keyspaces.get(getTenant().getName());
    		if(ks == null) throw new RuntimeException("Keyspace does not exist");
    		Store st = ks.stores.get(storeName);
    		if(st != null) ks.stores.remove(storeName);
		}
    }
    
    @Override public void commit(DBTransaction dbTran) {
        synchronized(m_sync) {
            String keyspace = dbTran.getTenant().getName();
            Keyspace ks = m_Keyspaces.get(keyspace);
            if(ks == null) throw new RuntimeException("Keyspace " + keyspace + " does not exist");
            //1. update
            for(ColumnUpdate mutation: dbTran.getColumnUpdates()) {
                String table = mutation.getStoreName();
                Store store = ks.stores.get(table);
                if(store == null) {
                    store = new Store(table);
                    ks.stores.put(table, store);
                }
                String row = mutation.getRowKey();
                Row mrow = store.rows.get(row);
                if(mrow == null) {
                    mrow = new Row(row);
                    store.rows.put(row, mrow);
                }
                DColumn c = mutation.getColumn();
                mrow.columns.put(c.getName(), c.getRawValue());
            }
            //2. delete columns
            for(ColumnDelete mutation: dbTran.getColumnDeletes()) {
                String table = mutation.getStoreName();
                Store store = ks.stores.get(table);
                if(store == null) {
                    store = new Store(table);
                    ks.stores.put(table, store);
                }
                String row = mutation.getRowKey();
                Row mrow = store.rows.get(row);
                if(mrow == null) {
                    mrow = new Row(row);
                    store.rows.put(row, mrow);
                }
                String column = mutation.getColumnName();
                mrow.columns.remove(column);
            }
            //3. delete rows
            for(RowDelete mutation: dbTran.getRowDeletes()) {
                String table = mutation.getStoreName();
                Store store = ks.stores.get(table);
                if(store == null) {
                    store = new Store(table);
                    ks.stores.put(table, store);
                }
                String row = mutation.getRowKey();
                store.rows.remove(row);
            }
        }
    }
    
    private Keyspace getKeyspace(String namespace) {
		Keyspace ks = m_Keyspaces.get(namespace);
		if(ks == null) {
			ks = new Keyspace(namespace);
			m_Keyspaces.put(namespace, ks);
		}
		return ks;
    }
    
    @Override public List<DColumn> getColumns(String storeName, String rowKey, String startColumn, String endColumn, int count) {
        synchronized (m_sync) {
            Keyspace ks = getKeyspace(getTenant().getName());
            Store st = ks.stores.get(storeName);
            if(st == null) throw new RuntimeException("Store does not exist");
            List<DColumn> list = new ArrayList<>();
            Row r = st.rows.get(rowKey);
            if(r == null) return list;
            for(Map.Entry<String, byte[]> e: r.columns.entrySet()) {
                if(startColumn != null && startColumn.compareTo(e.getKey()) > 0) continue;
                if(endColumn != null && endColumn.compareTo(e.getKey()) <= 0) continue;
                list.add(new DColumn(e.getKey(), e.getValue()));
                if(list.size() >= count) break;
            }
            return list;
        }
    }

    @Override public List<DColumn> getColumns(String storeName, String rowKey, Collection<String> columnNames) {
        synchronized (m_sync) {
            Keyspace ks = getKeyspace(getTenant().getName());
            Store st = ks.stores.get(storeName);
            if(st == null) throw new RuntimeException("Store does not exist");
            List<DColumn> list = new ArrayList<>();
            Row r = st.rows.get(rowKey);
            if(r == null) return list;
            for(String columnName: columnNames) {
                byte[] data = r.columns.get(columnName);
                if(data == null) continue;
                list.add(new DColumn(columnName, data));
            }
            return list;
        }
    }

    @Override public List<String> getRows(String storeName, String continuationToken, int count) {
        synchronized (m_sync) {
            List<String> list = new ArrayList<>();
            Keyspace ks = getKeyspace(getTenant().getName());
            Store st = ks.stores.get(storeName);
            if(st == null) return list;
            for(Row row: st.rows.values()) {
                if(continuationToken != null && continuationToken.compareTo(row.getKey()) >= 0) continue;
                list.add(row.getKey());
                if(list.size() >= count) break;
            }
            return list;
        }
    }


}
