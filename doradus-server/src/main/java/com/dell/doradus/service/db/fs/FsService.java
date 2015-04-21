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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dell.doradus.core.ServerConfig;
import com.dell.doradus.olap.io.BSTR;
import com.dell.doradus.service.db.DBService;
import com.dell.doradus.service.db.DBTransaction;
import com.dell.doradus.service.db.DColumn;
import com.dell.doradus.service.db.DRow;
import com.dell.doradus.service.db.Tenant;
import com.dell.doradus.service.tenant.UserDefinition;
import com.dell.doradus.utilities.Timer;

public class FsService extends DBService {
    protected final Logger m_logger = LoggerFactory.getLogger(getClass().getSimpleName());
    private static final FsService INSTANCE = new FsService();
    private final Object m_sync = new Object();
    private static final BSTR EMPTY = new BSTR(new byte[0]);
    
    private FsStorage m_storage = new FsStorage();
    private Database m_database;
    
    private FsService() { }

    public static FsService instance() { return INSTANCE; }
    
    @Override public void initService() {
        ServerConfig config = ServerConfig.getInstance();
        m_logger.info("Using FS API");
        m_logger.debug("Default application keyspace: {}", config.keyspace);
    }

    @Override public void startService() {
        m_database = new Database("c:/temp/doradus.dat");
        Timer t = new Timer();
        while(!m_database.isEnd()) {
            int flag = m_database.readNextFlag();
            BSTR keyspace = m_database.readNext();
            BSTR store = m_database.readNext();
            BSTR row = m_database.readNext();
            BSTR column = m_database.readNext();
            if(flag == 1) { // add
                long offset = m_database.position();
                BSTR value = m_database.readNext();
                m_storage.add(keyspace, store, row, column, value, offset);
            } else if (flag == 2) { // delete
                m_storage.delete(keyspace, store, row, column);
            }
        }
        m_logger.info("Database loaded in {}", t);
    }
    
    public void flush(String dbfile) {
        Database db = new Database(dbfile);
        synchronized (m_sync) {
            for(FsKeyspace keyspace: m_storage.getKeyspaces()) {
                for(FsStore store: keyspace.getStores()) {
                    for(FsRow row: store.getRows()) {
                        for(FsColumn column: row.getColumns()) {
                            BSTR value = null;
                            if(column.hasValue()) value = column.getValue();
                            else {
                                value = m_database.read(column.getStoredOffset());
                            }
                            db.writeFlag(1); // add
                            db.write(keyspace.getName());
                            db.write(store.getName());
                            db.write(row.getName());
                            db.write(column.getName());
                            db.write(value);
                        }
                    }
                }
            }
        
        }        
        db.flush();
        db.close();
    }
    
    @Override public void stopService() {
        m_database.close();
        m_database = null;
        m_database = null;
    }
    
    private void add(BSTR keyspace, BSTR store, BSTR row, BSTR column, BSTR value) {
        synchronized (m_sync) {
            m_database.writeFlag(1); // add
            m_database.write(keyspace);
            m_database.write(store);
            m_database.write(row);
            m_database.write(column);
            long offset = m_database.length();
            m_database.write(value);
            m_database.flush();
            m_storage.add(keyspace, store, row, column, value, offset);
        }
    }

    private void delete(BSTR keyspace, BSTR store, BSTR row, BSTR column) {
        synchronized (m_sync) {
            m_database.writeFlag(2); // delete
            m_database.write(keyspace);
            m_database.write(store);
            m_database.write(row);
            m_database.write(column);
            m_storage.delete(keyspace, store, row, column);
            m_database.flush();
        }
    }
    
    @Override public void createTenant(Tenant tenant, Map<String, String> options) {
        BSTR keyspace = new BSTR(tenant.getKeyspace());
        add(keyspace, EMPTY, EMPTY, EMPTY, EMPTY);
    }

    @Override public void modifyTenant(Tenant tenant, Map<String, String> options) {}
    
    @Override public void dropTenant(Tenant tenant) {
        BSTR keyspace = new BSTR(tenant.getKeyspace());
        delete(keyspace, EMPTY, EMPTY, EMPTY);
    }
    
    @Override public void addUsers(Tenant tenant, Iterable<UserDefinition> users) {
        throw new RuntimeException("This method is not supported for the Memory API");
    }
    
    @Override public void modifyUsers(Tenant tenant, Iterable<UserDefinition> users) {
        throw new RuntimeException("This method is not supported for the Memory API");
    }
    
    @Override public void deleteUsers(Tenant tenant, Iterable<UserDefinition> users) {
        throw new RuntimeException("This method is not supported for the Memory API");
    }
    
    @Override public Collection<Tenant> getTenants() {
        List<Tenant> tenants = new ArrayList<>();
        synchronized (m_sync) {
            for (FsKeyspace keyspace : m_storage.getKeyspaces()) {
                tenants.add(new Tenant(keyspace.getName().toString()));
            }
        }
        return tenants;
    }

    @Override public void createStoreIfAbsent(Tenant tenant, String storeName, boolean bBinaryValues) {
        BSTR keyspace = new BSTR(tenant.getKeyspace());
        BSTR store = new BSTR(storeName);
        add(keyspace, store, EMPTY, EMPTY, EMPTY);
    }
    
    @Override public void deleteStoreIfPresent(Tenant tenant, String storeName) {
        BSTR keyspace = new BSTR(tenant.getKeyspace());
        BSTR store = new BSTR(storeName);
        delete(keyspace, store, EMPTY, EMPTY);
    }
    
    @Override public DBTransaction startTransaction(Tenant tenant) {
   		return new FsTransaction(tenant.getKeyspace());
    }
    
    @Override public void commit(DBTransaction dbTran) {
    	FsTransaction t = (FsTransaction)dbTran;
    	BSTR keyspace = new BSTR(t.getKeyspace());
		for(Map.Entry<String, Map<String, List<DColumn>>> e: t.getUpdateMap().entrySet()) {
			BSTR store = new BSTR(e.getKey());
			Map<String, List<DColumn>> rows = e.getValue();
			for(Map.Entry<String, List<DColumn>> r: rows.entrySet()) {
				BSTR row = new BSTR(r.getKey());
				List<DColumn> columns = r.getValue();
				for(DColumn c: columns) {
					BSTR column = new BSTR(c.getName());
					BSTR value = new BSTR(c.getRawValue());
					add(keyspace, store, row, column, value);
				}
			}
		}
		for(Map.Entry<String, Map<String, List<String>>> e: t.getDeleteMap().entrySet()) {
			BSTR store = new BSTR(e.getKey());
			Map<String, List<String>> rows = e.getValue();
			for(Map.Entry<String, List<String>> r: rows.entrySet()) {
				BSTR row = new BSTR(r.getKey());
				List<String> columns = r.getValue();
				if(columns == null) {
				    delete(keyspace, store, row, EMPTY);
				} else {
    				for(String c: columns) {
    				    BSTR value = new BSTR(c);
                        delete(keyspace, store, row, value);
    				}
				}
			}
		}
    }
    
    @Override public Iterator<DColumn> getAllColumns(Tenant tenant, String storeName, String rowKey) {
        return getColumnSlice(tenant, storeName, rowKey, null, null);
    }

    @Override public Iterator<DColumn> getColumnSlice(Tenant tenant, String storeName, String rowKey,
                                            String startCol, String endCol, boolean reversed) {
    	if(reversed) throw new RuntimeException("Not supported");
        synchronized (m_sync) {
            BSTR s_keyspace = new BSTR(tenant.getKeyspace());
            BSTR s_store = new BSTR(storeName);
            BSTR s_row = new BSTR(rowKey);
            
            FsKeyspace keyspace = m_storage.getKeyspace(s_keyspace);
            if(keyspace == null) return new ArrayList<DColumn>(0).iterator();
            FsStore store = keyspace.getStore(s_store);
            if(store == null) return new ArrayList<DColumn>(0).iterator();
            FsRow row = store.getRow(s_row);
            if(row == null) return new ArrayList<DColumn>(0).iterator();
            List<FsColumn> columns = row.getColumns();
            List<DColumn> result = new ArrayList<DColumn>();
            BSTR start = startCol == null ? null : new BSTR(startCol);
            BSTR end = endCol == null ? null : new BSTR(endCol);
            
            for(FsColumn column: columns) {
                if(start != null && BSTR.compare(start, column.getName()) > 0) continue;
                if(end != null && BSTR.compare(end, column.getName()) <= 0) break;
                if(column.hasValue()) {
                    result.add(new DColumn(column.getName().toString(), column.getValue().buffer));
                } else {
                    BSTR value = m_database.read(column.getStoredOffset());
                    result.add(new DColumn(column.getName().toString(), value.buffer));
                }
            }
            
            return result.iterator();
        }
    }

    @Override public Iterator<DColumn> getColumnSlice(Tenant tenant, String storeName, String rowKey,
                                            String startCol, String endCol) {
        return getColumnSlice(tenant, storeName, rowKey, startCol, endCol, false);
    }

    @Override public DColumn getColumn(Tenant tenant, String storeName, String rowKey, String colName) {
    	synchronized (m_sync) {
    	    BSTR s_keyspace = new BSTR(tenant.getKeyspace());
    	    BSTR s_store = new BSTR(storeName);
    	    BSTR s_row = new BSTR(rowKey);
    	    BSTR s_column = new BSTR(colName);
    	    
    	    FsKeyspace keyspace = m_storage.getKeyspace(s_keyspace);
    	    if(keyspace == null) return null;
    	    FsStore store = keyspace.getStore(s_store);
    	    if(store == null) return null;
    	    FsRow row = store.getRow(s_row);
    	    if(row == null) return null;
    	    FsColumn column = row.getColumn(s_column);
    	    if(column == null) return null;
    	    if(column.hasValue()) {
    	        return new DColumn(column.getName().toString(), column.getValue().buffer);
    	    } else {
    	        BSTR value = m_database.read(column.getStoredOffset());
                return new DColumn(column.getName().toString(), value.buffer);
    	    }
		}
    }

    @Override public Iterator<DRow> getAllRowsAllColumns(Tenant tenant, String storeName) {
        synchronized (m_sync) {
            BSTR s_keyspace = new BSTR(tenant.getKeyspace());
            BSTR s_store = new BSTR(storeName);
            
            FsKeyspace keyspace = m_storage.getKeyspace(s_keyspace);
            if(keyspace == null) return new ArrayList<DRow>(0).iterator();
            FsStore store = keyspace.getStore(s_store);
            if(store == null) return new ArrayList<DRow>(0).iterator();
            
            List<FsRow> rows = store.getRows();
            List<DRow> result = new ArrayList<DRow>();
            for(FsRow row: rows) {
                List<FsColumn> columns = row.getColumns();
                List<DColumn> r = new ArrayList<DColumn>();
                
                for(FsColumn column: columns) {
                    if(column.hasValue()) {
                        r.add(new DColumn(column.getName().toString(), column.getValue().buffer));
                    } else {
                        BSTR value = m_database.read(column.getStoredOffset());
                        r.add(new DColumn(column.getName().toString(), value.buffer));
                    }
                }
                result.add(new RowIter(row.getName().toString(), r));
            }
            return result.iterator();
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
