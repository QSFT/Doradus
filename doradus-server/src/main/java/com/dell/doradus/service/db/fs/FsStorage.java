package com.dell.doradus.service.db.fs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.dell.doradus.olap.io.BSTR;

public class FsStorage {
    public static final int MAX_MEM_VALUE = 64;
    private HashMap<BSTR, FsKeyspace> m_keyspaces = new HashMap<>();
    
    public FsStorage() { }
    
    public FsKeyspace getKeyspace(BSTR name) {
        return m_keyspaces.get(name);
    }

    public FsKeyspace getOrCreateKeyspace(BSTR name) {
        FsKeyspace keyspace = m_keyspaces.get(name);
        if(keyspace == null) {
            keyspace = new FsKeyspace(name);
            m_keyspaces.put(name, keyspace);
        }
        return keyspace;
    }

    
    public void deleteKeyspace(BSTR name) {
        m_keyspaces.remove(name);
    }
    
    public List<FsKeyspace> getKeyspaces() {
        return new ArrayList<FsKeyspace>(m_keyspaces.values());
    }

    
    public void add(BSTR keyspaceName, BSTR storeName, BSTR rowName, BSTR columnName, BSTR columnValue, long offset) {
        if(keyspaceName == null || keyspaceName.length == 0) return;
        FsKeyspace keyspace = getOrCreateKeyspace(keyspaceName);
        if(storeName == null || storeName.length == 0) return;
        FsStore store = keyspace.getOrCreateStore(storeName);
        if(rowName == null || rowName.length == 0) return;
        FsRow row = store.getOrCreateRow(rowName);
        if(columnName == null || columnName.length == 0) return;
        FsColumn column = row.getOrCreateColumn(columnName);
        if(columnValue.length < MAX_MEM_VALUE) column.setValue(columnValue);
        else column.setValueRef(offset);
    }

    public void delete(BSTR keyspaceName, BSTR storeName, BSTR rowName, BSTR columnName) {
        if(keyspaceName == null || keyspaceName.length == 0) return;
        FsKeyspace keyspace = getKeyspace(keyspaceName);
        if(keyspace == null) return;
        if(storeName == null || storeName.length == 0) {
            deleteKeyspace(keyspaceName);
            return;
        }
        FsStore store = keyspace.getStore(storeName);
        if(store == null) return;
        if(rowName == null || rowName.length == 0) {
            keyspace.deleteStore(storeName);
            return;
        }
        FsRow row = store.getRow(rowName);
        if(row == null) return;
        if(columnName == null || columnName.length == 0) {
            store.deleteRow(rowName);
        }
        FsColumn column = row.getColumn(columnName);
        if(column == null) return;
        row.deleteColumn(columnName);
    }
    
    
}
