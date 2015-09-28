package com.dell.doradus.service.db.fs;

import java.util.Collection;
import java.util.HashMap;
import java.util.Set;

import com.dell.doradus.olap.io.BSTR;

public class FsMemStore {
    private String m_name;
    private FsDataStore m_dataStore;
    private HashMap<BSTR, FsRow> m_rows = new HashMap<>();

    public FsMemStore(String name, FsDataStore dataStore) {
        m_name = name;
        m_dataStore = dataStore;
    }
    
    public String getName() { return m_name; }
    
    public FsRow getRow(BSTR row) {return m_rows.get(row); }
    
    public int getRowsCount() { return m_rows.size(); }
    
    public FsRow getOrCreateRow(BSTR row) {
        FsRow r = m_rows.get(row);
        if(r == null) {
            r = new FsRow(row, m_dataStore);
            m_rows.put(row, r);
        }
        return r;
    }
    
    public Collection<FsRow> getRows() { return m_rows.values(); }
    
    public FsColumn createColumn(BSTR row, int operation, BSTR column, byte[] value) {
        FsRow r = getOrCreateRow(row);
        return r.createColumn(operation, column, value);
    }

    public FsColumn deleteColumn(BSTR row, BSTR column) {
        return createColumn(row, FsMutation.DELETE_COLUMN, column, FileUtils.EMPTY_BYTES);
    }
    
    public void deleteRow(BSTR row) {
        FsRow r = getOrCreateRow(row);
        r.deleteRow();
    }
    
    public void clear() {
        m_rows.clear();
    }
    
    
    public void getColumns(BSTR rowKey, FsReadColumns columns, Collection<BSTR> columnNames) {
        FsRow row = getRow(rowKey);
        if(row == null) return;
        row.getColumns(columns, columnNames);
    }
    
    public void getColumns(BSTR rowKey, FsReadColumns columns, FsColumn startColumn, FsColumn endColumn, int count) {
        FsRow row = getRow(rowKey);
        if(row == null) return;
        row.getColumns(columns, startColumn, endColumn, count);
    }
    
    public void getRows(Set<FsRow> rows, BSTR continuationToken) {
        for(FsRow row: getRows()) {
            BSTR rowKey = row.getName();
            if(rows.contains(rowKey)) continue;
            if(continuationToken != null && continuationToken.compareTo(rowKey) >= 0) continue;
            rows.add(row);
        }
    }
    
    @Override
    public String toString() {
        return m_name + "(" + m_rows.size() + ") rows";
    }
    
}
