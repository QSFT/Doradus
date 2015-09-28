package com.dell.doradus.service.db.fs;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import com.dell.doradus.olap.io.BSTR;

public class FsRow implements Comparable<FsRow> {
    private BSTR m_name;
    private FsDataStore m_dataStore;
    private HashMap<BSTR, FsColumn> m_columnsMap = new HashMap<>();
    private List<FsColumn> m_columnsList = new ArrayList<>();
    private boolean m_bSorted = true;
    private boolean m_bDeleted;

    public FsRow(BSTR name, FsDataStore dataStore) {
        m_name = name;
        m_dataStore = dataStore;
    }
    
    public BSTR getName() { return m_name; }
    
    public List<FsColumn> getColumns() {
        if(!m_bSorted) {
            Collections.sort(m_columnsList);
            m_bSorted = true;
        }
        return m_columnsList;
    }
    
    public void getColumns(FsReadColumns columns, Collection<BSTR> columnNames) {
        if(m_bDeleted) columns.setRowDeleted();
        for(BSTR columnName: columnNames) {
            FsColumn c = m_columnsMap.get(columnName);
            if(c == null || columns.containsColumn(c)) continue;
            columns.addColumn(c);
        }
    }
    
    public void getColumns(FsReadColumns columns, FsColumn startColumn, FsColumn endColumn, int count) {
        if(m_bDeleted) columns.setRowDeleted();
        int columnsCount = 0;
        
        if(!m_bSorted) {
            Collections.sort(m_columnsList);
            m_bSorted = true;
        }

        int startIndex = 0;
        int endIndex = m_columnsList.size();
        if(startColumn != null) {
            startIndex = Collections.binarySearch(m_columnsList, startColumn);
            if(startIndex < 0) startIndex = -startIndex - 1; // see binarySearch return value
        }
        if(endColumn != null) {
            endIndex = Collections.binarySearch(m_columnsList, endColumn);
            if(endIndex < 0) endIndex = -endIndex - 1; // see binarySearch return value
        }
        
        for(int i = startIndex; i < endIndex; i++) {
            FsColumn c = m_columnsList.get(i);
            if(columns.containsColumn(c)) continue;
            columns.addColumn(c);
            if(!c.isColumnDelete()) {
                columnsCount++;
                if(columnsCount >= count) break;
            }
        }
    }

    public int getColumnsCount() { return m_columnsMap.size(); }
    
    public FsColumn createColumn(int operation, BSTR columnName, byte[] value) {
        FsColumn c = m_columnsMap.get(columnName);
        if(c == null) {
            c = new FsColumn(operation, columnName, value);
            m_columnsMap.put(columnName, c);
            m_columnsList.add(c);
            m_bSorted = false;
        } else {
            if(c.isExternalValue() && operation != FsMutation.UPDATE_LARGE_COLUMN) {
                m_dataStore.delete(m_name.toString(), columnName.toString());
            }
            c.set(operation, columnName, value);
        }
        return c;
    }

    public void deleteRow() {
        m_columnsMap.clear();
        m_columnsList.clear();
        m_bSorted = true;
        m_bDeleted = true;
    }
    
    public boolean isDeleted() { return m_bDeleted; }
    
    @Override
    public String toString() {
        return m_name.toString() + " (" + m_columnsMap.size() + ") columns";
    }
    
    @Override
    public int hashCode() {
        return m_name.hashCode();
    }
    
    @Override
    public boolean equals(Object obj) {
        return m_name.equals((FsRow)obj);
    }

    @Override
    public int compareTo(FsRow o) {
        return m_name.compareTo(o.m_name);
    }
}
