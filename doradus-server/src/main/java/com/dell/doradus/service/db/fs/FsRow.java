package com.dell.doradus.service.db.fs;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import com.dell.doradus.olap.io.BSTR;

public class FsRow {
    private BSTR m_name;
    private TreeMap<BSTR, FsColumn> m_columns = new TreeMap<>();
    
    public FsRow(BSTR name) { m_name = name; }
    
    public BSTR getName() { return m_name; }
    
    public FsColumn getColumn(BSTR name) {
        return m_columns.get(name);
    }

    public FsColumn getOrCreateColumn(BSTR name) {
        FsColumn column = m_columns.get(name);
        if(column == null) {
            column = new FsColumn(name);
            m_columns.put(name, column);
        }
        return column;
    }
    
    public void deleteColumn(BSTR name) {
        m_columns.remove(name);
    }
    
    public List<FsColumn> getColumns() {
        List<FsColumn> columns = new ArrayList<>(m_columns.values());
        return columns;
    }
}
