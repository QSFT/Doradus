package com.dell.doradus.service.db.fs;

import java.util.HashSet;
import java.util.Set;

public class FsReadColumns {
    private Set<FsColumn> m_columns = new HashSet<>();
    private boolean m_bRowDeleted;
    
    public FsReadColumns() {
        
    }
    
    public void setRowDeleted() { m_bRowDeleted = true; }
    public boolean isRowDeleted() { return m_bRowDeleted; }
    
    public Set<FsColumn> getColumns() { return m_columns; }
    public boolean containsColumn(FsColumn c) { return m_columns.contains(c); }
    public void addColumn(FsColumn c) { m_columns.add(c); }
}
