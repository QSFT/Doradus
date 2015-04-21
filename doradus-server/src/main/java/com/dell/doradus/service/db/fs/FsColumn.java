package com.dell.doradus.service.db.fs;

import com.dell.doradus.olap.io.BSTR;

public class FsColumn implements Comparable<FsColumn> {
    private BSTR m_name;
    private BSTR m_value;
    private long m_offset;
    
    public FsColumn(BSTR name) { m_name = name; }
    
    public BSTR getName() { return m_name; }
    public boolean hasValue() { return m_value != null; }
    public BSTR getValue() { return m_value; }
    public long getStoredOffset() { return m_offset; }
    
    public void setValue(BSTR value) {
        m_value = value;
        m_offset = -1;
    } 
    public void setValueRef(long offset) {
        m_offset = offset;
        m_value = null;
    }

    @Override public int compareTo(FsColumn o) {
        return BSTR.compare(m_name, o.m_name);
    }
    
}
