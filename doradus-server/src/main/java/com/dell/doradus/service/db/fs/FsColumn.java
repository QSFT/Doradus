package com.dell.doradus.service.db.fs;

import com.dell.doradus.common.Utils;
import com.dell.doradus.olap.io.BSTR;

public class FsColumn implements Comparable<FsColumn> {
    private int m_operation; //see FsMutation
    private BSTR m_name;
    private byte[] m_value;

    public FsColumn(int operation, BSTR name, byte[] value) {
        m_operation = operation;
        m_name = name;
        m_value = value;
    }
    
    public void set(int operation, BSTR name, byte[] value) {
        m_operation = operation;
        m_name = name;
        m_value = value;
    }
    
    public int getOperation() { return m_operation; }
    public BSTR getName() { return m_name; }
    public byte[] getValue() { return m_value; }
    
    public boolean isRowDelete() { return m_operation == FsMutation.DELETE_ROW; }
    public boolean isColumnDelete() { return m_operation == FsMutation.DELETE_COLUMN; }
    public boolean isInplaceValue() { return m_operation == FsMutation.UPDATE_COLUMN; }
    public boolean isExternalValue() { return m_operation == FsMutation.UPDATE_LARGE_COLUMN; }
    
    @Override
    public String toString() {
        return m_name.toString() + (m_value == null ? null : Utils.toString(m_value));
    }
    
    @Override
    public int hashCode() {
        return m_name.hashCode();
    }
    
    @Override
    public boolean equals(Object obj) {
        return m_name.equals(((FsColumn)obj).m_name);
    }

    @Override
    public int compareTo(FsColumn x) {
        return m_name.compareTo(x.m_name);
    }
}
