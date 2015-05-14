package com.dell.doradus.logservice;

import java.util.Date;

public class LogRecord {
    private long m_timestamp;
    private Str[] m_values;
    
    public LogRecord() {
        setFieldsCount(0);
    }
    
    public void setFieldsCount(int fieldsCount) {
        if(m_values != null && m_values.length == fieldsCount) return;
        m_values = new Str[fieldsCount];
        for(int i = 0; i < fieldsCount; i++) {
            m_values[i] = new Str();
        }
    }
    
    public void clear() {
        m_timestamp = 0;
        for(int i = 0; i < m_values.length; i++) {
            m_values[i].clear();
        }
    }
    
    public long getTimestamp() { return m_timestamp; }
    public Date getDate() { return new Date(m_timestamp); }
    
    public int fieldsCount() { return m_values.length; }
    public Str fieldValue(int index) { return m_values[index]; }
   
    protected void setTimestamp(long timestamp) {
        m_timestamp = timestamp;
    }
    
}
