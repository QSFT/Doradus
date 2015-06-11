package com.dell.doradus.logservice;

import com.dell.doradus.olap.io.BSTR;
import com.dell.doradus.search.FieldSet;
import com.dell.doradus.search.SearchResult;
import com.dell.doradus.search.aggregate.SortOrder;

public class LogEntry implements Comparable<LogEntry> {
    private int m_doc;
    private long m_timestamp;
    private BSTR[] m_fields;
    private BSTR[] m_values;
    private boolean m_bSortDescending;
    private DateFormatter m_formatter = new DateFormatter();
    
    public LogEntry(BSTR[] fields, boolean sortDescending) {
        m_fields = fields;
        m_values = new BSTR[fields.length];
        for(int i = 0; i < fields.length; i++) m_values[i] = new BSTR();
        m_bSortDescending = sortDescending;
    }
    
    public long getTimestamp() { return m_timestamp; }
    
    public void set(ChunkReader reader, int doc) {
        m_doc = doc;
        m_timestamp = reader.getTimestamp(doc);
        for(int i = 0; i < m_fields.length; i++) {
            int index = reader.getFieldIndex(m_fields[i]);
            if(index < 0) m_values[i].length = 0;
            else reader.getFieldValue(doc, index, m_values[i]);
        }
    }

    @Override public int compareTo(LogEntry o) {
        int c = 0;
        if(m_timestamp > o.m_timestamp) c = 1;
        else if(m_timestamp < o.m_timestamp) c = -1;
        if(c == 0) {
            if(m_doc > o.m_doc) c = 1;
            else if(m_doc < o.m_doc) c = -1;
        }
        if(m_bSortDescending) c = -c;
        return c;
    }
    
    public SearchResult createSearchResult(FieldSet fieldSet, SortOrder[] orders) {
        SearchResult result = new SearchResult();
        result.fieldSet = fieldSet;
        result.orders = orders;
        String timestamp = m_formatter.format(m_timestamp);
        result.scalars.put("_ID", timestamp);
        result.scalars.put("Timestamp", timestamp);
        for(int i = 0; i < m_fields.length; i++) {
            String field = m_fields[i].toString();
            String fieldAlias = fieldSet.ScalarFieldAliases.get(field);
            if(fieldAlias != null) field = fieldAlias;
            String value = m_values[i].toString();
            result.scalars.put(field, value);
        }
        return result;
    }
    
}
