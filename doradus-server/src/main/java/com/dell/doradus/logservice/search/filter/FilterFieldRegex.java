package com.dell.doradus.logservice.search.filter;

import java.util.regex.Pattern;

import com.dell.doradus.logservice.ChunkReader;
import com.dell.doradus.olap.io.BSTR;

public class FilterFieldRegex implements IFilter {
    private BSTR m_field;
    private int m_lastFieldIndex = -1;
    private ChunkReader m_lastReader = null;
    private String m_regex;
    
    public FilterFieldRegex(String field, String regex) {
        m_field = new BSTR(field);
        m_regex = regex;
    }
    
    @Override public boolean check(ChunkReader reader, int doc) {
        if(reader != m_lastReader) {
            m_lastReader = reader;
            m_lastFieldIndex = reader.getFieldIndex(m_field);
        }
        if(m_lastFieldIndex < 0) return false;
        String value = reader.getFieldValue(doc, m_lastFieldIndex);
        return Pattern.matches(m_regex, value);
    }
    
}
