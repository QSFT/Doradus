package com.dell.doradus.logservice.search.filter;

import java.util.Locale;

import com.dell.doradus.common.Utils;
import com.dell.doradus.logservice.ChunkReader;
import com.dell.doradus.olap.io.BSTR;

public class FilterFieldPatternGeneral implements IFilter {
    private BSTR m_field;
    private int m_lastFieldIndex = -1;
    private ChunkReader m_lastReader = null;
    private String m_pattern;
    
    public FilterFieldPatternGeneral(String field, String pattern) {
        m_field = new BSTR(field);
        m_pattern = pattern.toLowerCase(Locale.ROOT);
    }
    
    @Override public boolean check(ChunkReader reader, int doc) {
        if(reader != m_lastReader) {
            m_lastReader = reader;
            m_lastFieldIndex = reader.getFieldIndex(m_field);
        }
        if(m_lastFieldIndex < 0) return false;
        String value = reader.getFieldValue(doc, m_lastFieldIndex);
        value = value.toLowerCase(Locale.ROOT);
        return Utils.matchesPattern(value, m_pattern);
    }
    
}
