package com.dell.doradus.logservice.search.filter;

import java.util.Locale;

import com.dell.doradus.logservice.ChunkReader;
import com.dell.doradus.logservice.search.StrRef;
import com.dell.doradus.olap.io.BSTR;

public class FilterFieldEqualsFast implements IFilter {
    private BSTR m_field;
    private BSTR m_upper;
    private BSTR m_lower;
    private StrRef m_value = new StrRef();
    private int m_lastFieldIndex = -1;
    private ChunkReader m_lastReader = null;
    
    public FilterFieldEqualsFast(String field, String pattern) {
        m_field = new BSTR(field);
        pattern = pattern.toUpperCase(Locale.ROOT);
        m_upper = new BSTR(pattern);
        pattern = pattern.toLowerCase(Locale.ROOT);
        m_lower = new BSTR(pattern);
        if(m_upper.length != m_lower.length) throw new IncompatibleCaseException();
    }
    
    @Override public boolean check(ChunkReader reader, int doc) {
        if(reader != m_lastReader) {
            m_lastReader = reader;
            m_lastFieldIndex = reader.getFieldIndex(m_field);
        }
        if(m_lastFieldIndex < 0) return false;
        reader.getFieldValue(doc, m_lastFieldIndex, m_value);
        return m_value.equals(m_upper, m_lower);
    }
    
}
