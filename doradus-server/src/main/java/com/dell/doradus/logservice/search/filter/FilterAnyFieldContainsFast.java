package com.dell.doradus.logservice.search.filter;

import java.util.Locale;

import com.dell.doradus.logservice.ChunkReader;
import com.dell.doradus.logservice.search.StrRef;
import com.dell.doradus.olap.io.BSTR;

public class FilterAnyFieldContainsFast implements IFilter {
    private BSTR m_upper;
    private BSTR m_lower;
    private StrRef m_value = new StrRef();
    
    public FilterAnyFieldContainsFast(String pattern) {
        pattern = pattern.toUpperCase(Locale.ROOT);
        m_upper = new BSTR(pattern);
        pattern = pattern.toLowerCase(Locale.ROOT);
        m_lower = new BSTR(pattern);
        if(m_upper.length != m_lower.length) throw new IncompatibleCaseException();
    }
    
    @Override public boolean check(ChunkReader reader, int doc) {
        for(int i = 0; i < reader.fieldsCount(); i++) {
            reader.getFieldValue(doc, i, m_value);
            if(m_value.contains(m_upper, m_lower)) return true;
        }
        return false;
    }
    
}
