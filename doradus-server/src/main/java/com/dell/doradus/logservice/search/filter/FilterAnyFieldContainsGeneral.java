package com.dell.doradus.logservice.search.filter;

import java.util.Locale;

import com.dell.doradus.logservice.ChunkReader;

public class FilterAnyFieldContainsGeneral implements IFilter {
    private String m_pattern;
    
    public FilterAnyFieldContainsGeneral(String pattern) {
        m_pattern = pattern.toLowerCase(Locale.ROOT);
    }
    
    @Override public boolean check(ChunkReader reader, int doc) {
        for(int i = 0; i < reader.fieldsCount(); i++) {
            String value = reader.getFieldValue(doc, i);
            value = value.toLowerCase(Locale.ROOT);
            if(value.indexOf(m_pattern) >= 0) return true;
        }
        return false;
    }
    
}
