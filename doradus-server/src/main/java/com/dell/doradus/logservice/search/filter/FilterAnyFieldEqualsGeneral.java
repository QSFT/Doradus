package com.dell.doradus.logservice.search.filter;

import java.util.Locale;

import com.dell.doradus.logservice.ChunkReader;

public class FilterAnyFieldEqualsGeneral implements IFilter {
    private String m_pattern;
    
    public FilterAnyFieldEqualsGeneral(String pattern) {
        m_pattern = pattern.toLowerCase(Locale.ROOT);
    }
    
    @Override public boolean check(ChunkReader reader, int doc) {
        for(int i = 0; i < reader.fieldsCount(); i++) {
            String value = reader.getFieldValue(doc, i);
            value = value.toLowerCase(Locale.ROOT);
            if(value.equals(m_pattern)) return true;
        }
        return false;
    }
    
}
