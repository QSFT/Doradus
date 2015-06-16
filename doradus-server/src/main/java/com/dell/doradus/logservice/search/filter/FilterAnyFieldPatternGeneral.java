package com.dell.doradus.logservice.search.filter;

import java.util.Locale;

import com.dell.doradus.common.Utils;
import com.dell.doradus.logservice.ChunkReader;

public class FilterAnyFieldPatternGeneral implements IFilter {
    private String m_pattern;
    
    public FilterAnyFieldPatternGeneral(String pattern) {
        m_pattern = pattern.toLowerCase(Locale.ROOT);
    }
    
    @Override public boolean check(ChunkReader reader, int doc) {
        for(int i = 0; i < reader.fieldsCount(); i++) {
            String value = reader.getFieldValue(doc, i);
            value = value.toLowerCase(Locale.ROOT);
            if(Utils.matchesPattern(value, m_pattern)) return true;
        }
        return false;
    }
    
}
