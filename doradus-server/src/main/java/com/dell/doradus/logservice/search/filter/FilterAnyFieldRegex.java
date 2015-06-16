package com.dell.doradus.logservice.search.filter;

import java.util.regex.Pattern;

import com.dell.doradus.logservice.ChunkReader;

public class FilterAnyFieldRegex implements IFilter {
    private String m_regex;
    
    public FilterAnyFieldRegex(String regex) {
        m_regex = regex;
    }
    
    @Override public boolean check(ChunkReader reader, int doc) {
        for(int i = 0; i < reader.fieldsCount(); i++) {
            String value = reader.getFieldValue(doc, i);
            if(Pattern.matches(m_regex, value)) return true;
        }
        return false;
    }
    
}
