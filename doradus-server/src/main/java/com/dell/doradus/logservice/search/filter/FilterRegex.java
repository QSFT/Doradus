package com.dell.doradus.logservice.search.filter;

import java.util.regex.Pattern;

import com.dell.doradus.common.Utils;
import com.dell.doradus.logservice.ChunkField;
import com.dell.doradus.olap.store.BitVector;

public class FilterRegex implements IValuesFilter {
    private String m_regex;
    
    public FilterRegex(String regex) {
        m_regex = regex;
    }

    public void check(ChunkField field, BitVector values) {
        int[] offsets = field.getOffsets();
        int[] lengths = field.getLengths();
        byte[] buffer = field.getBuffer();
        for(int i = 0; i < offsets.length; i++) {
            String value = Utils.toString(buffer, offsets[i], lengths[i]);
            if(Pattern.matches(m_regex, value)) {
                values.set(i);
            }
        }
    }
    
}
