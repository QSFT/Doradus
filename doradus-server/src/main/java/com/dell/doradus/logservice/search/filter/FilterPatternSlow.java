package com.dell.doradus.logservice.search.filter;

import com.dell.doradus.common.Utils;
import com.dell.doradus.logservice.ChunkField;
import com.dell.doradus.olap.store.BitVector;

public class FilterPatternSlow implements IValuesFilter {
    private String m_pattern;
    
    public FilterPatternSlow(String pattern) {
        m_pattern = pattern;
    }

    public void check(ChunkField field, BitVector values) {
        int[] offsets = field.getOffsets();
        int[] lengths = field.getLengths();
        byte[] buffer = field.getBuffer();
        for(int i = 0; i < offsets.length; i++) {
            String value = Utils.toString(buffer, offsets[i], lengths[i]);
            if(Utils.matchesPattern(value, m_pattern)) {
                values.set(i);
            }
        }
    }
    
}
