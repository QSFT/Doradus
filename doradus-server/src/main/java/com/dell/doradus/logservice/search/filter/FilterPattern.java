package com.dell.doradus.logservice.search.filter;

import com.dell.doradus.logservice.ChunkField;
import com.dell.doradus.logservice.pattern.Pattern;
import com.dell.doradus.olap.store.BitVector;

public class FilterPattern implements IValuesFilter {
    private Pattern m_pattern;
    
    public FilterPattern(String pattern) {
        m_pattern = new Pattern(pattern);
    }

    public void check(ChunkField field, BitVector values) {
        int[] offsets = field.getOffsets();
        int[] lengths = field.getLengths();
        byte[] buffer = field.getBuffer();
        for(int i = 0; i < offsets.length; i++) {
            if(m_pattern.match(buffer, offsets[i], offsets[i] + lengths[i])) {
                values.set(i);
            }
        }
    }
    
}
