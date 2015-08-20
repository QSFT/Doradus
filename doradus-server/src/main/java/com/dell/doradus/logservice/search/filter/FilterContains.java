package com.dell.doradus.logservice.search.filter;

import com.dell.doradus.logservice.ChunkField;
import com.dell.doradus.logservice.pattern.Substr;
import com.dell.doradus.olap.store.BitVector;

public class FilterContains implements IValuesFilter {
    private Substr m_substr;
    
    public FilterContains(String substr) {
        m_substr = new Substr(substr);
    }

    public void check(ChunkField field, BitVector values) {
        //optimization: keep track of prefixes and suffixes.
        //If value_i does not contain substr then search in value_{i+1}[prefix - substr_length, length - suffix + substr_length] 
        //If value_i does contains substr in [0..prefix - substr_length] or [length - suffix..length] then so does value[i+1]  
        
        
        int[] offsets = field.getOffsets();
        int[] lengths = field.getLengths();
        int[] prefixes = field.getPrefixes();
        int[] suffixes = field.getSuffixes();
        byte[] buffer = field.getBuffer();
        int lastMatch = -1;
        for(int i = 0; i < offsets.length; i++) {
            if(lastMatch == -1) {
                int start = Math.max(0, prefixes[i] - m_substr.length + 1);
                int end = Math.min(lengths[i], lengths[i] - suffixes[i] + m_substr.length - 1);
                lastMatch = m_substr.contains(buffer, offsets[i] + start, offsets[i] + end);
            } else {
                if(lastMatch <= prefixes[i] - m_substr.length) {
                    //match in common prefix; do nothing
                    lastMatch += offsets[i];
                } else if(lastMatch >= lengths[i - 1] - suffixes[i]) {
                    //match in common suffix; fix lastMatch by the difference in lengths
                    lastMatch += lengths[i] - lengths[i - 1] + offsets[i];
                } else {
                    //no match in prefix
                    //note that even if match is in the suffix we need to re-search to find a earlier match
                    int start = Math.max(0, prefixes[i] - m_substr.length + 1);
                    int end = lengths[i];
                    lastMatch = m_substr.contains(buffer, offsets[i] + start, offsets[i] + end);
                }
            }

            if(lastMatch < 0) continue;
            lastMatch -= offsets[i];
            if(lastMatch < -1) {
                System.out.println("AAA: " + lastMatch);
            }
            values.set(i);
            
            //if(m_substr.contains(buffer, offsets[i], offsets[i] + lengths[i]) >= 0) {
            //    values.set(i);
            //}
            
        }
    }
    
}
