package com.dell.doradus.logservice.search.filter;

import com.dell.doradus.logservice.ChunkField;
import com.dell.doradus.logservice.search.StrRef;
import com.dell.doradus.olap.io.BSTR;
import com.dell.doradus.olap.store.BitVector;
import com.dell.doradus.search.query.RangeQuery;

public class FilterFieldRange implements IValuesFilter {
    private BSTR m_min;
    private BSTR m_max;
    private boolean m_minInclusive;
    private boolean m_maxInclusive;
    private StrRef m_value;
    
    public FilterFieldRange(RangeQuery rq) {
        if(rq.min != null) {
            m_min = new BSTR(rq.min);
            m_minInclusive = rq.minInclusive;
        }
        if(rq.max != null) {
            m_max = new BSTR(rq.max);
            m_maxInclusive = rq.maxInclusive;
        }
        
        m_value = new StrRef();
    }


    @Override public void check(ChunkField field, BitVector values) {
        int[] offsets = field.getOffsets();
        int[] lengths = field.getLengths();
        byte[] buffer = field.getBuffer();
        for(int i = 0; i < offsets.length; i++) {
            m_value.set(buffer, offsets[i], lengths[i]);
            if(check(m_value)) {
                values.set(i);
            }
        }
    }
    
    private boolean check(StrRef value) {
        if(m_min != null) {
            int c = m_value.compare(m_min);
            if(m_minInclusive && c < 0) return false;
            else if(c <= 0) return false;
        }
        if(m_max != null) {
            int c = m_value.compare(m_max);
            if(m_maxInclusive && c > 0) return false;
            else if(c >= 0) return false;
        }
        return true;
    }


}
