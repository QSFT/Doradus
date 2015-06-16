package com.dell.doradus.logservice.search.filter;

import com.dell.doradus.logservice.ChunkReader;
import com.dell.doradus.logservice.search.StrRef;
import com.dell.doradus.olap.io.BSTR;
import com.dell.doradus.search.query.RangeQuery;

public class FilterFieldRange implements IFilter {
    private BSTR m_field;
    private int m_lastFieldIndex = -1;
    private ChunkReader m_lastReader = null;
    private BSTR m_min;
    private BSTR m_max;
    private boolean m_minInclusive;
    private boolean m_maxInclusive;
    private StrRef m_value;
    
    public FilterFieldRange(RangeQuery rq) {
        m_field = new BSTR(rq.field);
        
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
    
    @Override public boolean check(ChunkReader reader, int doc) {
        if(reader != m_lastReader) {
            m_lastReader = reader;
            m_lastFieldIndex = reader.getFieldIndex(m_field);
        }
        
        if(m_lastFieldIndex < 0) return false;
        
        reader.getFieldValue(doc, m_lastFieldIndex, m_value);
        
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
