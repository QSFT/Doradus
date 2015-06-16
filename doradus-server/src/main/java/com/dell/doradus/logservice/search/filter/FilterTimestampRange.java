package com.dell.doradus.logservice.search.filter;

import com.dell.doradus.common.Utils;
import com.dell.doradus.logservice.ChunkReader;
import com.dell.doradus.search.query.RangeQuery;

public class FilterTimestampRange implements IFilter {
    private long m_minTimestamp = 0;
    private long m_maxTimestamp = Long.MAX_VALUE;
    
    public FilterTimestampRange(RangeQuery rq) {
        if(rq.min != null) {
            m_minTimestamp = Utils.parseDate(rq.min).getTimeInMillis();
            if(!rq.minInclusive) m_minTimestamp++;
        }
        if(rq.max != null) {
            m_maxTimestamp = Utils.parseDate(rq.max).getTimeInMillis();
            if(rq.maxInclusive) m_maxTimestamp++;
        }
    }
    
    @Override public boolean check(ChunkReader reader, int doc) {
        long timestamp = reader.getTimestamp(doc);
        return timestamp >= m_minTimestamp && timestamp < m_maxTimestamp;
    }
    
}
