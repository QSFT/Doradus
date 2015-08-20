package com.dell.doradus.logservice.search.filter;

import com.dell.doradus.common.Utils;
import com.dell.doradus.logservice.ChunkInfo;
import com.dell.doradus.logservice.ChunkReader;
import com.dell.doradus.olap.store.BitVector;
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
    
    @Override public void check(ChunkReader reader, BitVector docs) {
        long[] timestamps = reader.getTimestampField().getTimestamps();
        for(int i = 0; i < timestamps.length; i++) {
            long timestamp = timestamps[i];
            if(timestamp >= m_minTimestamp && timestamp < m_maxTimestamp) {
                docs.set(i);
            }
        }
    }
    
    @Override public int check(ChunkInfo info) {
        if(info.getMaxTimestamp() < m_minTimestamp) return -1;
        if(info.getMinTimestamp() >= m_maxTimestamp) return -1;
        if(info.getMinTimestamp() >= m_minTimestamp && info.getMaxTimestamp() < m_maxTimestamp) return 1;
        return 0;
    }
    
}
