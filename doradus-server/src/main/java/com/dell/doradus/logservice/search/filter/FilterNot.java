package com.dell.doradus.logservice.search.filter;

import com.dell.doradus.logservice.ChunkInfo;
import com.dell.doradus.logservice.ChunkReader;
import com.dell.doradus.olap.store.BitVector;

public class FilterNot implements IFilter {
    private IFilter m_filter;

    public FilterNot(IFilter filter) {
        m_filter = filter;
    }
    
    @Override public void check(ChunkReader reader, BitVector docs) {
        m_filter.check(reader, docs);
        docs.not();
    }
    
    @Override public int check(ChunkInfo info) {
        int c = m_filter.check(info);
        return -c;
    }
}
