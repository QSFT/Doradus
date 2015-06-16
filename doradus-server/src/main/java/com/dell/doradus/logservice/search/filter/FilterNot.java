package com.dell.doradus.logservice.search.filter;

import com.dell.doradus.logservice.ChunkReader;

public class FilterNot implements IFilter {
    private IFilter m_filter;

    public FilterNot(IFilter filter) {
        m_filter = filter;
    }
    
    @Override public boolean check(ChunkReader reader, int doc) {
        return !m_filter.check(reader, doc);
    }
}
