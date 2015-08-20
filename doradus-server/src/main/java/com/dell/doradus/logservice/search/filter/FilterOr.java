package com.dell.doradus.logservice.search.filter;

import java.util.ArrayList;
import java.util.List;

import com.dell.doradus.logservice.ChunkInfo;
import com.dell.doradus.logservice.ChunkReader;
import com.dell.doradus.olap.store.BitVector;

public class FilterOr implements IFilter {
    private List<IFilter> m_filters = new ArrayList<>();
    
    public FilterOr() {}

    public FilterOr(IFilter... filters) {
        for(IFilter filter: filters) {
            m_filters.add(filter);
        }
    }
    
    public void add(IFilter filter) {
        m_filters.add(filter);
    }
    
    @Override public void check(ChunkReader reader, BitVector docs) {
        BitVector temp = new BitVector(docs.size());
        for(IFilter filter: m_filters) {
            temp.clearAll();
            filter.check(reader, temp);
            docs.or(temp);
            if(docs.isAllBitsSet()) break;
        }
    }
    
    @Override public int check(ChunkInfo info) {
        // any filter reports all docs => all docs
        // all filters report no docs => no docs
        boolean hasZero = false;
        for(IFilter filter: m_filters) {
            int c = filter.check(info);
            if(c == 1) return 1;
            if(c == 0) hasZero = true;
        }
        return hasZero ? 0 : -1;
    }
}
