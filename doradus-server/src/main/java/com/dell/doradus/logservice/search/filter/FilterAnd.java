package com.dell.doradus.logservice.search.filter;

import java.util.ArrayList;
import java.util.List;

import com.dell.doradus.logservice.ChunkInfo;
import com.dell.doradus.logservice.ChunkReader;
import com.dell.doradus.olap.store.BitVector;

public class FilterAnd implements IFilter {
    private List<IFilter> m_filters = new ArrayList<>();
    
    public FilterAnd() {}

    public FilterAnd(IFilter... filters) {
        for(IFilter filter: filters) {
            m_filters.add(filter);
        }
    }
    
    public void add(IFilter filter) {
        m_filters.add(filter);
    }
    
    @Override public void check(ChunkReader reader, BitVector docs) {
        BitVector temp = new BitVector(docs.size());
        docs.setAll();
        for(IFilter filter: m_filters) {
            temp.clearAll();
            filter.check(reader, temp);
            docs.and(temp);
            if(docs.isAllBitsCleared()) break;
        }
    }
    
    @Override public int check(ChunkInfo info) {
        // any filter reports no docs => no docs
        // all filters report all docs => all docs
        boolean hasZero = false;
        for(IFilter filter: m_filters) {
            int c = filter.check(info);
            if(c == -1) return -1;
            if(c == 0) hasZero = true;
        }
        return hasZero ? 0 : 1;
    }

    
}
