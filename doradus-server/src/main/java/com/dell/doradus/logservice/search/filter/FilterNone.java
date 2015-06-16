package com.dell.doradus.logservice.search.filter;

import com.dell.doradus.logservice.ChunkReader;

public class FilterNone implements IFilter {
    
    public FilterNone() {}
    
    @Override public boolean check(ChunkReader reader, int doc) {
        return false;
    }
    
}
