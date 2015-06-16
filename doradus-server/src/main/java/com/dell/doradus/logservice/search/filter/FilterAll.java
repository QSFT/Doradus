package com.dell.doradus.logservice.search.filter;

import com.dell.doradus.logservice.ChunkReader;

public class FilterAll implements IFilter {
    
    public FilterAll() {}
    
    @Override public boolean check(ChunkReader reader, int doc) {
        return true;
    }
    
}
