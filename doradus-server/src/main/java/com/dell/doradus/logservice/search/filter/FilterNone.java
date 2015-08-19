package com.dell.doradus.logservice.search.filter;

import com.dell.doradus.logservice.ChunkInfo;
import com.dell.doradus.logservice.ChunkReader;
import com.dell.doradus.olap.store.BitVector;

public class FilterNone implements IFilter {
    
    public FilterNone() {}
    
    @Override public void check(ChunkReader reader, BitVector docs) {
        docs.clearAll();
    }
    
    @Override public int check(ChunkInfo info) {
        return 1;
    }
    
}
