package com.dell.doradus.logservice.search.filter;

import com.dell.doradus.logservice.ChunkReader;

public interface IFilter {
    public boolean check(ChunkReader reader, int doc);
}
