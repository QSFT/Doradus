package com.dell.doradus.logservice.search.filter;

import com.dell.doradus.logservice.ChunkInfo;
import com.dell.doradus.logservice.ChunkReader;
import com.dell.doradus.olap.store.BitVector;

public interface IFilter {
    
    //populates docs with numbers of documents that satisfy the query
    public void check(ChunkReader reader, BitVector docs);
    
    //returns 1, if all records in {info} satisfy the query filter;
    //-1 if none, or 0 if unknown, i.e. we need to read data.
    public int check(ChunkInfo info);
}
