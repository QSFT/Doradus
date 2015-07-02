package com.dell.doradus.logservice.search.filter;

import com.dell.doradus.logservice.ChunkReader;

public interface IFilter {
    //returns true if log record number {doc} in {reader} satisfies query filter
    public boolean check(ChunkReader reader, int doc);
    //returns 1, if all records in {info} satisfy the query filter;
    //-1 if none, or 0 if unknown, i.e. we need to read data.
    //public int check(ChunkInfo info);
}
