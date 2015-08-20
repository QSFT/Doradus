package com.dell.doradus.logservice.search.filter;

import com.dell.doradus.logservice.ChunkField;
import com.dell.doradus.olap.store.BitVector;

public interface IValuesFilter {
    
    public void check(ChunkField field, BitVector values);
    
}
