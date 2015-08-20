package com.dell.doradus.logservice.search.filter;

import com.dell.doradus.logservice.ChunkField;
import com.dell.doradus.logservice.ChunkInfo;
import com.dell.doradus.logservice.ChunkReader;
import com.dell.doradus.olap.store.BitVector;

public class FilterAnyField implements IFilter {
    private IValuesFilter m_valuesFilter; 
    
    public FilterAnyField(IValuesFilter valuesFilter) {
        m_valuesFilter = valuesFilter;
    }
    
    @Override public void check(ChunkReader reader, BitVector docs) {
        for(int fieldIndex = 0; fieldIndex < reader.fieldsCount(); fieldIndex++) {
            ChunkField field = reader.getField(fieldIndex);
            BitVector values = new BitVector(field.getValuesCount());
            m_valuesFilter.check(field, values);
            if(values.isAllBitsCleared()) continue;
            int[] indexes = field.getIndexes();
            for(int i = 0; i < indexes.length; i++) {
                if(values.get(indexes[i])) {
                    docs.set(i);
                }
            }
            if(docs.isAllBitsSet()) break;
        }
    }
    
    @Override public int check(ChunkInfo info) { return 0; }
    
}
