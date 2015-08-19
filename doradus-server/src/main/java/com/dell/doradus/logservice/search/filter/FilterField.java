package com.dell.doradus.logservice.search.filter;

import com.dell.doradus.logservice.ChunkField;
import com.dell.doradus.logservice.ChunkInfo;
import com.dell.doradus.logservice.ChunkReader;
import com.dell.doradus.olap.io.BSTR;
import com.dell.doradus.olap.store.BitVector;

public class FilterField implements IFilter {
    private BSTR m_fieldName;
    private IValuesFilter m_valuesFilter; 
    
    public FilterField(String field, IValuesFilter valuesFilter) {
        m_fieldName = new BSTR(field);
        m_valuesFilter = valuesFilter;
    }
    
    @Override public void check(ChunkReader reader, BitVector docs) {
        int fieldIndex = reader.getFieldIndex(m_fieldName);
        if(fieldIndex < 0) return;
        ChunkField field = reader.getField(fieldIndex);
        BitVector values = new BitVector(field.getValuesCount());
        m_valuesFilter.check(field, values);
        if(values.isAllBitsCleared()) return;
        int[] indexes = field.getIndexes();
        for(int i = 0; i < indexes.length; i++) {
            if(values.get(indexes[i])) {
                docs.set(i);
            }
        }
    }
    
    @Override public int check(ChunkInfo info) { return 0; }
    
}
