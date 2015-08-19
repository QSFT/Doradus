package com.dell.doradus.logservice.search;

import com.dell.doradus.logservice.ChunkInfo;
import com.dell.doradus.logservice.search.filter.IFilter;
import com.dell.doradus.olap.aggregate.AggregationResult;
import com.dell.doradus.olap.aggregate.MetricValueCount;
import com.dell.doradus.olap.aggregate.MetricValueSet;
import com.dell.doradus.olap.store.BitVector;

public class AggregateCollectorNoField extends AggregateCollector {
    private IFilter m_filter;
    private long m_documents;
    
    public AggregateCollectorNoField(IFilter filter) {
        m_filter = filter;
    }

    @Override public void addChunk(ChunkInfo info) {
        int c = m_filter.check(info);
        if(c == -1) return;
        if(c == 1) {
            m_documents += info.getEventsCount();
            return;
        }
        BitVector bv = new BitVector(info.getEventsCount());
        super.read(info);
        m_filter.check(m_reader, bv);
        m_documents += bv.bitsSet();
    }

    @Override public AggregationResult getResult() {
        int count = (int)m_documents;
        AggregationResult result = new AggregationResult();
        result.documentsCount = count;
        result.summary = new AggregationResult.AggregationGroup();
        result.summary.id = null;
        result.summary.name = "*";
        result.summary.metricSet = new MetricValueSet(1);
        MetricValueCount c = new MetricValueCount();
        c.metric = count;
        result.summary.metricSet.values[0] = c; 
        return result;
    }
    
}
