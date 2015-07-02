package com.dell.doradus.logservice.search;

import com.dell.doradus.logservice.ChunkInfo;
import com.dell.doradus.logservice.search.filter.FilterAll;
import com.dell.doradus.logservice.search.filter.IFilter;
import com.dell.doradus.olap.aggregate.AggregationResult;
import com.dell.doradus.olap.aggregate.MetricValueCount;
import com.dell.doradus.olap.aggregate.MetricValueSet;

public class AggregateCollectorNoField extends AggregateCollector {
    private IFilter m_filter;
    private long m_documents;
    
    public AggregateCollectorNoField(IFilter filter) {
        m_filter = filter;
    }

    @Override public void addChunk(ChunkInfo info) {
        //optimization for count-star query.
        //todo: make filter check ChunkInfo to get rid of this
        if(m_filter instanceof FilterAll) {
            m_documents += info.getEventsCount();
            return;
        }
        
        super.read(info);
        for(int i = 0; i < m_reader.size(); i++) {
            if(!m_filter.check(m_reader, i)) continue;
            m_documents++;
        }
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
