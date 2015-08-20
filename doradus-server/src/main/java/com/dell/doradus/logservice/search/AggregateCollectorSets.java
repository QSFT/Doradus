package com.dell.doradus.logservice.search;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.dell.doradus.logservice.ChunkInfo;
import com.dell.doradus.logservice.search.filter.FilterBuilder;
import com.dell.doradus.logservice.search.filter.IFilter;
import com.dell.doradus.olap.aggregate.AggregationResult;
import com.dell.doradus.olap.aggregate.MetricValueCount;
import com.dell.doradus.olap.aggregate.MetricValueSet;
import com.dell.doradus.olap.store.BitVector;
import com.dell.doradus.search.query.Query;

public class AggregateCollectorSets extends AggregateCollector {
    private IFilter m_filter;
    private List<IFilter> m_filters;
    private List<String> m_aliases;
    private long m_documents;
    private int[] m_counts;
    
    public AggregateCollectorSets(IFilter filter, List<Query> filters, List<String> aliases) {
        m_filter = filter;
        m_filters = new ArrayList<>(filters.size());
        for(Query qu: filters) {
            m_filters.add(FilterBuilder.build(qu));
        }
        m_aliases = aliases;
        m_counts = new int[m_filters.size()];
    }
    
    @Override public void addChunk(ChunkInfo info) {
        int c = m_filter.check(info);
        if(c == -1) return;
        super.read(info);
        BitVector bv = new BitVector(info.getEventsCount());
        if(c == 1) bv.setAll();
        else m_filter.check(m_reader, bv);
        m_documents += bv.bitsSet();

        BitVector bv2 = new BitVector(info.getEventsCount());
        for(int i = 0; i < m_counts.length; i++) {
            bv2.clearAll();
            c = m_filters.get(i).check(info);
            if(c == 1) bv2.setAll();
            else if(c == 0) m_filters.get(i).check(m_reader, bv2);
            bv2.and(bv);
            m_counts[i] += bv2.bitsSet();
        }
        bv.clearAll();
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
        for(int i = 0; i < m_counts.length; i++) {
            String group = m_aliases.get(i);
            AggregationResult.AggregationGroup g = new AggregationResult.AggregationGroup();
            g.id = group;
            g.name = group;
            g.metricSet = new MetricValueSet(1);
            MetricValueCount cc = new MetricValueCount();
            cc.metric = m_counts[i];
            g.metricSet.values[0] = cc;
            result.groups.add(g);
        }
        result.groupsCount = result.groups.size();
        Collections.sort(result.groups);
        return result;
    }
    
}
