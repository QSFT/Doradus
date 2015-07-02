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
        super.read(info);
        for(int i = 0; i < m_reader.size(); i++) {
            if(!m_filter.check(m_reader, i)) continue;
            for(int j = 0; j < m_counts.length; j++) {
                if(m_filters.get(j).check(m_reader, i)) m_counts[j]++;
            }
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
