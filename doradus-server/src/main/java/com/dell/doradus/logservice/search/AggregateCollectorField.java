package com.dell.doradus.logservice.search;

import java.util.Collections;

import com.dell.doradus.logservice.ChunkInfo;
import com.dell.doradus.logservice.search.filter.IFilter;
import com.dell.doradus.olap.aggregate.AggregationResult;
import com.dell.doradus.olap.aggregate.MetricValueCount;
import com.dell.doradus.olap.aggregate.MetricValueSet;
import com.dell.doradus.olap.collections.strings.BstrSet;
import com.dell.doradus.olap.io.BSTR;
import com.dell.doradus.olap.store.BitVector;
import com.dell.doradus.olap.store.IntList;

public class AggregateCollectorField extends AggregateCollector {
    private IFilter m_filter;
    private long m_documents;
    private BSTR m_field;
    IntList m_list = new IntList();
    BstrSet m_fields = new BstrSet();
    BSTR temp = new BSTR();
    
    
    public AggregateCollectorField(IFilter filter, String field) {
        m_filter = filter;
        m_field = new BSTR(field);
    }

    @Override public void addChunk(ChunkInfo info) {
        int c = m_filter.check(info);
        if(c == -1) return;
        BitVector bv = new BitVector(info.getEventsCount());
        super.read(info);
        
        if(c == 1) {
            bv.setAll();
        } else {
            m_filter.check(m_reader, bv);
        }
        
        int index = m_reader.getFieldIndex(m_field);
        if(index < 0) return;
        
        for(int i = 0; i < bv.size(); i++) {
            if(!bv.get(i)) continue;
            m_reader.getFieldValue(i, index, temp);
            int pos = m_fields.add(temp);
            if(pos == m_list.size()) m_list.add(1);
            else m_list.set(pos, m_list.get(pos) + 1);
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
        for(int i = 0; i < m_fields.size(); i++) {
            AggregationResult.AggregationGroup g = new AggregationResult.AggregationGroup();
            g.id = m_fields.get(i).toString();
            g.name = g.id.toString();
            g.metricSet = new MetricValueSet(1);
            MetricValueCount cc = new MetricValueCount();
            cc.metric = m_list.get(i);
            g.metricSet.values[0] = cc;
            result.groups.add(g);
        }
        result.groupsCount = result.groups.size();
        Collections.sort(result.groups);
        return result;
    }
    
}
