package com.dell.doradus.logservice.search;

import java.util.Calendar;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.TimeZone;

import com.dell.doradus.common.Utils;
import com.dell.doradus.logservice.ChunkInfo;
import com.dell.doradus.logservice.search.filter.IFilter;
import com.dell.doradus.olap.aggregate.AggregationResult;
import com.dell.doradus.olap.aggregate.MetricValueCount;
import com.dell.doradus.olap.aggregate.MetricValueSet;
import com.dell.doradus.olap.collections.BdLongSet;
import com.dell.doradus.olap.store.IntList;

public class AggregateCollectorTimestamp extends AggregateCollector {
    private IFilter m_filter;
    private long m_documents;
    private String m_truncate;
    private TimeZone m_zone;
    private long m_divisor;
    private GregorianCalendar m_Calendar;
    IntList m_list = new IntList();
    BdLongSet m_fields = new BdLongSet(1024);
    
    
    public AggregateCollectorTimestamp(IFilter filter, String truncate, String timeZone) {
        m_filter = filter;
        
        if(timeZone != null) m_zone = TimeZone.getTimeZone(timeZone);
        else m_zone = Utils.UTC_TIMEZONE;
        m_truncate = truncate;
        if(m_truncate == null) m_truncate = "SECOND"; 
        m_truncate = m_truncate.toUpperCase();
        if ("SECOND".equals(m_truncate)) m_divisor = 1000;
        else if ("MINUTE".equals(m_truncate)) m_divisor = 60 * 1000;
        else if ("HOUR".equals(m_truncate)) m_divisor = 3600 * 1000;
        else m_divisor = 1 * 3600 * 1000;
        m_Calendar = (GregorianCalendar)GregorianCalendar.getInstance(m_zone);
    }
    
    @Override public void addChunk(ChunkInfo info) {
        super.read(info);
        for(int i = 0; i < m_reader.size(); i++) {
            if(!m_filter.check(m_reader, i)) continue;
            long timestamp = m_reader.getTimestamp(i);
            long value = timestamp / m_divisor;
            int pos = m_fields.add(value);
            if(pos == m_list.size()) m_list.add(1);
            else m_list.set(pos, m_list.get(pos) + 1);
            m_documents++;
        }
    }

    
    private String getField(long value) {
        m_Calendar.setTimeInMillis(value * m_divisor);
        if ("SECOND".equals(m_truncate) || "MINUTE".equals(m_truncate) || "HOUR".equals(m_truncate)) {
            return Utils.formatDate(m_Calendar, Calendar.SECOND);
        }
        else if ("DAY".equals(m_truncate)) {
            return Utils.formatDate(m_Calendar, Calendar.DATE);
        }
        else if("WEEK".equals(m_truncate)) {
            GregorianCalendar calendar = Utils.truncateToWeek(m_Calendar);
            return Utils.formatDate(calendar, Calendar.DATE);
        }
        else if ("MONTH".equals(m_truncate)) {
            m_Calendar.set(Calendar.DAY_OF_MONTH, 1);
            return Utils.formatDate(m_Calendar, Calendar.DATE);
        }
        else if ("QUARTER".equals(m_truncate)) {
            m_Calendar.set(Calendar.DAY_OF_MONTH, 1);
            m_Calendar.set(Calendar.MONTH, m_Calendar.get(Calendar.MONTH) / 3 * 3);
            return Utils.formatDate(m_Calendar, Calendar.DATE);
        }
        else if ("YEAR".equals(m_truncate)) {
            return Utils.formatDate(m_Calendar, Calendar.YEAR);
        }
        else throw new IllegalArgumentException("Unknown truncate function: " + m_truncate);
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
        String lastGroup = "";
        MetricValueCount cc = null;
        for(int i = 0; i < m_fields.size(); i++) {
            String group = getField(m_fields.get(i));
            if(group.equals(lastGroup)) {
                cc.metric += m_list.get(i);
                continue;
            }
            lastGroup = group;
            AggregationResult.AggregationGroup g = new AggregationResult.AggregationGroup();
            g.id = group;
            g.name = group;
            g.metricSet = new MetricValueSet(1);
            cc = new MetricValueCount();
            cc.metric = m_list.get(i);
            g.metricSet.values[0] = cc;
            result.groups.add(g);
        }
        result.groupsCount = result.groups.size();
        Collections.sort(result.groups);
        return result;
    }
    
}
