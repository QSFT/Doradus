package com.dell.doradus.logservice.search;

import java.util.List;

import com.dell.doradus.logservice.LogEntry;
import com.dell.doradus.search.FieldSet;
import com.dell.doradus.search.SearchResultList;
import com.dell.doradus.search.aggregate.SortOrder;
import com.dell.doradus.search.util.HeapList;

public class SearchCollector {
    private HeapList<LogEntry> m_heap;
    private long m_maxTimestamp = -1;
    private long m_minTimestamp = -1;
    
    public SearchCollector(int size) {
        m_heap = new HeapList<>(size);
    }
    
    public LogEntry add(LogEntry entry) {
        m_maxTimestamp = -1;
        m_minTimestamp = -1;
        return m_heap.AddEx(entry);
    }
    
    public int size() { return m_heap.getCount(); }
    
    public List<LogEntry> getEntries() { return m_heap.values(); }
    
    public long getMinTimestamp() {
        if(m_heap.getCount() == 0) return 0;
        if(m_minTimestamp >= 0) return m_minTimestamp;
        Object[] array = m_heap.getArray();
        long timestamp = Long.MAX_VALUE;
        for(int i = 0; i < m_heap.getCount(); i++) {
            long time = ((LogEntry)array[i + 1]).getTimestamp();
            if(time < timestamp) timestamp = time;
        }
        m_minTimestamp = timestamp;
        return timestamp;
    }
    
    public long getMaxTimestamp() {
        if(m_heap.getCount() == 0) return Long.MAX_VALUE;
        if(m_maxTimestamp >= 0) return m_maxTimestamp;
        Object[] array = m_heap.getArray();
        long timestamp = 0;
        for(int i = 0; i < m_heap.getCount(); i++) {
            long time = ((LogEntry)array[i + 1]).getTimestamp();
            if(time > timestamp) timestamp = time;
        }
        m_maxTimestamp = timestamp;
        return timestamp;
    }
    
    public SearchResultList getSearchResult(FieldSet fieldSet, SortOrder[] orders) {
        SearchResultList list = new SearchResultList();
        LogEntry[] entries = m_heap.GetValues(LogEntry.class);
        for(LogEntry e: entries) {
            list.results.add(e.createSearchResult(fieldSet, orders));
        }
        return list;
    }
}
