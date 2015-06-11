package com.dell.doradus.logservice.search;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import com.dell.doradus.logservice.ChunkInfo;

public class SortedChunkIterator implements Iterator<ChunkInfo> {
    private Iterator<ChunkInfo> m_iterator;
    private boolean m_bReverse;
    private List<ChunkInfo> m_buffer = new ArrayList<>(1000);
    private int m_position = 0;
    
    public SortedChunkIterator(Iterator<ChunkInfo> iterator, boolean reverse) {
        m_iterator = iterator;
        m_bReverse = reverse;
    }
    
    @Override public boolean hasNext() { return m_position < m_buffer.size() || m_iterator.hasNext(); }

    @Override public ChunkInfo next() {
        if(m_position < m_buffer.size()) return m_buffer.get(m_position++);
        m_buffer.clear();
        m_position = 0;
        while(m_iterator.hasNext() && m_buffer.size() < 1000) {
            m_buffer.add(new ChunkInfo(m_iterator.next()));
        }
        Collections.sort(m_buffer, new Comparator<ChunkInfo>() {
            @Override public int compare(ChunkInfo x, ChunkInfo y) {
                if(m_bReverse) {
                    return y.getMaxTimestamp() < x.getMaxTimestamp() ? -1 : y.getMaxTimestamp() > x.getMaxTimestamp() ? 1 : 0;
                } else {
                    return x.getMinTimestamp() < y.getMinTimestamp() ? -1 : x.getMinTimestamp() > y.getMinTimestamp() ? 1 : 0;
                }
            }});
        return m_buffer.get(m_position++);
    }

    @Override public void remove() { throw new RuntimeException("Remove not implemented"); }
    
    
}
