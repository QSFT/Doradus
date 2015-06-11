package com.dell.doradus.logservice.search;

import com.dell.doradus.logservice.ChunkInfo;

public class SortedChunkIterable implements Iterable<ChunkInfo> {
    private Iterable<ChunkInfo> m_iterable;
    private boolean m_bReverse;
    
    public SortedChunkIterable(Iterable<ChunkInfo> iterable, boolean reverse) {
        m_iterable = iterable;
        m_bReverse = reverse;
    }

    @Override public SortedChunkIterator iterator() {
        return new SortedChunkIterator(m_iterable.iterator(), m_bReverse);
    }
    
}
