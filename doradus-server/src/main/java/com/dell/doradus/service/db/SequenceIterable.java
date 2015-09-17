package com.dell.doradus.service.db;

import java.util.Iterator;

public class SequenceIterable<T> implements Iterable<T> {
    private Sequence<T> m_sequence;
    
    public SequenceIterable(Sequence<T> sequence) {
        m_sequence = sequence;
    }

    @Override public Iterator<T> iterator() {
        return new SequenceIterator<T>(m_sequence);
    }

}
