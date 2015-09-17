package com.dell.doradus.service.db;

import java.util.Iterator;

public class SequenceIterator<T> implements Iterator<T> {
    private Sequence<T> m_sequence;
    private boolean m_bStarted = false;
    private T m_next;
    
    public SequenceIterator(Sequence<T> sequence) {
        m_sequence = sequence;
    }

    @Override public boolean hasNext() {
        if(!m_bStarted) {
            m_bStarted = true;
            m_next = m_sequence.next();
        }
        return m_next != null;
    }

    @Override public T next() {
        if(!hasNext()) throw new RuntimeException("Reading past the end of the iterator");
        T next = m_next;
        m_next = m_sequence.next();
        return next;
    }

}
