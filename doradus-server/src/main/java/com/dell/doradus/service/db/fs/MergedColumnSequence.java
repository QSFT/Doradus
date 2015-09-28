package com.dell.doradus.service.db.fs;

import java.util.ArrayList;

import com.dell.doradus.search.util.HeapList;

public class MergedColumnSequence implements IColumnSequence {
    private ArrayList<Seq> m_list = new ArrayList<>();
    private Seq m_current;
    private FsColumn m_currentColumn;
    private HeapList<Seq> m_heap;
    
    public MergedColumnSequence() {
    }

    public void add(int generation, IColumnSequence sequence) {
        m_list.add(new Seq(generation, sequence));
    }
    
    @Override
    public FsColumn next() {
        while(true) {
            FsColumn nextColumn = moveNext();
            if(nextColumn == null) return null;
            if(m_currentColumn != null && m_currentColumn.equals(nextColumn)) continue;
            m_currentColumn = nextColumn;
            return m_currentColumn;
        }
    }
    
    
    private FsColumn moveNext() {
        if(m_heap == null) {
            if(m_list.size() == 0) return null;
            m_heap = new HeapList<>(m_list.size() - 1);
            m_heap.setInverse(true);
            for(Seq seq: m_list) {
                seq.next();
                m_current = m_heap.AddEx(seq);
            }
        } else {
            m_current.next();
            m_current = m_heap.AddEx(m_current);
        }
        return m_current.getColumn();
    }
    
    @Override
    public boolean isRowDeleted() {
        throw new RuntimeException("isRowDeleted not supported");
    }
    
    class Seq implements Comparable<Seq> {
        private IColumnSequence m_sequence;
        private FsColumn m_current;
        private int m_generation;
        
        public Seq(int generation, IColumnSequence sequence) {
            m_sequence = sequence;
            m_generation = generation;
        }
        
        public void next() {
            m_current = m_sequence.next();
        }
        
        public FsColumn getColumn() {
            return m_current;
        }
        
        @Override
        public int compareTo(Seq x) {
            if(m_current == null && x.m_current == null) return 0;
            //null comes first
            if(m_current == null) return 1;
            if(x.m_current == null) return -1;
            int c = m_current.compareTo(x.m_current);
            if(c != 0) return c;
            //same column. Later values always come before older ones.
            return x.m_generation - m_generation;
        }
        
        
    }
}
