package com.dell.doradus.spider2;

import java.util.Iterator;

import com.dell.doradus.service.db.Tenant;

public class TableIterator implements Iterator<S2Object> {
    private Spider2 m_s2;
    private Tenant m_tenant;
    private String m_application;
    private String m_table;
    
    private Chunk m_currentChunk;
    private int m_position;

    public TableIterator(Spider2 s2, Tenant tenant, String application, String table) {
        m_s2 = s2;
        m_tenant = tenant;
        m_application = application;
        m_table = table;
        m_currentChunk = s2.getObjects(tenant, application, table, Binary.EMPTY);
    }
    
    @Override public boolean hasNext() {
        return m_currentChunk != null && m_position < m_currentChunk.size();
    }

    @Override public S2Object next() {
        S2Object obj = m_currentChunk.get(m_position++);
        if(m_position == m_currentChunk.size()) {
            Binary nextId = m_currentChunk.getNextId();
            if(Binary.EMPTY.equals(nextId)) {
                m_currentChunk = null;
            } else {
                m_currentChunk = m_s2.getObjects(m_tenant, m_application, m_table, nextId);
            }
            m_position = 0;
        }
        return obj;
    }

    @Override public void remove() { throw new RuntimeException("Remove not implemented"); }
    
    
}
