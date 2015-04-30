package com.dell.doradus.spider2;

import java.util.Iterator;
import java.util.List;

import com.dell.doradus.service.db.Tenant;

public class TableIterator implements Iterator<S2Object> {
    private Spider2 m_s2;
    private Tenant m_tenant;
    private String m_application;
    private String m_table;
    
    private List<S2Object> m_currentList;
    private int m_position;

    public TableIterator(Spider2 s2, Tenant tenant, String application, String table) {
        m_s2 = s2;
        m_tenant = tenant;
        m_application = application;
        m_table = table;
        m_currentList = s2.getObjects(tenant, application, table, null);
    }
    
    @Override public boolean hasNext() {
        return m_currentList != null && m_position < m_currentList.size();
    }

    @Override public S2Object next() {
        S2Object obj = m_currentList.get(m_position++);
        if(m_position == m_currentList.size()) {
            m_currentList = m_s2.getObjects(m_tenant, m_application, m_table, obj.getId());
            m_position = 0;
        }
        return obj;
    }

    @Override public void remove() { throw new RuntimeException("Remove not implemented"); }
    
    
}
