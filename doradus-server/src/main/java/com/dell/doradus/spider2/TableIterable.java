package com.dell.doradus.spider2;

import java.util.Iterator;

import com.dell.doradus.service.db.Tenant;

public class TableIterable implements Iterable<S2Object> {
    private Spider2 m_s2;
    private Tenant m_tenant;
    private String m_application;
    private String m_table;
    
    public TableIterable(Spider2 s2, Tenant tenant, String application, String table) {
        m_s2 = s2;
        m_tenant = tenant;
        m_application = application;
        m_table = table;
    }

    @Override public Iterator<S2Object> iterator() {
        return new TableIterator(m_s2, m_tenant, m_application, m_table);
    }
    
}
