package com.dell.doradus.logservice;

import java.util.Iterator;

import com.dell.doradus.service.db.Tenant;

public class ChunkIterable implements Iterable<ChunkReader> {
    private Tenant m_tenant;
    private String m_application;
    private String m_partition;
    
    public ChunkIterable(Tenant tenant, String application, String partition) {
        m_tenant = tenant;
        m_application = application;
        m_partition = partition;
    }

    @Override public Iterator<ChunkReader> iterator() {
        return new ChunkIterator(m_tenant, m_application, m_partition);
    }
    
}
