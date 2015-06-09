package com.dell.doradus.logservice;

import java.util.Iterator;

import com.dell.doradus.service.db.Tenant;

public class ChunkIterable implements Iterable<ChunkInfo> {
    private Tenant m_tenant;
    private String m_store;
    private String m_partition;
    
    public ChunkIterable(Tenant tenant, String store, String partition) {
        m_tenant = tenant;
        m_store = store;
        m_partition = partition;
    }

    @Override public Iterator<ChunkInfo> iterator() {
        return new ChunkIterator(m_tenant, m_store, m_partition);
    }
    
}
