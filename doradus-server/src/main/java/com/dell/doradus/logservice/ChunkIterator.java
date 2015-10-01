package com.dell.doradus.logservice;

import java.util.Iterator;

import com.dell.doradus.service.db.DBService;
import com.dell.doradus.service.db.DColumn;
import com.dell.doradus.service.db.Tenant;

public class ChunkIterator implements Iterator<ChunkInfo> {
    private Tenant m_tenant;
    private String m_store;
    private String m_partition;
    private Iterator<DColumn> m_iterator;
    private ChunkInfo m_chunkInfo;

    public ChunkIterator(Tenant tenant, String store, String partition) {
        m_tenant = tenant;
        m_store = store;
        m_partition = partition;
        m_iterator = DBService.instance(m_tenant).getAllColumns(m_store, "partitions_" + m_partition).iterator();
        m_chunkInfo = new ChunkInfo();
    }
    
    @Override public boolean hasNext() { return m_iterator != null && m_iterator.hasNext(); }

    @Override public ChunkInfo next() {
        DColumn c = m_iterator.next();
        m_chunkInfo.set(m_partition, c.getName(), c.getRawValue());
        return m_chunkInfo;
    }

    @Override public void remove() { throw new RuntimeException("Remove not implemented"); }
    
    
}
