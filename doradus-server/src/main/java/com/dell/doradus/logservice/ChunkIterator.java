package com.dell.doradus.logservice;

import java.util.Iterator;

import com.dell.doradus.service.db.DBService;
import com.dell.doradus.service.db.DColumn;
import com.dell.doradus.service.db.Tenant;

public class ChunkIterator implements Iterator<ChunkReader> {
    private Tenant m_tenant;
    private String m_store;
    private String m_partition;
    private Iterator<DColumn> m_iterator;
    private ChunkReader m_chunk;

    public ChunkIterator(Tenant tenant, String store, String partition) {
        m_tenant = tenant;
        m_store = store;
        m_partition = partition;
        m_iterator = DBService.instance().getAllColumns(m_tenant, m_store, "partitions_" + m_partition);
        m_chunk = new ChunkReader();
    }
    
    @Override public boolean hasNext() { return m_iterator != null && m_iterator.hasNext(); }

    @Override public ChunkReader next() {
        String chunkid = m_iterator.next().getName();
        DColumn column = DBService.instance().getColumn(m_tenant, m_store, m_partition, chunkid);
        m_chunk.read(column.getRawValue());
        return m_chunk;
    }

    @Override public void remove() { throw new RuntimeException("Remove not implemented"); }
    
    
}
