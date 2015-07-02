package com.dell.doradus.logservice.search;

import com.dell.doradus.logservice.ChunkInfo;
import com.dell.doradus.logservice.ChunkReader;
import com.dell.doradus.logservice.LogService;
import com.dell.doradus.olap.aggregate.AggregationResult;
import com.dell.doradus.service.db.Tenant;

public abstract class AggregateCollector {
    protected LogService m_logService;
    protected Tenant m_tenant;
    protected String m_application;
    protected String m_table;
    protected ChunkReader m_reader;
    
    public AggregateCollector() {}

    abstract public void addChunk(ChunkInfo info);
    abstract public AggregationResult getResult();
    
    public void setContext(LogService logService, Tenant tenant, String application, String table) {
        m_logService = logService;
        m_tenant = tenant;
        m_application = application;
        m_table = table;
    }
    
    protected void read(ChunkInfo info) {
        if(m_reader == null) m_reader = new ChunkReader();
        m_logService.readChunk(m_tenant, m_application, m_table, info, m_reader);
    }
    
    
}
