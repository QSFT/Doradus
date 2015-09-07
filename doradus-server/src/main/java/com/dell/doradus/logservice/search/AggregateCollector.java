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
    protected String m_pattern;
    
    public AggregateCollector() {}

    abstract public void addChunk(ChunkInfo info);
    abstract public AggregationResult getResult();
    
    public void setContext(LogService logService, Tenant tenant, String application, String table, String pattern) {
        m_logService = logService;
        m_tenant = tenant;
        m_application = application;
        m_table = table;
        m_pattern = pattern;
    }
    
    //public void 
    
    protected void read(ChunkInfo info) {
        if(m_reader == null) {
            m_reader = new ChunkReader();
            if(m_pattern != null) m_reader.setSyntheticFields(m_pattern);
        }
        m_logService.readChunk(m_tenant, m_application, m_table, info, m_reader);
    }
    
    
}
