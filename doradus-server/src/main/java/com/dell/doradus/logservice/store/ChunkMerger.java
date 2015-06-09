package com.dell.doradus.logservice.store;

import java.util.List;

import com.dell.doradus.logservice.ChunkInfo;
import com.dell.doradus.logservice.ChunkReader;
import com.dell.doradus.logservice.LogService;
import com.dell.doradus.olap.io.BSTR;
import com.dell.doradus.service.db.Tenant;

public class ChunkMerger {
    private LogService m_logService;
    private Tenant m_tenant;
    private String m_application;
    private String m_table;
    private ChunkWriter m_writer;
    
    public ChunkMerger(LogService logService, Tenant tenant, String application, String table) {
        m_logService = logService;
        m_tenant = tenant;
        m_application = application;
        m_table = table;
        m_writer = new ChunkWriter();
    }
    
    public Tenant getTenant() { return m_tenant; }
    public String getApplication() { return m_application; }
    public String getTable() { return m_table; }
    
    public ChunkWriter getWriter() { return m_writer; }
    
    public byte[] mergeChunks(List<ChunkInfo> infos) {
        int size = 0;
        int[] docOffsets = new int[infos.size()];
        for(int i = 0; i < infos.size(); i++) {
            ChunkInfo info = infos.get(i);
            docOffsets[i] = size;
            size += info.getEventsCount();
        }
        ChunkReader reader = new ChunkReader();
        BSTR value = new BSTR();
        
        m_writer.create(size);
        
        for(int segment = 0; segment < infos.size(); segment++) {
            ChunkInfo info = infos.get(segment);
            int docOffset = docOffsets[segment];
            m_logService.readChunk(m_tenant, m_application, m_table, info, reader);
            for(int doc = 0; doc < reader.size(); doc++) {
                m_writer.setTimestamp(docOffset + doc, reader.getTimestamp(doc));
            }
            
            List<BSTR> fields = reader.getFieldNames();
            
            for(int fieldIndex = 0; fieldIndex < fields.size(); fieldIndex++) {
                FieldBuilder fb = m_writer.getFieldBulider(fields.get(fieldIndex));
                for(int doc = 0; doc < reader.size(); doc++) {
                    reader.getFieldValue(doc, fieldIndex, value);
                    if(value.length <= 0) continue;
                    fb.add(docOffset + doc, value);
                }
            }
            
        }

        return m_writer.getData();
    }
}
