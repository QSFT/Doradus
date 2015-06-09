package com.dell.doradus.logservice.store;

import java.util.Set;

import com.dell.doradus.olap.OlapBatch;
import com.dell.doradus.olap.OlapDocument;
import com.dell.doradus.olap.io.BSTR;

public class BatchWriter {
    private ChunkWriter m_writer;
    
    public BatchWriter() {
        m_writer = new ChunkWriter();
    }
    
    public ChunkWriter getWriter() { return m_writer; }
    
    public Set<BSTR> getFields() { return m_writer.getFields(); }
    
    public byte[] writeChunk(OlapBatch batch) {
        return writeChunk(batch, 0, batch.size());
    }
    
    public byte[] writeChunk(OlapBatch batch, int start, int size) {
        m_writer.create(size);
        for(int doc = 0; doc < size; doc++) {
            OlapDocument d = batch.get(start + doc);
            m_writer.setTimestamp(doc, d.getId());
            
            for(int i = 0; i < d.getFieldsCount(); i++) {
                FieldBuilder fb = m_writer.getFieldBulider(d.getFieldNameBinary(i));
                fb.add(doc, d.getFieldValueBinary(i));
            }
        }

        return m_writer.getData();
    }
}
