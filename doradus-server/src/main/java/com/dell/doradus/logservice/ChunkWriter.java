package com.dell.doradus.logservice;

import java.util.HashMap;
import java.util.Map;

import com.dell.doradus.olap.OlapBatch;
import com.dell.doradus.olap.OlapDocument;
import com.dell.doradus.olap.collections.MemoryStream;
import com.dell.doradus.olap.io.BSTR;

public class ChunkWriter {
    private Temp m_temp;
    private Map<BSTR, FieldBuilder> m_fields;
    private TimestampBuilder m_timestamps;
    private MemoryStream m_output;
    
    
    public ChunkWriter() {
        m_temp = new Temp();
        m_fields = new HashMap<>();
        m_output = new MemoryStream();
    }
    
    public byte[] writeChunk(OlapBatch batch) {
        m_fields.clear();
        int size = batch.size();
        m_timestamps = new TimestampBuilder(size);
        
        for(int doc = 0; doc < size; doc++) {
            OlapDocument d = batch.get(doc);
            m_timestamps.add(doc, d.getId());
            
            for(int i = 0; i < d.getFieldsCount(); i++) {
                BSTR field = d.getFieldNameBinary(i);
                FieldBuilder fb = m_fields.get(field);
                if(fb == null) {
                    field = new BSTR(field);
                    fb = new FieldBuilder(field, size);
                    m_fields.put(field, fb);
                }
                fb.add(doc, d.getFieldValueBinary(i));
            }
        }
        
        m_output.clear();
        m_output.writeByte((byte)1); // version
        m_output.writeVInt(size);
        m_output.writeVInt(m_fields.size());
        m_timestamps.flush(m_output, m_temp);
        for(FieldBuilder fb: m_fields.values()) {
            fb.flush(m_output, m_temp);
        }
        return m_output.toArray();
    }
}
