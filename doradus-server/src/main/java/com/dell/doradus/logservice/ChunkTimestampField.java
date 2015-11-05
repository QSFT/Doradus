package com.dell.doradus.logservice;

import com.dell.doradus.logservice.store.Temp;
import com.dell.doradus.olap.collections.MemoryStream;

public class ChunkTimestampField {
    private int m_size;
    private int m_offset;
    private long[] m_timestamps;
    private MemoryStream m_input;
    
    private int m_CmpSize;
    
    public ChunkTimestampField(int size, MemoryStream input) {
        m_size = size;
        m_offset = input.position();
        m_input = input;
        Temp.skipCompressed(input);
        m_CmpSize = input.position() - m_offset; 
    }
    
    public void readField() {
        if(m_timestamps != null) return;
        
        m_input.seek(m_offset);
        m_timestamps = new long[m_size];
        MemoryStream s_ts = Temp.readCompressed(m_input);
        long last = 0;
        for(int i = 0; i < m_size; i++) {
            last += s_ts.readVLong();
            m_timestamps[i] = last;
        }
    }
    
    public int size() { return m_size; }
    
    public long getTimestamp(int doc) {
        readField();
        return m_timestamps[doc];
    }
    
    public long[] getTimestamps() {
        readField();
        return m_timestamps;
    }
    
    public void printSize(StringBuilder sb) {
    	sb.append("Timestamp: " + m_CmpSize + "\n");
    }
    
}
