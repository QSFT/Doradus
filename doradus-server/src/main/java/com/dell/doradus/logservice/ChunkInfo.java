package com.dell.doradus.logservice;

import com.dell.doradus.logservice.store.ChunkWriter;
import com.dell.doradus.olap.collections.MemoryStream;


public class ChunkInfo {
    private String m_partition;
    private String m_chunkId;
    private int m_eventsCount;
    private long m_minTimestamp;
    private long m_maxTimestamp;

    public ChunkInfo() { }

    public String getPartition() { return m_partition; }
    public String getChunkId() { return m_chunkId; }
    public int getEventsCount() { return m_eventsCount; }
    public long getMinTimestamp() { return m_minTimestamp; }
    public long getMaxTimestamp() { return m_maxTimestamp; }
    
    public void set(String partition, String chunkId, ChunkWriter writer) {
        m_partition = partition;
        m_chunkId = chunkId;
        m_eventsCount = writer.getSize();
        m_minTimestamp = writer.getMinTimestamp();
        m_maxTimestamp = writer.getMaxTimestamp();
    }

    public void set(String partition, String chunkId, byte[] data) {
        m_partition = partition;
        m_chunkId = chunkId;
        MemoryStream ms = new MemoryStream(data);
        int version = ms.readByte();
        if(version != 1) throw new RuntimeException("Unknown version");
        m_eventsCount = ms.readInt();
        m_minTimestamp = ms.readLong();
        m_maxTimestamp = ms.readLong();
    }
    
    public byte[] getByteData() {
        byte[] data = new byte[21];
        MemoryStream ms = new MemoryStream(data);
        ms.writeByte((byte)1); // version
        ms.writeInt(m_eventsCount);
        ms.writeLong(m_minTimestamp);
        ms.writeLong(m_maxTimestamp);
        return data;
    }
    
}
