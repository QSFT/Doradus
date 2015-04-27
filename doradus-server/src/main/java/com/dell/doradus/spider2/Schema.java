package com.dell.doradus.spider2;

import java.util.TreeSet;

public class Schema {
    private TreeSet<String> m_ids = new TreeSet<String>();
    
    public Schema() {}
    
    public int size() { return m_ids.size(); }
    public TreeSet<String> getIds() { return m_ids; }
    public void addId(String id) { m_ids.add(id); }
    
    public String getChunkId(String objectId) {
        return m_ids.floor(objectId);
    }
    
    public String getChunkIdAfter(String objectId) {
        if(objectId == null || "".equals(objectId)) return m_ids.first();
        else return m_ids.higher(objectId);
    }
    
    public byte[] toByteArray() {
        MemoryStream buffer = new MemoryStream();
        for(String id: m_ids) {
            buffer.writeString(id);
        }
        byte[] data = buffer.toArray();
        data = ChunkCompression.compress(data);
        return data;
    }
    
    public static Schema fromByteArray(byte[] data) {
        data = ChunkCompression.decompress(data);
        Schema schema = new Schema();
        MemoryStream buffer = new MemoryStream(data);
        while(!buffer.end()) {
            String id = buffer.readString();
            schema.addId(id);
        }
        return schema;
    }
}
