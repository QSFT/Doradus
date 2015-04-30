package com.dell.doradus.spider2;

import java.util.TreeSet;

public class Schema {
    private TreeSet<Binary> m_ids = new TreeSet<Binary>();
    
    public Schema() {}
    
    public int size() { return m_ids.size(); }
    public TreeSet<Binary> getIds() { return m_ids; }
    public void addId(Binary id) { m_ids.add(id); }
    
    public Binary getChunkId(Binary objectId) {
        return m_ids.floor(objectId);
    }
    
    public Binary getChunkIdAfter(Binary objectId) {
        if(objectId == null) return m_ids.first();
        else return m_ids.higher(objectId);
    }
    
    public byte[] toByteArray() {
        MemoryStream buffer = new MemoryStream();
        for(Binary id: m_ids) {
            buffer.write(id);
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
            Binary id = buffer.readBinary();
            schema.addId(id);
        }
        return schema;
    }
}
