package com.dell.doradus.spider2;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.TreeMap;

import com.dell.doradus.spider2.jsonbuild.JMapNode;
import com.dell.doradus.spider2.jsonbuild.JNode;

//list of objects sorted by id and stored in Cassandra with lz4 compression
//all objects ids are greader or equal than chunkId.
//initially, there is only one chunk with chunkId = ""
//if the chunk gets too big, it is split in two, first chunk gets id of
//the parent chunk and its first half of objects and second chunk
//gets chunkId of the median object and last half of objects
public class Chunk {
    private String m_chunkId;
    private TreeMap<String, S2Object> m_objects = new TreeMap<>();

    public Chunk(String chunkId) { m_chunkId = chunkId; }
    
    public String getChunkId() { return m_chunkId; }
    
    public int size() { return m_objects.size(); }
    
    public TreeMap<String, S2Object> getObjectsMap() { return m_objects; }
    public Collection<S2Object> getObjects() { return m_objects.values(); }
    
    public void add(S2Object obj) {
        m_objects.put(obj.getId(), obj);
    }
    
    public byte[] toByteArray() {
        MemoryStream buffer = new MemoryStream();
        for(S2Object obj: m_objects.values()) {
            JMapNode unode = obj.getData();
            byte[] bytes = unode.getBytes();
            buffer.writeVInt(bytes.length);
            buffer.write(bytes, 0, bytes.length);
        }
        byte[] data = buffer.toArray();
        data = ChunkCompression.compress(data);
        return data;
    }
    
    public static Chunk fromByteArray(String chunkId, byte[] data) {
        data = ChunkCompression.decompress(data);
        Chunk chunk = new Chunk(chunkId);
        MemoryStream buffer = new MemoryStream(data);
        while(!buffer.end()) {
            int length = buffer.readVInt();
            byte[] bytes = new byte[length];
            buffer.read(bytes, 0, length);
            JMapNode node = (JMapNode)JNode.fromBytes(bytes);
            S2Object obj = new S2Object(node);
            chunk.add(obj);
        }
        return chunk;
    }
    
    //actually, we can split in more than two chunks, we need all chunks to be smaller than
    //maxSize. First chunk always gets parent chunk's id
    public List<Chunk> split(int maxSize) {
        int size = size();
        if(size <= maxSize) throw new RuntimeException("Chunk is too small to split");
        int splitParts = (size + maxSize - 1) / maxSize;
        int splitSize = (size + splitParts - 1) / splitParts;
        List<Chunk> chunks = new ArrayList<Chunk>(splitParts);
        //first chunk gets the id of the parent chunk
        Chunk chunk = new Chunk(getChunkId());
        chunks.add(chunk);
        for(S2Object obj: getObjects()) {
            if(chunk.size() >= splitSize) {
                chunk = new Chunk(obj.getId());
                chunks.add(chunk);
            }
            chunk.add(obj);
        }
        return chunks;
    }
}
