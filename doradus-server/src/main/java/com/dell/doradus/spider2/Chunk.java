package com.dell.doradus.spider2;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

//list of objects sorted by id and stored in Cassandra with lz4 compression
//all objects ids are greader or equal than chunkId.
//initially, there is only one chunk with chunkId = ""
//if the chunk gets too big, it is split in two, first chunk gets id of
//the parent chunk and its first half of objects and second chunk
//gets chunkId of the median object and last half of objects
public class Chunk {
    private Binary m_chunkId;
    private Binary m_nextId;
    private List<S2Object> m_objects = new ArrayList<>();

    public Chunk(Binary chunkId, Binary nextId) {
        m_chunkId = chunkId;
        m_nextId = nextId;
    }

    public Chunk(Binary chunkId, Binary nextId, List<S2Object> objects) {
        m_chunkId = chunkId;
        m_nextId = nextId;
        m_objects = objects;
    }
    
    public Binary getChunkId() { return m_chunkId; }
    public Binary getNextId() { return m_nextId; }
    public int size() { return m_objects.size(); }
    public S2Object get(int index) { return m_objects.get(index); }
    public Collection<S2Object> getObjects() { return m_objects; }
    
    public void setNextId(Binary nextId) {
        m_nextId = nextId;
    }
    
    public void add(S2Object obj) {
        m_objects.add(obj);
    }
    
    public byte[] toByteArray() {
        MemoryStream buffer = new MemoryStream();
        buffer.write(m_chunkId);
        buffer.write(m_nextId);
        for(S2Object obj: m_objects) {
            obj.write(buffer);
        }
        byte[] data = buffer.toArray();
        data = ChunkCompression.compress(data);
        return data;
    }
    
    public static Chunk fromByteArray(byte[] data) {
        data = ChunkCompression.decompress(data);
        MemoryStream buffer = new MemoryStream(data);
        Binary chunkId = buffer.readBinary();
        Binary nextId = buffer.readBinary();
        Chunk chunk = new Chunk(chunkId, nextId);
        
        while(!buffer.end()) {
            S2Object obj = S2Object.read(buffer);
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
        Chunk chunk = new Chunk(getChunkId(), getNextId());
        chunks.add(chunk);
        for(S2Object obj: getObjects()) {
            if(chunk.size() >= splitSize) {
                chunk.setNextId(obj.getId());
                chunk = new Chunk(obj.getId(), getNextId());
                chunks.add(chunk);
            }
            chunk.add(obj);
        }
        return chunks;
    }
}
