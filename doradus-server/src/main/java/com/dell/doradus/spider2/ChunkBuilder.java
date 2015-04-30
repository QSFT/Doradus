package com.dell.doradus.spider2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

//list of objects sorted by id and stored in Cassandra with lz4 compression
//all objects ids are greader or equal than chunkId.
//initially, there is only one chunk with chunkId = ""
//if the chunk gets too big, it is split in two, first chunk gets id of
//the parent chunk and its first half of objects and second chunk
//gets chunkId of the median object and last half of objects
public class ChunkBuilder {
    private Binary m_chunkId;
    private Map<Binary, S2Object> m_objects = new HashMap<>(); 

    public ChunkBuilder(Chunk chunk) {
        m_chunkId = chunk.getChunkId();
        for(S2Object obj: chunk.getObjects()) {
            m_objects.put(obj.getId(), obj);
        }
    }
    
    public Binary getChunkId() { return m_chunkId; }
    
    public void add(S2Object obj) {
        m_objects.put(obj.getId(), obj);
    }

    public Chunk getChunk() {
        List<S2Object> objects = new ArrayList<>(m_objects.values());
        Collections.sort(objects);
        Chunk chunk = new Chunk(m_chunkId, objects);
        return chunk;
    }
    
}
