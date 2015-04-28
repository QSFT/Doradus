package com.dell.doradus.spider2.fastjson;

import java.util.Iterator;

import com.dell.doradus.spider2.Binary;
import com.dell.doradus.spider2.MemoryStream;

public class MapIterator implements Iterator<MapEntry> {
    private MemoryStream m_stream;
    private int m_position;
    private int m_offset;

    public MapIterator(MemoryStream stream, int position) {
        m_stream = stream;
        m_position = position;
        m_stream.seek(m_position);
        m_offset = m_stream.readInt();
    }
    
    @Override public boolean hasNext() { return m_offset != 0; }

    @Override public MapEntry next() {
        if(!hasNext()) throw new RuntimeException("Iterator finished");
        m_stream.seek(m_position + 4);
        Binary key = m_stream.readBinary();
        JsonNode node = new JsonNode(m_stream, m_stream.position());
        MapEntry entry = new MapEntry(key, node);
        m_position += m_offset;
        m_stream.seek(m_position);
        m_offset = m_stream.readInt();
        return entry;
    }

    @Override public void remove() { throw new RuntimeException("Remove not implemented"); }
    
    
}
