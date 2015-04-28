package com.dell.doradus.spider2.fastjson;

import java.util.Iterator;

import com.dell.doradus.spider2.MemoryStream;

public class ArrayIterator implements Iterator<JsonNode> {
    private MemoryStream m_stream;
    private int m_position;
    private int m_offset;

    public ArrayIterator(MemoryStream stream, int position) {
        m_stream = stream;
        m_position = position;
        m_stream.seek(m_position);
        m_offset = m_stream.readInt();
    }
    
    @Override public boolean hasNext() { return m_offset != 0; }

    @Override public JsonNode next() {
        if(!hasNext()) throw new RuntimeException("Iterator finished");
        JsonNode node = new JsonNode(m_stream, m_position + 4);
        m_position += m_offset;
        m_stream.seek(m_position);
        m_offset = m_stream.readInt();
        return node;
    }

    @Override public void remove() { throw new RuntimeException("Remove not implemented"); }
    
    
}
