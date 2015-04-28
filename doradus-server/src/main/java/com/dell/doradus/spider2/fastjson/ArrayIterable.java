package com.dell.doradus.spider2.fastjson;

import java.util.Iterator;

import com.dell.doradus.spider2.MemoryStream;

public class ArrayIterable implements Iterable<JsonNode> {
    private MemoryStream m_stream;
    private int m_position;
    
    
    public ArrayIterable(MemoryStream stream, int position) {
        m_stream = stream;
        m_position = position;
    }

    @Override public Iterator<JsonNode> iterator() {
        return new ArrayIterator(m_stream, m_position);
    }
    
}
