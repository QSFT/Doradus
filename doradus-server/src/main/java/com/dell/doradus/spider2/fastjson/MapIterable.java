package com.dell.doradus.spider2.fastjson;

import java.util.Iterator;

import com.dell.doradus.spider2.MemoryStream;

public class MapIterable implements Iterable<MapEntry> {
    private MemoryStream m_stream;
    private int m_position;
    
    
    public MapIterable(MemoryStream stream, int position) {
        m_stream = stream;
        m_position = position;
    }

    @Override public Iterator<MapEntry> iterator() {
        return new MapIterator(m_stream, m_position);
    }
    
}
