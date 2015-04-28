package com.dell.doradus.spider2.fastjson;

import com.dell.doradus.spider2.Binary;
import com.dell.doradus.spider2.MemoryStream;

public class JsonNode {
    public static final byte TYPE_NULL = -1;
    public static final byte TYPE_FALSE = 0;
    public static final byte TYPE_TRUE = 1;
    public static final byte TYPE_LONG = 2;
    public static final byte TYPE_DOUBLE = 3;
    public static final byte TYPE_STRING = 4;
    public static final byte TYPE_ARRAY = 5;
    public static final byte TYPE_MAP = 6;
    
    private MemoryStream m_stream;
    private int m_position;
    private byte m_type;
    
    public JsonNode(byte[] data) {
        this(new MemoryStream(data), 0);
    }
    
    protected JsonNode(MemoryStream stream, int position) {
        m_stream = stream;
        m_position = position;
        m_stream.seek(position);
        m_type = (byte)m_stream.readByte();
        
        if(!isNull() && !isBoolean() && !isNumber() && !isString() && !isArray() && !isMap()) {
            throw new RuntimeException("Invalid node type: " + m_type);
        }
    }
    
    public byte getType() { return m_type; }
    
    public boolean isNull() { return m_type == TYPE_NULL; }
    public boolean isFalse() { return m_type == TYPE_FALSE; }
    public boolean isTrue() { return m_type == TYPE_TRUE; }
    public boolean isBoolean() { return isFalse() || isTrue(); }
    public boolean isLong() { return m_type == TYPE_LONG; }
    public boolean isDouble() { return m_type == TYPE_DOUBLE; }
    public boolean isNumber() { return isLong() || isDouble(); }
    public boolean isString() { return m_type == TYPE_STRING; }
    public boolean isArray() { return m_type == TYPE_ARRAY; }
    public boolean isMap() { return m_type == TYPE_MAP; }
    
    public boolean asBoolean() {
        if(m_type == TYPE_TRUE) return true;
        if(m_type == TYPE_FALSE) return false;
        throw new RuntimeException("boolean type expected: " + m_type);
    }
    
    public long asLong() {
        if(m_type != TYPE_LONG) throw new RuntimeException("long type expected: " + m_type);
        m_stream.seek(m_position + 1); 
        long value = m_stream.readLong();
        return value;
    }

    public double asDouble() {
        if(m_type != TYPE_DOUBLE) throw new RuntimeException("Double type expected: " + m_type);
        m_stream.seek(m_position + 1); 
        long value = m_stream.readLong();
        double dvalue = Double.longBitsToDouble(value);
        return dvalue;
    }

    public Binary asString() {
        if(m_type != TYPE_STRING) return null;
        m_stream.seek(m_position + 1); 
        Binary bstr = m_stream.readBinary();
        return bstr;
    }

    public ArrayIterable getArrayValues() {
        if(m_type != TYPE_ARRAY) throw new RuntimeException("Array expected: " + m_type);
        return new ArrayIterable(m_stream, m_position + 1);
    }

    public MapIterable getMapValues() {
        if(m_type != TYPE_MAP) throw new RuntimeException("Map expected: " + m_type);
        return new MapIterable(m_stream, m_position + 1);
    }
    
    public JsonNode getNode(Binary key) {
        if(m_type != TYPE_MAP) throw new RuntimeException("Map expected: " + m_type);
        int position = m_position + 1;
        m_stream.seek(position);
        int offset = m_stream.readInt();
        while(true) {
            if(offset == 0) return null;
            if(m_stream.compareWith(key)) {
                return new JsonNode(m_stream, m_stream.position());
            }
            position += offset;
            m_stream.seek(position);
            offset = m_stream.readInt();
        }
    }
    
}
