package com.dell.doradus.spider2.jsonbuild;

import com.dell.doradus.spider2.MemoryStream;
import com.dell.doradus.spider2.fastjson.JsonNode;

public class JLongNode extends JNode {
    private long m_value;
    
    public JLongNode(long value) { m_value = value; }
    public long getValue() { return m_value; }

    @Override protected void write(StringBuilder sb, boolean pretty, int level) {
        sb.append(Long.toString(m_value));
    }

    @Override protected void write(MemoryStream stream) {
        stream.writeByte(JsonNode.TYPE_LONG);
        stream.writeLong(m_value);
    }
    
    @Override protected void read(MemoryStream stream) {
        m_value = stream.readLong();
    }
    
}
