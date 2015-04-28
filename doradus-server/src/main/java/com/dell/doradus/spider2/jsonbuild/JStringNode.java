package com.dell.doradus.spider2.jsonbuild;

import com.dell.doradus.spider2.MemoryStream;
import com.dell.doradus.spider2.json.Node;

public class JStringNode extends JNode {
    private String m_value;
    
    public JStringNode(String value) { m_value = value; }
    
    public String getValue() { return m_value; }
    
    @Override protected void write(StringBuilder sb, boolean pretty, int level) {
        sb.append('"');
        sb.append(JsonEscape.escape(m_value));
        sb.append('"');
    }
    
    @Override protected void write(MemoryStream stream) {
        stream.writeByte(Node.TYPE_STRING);
        stream.writeString(m_value);
    }
    
    @Override protected void read(MemoryStream stream) {
        m_value = stream.readString();
    }
    
    
}