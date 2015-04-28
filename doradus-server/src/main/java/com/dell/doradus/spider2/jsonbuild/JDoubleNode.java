package com.dell.doradus.spider2.jsonbuild;

import com.dell.doradus.spider2.MemoryStream;
import com.dell.doradus.spider2.json.Node;

public class JDoubleNode extends JNode {
    private double m_value;
    
    public JDoubleNode(double value) { m_value = value; }
    public double getValue() { return m_value; }
    
    @Override protected void write(StringBuilder sb, boolean pretty, int level) {
        sb.append(Double.toString(m_value));
    }
    
    @Override protected void write(MemoryStream stream) {
        stream.writeByte(Node.TYPE_DOUBLE);
        stream.writeDouble(m_value);
    }
   
    @Override protected void read(MemoryStream stream) {
        m_value = stream.readDouble();
    }
    
}