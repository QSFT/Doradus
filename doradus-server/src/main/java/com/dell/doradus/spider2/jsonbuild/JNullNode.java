package com.dell.doradus.spider2.jsonbuild;

import com.dell.doradus.spider2.MemoryStream;
import com.dell.doradus.spider2.json.Node;

public class JNullNode extends JNode {
    public static final JNullNode instance = new JNullNode();
    
    private JNullNode() {}
    
    @Override protected void write(StringBuilder sb, boolean pretty, int level) {
        sb.append("null");
    }
    
    @Override protected void write(MemoryStream stream) {
        stream.writeByte(Node.TYPE_NULL);
    }
    
    @Override protected void read(MemoryStream stream) {
    }
    
}