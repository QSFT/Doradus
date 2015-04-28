package com.dell.doradus.spider2.jsonbuild;

import com.dell.doradus.spider2.MemoryStream;
import com.dell.doradus.spider2.fastjson.JsonNode;

public class JTrueNode extends JNode {
    public static final JTrueNode instance = new JTrueNode();
    
    private JTrueNode() {}
    
    @Override protected void write(StringBuilder sb, boolean pretty, int level) {
        sb.append("true");
    }
    
    @Override protected void write(MemoryStream stream) {
        stream.writeByte(JsonNode.TYPE_TRUE);
    }
    
    @Override protected void read(MemoryStream stream) {
    }
    
}