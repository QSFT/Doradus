package com.dell.doradus.spider2.jsonbuild;

import com.dell.doradus.spider2.MemoryStream;
import com.dell.doradus.spider2.fastjson.JsonNode;

public class JFalseNode extends JNode {
    public static final JFalseNode instance = new JFalseNode();
    private JFalseNode() {}
    
    @Override protected void write(StringBuilder sb, boolean pretty, int level) {
        sb.append("false");
    }
    
    @Override protected void write(MemoryStream stream) {
        stream.writeByte(JsonNode.TYPE_FALSE);
    }

    @Override protected void read(MemoryStream stream) {
    }
    
}