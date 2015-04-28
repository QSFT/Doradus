package com.dell.doradus.spider2.jsonbuild;

import com.dell.doradus.spider2.MemoryStream;
import com.dell.doradus.spider2.json.Node;


public abstract class JNode {

    protected abstract void write(StringBuilder sb, boolean pretty, int level);
    protected abstract void write(MemoryStream stream);
    protected abstract void read(MemoryStream stream);
    
    public static JNode fromBytes(byte[] bytes) {
        MemoryStream stream = new MemoryStream(bytes);
        return load(stream);
    }
    
    protected static JNode load(MemoryStream stream) {
        byte type = (byte)stream.readByte();
        JNode node = create(type);
        node.read(stream);
        return node;
    }
    
    protected static JNode create(byte type) {
        switch(type) {
        case Node.TYPE_NULL: return JNullNode.instance;
        case Node.TYPE_FALSE: return JFalseNode.instance;
        case Node.TYPE_TRUE: return JTrueNode.instance;
        case Node.TYPE_LONG: return new JLongNode(0);
        case Node.TYPE_DOUBLE: return new JDoubleNode(0);
        case Node.TYPE_STRING: return new JStringNode(null);
        case Node.TYPE_ARRAY: return new JArrayNode();
        case Node.TYPE_MAP: return new JMapNode();
        default: throw new RuntimeException("Invalid json node: " + type);
        }
    }
    
    public String getString(boolean pretty) {
        StringBuilder sb = new StringBuilder();
        write(sb, pretty, 0);
        return sb.toString();
    }
    
    public byte[] getBytes() {
        MemoryStream stream = new MemoryStream();
        write(stream);
        return stream.toArray();
    }
    
    @Override public String toString() {
        return getString(true);
    }
    
}
