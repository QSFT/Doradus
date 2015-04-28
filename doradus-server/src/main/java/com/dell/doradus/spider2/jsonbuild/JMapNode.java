package com.dell.doradus.spider2.jsonbuild;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import com.dell.doradus.spider2.MemoryStream;
import com.dell.doradus.spider2.json.Node;

public class JMapNode extends JNode {
    private LinkedHashMap<String, JNode> children = new LinkedHashMap<>(2);
    
    public JMapNode() {}
    
    public void addNull(String key) {
        children.put(key, JNullNode.instance);
    }
    
    public void addBoolean(String key, boolean value) {
        children.put(key, value ? JTrueNode.instance : JFalseNode.instance);
    }
    
    public void addNumber(String key, long value) {
        children.put(key, new JLongNode(value));
    }
    
    public void addNumber(String key, double value) {
        children.put(key, new JDoubleNode(value));
    }
    
    public void addString(String key, String value) {
        children.put(key, new JStringNode(value));
    }
    
    public JArrayNode addArray(String key) {
        JArrayNode array = new JArrayNode();
        children.put(key, array);
        return array;
    }
    public JMapNode addMap(String key) {
        JMapNode map = new JMapNode();
        children.put(key, map);
        return map;
    }
    
    @Override protected void write(MemoryStream stream) {
        stream.writeByte(Node.TYPE_MAP);
        for(Map.Entry<String, JNode> entry: children.entrySet()) {
            int position = stream.position();
            stream.writeInt(0);
            stream.writeString(entry.getKey());
            entry.getValue().write(stream);
            int nextPosition = stream.position();
            stream.seek(position);
            stream.writeInt(nextPosition - position);
            stream.seek(nextPosition);
        }
        stream.writeInt(0);
    }
    
    @Override protected void read(MemoryStream stream) {
        while(stream.readInt() != 0) {
            String key = stream.readString();
            JNode node = JNode.load(stream);
            children.put(key, node);
        }
    }

    public JNode getChild(String key) {
        return children.get(key);
    }
    
    public String getString(String key) {
        JNode node = getChild(key);
        if(node instanceof JStringNode) return ((JStringNode)node).getValue();
        else return null;
    }
    
    public Set<Map.Entry<String, JNode>> getChildren() {
        return children.entrySet();
    }
    
    @Override protected void write(StringBuilder sb, boolean pretty, int level) {
        sb.append('{');
        int l = 0;
        for(Map.Entry<String, JNode> entry: children.entrySet()) {
            if(l != 0) sb.append(',');
            if(pretty) {
                sb.append('\n');
                for(int i = 0; i < level + 4; i++) sb.append(' ');
            }
            sb.append('\"');
            sb.append(JsonEscape.escape(entry.getKey()));
            sb.append('\"');
            sb.append(':');
            if(pretty) sb.append(' ');
            entry.getValue().write(sb, pretty, level + 4);
            l++;
        }
        if(pretty) {
            sb.append('\n');
            for(int i = 0; i < level; i++) sb.append(' ');
        }
        sb.append('}');
    }
    
    
}