package com.dell.doradus.spider2.jsonbuild;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.dell.doradus.spider2.MemoryStream;
import com.dell.doradus.spider2.json.Node;

public class JArrayNode extends JNode {
    private List<JNode> children = new ArrayList<>(2);
    
    public JArrayNode() {}
    
    public void addNull() {
        children.add(JNullNode.instance);
    }
    
    public void addBoolean(boolean value) {
        children.add(value ? JTrueNode.instance : JFalseNode.instance);
    }
    
    public void addNumber(long value) {
        children.add(new JLongNode(value));
    }
    
    public void addNumber(double value) {
        children.add(new JDoubleNode(value));
    }
    
    public void addString(String value) {
        children.add(new JStringNode(value));
    }
    
    public JArrayNode addArray() {
        JArrayNode array = new JArrayNode();
        children.add(array);
        return array;
    }
    public JMapNode addMap() {
        JMapNode map = new JMapNode();
        children.add(map);
        return map;
    }
    
    public Collection<JNode> getChildren() { return children; }
    
    @Override protected void write(MemoryStream stream) {
        stream.writeByte(Node.TYPE_ARRAY);
        for(JNode node: children) {
            int position = stream.position();
            stream.writeInt(0);
            node.write(stream);
            int nextPosition = stream.position();
            stream.seek(position);
            stream.writeInt(nextPosition - position);
            stream.seek(nextPosition);
        }
        stream.writeInt(0);
    }
    
    @Override protected void read(MemoryStream stream) {
        while(stream.readInt() != 0) {
            JNode node = JNode.load(stream);
            children.add(node);
        }
    }
    
    
    @Override protected void write(StringBuilder sb, boolean pretty, int level) {
        sb.append('[');
        int l = 0;
        for(JNode node: children) {
            if(l != 0) sb.append(',');
            if(pretty) {
                sb.append('\n');
                for(int i = 0; i < level + 4; i++) sb.append(' ');
            }
            node.write(sb, pretty, level + 4);
            l++;
        }
        if(pretty) {
            sb.append('\n');
            for(int i = 0; i < level; i++) sb.append(' ');
        }
        sb.append(']');
    }
    
    
}