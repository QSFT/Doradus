package com.dell.doradus.spider2.jsonbuild;

import com.dell.doradus.spider2.MemoryStream;
import com.dell.doradus.spider2.json.Node;

public class JStringNode extends JNode {
    private String m_value;
    
    public JStringNode(String value) { m_value = value; }
    
    public String getValue() { return m_value; }
    
    @Override protected void write(StringBuilder sb, boolean pretty, int level) {
        sb.append('"');
        sb.append(escape(m_value));
        sb.append('"');
    }
    
    @Override protected void write(MemoryStream stream) {
        stream.writeByte(Node.TYPE_STRING);
        stream.writeString(m_value);
    }
    
    @Override protected void read(MemoryStream stream) {
        m_value = stream.readString();
    }
    
    public static String escape(String value) {
        boolean needsEscaping = false;
        {
            for(int i = 0; i < value.length(); i++) {
                char ch = value.charAt(i);
                if(ch == '"' || ch >= 128) {
                    needsEscaping = true;
                    break;
                }
            }
            if(!needsEscaping) return value;
        }
        
        StringBuilder sb = new StringBuilder();
        for(int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            if(ch == '"') {
                sb.append('\\');
                sb.append(ch);
            }
            else if(ch < 128) sb.append(ch);
            else {
                sb.append("\\u");
                String chstr = Integer.toString(ch, 16);
                while(chstr.length() < 4) chstr = '0' + chstr;
                sb.append(chstr);
            }
        }
        return sb.toString();
    }
    
}