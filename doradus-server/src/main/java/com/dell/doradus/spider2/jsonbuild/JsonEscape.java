package com.dell.doradus.spider2.jsonbuild;

public class JsonEscape {
    
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
