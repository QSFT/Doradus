package com.dell.doradus.logservice.pattern;




public class Pattern {
    public Substr[] parts;
    public boolean matchStart = true;
    public boolean matchEnd = true;

    public Pattern(String pattern) {
        if(pattern.charAt(0) == '*') {
            pattern = pattern.substring(1);
            matchStart = false;
        }
        if(pattern.charAt(pattern.length() - 1) == '*') {
            pattern = pattern.substring(0, pattern.length() - 1);
            matchEnd = false;
        }
        String[] parts = pattern.split("\\*");
        this.parts = new Substr[parts.length];
        for(int i = 0; i < parts.length; i++) {
            this.parts[i] = new Substr(parts[i]);
        }
    }
    
    public boolean match(byte[] data, int beginIndex, int endIndex) {
        //empty string
        if(parts.length == 0) {
            if(matchStart && matchEnd) return beginIndex == endIndex;
            else return true;
        }
        else if(parts.length == 1) {
            return parts[0].match(data, beginIndex, endIndex, matchStart, matchEnd) >= 0;
        }
        else {
            int start = parts[0].match(data, beginIndex, endIndex, matchStart, false);
            if(start < 0) return false;
            start += parts[0].length;
            for(int i = 1; i < parts.length - 1; i++) {
                start = parts[i].match(data, start, endIndex, false, false);
                if(start < 0) return false;
                start += parts[i].length;
            }
            start = parts[parts.length - 1].match(data, start, endIndex, false, matchEnd);
            if(start < 0) return false;
            return true;
        }
        
        
    }
    
}
