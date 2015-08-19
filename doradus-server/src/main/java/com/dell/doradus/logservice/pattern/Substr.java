package com.dell.doradus.logservice.pattern;

import java.util.Locale;

import com.dell.doradus.common.Utils;


public class Substr {
    public byte[] upper;
    public byte[] lower;
    public int length;

    public Substr(String pattern) {
        create(pattern);
    }
    
    public Substr(String pattern, int beginIndex, int endIndex) {
        pattern = pattern.substring(beginIndex, endIndex);
        create(pattern);
    }
    
    private void create(String pattern) {
        pattern = pattern.toUpperCase(Locale.ROOT);
        upper = Utils.toBytes(pattern);
        pattern = pattern.toLowerCase(Locale.ROOT);
        lower = Utils.toBytes(pattern);
        length = upper.length;
        if(length != lower.length) throw new IncompatibleCaseException();
    }

    public int match(byte[] data, int beginIndex, int endIndex, boolean matchStart, boolean matchEnd) {
        if(matchStart && matchEnd) return equals(data, beginIndex, endIndex) ? beginIndex : -1;
        else if(matchStart) return startsWith(data, beginIndex, endIndex) ? beginIndex : -1;
        else if(matchEnd) return endsWith(data, beginIndex, endIndex) ? endIndex - length : -1;
        else return contains(data, beginIndex, endIndex);
    }
    
    public boolean equals(byte[] data, int beginIndex, int endIndex) {
        if(endIndex - beginIndex != length) return false;
        return match(data, beginIndex);
    }
    
    public boolean startsWith(byte[] data, int beginIndex, int endIndex) {
        if(endIndex - beginIndex < length) return false;
        return match(data, beginIndex);
    }

    public boolean endsWith(byte[] data, int beginIndex, int endIndex) {
        if(endIndex - beginIndex < length) return false;
        return match(data, endIndex - length);
    }

    public int contains(byte[] data, int beginIndex, int endIndex) {
        if(endIndex - beginIndex < length) return -1;
        for(int i = beginIndex; i <= endIndex - length; i++) {
            if(match(data, i)) return i;
        }
        return -1;
    }
    
    private boolean match(byte[] data, int beginIndex) {
        for(int i = 0; i < length; i++) {
            byte b = data[beginIndex + i];
            if(b != upper[i] && b != lower[i]) return false;
        }
        return true;
    }
    
}
