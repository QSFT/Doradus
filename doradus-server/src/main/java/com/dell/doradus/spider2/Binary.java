package com.dell.doradus.spider2;

import com.dell.doradus.common.Utils;

public class Binary implements Comparable<Binary> {
    public static final Binary EMPTY = new Binary(""); 
    
    private byte[] m_data;
    
    public Binary(String string) {
        m_data = Utils.toBytes(string);
    }

    public Binary(byte[] data) {
        m_data = data;
    }
    
    public Binary(byte[] data, int offset, int length) {
        m_data = new byte[length];
        System.arraycopy(data, offset, m_data, 0, length);
    }
    
    public int length() { return m_data.length; }
    public byte[] getBuffer() { return m_data; }
    public String getString() { return Utils.toString(m_data); }
    
    @Override public String toString() {
        return Utils.toString(m_data);
    }
    
    @Override public int hashCode() {
        int c = 0;
        for (int i = 0; i < m_data.length; i++) {
            c *= 31;
            c += m_data[i];
        }
        return c;
    }
    
    @Override public boolean equals(Object obj) {
        Binary other = (Binary)obj;
        if (m_data.length != other.m_data.length) return false;
        for (int i = 0; i < m_data.length; i++) {
            if (m_data[i] != other.m_data[i]) return false;
        }
        return true;
    }

    @Override public int compareTo(Binary o) {
        int l = Math.min(m_data.length, o.m_data.length);
        for (int i = 0; i < l; i++) {
            int bx = (int)(char)m_data[i];
            int by = (int)(char)o.m_data[i];
            if (bx < by) return -1;
            if (bx > by) return 1;
        }
        if (m_data.length < o.m_data.length) return -1;
        else if (m_data.length > o.m_data.length) return 1;
        else return 0;
    }
    
}
