package com.dell.doradus.logservice.search;

import com.dell.doradus.olap.io.BSTR;

public class StrRef {
    private byte[] m_data;
    private int m_offset;
    private int m_length;
    
    public StrRef() {}
    
    public void set(byte[] data, int offset, int length) {
        m_data = data;
        m_offset = offset;
        m_length = length;
    }
    
    public byte[] getData() { return m_data; }
    public int getOffset() { return m_offset; }
    public int getLength() { return m_length; }
    
    public int compare(BSTR str) {
        int len = m_length < str.length ? m_length : str.length;
        for (int i = 0; i < len; i++)
        {
            int bx = (int)(char)m_data[m_offset + i];
            int by = (int)(char)str.buffer[i];
            if (bx < by) return -1;
            if (bx > by) return 1;
        }
        if (m_length < str.length) return -1;
        else if (m_length > str.length) return 1;
        else return 0;
    }
    
    public boolean isEqual(BSTR str) {
        if(m_length != str.length) return false;
        for (int i = 0; i < m_length; i++)
        {
            if(m_data[m_offset + i] != str.buffer[i]) return false;
        }
        return true;
    }
    
    public boolean contains(BSTR upper, BSTR lower) {
        for(int pos = 0; pos <= m_length - upper.length; pos++) {
            if(isSubstring(pos, upper, lower)) return true;
        }
        return false;
    }

    public boolean equals(BSTR upper, BSTR lower) {
        return isSubstring(0, upper, lower);
    }
    
    private boolean isSubstring(int pos, BSTR upper, BSTR lower) {
        if(pos + upper.length > m_length) return false;
        for(int i = 0; i < upper.length; i++) {
            byte b = m_data[m_offset + pos + i]; 
            if(b != upper.buffer[i] && b != lower.buffer[i]) return false;
        }
        return true;
    }
    

    public boolean matchesPattern(BSTR upper, BSTR lower) {
        // Move through string as it matches pattern.
        int strInx = 0;
        int patInx = 0;
        while (strInx < m_length) {
            // Did we consume all pattern chars?
            if (patInx >= upper.length) {
                // Pattern ended but more chars in string
                return false;
            }

            if (upper.buffer[patInx] == '*') {
                // Multi-char wildcard; start by skipping all next wildcard chars
                do patInx++;
                while (patInx < upper.length && (upper.buffer[patInx] == '*' || upper.buffer[patInx] == '?'));
                if (patInx >= upper.length) {
                    // Rest of pattern was wildcards; string is considered matched.
                    return true;
                }

                // See if string contains the current non-wildcard pattern char subset
                boolean bSubsetMatched = false;
                int strStartInx = strInx;
                do {
                    // Skip to next string char that matches current char in pattern
                    strInx = strStartInx;
                    while (strInx < m_length && m_data[m_offset + strInx] != upper.buffer[patInx] && m_data[m_offset + strInx] != lower.buffer[patInx]) {
                        strInx++;
                    }
                    if (strInx >= m_length) {
                        // Hit end of string without finding a match.
                        return false;
                    }

                    // See how far string and pattern characters match.
                    int subPatInx = patInx;
                    do {
                        // Current string and subset chars match; skip both.
                        subPatInx++;
                        strInx++;
                    } while (strInx < m_length &&
                             subPatInx < upper.length &&
                             upper.buffer[subPatInx] != '*' &&
                             (m_data[m_offset + strInx] == upper.buffer[subPatInx] ||
                              m_data[m_offset + strInx] == lower.buffer[subPatInx] ||
                              upper.buffer[subPatInx] == '?'));
                    if ((subPatInx >= upper.length && strInx >= m_length) ||
                        (subPatInx < upper.length && upper.buffer[subPatInx] == '*')) {
                        // String matched pattern subset (*) or entire rest of pattern.
                        bSubsetMatched = true;
                        patInx = subPatInx;
                    } else {
                        strStartInx++;
                    }
                } while (!bSubsetMatched);
            } else if (upper.buffer[patInx] == '?' ||
                       m_data[m_offset + strInx] == upper.buffer[patInx] ||
                       m_data[m_offset + strInx] == lower.buffer[patInx]) {
                // single char matched; advance to next char
                strInx++;
                patInx++;
            } else {
                return false;                 // String char didn't match pattern char
            }
        }

        // If we get here, we hit the end of string; it matches the pattern if the
        // rest of the pattern consists only of '*'
        while (patInx < upper.length && upper.buffer[patInx] == '*') {
            patInx++;
        }
        return patInx >= upper.length;
    }   // matchesPattern
    
}
