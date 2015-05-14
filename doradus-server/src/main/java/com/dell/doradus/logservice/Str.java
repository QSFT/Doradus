/*
 * Copyright (C) 2014 Dell, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dell.doradus.logservice;

import com.dell.doradus.common.Utils;
import com.dell.doradus.olap.io.BSTR;

public class Str implements Comparable<Str>  {
    private static final byte[] EMPTY_BUFFER = new byte[0]; 
    private byte[] m_buffer;
    private int m_offset;
    private int m_length;

    public Str() {
        clear();
    }

    public Str(byte[] buffer, int offset, int length)
    {
        set(buffer, offset, length);
    }

    public void set(byte[] buffer, int offset, int length)
    {
        m_buffer = buffer;
        m_offset = offset;
        m_length = length;
    }
    
    public void clear() {
        m_buffer = EMPTY_BUFFER;
        m_offset = 0;
        m_length = 0;
    }
    
    public void toBSTR(BSTR value) {
        value.assertLength(m_length);
        value.length = m_length;
        System.arraycopy(m_buffer, m_offset, value.buffer, 0, m_length);
    }
    
    @Override public int compareTo(Str other) {
		return Str.compare(this, other);
	}
    
    @Override public boolean equals(Object obj) {
    	if(obj instanceof Str) return Str.isEqual(this, (Str)obj);
    	else return false;
    };
	
    @Override public int hashCode() {
    	return Str.hashCode(this);
    };
    
    @Override public String toString() {
    	return Utils.toString(m_buffer, m_offset, m_length);
    };
    
    public static int compare(Str x, Str y)
    {
        int l = Math.min(x.m_length, y.m_length);
        for (int i = 0; i < l; i++)
        {
            int bx = (int)(char)x.m_buffer[i];
            int by = (int)(char)y.m_buffer[i];
            if (bx < by) return -1;
            if (bx > by) return 1;
        }
        if (x.m_length < y.m_length) return -1;
        else if (x.m_length > y.m_length) return 1;
        else return 0;
    }

    public static boolean isEqual(Str x, Str y)
    {
    	if(x == y) return true;
        if (x == null || y == null) return false;
        if (x.m_length != y.m_length) return false;
        for (int i = 0; i < x.m_length; i++)
        {
            if (x.m_buffer[i] != y.m_buffer[i]) return false;
        }
        return true;
    }

    public static int hashCode(Str x)
    {
        int c = 0;
        for (int i = 0; i < x.m_length; i++)
        {
            c *= 31;
            c += x.m_buffer[i];
        }
        return c;
    }
	 
}
