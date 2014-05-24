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

package com.dell.doradus.olap.io;

import com.dell.doradus.common.Utils;

public class BSTR implements Comparable<BSTR>  {
    public byte[] buffer;
    public int length;

    public BSTR() {
        buffer = new byte[16 * 1024];
    }

    public void assertLength(int length) {
        if (buffer == null || buffer.length < length) {
        	byte[] newbuffer = new byte[length * 2];
        	for(int i = 0; i < buffer.length; i++) {
        		newbuffer[i] = buffer[i];
        	}
        	buffer = newbuffer;
        }
    }

    public BSTR(String str)
    {
        buffer = Utils.toBytes(str);
        length = buffer.length;
    }

    public BSTR(BSTR other)
    {
        length = other.length;
        if(length < 0) length = 0;
        buffer = new byte[length];
        System.arraycopy(other.buffer, 0, buffer, 0, length);
    }

    public BSTR(byte[] bytes)
    {
        buffer = bytes;
        length = buffer.length;
    }
    
    public BSTR(long value) {
    	length = 8;
    	buffer = new byte[8];
    	buffer[7] = (byte)value;
		value >>>= 8;
        buffer[6] = (byte)value;
		value >>>= 8;
		buffer[5] = (byte)value;
		value >>>= 8;
		buffer[4] = (byte)value;
		value >>>= 8;
		buffer[3] = (byte)value;
		value >>>= 8;
		buffer[2] = (byte)value;
		value >>>= 8;
		buffer[1] = (byte)value;
		value >>>= 8;
		buffer[0] = (byte)value;
    }
    
    public void set(Utf8Encoder encoder, String value)
    {
    	assertLength(value.length() * 4);
    	length = encoder.encode(value, buffer);
        //set(bytes);
    }

    public void set(BSTR other)
    {
    	set(other.buffer, other.length);
    }

    public void set(byte[] data)
    {
    	set(data, data.length);
    }

    private void set(byte[] data, int len) {
    	assertLength(len);
    	System.arraycopy(data, 0, buffer, 0, len);
    	length = len;
    }
    
    @Override public int compareTo(BSTR other) {
		return BSTR.compare(this, other);
	}
    
    @Override public boolean equals(Object obj) {
    	if(obj instanceof BSTR) return BSTR.isEqual(this, (BSTR)obj);
    	else return false;
    };
	
    @Override public int hashCode() {
    	return BSTR.hashCode(this);
    };
    
    @Override public String toString() {
    	return new String(buffer, 0, length, Utils.UTF8_CHARSET);
    };
	
    
    public static int compare(BSTR x, BSTR y)
    {
        int l = Math.min(x.length, y.length);
        for (int i = 0; i < l; i++)
        {
            int bx = (int)(char)x.buffer[i];
            int by = (int)(char)y.buffer[i];
            if (bx < by) return -1;
            if (bx > by) return 1;
        }
        if (x.length < y.length) return -1;
        else if (x.length > y.length) return 1;
        else return 0;
    }

    public static boolean isEqual(BSTR x, BSTR y)
    {
    	if(x == y) return true;
        if (x == null || y == null) return false;
        if (x.length != y.length) return false;
        for (int i = 0; i < x.length; i++)
        {
            if (x.buffer[i] != y.buffer[i]) return false;
        }
        return true;
    }

    public static int hashCode(BSTR x)
    {
        int c = 0;
        for (int i = 0; i < x.length; i++)
        {
            c *= 31;
            c += x.buffer[i];
        }
        return c;
    }
	 
}
