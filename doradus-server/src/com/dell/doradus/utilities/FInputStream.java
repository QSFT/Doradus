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

package com.dell.doradus.utilities;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import com.dell.doradus.olap.io.BSTR;

public class FInputStream {
	private static final int BUFFER_SIZE = 128 * 1024;
	private FileInputStream m_input;
	private long m_length;
    private byte[] m_buffer = new byte[BUFFER_SIZE];
    private long m_buffersCount;

    private long m_bufferNumber;
    private int m_bufferLength;
    private int m_positionInBuffer;

    public FInputStream(String fileName)
    {
    	try {
        	File file = new File(fileName);
			m_input = new FileInputStream(file);
	    	m_length = file.length();
	        m_buffersCount = (m_length + BUFFER_SIZE - 1) / BUFFER_SIZE;
	        m_bufferNumber = -1;
	        m_bufferLength = 0;
	        m_positionInBuffer = 0;
		} catch (IOException e) { throw new RuntimeException(e); }
    }
    
    public void close() {
    	try { 
    		m_input.close();
		} catch (IOException e) { throw new RuntimeException(e); }
    }
    
    public long length() { return m_length; }

    public long position() {
    	return m_bufferNumber < 0 ? 0 : m_bufferNumber * BUFFER_SIZE + m_positionInBuffer;
    }
    
    public boolean end() { return 
    		(m_positionInBuffer == m_bufferLength && m_bufferNumber == m_buffersCount - 1) ||
    		(m_positionInBuffer == 0 && m_bufferNumber == m_buffersCount); }
    
    public void seek(long position)
    {
        if (position == m_length && m_length == m_buffersCount * BUFFER_SIZE)
        {
            m_positionInBuffer = 0;
            m_bufferLength = 0;
            m_bufferNumber = m_buffersCount;
        }
        else
        {
            long bufferNumber = position / BUFFER_SIZE;
            readBuffer(bufferNumber);
            m_positionInBuffer = (int)(position % BUFFER_SIZE);
            if (m_positionInBuffer > m_bufferLength) throw new RuntimeException("End of stream");
        }
    }

    private void readBuffer(long bufferNumber)
    {
        if (m_bufferNumber == bufferNumber) return;
        if (bufferNumber < 0 || bufferNumber >= m_buffersCount) throw new RuntimeException("End of stream");
        m_bufferNumber = bufferNumber;
        try {
			m_bufferLength = m_input.read(m_buffer, 0, m_buffer.length);
		} catch (IOException e) { throw new RuntimeException(e); }
        m_positionInBuffer = 0;
    }
    
    public int read(byte[] buffer, int offset, int count)
    {
        int cnt = count;
        while (count > 0)
        {
            if (m_positionInBuffer == m_bufferLength) readBuffer(m_bufferNumber + 1);
            int toRead = Math.min(m_bufferLength - m_positionInBuffer, count);
            System.arraycopy(m_buffer, m_positionInBuffer, buffer, offset, toRead);
            count -= toRead;
            m_positionInBuffer += toRead;
            offset += toRead;
        }
        return cnt;
    }

    public int readByte()
    {
        if (m_positionInBuffer == m_bufferLength) readBuffer(m_bufferNumber + 1);
        return m_buffer[m_positionInBuffer++] & 0xFF;
    }

    public int readVInt()
    {
        int b = readByte();
        int u = b & 127;
        int shift = 7;
        while (b > 127)
        {
            b = readByte();
            u += (b & 127) << shift;
            shift += 7;
        }
        return u;
    }

    public long readVLong()
    {
        long b = readByte();
        long u = b & 127;
        int shift = 7;
        while (b > 127)
        {
            b = readByte();
            u += (b & 127) << shift;
            shift += 7;
        }
        return u;
    }

    public int readInt()
    {
        int u =
            readByte() |
            readByte() << 8 |
            readByte() << 16 |
            readByte() << 24;
        return u;
    }

    public long readLong()
    {
        long u =
            (long)readByte() |
            (long)readByte() << 8 |
            (long)readByte() << 16 |
            (long)readByte() << 24 |
            (long)readByte() << 32 |
            (long)readByte() << 40 |
            (long)readByte() << 48 |
            (long)readByte() << 56;
        return u;
    }

    public void read(BSTR bstr) {
    	bstr.length = readVInt();
    	bstr.assertLength(bstr.length);
    	read(bstr.buffer, 0, bstr.length);
    }
    
    public String readString() {
    	int i = readVInt();
    	byte[] b = new byte[i];
    	read(b, 0, i);
    	BSTR bstr = new BSTR(b);
    	return bstr.toString();
    }

    public void readVString(BSTR bstr) {
    	if(bstr.length < 0) bstr.length = 0;
    	int pfx = readVInt();
    	int len = readVInt();
    	bstr.length = pfx + len;
    	bstr.assertLength(bstr.length);
    	read(bstr.buffer, pfx, len);
    	
    }
    
}


