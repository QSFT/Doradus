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

public class VOutputStream {
	private IBufferWriter m_bufferWriter;
    private byte[] m_buffer = new byte[VDirectory.CHUNK_SIZE];
    private int m_buffersCount;
    private int m_positionInBuffer;
    private BSTR m_cur = new BSTR();

    public VOutputStream(IBufferWriter bufferWriter)
    {
    	m_bufferWriter = bufferWriter;
        m_buffersCount = 0;
        m_positionInBuffer = 0;
    }

    public long position() { return m_buffersCount * m_buffer.length + m_positionInBuffer; }
    
    public void close()
    {
    	long length = position();
    	if(m_positionInBuffer != 0) writeBuffer();
    	m_bufferWriter.close(length);
    }

    private void writeBuffer()
    {
    	m_bufferWriter.writeBuffer(m_buffersCount, m_buffer, m_positionInBuffer);
    	m_buffersCount++;
    	m_positionInBuffer = 0;
    }
    
    public void writeByte(byte value)
    {
        m_buffer[m_positionInBuffer++] = value;
        if (m_positionInBuffer == m_buffer.length) writeBuffer();
    }

    public void write(byte[] buffer, int offset, int count)
    {
        while (count > 0)
        {
            int toCopy = Math.min(m_buffer.length - m_positionInBuffer, count);
            System.arraycopy(buffer, offset, m_buffer, m_positionInBuffer, toCopy);
            m_positionInBuffer += toCopy;
            count -= toCopy;
            offset += toCopy;
            if (m_positionInBuffer == m_buffer.length) writeBuffer();
        }
    }

    public void writeVInt(int value)
    {

        int u = value >>> 7;
        while (u != 0)
        {
            writeByte((byte)(value & 127 | 128));
            value = u;
            u >>>= 7;
        }
        writeByte((byte)value);
    }

    public void writeVLong(long value)
    {
        long u = ((long)value) >>> 7;
        while (u != 0)
        {
            writeByte((byte)(value & 127 | 128));
            value = u;
            u >>>= 7;
        }
        writeByte((byte)value);
    }

    public void writeShort(short value)
    {
        short u = value;
        writeByte((byte)u);
        u >>>= 8;
        writeByte((byte)u);
    }
    
    public void writeInt(int value)
    {
        int u = value;
        writeByte((byte)u);
        u >>>= 8;
        writeByte((byte)u);
        u >>>= 8;
        writeByte((byte)u);
        u >>>= 8;
        writeByte((byte)u);
    }

    public void writeLong(long value)
    {
        long u = (long)value;
        writeByte((byte)u);
        u >>>= 8;
        writeByte((byte)u);
        u >>>= 8;
        writeByte((byte)u);
        u >>>= 8;
        writeByte((byte)u);
        u >>>= 8;
        writeByte((byte)u);
        u >>>= 8;
        writeByte((byte)u);
        u >>>= 8;
        writeByte((byte)u);
        u >>>= 8;
        writeByte((byte)u);
        u >>>= 8;
    }

    public void write(BSTR bstr) {
    	writeVInt(bstr.length);
    	write(bstr.buffer, 0, bstr.length);
    }

    public void writeVString(BSTR bstr) {
    	int pfx = 0;
    	int len = Math.min(m_cur.length, bstr.length);
    	while(pfx < len && m_cur.buffer[pfx] == bstr.buffer[pfx]) pfx++;
    	writeVInt(pfx);
    	writeVInt(bstr.length - pfx);
    	write(bstr.buffer, pfx, bstr.length - pfx);
    	m_cur.assertLength(bstr.length);
    	System.arraycopy(bstr.buffer, pfx, m_cur.buffer, pfx, bstr.length - pfx);
    	m_cur.length = bstr.length;
    }
    
    public void writeString(String str) {
    	BSTR bstr = new BSTR(str);
    	write(bstr);
    }
    
}
