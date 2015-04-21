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

package com.dell.doradus.service.db.fs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;

import com.dell.doradus.olap.io.BSTR;

public class FOutputStream {
    static final int CHUNK_SIZE = 512;
    private SeekableByteChannel m_channel;
    
    private byte[] m_buffer;
    private int m_positionInBuffer;
    private long m_length;

    public FOutputStream(SeekableByteChannel channel)
    {
        m_channel = channel;
        m_buffer = new byte[CHUNK_SIZE];
        try {
            m_length = m_channel.size();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void writeBuffer()
    {
        try {
            m_length = m_channel.size();
            m_channel.position(m_length);
            ByteBuffer bb = ByteBuffer.wrap(m_buffer, 0, m_positionInBuffer);
            m_channel.write(bb);
            m_length += m_positionInBuffer;
            m_positionInBuffer = 0;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    public void flush() {
        writeBuffer();
    }
    
    public long length() {
        return m_length + m_positionInBuffer;
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

}
