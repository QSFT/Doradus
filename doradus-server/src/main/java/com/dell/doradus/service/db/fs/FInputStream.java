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

public class FInputStream {
    static final int CHUNK_SIZE = 512;
    private SeekableByteChannel m_channel;
    
    private byte[] m_buffer;
    private long m_length;
    private long m_bufferOffset;
    private int m_bufferLength;
    private int m_positionInBuffer;
    private ByteBuffer m_temp;

    public FInputStream(SeekableByteChannel channel)
    {
        m_channel = channel;
        m_bufferOffset = 0;
        m_buffer = new byte[CHUNK_SIZE];
        m_temp = ByteBuffer.wrap(m_buffer);
        refresh();
    }
    
    public void refresh() {
        try {
            m_length = m_channel.size();
        }catch(IOException e) {
            throw new RuntimeException(e);
        }
    }

    public long length() { return m_length; }
    public long position() { return m_bufferOffset + m_positionInBuffer; }
    public boolean isEnd() { return length() == position(); }

    public void seek(long position)
    {
        if(position >= m_bufferOffset && position <= m_bufferOffset + m_bufferLength) {
            m_positionInBuffer = (int)(position - m_bufferOffset);
        } else {
            readBuffer(position);
        }
    }

    private void readBuffer(long position)
    {
        try {
            m_channel.position(position);
            m_positionInBuffer = 0;
            m_temp.clear();
            m_bufferLength = m_channel.read(m_temp);
            m_bufferOffset = position;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    public int read(byte[] buffer, int offset, int count)
    {
        int cnt = count;
        while (count > 0)
        {
            if (m_positionInBuffer == m_bufferLength) readBuffer(m_bufferOffset + m_bufferLength);
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
        if (m_positionInBuffer == m_bufferLength) readBuffer(m_bufferOffset + m_bufferLength);
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

    public short readShort()
    {
        int u =
            readByte() |
            readByte() << 8;
        return (short)u;
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

    
    public BSTR readBSTR() {
        int length = readVInt();
        byte[] buffer = new byte[length];
        read(buffer, 0, length);
        return new BSTR(buffer);
    }
}


