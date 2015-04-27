package com.dell.doradus.spider2;

import com.dell.doradus.olap.io.BSTR;

public class MemoryStream {
    private byte[] m_buffer;
    private int m_length;
    private int m_position;
    
    public MemoryStream() { m_buffer = new byte[1024]; }

    public MemoryStream(int capacity) { m_buffer = new byte[capacity]; }
    
    public MemoryStream(byte[] buffer) {
        m_buffer = buffer;
        m_length = buffer.length;
    }
    
    
    public int length() { return m_length; }
    public int position() { return m_position; }
    public boolean end() { return m_position == m_length; }
    public void clear() { m_length = m_position = 0; }
    
    public void seek(int position) {
        if(position < 0) throw new RuntimeException("Cannot seek before the start");
        if(position > m_length) throw new RuntimeException("Cannot seek past the end");
        m_position = position;
    }

    public void skip(int bytes) { seek(m_position + bytes); }
    
    
    public byte[] getBuffer() { return m_buffer; }
    public byte[] toArray() {
        byte[] buf = new byte[m_length];
        System.arraycopy(m_buffer, 0, buf, 0, m_length);
        return buf;
    }
    
    private void assertLength(int newLength) {
        int length = m_buffer.length;
        if(length >= newLength) return;
        while(length < newLength) length *= 2;
        byte[] buf = new byte[length];
        System.arraycopy(m_buffer, 0, buf, 0, m_length);
        m_buffer = buf;
    }
    

    public void read(byte[] buffer, int offset, int count)
    {
        if(count > m_length - m_position) throw new RuntimeException("End of buffer");
        System.arraycopy(m_buffer, m_position, buffer, offset, count);
        m_position += count;
    }

    public int readByte()
    {
        if(m_position >= m_length) throw new RuntimeException("End of buffer");
        return m_buffer[m_position++] & 0xFF;
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

    
    
    public void writeByte(byte value)
    {
        assertLength(m_position + 1);
        m_buffer[m_position++] = value;
        if(m_position > m_length) m_length = m_position;
    }

    public void write(byte[] buffer, int offset, int count)
    {
        assertLength(m_position + count);
        System.arraycopy(buffer, offset, m_buffer, m_position, count);
        m_position += count;
        if(m_position > m_length) m_length = m_position;
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

    public void writeString(String str) {
        BSTR bstr = new BSTR(str);
        write(bstr);
    }
    
    
}
