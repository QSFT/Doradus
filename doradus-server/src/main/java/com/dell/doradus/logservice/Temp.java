package com.dell.doradus.logservice;

import com.dell.doradus.olap.collections.MemoryStream;
import com.dell.doradus.olap.io.BSTR;
import com.dell.doradus.olap.io.Compressor;

public class Temp {
    private MemoryStream[] m_streams;
    private BSTR m_bstr;
    
    public Temp() {
        m_streams = new MemoryStream[4];
        m_streams[0] = new MemoryStream();
        m_streams[1] = new MemoryStream();
        m_streams[2] = new MemoryStream();
        m_streams[3] = new MemoryStream();
        m_bstr = new BSTR();
    }
    
    public BSTR getBSTR() {
        m_bstr.length = 0;
        return m_bstr;
    }
    
    public MemoryStream getStream(int index) {
        m_streams[index].clear();
        return m_streams[index];
    }
    
    public static void writeCompressed(MemoryStream stream, MemoryStream source) {
        if(source.length() < 128) {
            stream.writeByte((byte)0);
            stream.writeVInt(source.length());
            stream.write(source.getBuffer(), 0, source.length());
        } else {
            byte[] data = source.toArray();
            data = Compressor.compress(data);
            stream.writeByte((byte)1);
            stream.writeVInt(data.length);
            stream.write(data, 0, data.length);
        }
    }
 
    public static MemoryStream readCompressed(MemoryStream input) {
        byte type = (byte)input.readByte();
        int length = input.readVInt();
        byte[] data = new byte[length];
        input.read(data, 0, length);
        if(type == 1) {
            data = Compressor.uncompress(data);
        }
        else if(type != 0) throw new RuntimeException("Invalid type");
        return new MemoryStream(data);
    }
}
