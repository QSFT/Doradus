package com.dell.doradus.service.db.fs;

import com.dell.doradus.olap.collections.MemoryStream;
import com.dell.doradus.olap.io.BSTR;

public class FsMutation {
    public static final int DELETE_ROW = 1;
    public static final int DELETE_COLUMN = 2;
    public static final int UPDATE_COLUMN = 3;
    public static final int UPDATE_LARGE_COLUMN = 4;
    
    // 1: delete row; 2: delete column; 3: update column with small value; 4: update column with large value
    private int m_operation;
    private BSTR m_row;
    private BSTR m_column;
    private byte[] m_value;
    
    public FsMutation() {
    }

    public FsMutation(int operation, String row, String column, byte[] value) {
        m_operation = operation;
        m_row = new BSTR(row);
        m_column = new BSTR(column);
        m_value = value;
    }
    
    public FsMutation(FsMutation x) {
        m_operation = x.m_operation;
        m_row = new BSTR(x.m_row);
        m_column = new BSTR(x.m_column);
        m_value = x.m_value;
    }
    
    
    public int getOperation() { return m_operation; }
    public BSTR getRow() { return m_row; }
    public BSTR getColumn() { return m_column; }
    public byte[] getValue() { return m_value; }
    
    public void read(MemoryStream stream) {
        m_operation = stream.readByte();
        m_row = stream.readString();
        m_column = stream.readString();
        m_value = new byte[stream.readVInt()];
        stream.read(m_value, 0, m_value.length);
    }

    public void write(MemoryStream stream) {
        stream.writeByte((byte)m_operation);
        stream.writeString(m_row);
        stream.writeString(m_column);
        stream.writeVInt(m_value.length);
        stream.write(m_value, 0, m_value.length);
    }

}
