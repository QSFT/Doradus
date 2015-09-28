package com.dell.doradus.service.db.fs;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import com.dell.doradus.olap.collections.MemoryStream;
import com.dell.doradus.olap.io.BSTR;
import com.dell.doradus.olap.io.Compressor;

public class FsTableWriter {
    private FsTableIndex m_index;
    private FileOutputStream m_stream;
    
    private BSTR m_row;
    private BSTR m_rangeStart;
    private BSTR m_column;
    private int m_rangeCount = 0;
    private MemoryStream m_rangeStream = new MemoryStream();
    
    public FsTableWriter(File file) {
        try {
            m_stream = new FileOutputStream(file);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        m_index = new FsTableIndex();
    }
    
    public FsTableIndex getIndex() {
        return m_index;
    }
    
    public void addRow(BSTR row, boolean isDeleted) {
        if(m_row != null && m_row.equals(row)) return;
        flushRange();
        m_row = row;
        m_index.addRow(row, isDeleted);
    }
    
    public void addColumn(BSTR columnName, int operation, byte[] value) {
        if(m_column != null && m_column.equals(columnName)) return;
        if(m_column == null) m_column = BSTR.EMPTY;
        m_rangeStream.writeVString(columnName, m_column);
        m_rangeStream.writeByte((byte)operation);
        if(operation == FsMutation.UPDATE_COLUMN) {
            m_rangeStream.writeVInt(value.length);
            m_rangeStream.write(value, 0, value.length);
        }
        m_column = columnName;
        if(m_rangeStart == null) m_rangeStart = columnName;
        m_rangeCount++;
        
        if(m_rangeCount % 1024 == 0) {
            flushRange();
        }
    }
    
    private void flushRange() {
        try {
            if(m_rangeCount == 0) return;
            byte[] data = m_rangeStream.toArray();
            data = Compressor.compress(data);
            m_index.getRow(m_row).addColumnRange(m_rangeStart, m_column, m_rangeCount, m_stream.getChannel().position(), data.length);
            m_stream.write(data, 0, data.length);
            m_rangeStart = null;
            m_rangeCount = 0;
            m_rangeStream.clear();
            m_column = null;
        }catch(IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    public void close() {
        flushRange();
        try {
            m_stream.getChannel().force(true);
            m_stream.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
       
    }
}
