package com.dell.doradus.service.db.fs;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

import com.dell.doradus.olap.collections.MemoryStream;
import com.dell.doradus.olap.io.BSTR;

public class FsTableIndex {
    private HashMap<BSTR, Row> m_rows = new HashMap<>(); 

    public FsTableIndex() {
    }
    
    public Row getRow(BSTR rowKey) { return m_rows.get(rowKey); }
    public Collection<Row> getRows() { return m_rows.values(); }

    public Row addRow(BSTR rowKey, boolean isDeleted) {
        Row row = new Row(rowKey, isDeleted);
        m_rows.put(rowKey, row);
        return row;
    }
    
    public byte[] write() {
        MemoryStream stream = new MemoryStream();
        stream.writeVInt(m_rows.size());
        for(Row row: getRows()) {
            stream.writeString(row.getKey());
            stream.writeBoolean(row.isDeleted());
            Collection<ColumnRange> ranges = row.getRanges();
            stream.writeVInt(ranges.size());
            for(ColumnRange range: ranges) {
                stream.writeString(range.getStart());
                stream.writeString(range.getEnd());
                stream.writeVInt(range.getCount());
                stream.writeLong(range.getOffset());
                stream.writeVInt(range.getLength());
            }
        }
        return stream.toArray();
    }
    
    public void write(File file) {
        FileUtils.write(file, write());
    }
    
    public static FsTableIndex read(File file) {
        FsTableIndex index = new FsTableIndex();
        byte[] data = FileUtils.read(file);
        if(data == null) throw new RuntimeException("Failed to read file " + file);
        index.read(data);
        return index;
    }
    
    public void read(byte[] data) {
        MemoryStream stream = new MemoryStream(data);
        m_rows.clear();
        int rowsCount = stream.readVInt();
        for(int i = 0; i < rowsCount; i++) {
            BSTR rowKey = stream.readString();
            boolean isDeleted = stream.readBoolean();
            Row row = addRow(rowKey, isDeleted);
            int rangesCount = stream.readVInt();
            for(int j = 0; j < rangesCount; j++) {
                BSTR start = stream.readString();
                BSTR end = stream.readString();
                int count = stream.readVInt();
                long offset = stream.readLong();
                int length = stream.readVInt();
                row.addColumnRange(start, end, count, offset, length);
            }
        }
    }
    
    static class ColumnRange {
        private BSTR m_start;
        private BSTR m_end;
        private int m_count;
        private long m_offset;
        private int m_length;
        
        public ColumnRange(BSTR start, BSTR end, int count, long offset, int length) {
            m_start = start;
            m_end = end;
            m_count = count;
            m_offset = offset;
            m_length = length;
        }
        
        public BSTR getStart() { return m_start; }
        public BSTR getEnd() { return m_end; }
        public int getCount() { return m_count; }
        public long getOffset() { return m_offset; }
        public int getLength() { return m_length; }
        
    }
    
    static class Row {
        private BSTR m_rowKey;
        private boolean m_bDeleted;
        private ArrayList<ColumnRange> m_ranges = new ArrayList<>();
        
        public Row(BSTR rowKey, boolean isDeleted) {
            m_rowKey = rowKey;
            m_bDeleted = isDeleted;
        }
        
        public void addColumnRange(BSTR start, BSTR end, int count, long offset, int length) {
            m_ranges.add(new ColumnRange(start, end, count, offset, length));
        }
        
        public BSTR getKey() { return m_rowKey; }
        public ArrayList<ColumnRange> getRanges() { return m_ranges; }
        public boolean isDeleted() { return m_bDeleted; }
    }
    
}
