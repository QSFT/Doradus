package com.dell.doradus.service.db.fs;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.dell.doradus.olap.collections.MemoryStream;
import com.dell.doradus.olap.io.BSTR;
import com.dell.doradus.olap.io.Compressor;

public class FsTable {
    private FsTableIndex m_index;
    private File m_tableFile;
    private FsDataStore m_dataStore;
    
    private List<FsColumn> m_lastRead;
    private long m_lastPosition = -1;
    
    public FsTable(File tableFile, FsDataStore m_dataStore) {
        m_tableFile = tableFile;
        File indexFile = new File(tableFile.getPath() + ".idx");
        m_index = FsTableIndex.read(indexFile);
    }
    
    
    public void getColumns(BSTR rowKey, FsReadColumns columns, Collection<BSTR> columnNames) {
        FsTableIndex.Row row = m_index.getRow(rowKey);
        if(row == null) return;
        ArrayList<BSTR> cols = new ArrayList<>(columnNames);
        Collections.sort(cols);
        for(FsTableIndex.ColumnRange range: row.getRanges()) {
            BSTR start = range.getStart();
            BSTR end = range.getEnd();
            end = new BSTR(end.toString() + "\0");
            int startPos = Collections.binarySearch(cols, start);
            if(startPos < 0) startPos = -startPos - 1;
            int endPos = Collections.binarySearch(cols, end);
            if(startPos < 0) startPos = -startPos - 1;
            if(startPos == endPos) continue; // if this range does not contain columnNames, we skip it

            List<FsColumn> list = read(range);
            for(FsColumn c: list) {
                if(columns.containsColumn(c)) continue;
                columns.addColumn(c);
            }
        }
    }
    
    public void getColumns(BSTR rowKey, FsReadColumns columns, FsColumn startColumn, FsColumn endColumn, int count) {
        FsTableIndex.Row row = m_index.getRow(rowKey);
        if(row == null) return;
        BSTR start = startColumn == null ? null : startColumn.getName();
        BSTR end = endColumn == null ? null : endColumn.getName();
        int columnsCount = 0;
        for(FsTableIndex.ColumnRange range: row.getRanges()) {
            if(end != null && end.compareTo(range.getStart()) <= 0) continue;
            if(start != null && start.compareTo(range.getEnd()) > 0) continue;
            List<FsColumn> list = read(range);
            for(FsColumn c: list) {
                if(start != null && start.compareTo(c.getName()) > 0) continue;
                if(end != null && end.compareTo(c.getName()) <= 0) break;
                if(columns.containsColumn(c)) continue;
                columns.addColumn(c);
                if(!c.isColumnDelete()) {
                    columnsCount++;
                    if(columnsCount >= count) break;
                }
            }
            if(columnsCount >= count) break;
        }
    }
    
    public void getRows(Set<FsRow> rows, BSTR continuationToken) {
        for(FsTableIndex.Row row: m_index.getRows()) {
            FsRow fsr = new FsRow(row.getKey(), m_dataStore);
            if(row.isDeleted()) fsr.deleteRow();
            if(rows.contains(fsr)) continue;
            if(continuationToken != null && continuationToken.compareTo(fsr.getName()) >= 0) continue;
            rows.add(fsr);
        }
    }
    
    
    public List<FsColumn> read(FsTableIndex.ColumnRange range) {
        if(m_lastPosition == range.getOffset()) return m_lastRead;
        
        try (RandomAccessFile raf = new RandomAccessFile(m_tableFile, "r")) {
            raf.seek(range.getOffset());
            byte[] data = new byte[range.getLength()];
            int len = raf.read(data);
            if(len != data.length) throw new RuntimeException("File read failed");
            data = Compressor.uncompress(data);
            List<FsColumn> columns = new ArrayList<>();
            MemoryStream stream = new MemoryStream(data);
            BSTR lastCol = new BSTR();
            for(int i = 0; i < range.getCount(); i++) {
                stream.readVString(lastCol);
                BSTR colName = new BSTR(lastCol);
                int operation = stream.readByte();
                byte[] value = FileUtils.EMPTY_BYTES;
                if(operation == FsMutation.UPDATE_COLUMN) {
                    value = new byte[stream.readVInt()];
                    stream.read(value, 0, value.length);
                }
                columns.add(new FsColumn(operation, colName, value));
            }
            if(!stream.end()) throw new RuntimeException("Invalid columns range");
            
            m_lastPosition = range.getOffset();
            m_lastRead = columns;
            
            return columns;
        }catch(IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    public void close() {
        
    }
}



