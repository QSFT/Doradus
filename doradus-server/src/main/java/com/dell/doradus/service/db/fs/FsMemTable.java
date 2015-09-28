package com.dell.doradus.service.db.fs;

import java.util.Collection;
import java.util.Set;

import com.dell.doradus.olap.collections.MemoryStream;
import com.dell.doradus.olap.io.BSTR;
import com.dell.doradus.olap.io.Compressor;

public class FsMemTable {
    private FsMemStore m_memStore;
    
    public FsMemTable(String store, FsDataStore dataStore) {
        m_memStore = new FsMemStore(store, dataStore);
    }
    
    public void deleteRow(BSTR row) {
        m_memStore.deleteRow(row);
    }
    
    public void deleteColumn(BSTR row, BSTR column) {
        m_memStore.deleteColumn(row, column);
    }
    
    public void updateColumn(BSTR row, BSTR column, byte[] value) {
        m_memStore.createColumn(row, FsMutation.UPDATE_COLUMN, column, value);
    }

    public void updateExternalColumn(BSTR row, BSTR column) {
        m_memStore.createColumn(row, FsMutation.UPDATE_LARGE_COLUMN, column, FileUtils.EMPTY_BYTES);
    }

    public void applyMutation(FsMutation mutation) {
        int operation = mutation.getOperation();
        if(operation == FsMutation.DELETE_ROW) {
            deleteRow(mutation.getRow());
        } else if(operation == FsMutation.DELETE_COLUMN) {
            deleteColumn(mutation.getRow(), mutation.getColumn());
        } else if(operation == FsMutation.UPDATE_COLUMN) {
            updateColumn(mutation.getRow(), mutation.getColumn(), mutation.getValue());
        } else if(operation == FsMutation.UPDATE_LARGE_COLUMN) {
            updateExternalColumn(mutation.getRow(), mutation.getColumn());
        }
    }
 
    public void write(FsTableWriter writer) {
        for(FsRow row: m_memStore.getRows()) {
            writer.addRow(row.getName(), row.isDeleted());
            for(FsColumn col: row.getColumns()) {
                writer.addColumn(col.getName(), col.getOperation(), col.getValue());
            }
        }
        
    }
    
    public byte[] write() {
        MemoryStream stream = new MemoryStream();
        BSTR lastRow = new BSTR("");
        for(FsRow row: m_memStore.getRows()) {
            BSTR lastColumn = new BSTR("");
            stream.writeVString(row.getName(), lastRow);
            lastRow = row.getName();
            stream.writeBoolean(row.isDeleted());
            stream.writeVInt(row.getColumnsCount());
            for(FsColumn col: row.getColumns()) {
                stream.writeVString(col.getName(), lastColumn);
                lastColumn = col.getName();
                stream.writeByte((byte)col.getOperation());
                if(col.getOperation() == FsMutation.UPDATE_COLUMN) {
                    byte[] value = col.getValue();
                    stream.writeVInt(value.length);
                    stream.write(value, 0, value.length);
                }
            }
        }
        byte[] data = stream.toArray();
        data = Compressor.compress(data);
        return data;
    }

    public void read(byte[] data) {
        data = Compressor.uncompress(data);
        MemoryStream stream = new MemoryStream(data);
        BSTR lastRow = new BSTR();
        while(!stream.end()) {
            stream.readVString(lastRow);
            BSTR rowKey = new BSTR(lastRow);
            boolean isDeleted = stream.readBoolean();
            if(isDeleted) {
                m_memStore.deleteRow(rowKey);
            }
            int columnsCount = stream.readVInt();
            BSTR lastColumn = new BSTR();
            for(int i = 0; i < columnsCount; i++) {
                stream.readVString(lastColumn);
                BSTR columnName = new BSTR(lastColumn);
                int operation = stream.readByte();
                byte[] value = FileUtils.EMPTY_BYTES;
                if(operation == FsMutation.UPDATE_COLUMN) {
                    value = new byte[stream.readVInt()];
                    stream.read(value, 0, value.length);
                }
                m_memStore.createColumn(rowKey, operation, columnName, value);
            }
        }
    }
    
    public void getColumns(BSTR rowKey, FsReadColumns columns, Collection<BSTR> columnNames) {
        m_memStore.getColumns(rowKey, columns, columnNames);
    }
    
    public void getColumns(BSTR rowKey, FsReadColumns columns, FsColumn startColumn, FsColumn endColumn, int count) {
        m_memStore.getColumns(rowKey, columns, startColumn, endColumn, count);
    }
    
    public void getRows(Set<FsRow> rows, BSTR continuationToken) {
        m_memStore.getRows(rows, continuationToken);
    }
    
}



