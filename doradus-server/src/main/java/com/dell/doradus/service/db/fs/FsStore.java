package com.dell.doradus.service.db.fs;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dell.doradus.olap.collections.MemoryStream;
import com.dell.doradus.olap.io.BSTR;
import com.dell.doradus.search.util.HeapList;
import com.dell.doradus.service.db.DColumn;
import com.dell.doradus.utilities.Timer;

public class FsStore {
    public static int LOGFILE_THRESHOLD = 25 * 1024 * 1024; // 25Mb
    private Object m_sync = new Object();
    private String m_name;
    private File m_root;
    private File m_logFile;
    private FsDataStore m_dataStore;
    private FsMemTable m_memTable;
    private List<FsTable> m_tables = new ArrayList<>();
    private Logger m_log = LoggerFactory.getLogger(getClass());

    public FsStore(File tenant, String storeName) {
        try {
            Timer t = new Timer();
            m_name = storeName;
            m_root = new File(tenant, storeName);
            if(!m_root.exists()) m_root.mkdir();
            m_logFile = new File(m_root, "Log");
            m_dataStore = new FsDataStore(m_root);
            m_memTable = new FsMemTable(storeName, m_dataStore);
            for(File f: m_root.listFiles()) {
                if(f.getName().startsWith("table") && !f.getName().endsWith(".idx")) {
                    m_tables.add(new FsTable(f, m_dataStore));
                    m_log.info("Store {} opened table {}", m_name, f.getName());
                }
            }
            replayLog();
            m_log.info("Store {} initialized in {}", m_name, t);
        }catch(IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    public String getStoreName() { return m_name; }
    
    public void addMutations(Map<String, List<DColumn>> columnUpdates, Map<String, List<String>> columnDeletes, List<String> rowDeletes) {
        try {
            if(columnUpdates == null) columnUpdates = new HashMap<>();
            if(columnDeletes == null) columnDeletes = new HashMap<>();
            if(rowDeletes == null) rowDeletes = new ArrayList<>();
            List<FsMutation> mutations = new ArrayList<>();
            for(String row: columnUpdates.keySet()) {
                for(DColumn column: columnUpdates.get(row)) {
                    String columnName = column.getName();
                    byte[] columnValue = column.getRawValue();
                    if(columnValue.length > 1024) {
                        synchronized(m_sync) {
                            m_dataStore.save(row, columnName, columnValue);
                        }
                        FsMutation mutation = new FsMutation(FsMutation.UPDATE_LARGE_COLUMN, row, columnName, FileUtils.EMPTY_BYTES);
                        mutations.add(mutation);
                    } else {
                        FsMutation mutation = new FsMutation(FsMutation.UPDATE_COLUMN, row, columnName, columnValue);
                        mutations.add(mutation);
                    }
                }
            }
            for(String row: columnDeletes.keySet()) {
                for(String columnName: columnDeletes.get(row)) {
                    FsMutation mutation = new FsMutation(FsMutation.DELETE_COLUMN, row, columnName, FileUtils.EMPTY_BYTES);
                    mutations.add(mutation);
                }
            }
            for(String row: rowDeletes) {
                FsMutation mutation = new FsMutation(FsMutation.DELETE_ROW, row, "", FileUtils.EMPTY_BYTES);
                mutations.add(mutation);
            }
            
            MemoryStream ms = new MemoryStream();
            for(FsMutation mutation: mutations) {
                mutation.write(ms);
            }
            
            synchronized (m_sync) {
                FileUtils.append(m_logFile, ms.toArray());
                for(FsMutation mutation: mutations) {
                    m_memTable.applyMutation(mutation);
                }
                if(m_logFile.length() > LOGFILE_THRESHOLD) {
                    flushTable();
                }
            }
        
        }catch(IOException e) {
            throw new RuntimeException(e);
        }
        
    }
    
    public void replayLog() throws IOException {
        Timer t = new Timer();
        if(!m_logFile.exists()) return;
        byte[] data = FileUtils.read(m_logFile);
        MemoryStream ms = new MemoryStream(data);
        FsMutation mutation = new FsMutation();
        while(!ms.end()) {
            mutation.read(ms);
            m_memTable.applyMutation(mutation);
        }
        m_log.info("Store {} replayed log in {}", m_name, t);
    }

    public void flushTable() throws IOException {
        Timer t = new Timer();
        File tableFile = new File(m_root, "table-" + m_tables.size());
        File indexFile = new File(tableFile.getPath() + ".idx");
        FsTableWriter writer = new FsTableWriter(tableFile);
        m_memTable.write(writer);
        writer.close();
        FsTableIndex index = writer.getIndex();
        index.write(indexFile);
        m_tables.add(new FsTable(tableFile, m_dataStore));
        m_logFile.delete();
        m_memTable = new FsMemTable(m_name, m_dataStore);
        m_log.info("Store {} flushed table in {}", m_name, t);
    }

    public List<String> getRows(String continuationToken, int count) {
        Timer t = new Timer();
        Set<FsRow> rows = new HashSet<>();
        BSTR cont = continuationToken == null ? null : new BSTR(continuationToken);
        synchronized (m_sync) {
            m_memTable.getRows(rows, cont);
            for(FsTable table: m_tables) {
                table.getRows(rows, cont);
            }
            List<String> list = new ArrayList<>();
            List<FsRow> allRows = new ArrayList<>(rows);
            Collections.sort(allRows);
            for(FsRow row: allRows) {
                if(row.isDeleted() && row.getColumnsCount() == 0) continue;
                list.add(row.getName().toString());
                if(list.size() >= count) break;
            }
            m_log.debug("Store {} get rows in {}", m_name, t);
            return list;
        }
    }

    public List<DColumn> getColumns(String row, String startColumn, String endColumn, int count) {
        Timer t = new Timer();
        FsReadColumns readColumns = getColumnsSlice(row, startColumn, endColumn, count);
        List<FsColumn> allList = new ArrayList<>(readColumns.getColumns());
        List<DColumn> list = getColumnsList(row, allList, count);
        m_log.debug("Store {} get columns slice for {} in {}", new Object[] {m_name, row, t});
        return list;
    }

    public List<DColumn> getColumns(String row, Collection<String> columnNames) {
        Timer t = new Timer();
        FsReadColumns readColumns = new FsReadColumns();
        Set<BSTR> colNames = new HashSet<BSTR>();
        for(String columnName: columnNames) colNames.add(new BSTR(columnName));
        BSTR rowKey = new BSTR(row);
        synchronized (m_sync) {
            m_memTable.getColumns(rowKey, readColumns, colNames);
            for(FsTable table: m_tables) {
                table.getColumns(rowKey, readColumns, colNames);
            }
            
            List<FsColumn> allList = new ArrayList<>(readColumns.getColumns());
            Collections.sort(allList);
            List<DColumn> list = new ArrayList<>();
            for(FsColumn c: allList) {
                if(c.isColumnDelete()) continue;
                String columnName = c.getName().toString();
                byte[] value = c.getValue();
                if(c.isExternalValue()) value = m_dataStore.load(row, columnName);
                list.add(new DColumn(columnName, value));
            }
            m_log.debug("Store {} get columns for {} in {}", new Object[] {m_name, row, t});
            return list;
        }
    }

    private List<DColumn> getColumnsList(String row, List<FsColumn> allList, int count) {
        HeapList<FsColumn> heap = new HeapList<>(count);
        for(FsColumn c: allList) {
            if(c.isColumnDelete()) continue;
            heap.Add(c);
        }
        allList = heap.values();
        List<DColumn> list = new ArrayList<>();
        for(FsColumn c: allList) {
            if(c.isColumnDelete()) continue;
            String columnName = c.getName().toString();
            byte[] value = c.getValue();
            if(c.isExternalValue()) {
                synchronized (m_sync) {
                    value = m_dataStore.load(row, columnName);
                }
            }
            list.add(new DColumn(columnName, value));
            if(list.size() >= count) break;
        }
        return list;
    }
    
    private FsReadColumns getColumnsSlice(String row, String startColumn, String endColumn, int count) {
        FsReadColumns readColumns = new FsReadColumns();
        FsColumn start = startColumn == null ? null : new FsColumn(-1, new BSTR(startColumn), FileUtils.EMPTY_BYTES);
        FsColumn end = endColumn == null ? null : new FsColumn(-1, new BSTR(endColumn), FileUtils.EMPTY_BYTES);
        BSTR rowKey = new BSTR(row);
        synchronized (m_sync) {
            m_memTable.getColumns(rowKey, readColumns, start, end, count);
            for(FsTable table: m_tables) {
                table.getColumns(rowKey, readColumns, start, end, count);
            }
        }
        return readColumns;
    }
    
    public void close() {
        for(FsTable table: m_tables) {
            table.close();
        }
    }
}
