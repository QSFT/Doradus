package com.dell.doradus.service.db.fs;

import java.io.File;

public class FsDataStore {
    private File m_dataDir;
    
    public FsDataStore(File storeDir) {
        m_dataDir = new File(storeDir, "data");
        if(!m_dataDir.exists()) m_dataDir.mkdir();
    }
    
    public void save(String row, String column, byte[] data) {
        File rowDir = new File(m_dataDir, FileUtils.encode(row));
        if(!rowDir.exists()) rowDir.mkdir();
        File file = new File(rowDir, FileUtils.encode(column));
        FileUtils.write(file, data);
    }
    
    public byte[] load(String row, String column) {
        File rowDir = new File(m_dataDir, FileUtils.encode(row));
        if(!rowDir.exists()) return null;
        File file = new File(rowDir, FileUtils.encode(column));
        if(!file.exists()) return null;
        return FileUtils.read(file);
    }
    
    public void delete(String  row, String column) {
        File rowDir = new File(m_dataDir, FileUtils.encode(row));
        if(!rowDir.exists()) return;
        File file = new File(rowDir, FileUtils.encode(column));
        if(file.exists()) file.delete();
    }
    
    public void deleteRow(String row) {
        File rowDir = new File(m_dataDir, FileUtils.encode(row));
        if(!rowDir.exists()) return;
        FileUtils.deleteDirectory(rowDir);
    }
}
