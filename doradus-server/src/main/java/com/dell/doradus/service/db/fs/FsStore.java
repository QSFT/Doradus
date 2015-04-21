package com.dell.doradus.service.db.fs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.dell.doradus.olap.io.BSTR;

public class FsStore {
    private BSTR m_name;
    private HashMap<BSTR, FsRow> m_rows = new HashMap<>();
    
    public FsStore(BSTR name) { m_name = name; }
    
    public BSTR getName() { return m_name; }

    public FsRow getRow(BSTR name) {
        return m_rows.get(name);
    }
    
    
    public FsRow getOrCreateRow(BSTR name) {
        FsRow row = m_rows.get(name);
        if(row == null) {
            row = new FsRow(name);
            m_rows.put(name, row);
        }
        return row;
    }
    
    public void deleteRow(BSTR name) {
        m_rows.remove(name);
    }
    
    public List<FsRow> getRows() {
        return new ArrayList<FsRow>(m_rows.values());
    }
}
