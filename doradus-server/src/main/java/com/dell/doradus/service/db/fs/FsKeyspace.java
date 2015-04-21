package com.dell.doradus.service.db.fs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.dell.doradus.olap.io.BSTR;

public class FsKeyspace {
    private BSTR m_name;
    private HashMap<BSTR, FsStore> m_stores = new HashMap<>();
    
    public FsKeyspace(BSTR name) { m_name = name; }
    
    public BSTR getName() { return m_name; }
    
    public FsStore getStore(BSTR name) {
        return m_stores.get(name);
    }

    public FsStore getOrCreateStore(BSTR name) {
        FsStore store = m_stores.get(name);
        if(store == null) {
            store = new FsStore(name);
            m_stores.put(name, store);
        }
        return store;
    }
    
    public void deleteStore(BSTR name) {
        m_stores.remove(name);
    }
    
    public List<FsStore> getStores() {
        return new ArrayList<FsStore>(m_stores.values());
    }
}

