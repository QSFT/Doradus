package com.dell.doradus.service.db;

import java.util.List;

public class RowSequence implements Sequence<DRow> {
    private String m_namespace;
    private String m_storeName;
    private int m_chunkSize;
    private List<String> m_currentList;
    private int m_pointer;

    public RowSequence(String namespace, String storeName, int chunkSize) {
        m_namespace = namespace;
        m_storeName = storeName;
        m_chunkSize = chunkSize;
    }
    
    @Override public DRow next() {
        if(m_currentList == null) {
            m_currentList = DBService.instance().getRows(m_namespace, m_storeName, null, m_chunkSize);
        }
        if(m_currentList.size() == 0) {
            return null;
        }
        if(m_pointer == m_currentList.size()) {
            String continuationToken = m_currentList.get(m_pointer - 1);
            m_currentList = DBService.instance().getRows(m_namespace, m_storeName, continuationToken, m_chunkSize);
            m_pointer = 0;
        }
        return new DRow(m_namespace, m_storeName, m_currentList.get(m_pointer++));
    }
    
}
