package com.dell.doradus.service.db;

import java.util.List;

public class ColumnSequence implements Sequence<DColumn> {
    private DRow m_row;
    private String m_startColumn;
    private String m_endColumn;
    private int m_chunkSize;
    private List<DColumn> m_currentList;
    private int m_pointer;

    public ColumnSequence(DRow row, String startColumn, String endColumn, int chunkSize) {
        m_row = row;
        m_startColumn = startColumn;
        m_endColumn = endColumn;
        m_chunkSize = chunkSize;
    }
    
    @Override public DColumn next() {
        if(m_currentList == null) {
            m_currentList = DBService.instance().getColumns(m_row.getNamespace(), m_row.getStoreName(), m_row.getRowKey(),
                    m_startColumn, m_endColumn, m_chunkSize);
        }
        if(m_currentList.size() == 0) {
            return null;
        }
        if(m_pointer == m_currentList.size()) {
            String newStartKey = m_currentList.get(m_pointer - 1).getName() + '\0';
            m_currentList = DBService.instance().getColumns(m_row.getNamespace(), m_row.getStoreName(), m_row.getRowKey(),
                    newStartKey, m_endColumn, m_chunkSize);
            m_pointer = 0;
        }
        if(m_currentList.size() == 0) {
            return null;
        }
        return m_currentList.get(m_pointer++);
    }
}
