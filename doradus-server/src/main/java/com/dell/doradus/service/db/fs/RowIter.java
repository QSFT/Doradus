package com.dell.doradus.service.db.fs;

import java.util.Iterator;
import java.util.List;

import com.dell.doradus.service.db.DColumn;
import com.dell.doradus.service.db.DRow;

public class RowIter implements DRow {
    private String m_row;
    private List<DColumn> m_columns;
    
    public RowIter(String row, List<DColumn> columns) {
        m_row = row;
        m_columns = columns;
    }

    @Override public String getKey() { return m_row; }

    @Override public Iterator<DColumn> getColumns() {
        return m_columns.iterator();
    }
}
