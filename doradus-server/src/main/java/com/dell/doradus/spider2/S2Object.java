package com.dell.doradus.spider2;

import com.dell.doradus.common.Utils;
import com.dell.doradus.core.IDGenerator;
import com.dell.doradus.spider2.jsonbuild.JMapNode;

public class S2Object implements Comparable<S2Object> {
    private String m_id;
    private JMapNode m_data;
    
    public S2Object(String id, JMapNode data) {
        m_id = id;
        m_data = data;
    }
    
    public S2Object(JMapNode data) {
        m_data = data;
        m_id = data.getString("_id");
        if(m_id == null) {
            m_id = Utils.base64FromBinary(IDGenerator.nextID());
            data.addString("_id", m_id);
        }
    }
    
    public String getId() { return m_id; }
    public JMapNode getData() { return m_data; }

    @Override public int hashCode() { return m_id.hashCode(); }
    @Override public boolean equals(Object obj) { return m_id.equals(((S2Object)obj).m_id); }
    @Override public String toString() { return m_data.getString(true); }
    @Override public int compareTo(S2Object o) { return m_id.compareTo(o.m_id); }
}
