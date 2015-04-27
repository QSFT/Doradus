package com.dell.doradus.spider2;

import com.dell.doradus.common.UNode;
import com.dell.doradus.common.Utils;
import com.dell.doradus.core.IDGenerator;

public class S2Object implements Comparable<S2Object> {
    private String m_id;
    private UNode m_data;
    
    public S2Object(String id, UNode data) {
        m_id = id;
        m_data = data;
    }
    
    public S2Object(UNode data) {
        m_data = data;
        m_id = data.getMemberValue("_id");
        if(m_id == null) {
            m_id = Utils.base64FromBinary(IDGenerator.nextID());
            data.addValueNode("_id", m_id);
        }
    }
    
    public String getId() { return m_id; }
    public UNode getData() { return m_data; }
    
    
    @Override public int hashCode() { return m_id.hashCode(); }
    @Override public boolean equals(Object obj) { return m_id.equals(((S2Object)obj).m_id); }
    @Override public String toString() { return m_data.toJSON(true); }
    @Override public int compareTo(S2Object o) { return m_id.compareTo(o.m_id); }
}
