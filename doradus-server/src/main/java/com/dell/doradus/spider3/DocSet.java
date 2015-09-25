package com.dell.doradus.spider3;

import java.util.HashSet;
import java.util.Set;

import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.FieldDefinition;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.common.Utils;
import com.dell.doradus.service.db.DBService;
import com.dell.doradus.service.db.DColumn;
import com.dell.doradus.service.db.Tenant;

public class DocSet {
    private Set<String> m_ids = new HashSet<>();
    
    public DocSet() {}
    
    public Set<String> getIDs() { return m_ids; }
    public void addId(String id) {
        m_ids.add(id);
    }
    
    public boolean contains(String id) {
        return m_ids.contains(id);
    }
    
    public void clear() {
        m_ids.clear();
    }
    
    public void and(DocSet set) {
        Set<String> newids = new HashSet<String>();
        for(String s: m_ids) {
            if(set.contains(s)) newids.add(s);
        }
        m_ids = newids;
    }

    public void or(DocSet set) {
        m_ids.addAll(set.m_ids);
    }

    public void andNot(DocSet set) {
        m_ids.removeAll(set.m_ids);
    }
    
    public void fillAll(TableDefinition tableDef) {
        ApplicationDefinition appDef = tableDef.getAppDef();
        Tenant tenant = Spider3.instance().getTenant(tableDef.getAppDef());
        String store = appDef.getAppName();
        String table = tableDef.getTableName();
        String row = table + "/_id";
        for(DColumn column: DBService.instance().getAllColumns(tenant, store, row)) {
            m_ids.add(column.getName());
        }
    }
    
    public void fillLink(FieldDefinition linkDef, DocSet linkedSet) {
        TableDefinition tableDef = linkDef.getTableDef();
        ApplicationDefinition appDef = tableDef.getAppDef();
        Tenant tenant = Spider3.instance().getTenant(tableDef.getAppDef());
        String store = appDef.getAppName();
        String table = tableDef.getTableName();
        String row = table + "/" + linkDef.getName();
        for(DColumn column: DBService.instance().getAllColumns(tenant, store, row)) {
            String[] nv = Spider3.split(column.getName());
            if(!linkedSet.contains(nv[1])) continue;
            m_ids.add(nv[0]);
        }
    }
    
    public void fillField(FieldDefinition fieldDef, String pattern) {
        TableDefinition tableDef = fieldDef.getTableDef();
        ApplicationDefinition appDef = tableDef.getAppDef();
        Tenant tenant = Spider3.instance().getTenant(tableDef.getAppDef());
        String store = appDef.getAppName();
        String table = tableDef.getTableName();
        String row = table + "/" + fieldDef.getName();
        for(DColumn column: DBService.instance().getAllColumns(tenant, store, row)) {
            if(fieldDef.isCollection()) {
                String[] nv = Spider3.split(column.getName());
                if(!Utils.matchesPattern(nv[1], pattern)) continue;
                m_ids.add(nv[0]);
            } else {
                if(!Utils.matchesPattern(column.getValue(), pattern)) continue;
                m_ids.add(column.getName());
            }
        }
    }

    
}
