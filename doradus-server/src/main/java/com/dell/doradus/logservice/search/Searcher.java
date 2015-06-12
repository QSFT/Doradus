package com.dell.doradus.logservice.search;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.FieldDefinition;
import com.dell.doradus.common.FieldType;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.common.Utils;
import com.dell.doradus.olap.io.BSTR;
import com.dell.doradus.search.FieldSet;
import com.dell.doradus.search.aggregate.SortOrder;
import com.dell.doradus.service.db.DBService;
import com.dell.doradus.service.db.DColumn;
import com.dell.doradus.service.db.Tenant;

public class Searcher {
    
    public static TableDefinition getTableDef(Tenant tenant, String application, String table) {
        String store = application + "_" + table;
        ApplicationDefinition appDef = new ApplicationDefinition();
        appDef.setAppName(application);
        TableDefinition tableDef = new TableDefinition(appDef, table);
        appDef.addTable(tableDef);
        FieldDefinition fieldDef = new FieldDefinition(tableDef);
        fieldDef.setType(FieldType.TIMESTAMP);
        fieldDef.setName("Timestamp");
        tableDef.addFieldDefinition(fieldDef);
        Iterator<DColumn> it = DBService.instance().getAllColumns(tenant, store, "fields");
        if(it != null) {
            while(it.hasNext()) {
                String field = it.next().getName();
                fieldDef = new FieldDefinition(tableDef);
                fieldDef.setType(FieldType.TEXT);
                fieldDef.setName(field);
                tableDef.addFieldDefinition(fieldDef);
            }
        }
        return tableDef;
    }
    
    public static BSTR[] getFields(FieldSet fieldSet) {
        List<BSTR> fields = new ArrayList<>(fieldSet.ScalarFields.size());
        for(String f: fieldSet.ScalarFields) {
            if("Timestamp".equals(f)) continue;
            fields.add(new BSTR(f));
        }
        return fields.toArray(new BSTR[fields.size()]);
    }
    
    public static boolean isSortDescending(SortOrder[] order) {
        if(order == null || order.length == 0) return false;
        Utils.require(order.length == 1, "Cannot sort by more than one value");
        Utils.require(order[0].items.size() == 1, "Cannot sort by link path");
        Utils.require("Timestamp".equals(order[0].items.get(0).name), "Only sort by timestamp is supported");
        return !order[0].ascending;
    }
    
}
