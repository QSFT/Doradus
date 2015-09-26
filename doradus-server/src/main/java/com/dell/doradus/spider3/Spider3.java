package com.dell.doradus.spider3;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.BatchResult;
import com.dell.doradus.common.DBObjectBatch;
import com.dell.doradus.common.FieldDefinition;
import com.dell.doradus.common.FieldType;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.common.Utils;
import com.dell.doradus.logservice.LogQuery;
import com.dell.doradus.search.SearchResultList;
import com.dell.doradus.service.db.DBService;
import com.dell.doradus.service.db.DColumn;
import com.dell.doradus.service.db.Tenant;
import com.dell.doradus.service.tenant.TenantService;
import com.dell.doradus.utilities.Timer;

/**
 * The following storage schema is used:
 * 
 * storeName=applicationName
 * 
 * dynamic fields:
 * 
 * "_fields" = { table1/field1=null, table2/field2=null, table3/field3=null, ..., tableN/fieldN=null }
 * 
 * ids:
 * "table/_id" = { id1=null, id2=null, ... }
 * 
 * sv fields:
 * 
 * "table/field" = { id1 = value1, id2 = value2, ... }
 * 
 * mv fields/links:
 * 
 * "table/field" = { id1\0v1, id1\0v2, id2\0v3, ... }
 * 
 * 
 */

public class Spider3 {
    private static Spider3 m_instance;
    private Logger m_logger = LoggerFactory.getLogger(getClass());
    private DBService m_service = DBService.instance(); 
    
    Spider3() {}
    
    public static Spider3 instance() {
        if(m_instance == null) m_instance = new Spider3();
        return m_instance;
    }
    

    public void createApplication(Tenant tenant, String application) {
        m_service.createStoreIfAbsent(tenant, application, true);
    }

    public void deleteApplication(Tenant tenant, String application) {
        DBService.instance().deleteStoreIfPresent(tenant, application);
    }

    public Tenant getTenant(ApplicationDefinition appDef) {
        String tenantName = appDef.getTenantName();
        if(tenantName == null) return TenantService.instance().getDefaultTenant();
        else return new Tenant(TenantService.instance().getTenantDefinition(tenantName));
    }
    
    public ApplicationDefinition addDynamicFields(ApplicationDefinition appDef) {
        Tenant tenant = getTenant(appDef);
        String store = appDef.getAppName();
        for(DColumn column: m_service.getAllColumns(tenant, store, "_fields")) {
            String[] nv = column.getName().split("/");
            String tableName = nv[0];
            String fieldName = nv[1];
            TableDefinition tableDef = appDef.getTableDef(tableName);
            if(tableDef == null) {
                tableDef = new TableDefinition(appDef);
                appDef.addTable(tableDef);
            }
            FieldDefinition fieldDef = tableDef.getFieldDef(fieldName);
            if(fieldDef == null) {
                fieldDef = new FieldDefinition(tableDef);
                fieldDef.setName(fieldName);
                fieldDef.setType(FieldType.TEXT);
                fieldDef.setCollection(false);
                tableDef.addFieldDefinition(fieldDef);
            }
        }
        return appDef;
    }
    
    public static String[] split(String value) {
        int idx = value.indexOf('\0');
        String[] nv = new String[2];
        nv[0] = value.substring(0, idx);
        nv[1] = value.substring(idx + 1);
        return nv;
    }
    
    public static String concat(String name, String value) {
        return name + "\0" + value;
    }
    
    public BatchResult addBatch(Tenant tenant, ApplicationDefinition appDef, String tableName, DBObjectBatch batch) {
        Timer t = new Timer();
        ObjectUpdater updater = new ObjectUpdater(tenant, appDef, tableName, batch);
        BatchResult batchResult = updater.update();
        m_logger.info("Added {} objects to {} in {}", new Object[] { batch.getObjectCount(), appDef.getAppName(), t.toString() });
        return batchResult;
    } 
    
    public SearchResultList search(Tenant tenant, ApplicationDefinition appDef, String tableName, LogQuery query) {
        appDef = addDynamicFields(appDef);
        TableDefinition tableDef = appDef.getTableDef(tableName);
        Utils.require(tableDef != null, "Unknown table: " + tableName);
        return Spider3Search.search(tableDef, query);
    }
    
}



