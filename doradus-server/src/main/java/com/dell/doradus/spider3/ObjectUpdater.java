package com.dell.doradus.spider3;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.BatchResult;
import com.dell.doradus.common.DBObject;
import com.dell.doradus.common.DBObjectBatch;
import com.dell.doradus.common.FieldDefinition;
import com.dell.doradus.common.ObjectResult;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.common.Utils;
import com.dell.doradus.service.db.DBService;
import com.dell.doradus.service.db.DBTransaction;
import com.dell.doradus.service.db.DColumn;
import com.dell.doradus.service.db.Tenant;

public class ObjectUpdater {
    private DBService m_service;
    private Tenant m_tenant;
    private ApplicationDefinition m_appDef;
    private String m_tableName;
    private DBObjectBatch m_batch;
    private Set<String> m_dynamicFields = new HashSet<>();
    private DBTransaction m_transaction;
    private BatchResult m_batchResult;
    
    
    public ObjectUpdater(Tenant tenant, ApplicationDefinition appDef, String tableName, DBObjectBatch batch) {
        m_tenant = tenant;
        m_appDef = appDef;
        m_tableName = tableName;
        m_batch = batch;
        m_appDef = Spider3.instance().addDynamicFields(m_appDef);
        m_service = DBService.instance();
        m_transaction = m_service.startTransaction(tenant);
        m_batchResult = new BatchResult();
    }
    
    public BatchResult update() {
        normalizeBatch();
        fillDynamicFields();
        for(DBObject object: m_batch.getObjects()) {
            if(object.isDeleted()) {
                deleteObject(object);
            } else {
                addFieldValues(object);
                removeFieldValues(object);
            }
        }
        DBService.instance().commit(m_transaction);
        return m_batchResult;
    }
    
    private void normalizeBatch() {
        for(DBObject object: m_batch.getObjects()) {
            String table = object.getTableName();
            if(table == null) {
                table = m_tableName;
                object.setTableName(table);
            }
            TableDefinition tableDef = m_appDef.getTableDef(table);
            Utils.require(tableDef != null || m_appDef.allowsAutoTables(),
                          "Unknown table for application '%s': %s", m_appDef.getAppName(), table);
            String id = object.getObjectID();
            if(id == null) {
                id = Utils.getUniqueId(); // 36 bytes!!! we need to make it smaller!
                object.setObjectID(id);
            }
        }
    }
    
    private void fillDynamicFields() {
        for(DBObject object: m_batch.getObjects()) {
            String table = object.getTableName();
            if(table == null) table = m_tableName;
            TableDefinition tableDef = m_appDef.getTableDef(table);
            for(String field: object.getUpdatedFieldNames()) {
                if(field.startsWith("_")) continue; // system field
                if(tableDef == null || tableDef.getFieldDef(field) == null) {
                    m_dynamicFields.add(table + "/" + field);
                }
            }
        }
        
        String store = m_appDef.getAppName();
        for(String field: m_dynamicFields) {
            m_transaction.addColumn(store, "_fields", field);
        }
    }
    
    private void addFieldValues(DBObject object) {
        String store = m_appDef.getAppName();
        String id = object.getObjectID();
        String table = object.getTableName();
        TableDefinition tableDef = m_appDef.getTableDef(table);
        ObjectResult objectResult = new ObjectResult(id);
        m_batchResult.addObjectResult(objectResult);
        
        m_transaction.addColumn(store, table + "/_id", id);
        
        for(String field: object.getUpdatedFieldNames()) {
            if(field.startsWith("_")) continue; // system field
            FieldDefinition fieldDef = tableDef == null ? null : tableDef.getFieldDef(field);
            Collection<String> fieldValues = object.getFieldValues(field);
            if(fieldValues == null) continue;
            String row = table + "/" + field;
            if(fieldDef == null || !fieldDef.isCollection()) {
                for(String value: fieldValues) {
                    m_transaction.addColumn(store, row, id, value);
                }
            } else if(fieldDef.isLinkField()) {
                String inverseLinkRow = fieldDef.getLinkExtent() + "/" + fieldDef.getLinkInverse();
                for(String value: fieldValues) {
                    m_transaction.addColumn(store, row, Spider3.concat(id, value));
                    m_transaction.addColumn(store, inverseLinkRow, Spider3.concat(value , id));
                }
            } else {
                for(String value: fieldValues) {
                    m_transaction.addColumn(store, row, Spider3.concat(id, value));
                }
            }
        }
    }
    
    private void removeFieldValues(DBObject object) {
        String store = m_appDef.getAppName();
        String id = object.getObjectID();
        String table = object.getTableName();
        TableDefinition tableDef = m_appDef.getTableDef(table);
        ObjectResult objectResult = new ObjectResult(id);
        m_batchResult.addObjectResult(objectResult);
        
        m_transaction.addColumn(store, table + "/_id", id);
        
        for(String field: object.getUpdatedFieldNames()) {
            if(field.startsWith("_")) continue; // system field
            FieldDefinition fieldDef = tableDef == null ? null : tableDef.getFieldDef(field);
            Collection<String> fieldValues = object.getRemoveValues(field);
            if(fieldValues == null) continue;
            String row = table + "/" + field;
            if(fieldDef == null || !fieldDef.isCollection()) {
                m_transaction.deleteColumn(store, row, id);
            } else if(fieldDef.isLinkField()) {
                String inverseLinkRow = fieldDef.getLinkExtent() + "/" + fieldDef.getLinkInverse();
                for(String value: fieldValues) {
                    m_transaction.deleteColumn(store, row, Spider3.concat(id, value));
                    m_transaction.deleteColumn(store, inverseLinkRow, Spider3.concat(value, id));
                }
            } else {
                for(String value: fieldValues) {
                    m_transaction.deleteColumn(store, row, Spider3.concat(id, value));
                }
            }
        }
    }
    
    private void deleteObject(DBObject object) {
        String store = m_appDef.getAppName();
        String id = object.getObjectID();
        String table = object.getTableName();
        TableDefinition tableDef = m_appDef.getTableDef(table);
        ObjectResult objectResult = new ObjectResult(id);
        m_batchResult.addObjectResult(objectResult);
        
        m_transaction.deleteColumn(store, table + "/_id", id);
        
        for(FieldDefinition fieldDef: tableDef.getFieldDefinitions()) {
            String row = table + "/" + fieldDef.getName();
            if(fieldDef.isScalarField()) {
                m_transaction.deleteColumn(store, row, id);
            } else if(fieldDef.isLinkField()) {
                String inverseLinkRow = fieldDef.getLinkExtent() + "/" + fieldDef.getLinkInverse();
                for(DColumn value: m_service.getColumnSlice(m_tenant, store, row, id, id + "~")) {
                    m_transaction.deleteColumn(store, row, value.getName());
                    String[] nv = Spider3.split(value.getName());
                    m_transaction.deleteColumn(store, inverseLinkRow, Spider3.concat(nv[1], nv[0]));
                }
            } else {
                for(DColumn value: m_service.getColumnSlice(m_tenant, store, row, id, id + "~")) {
                    m_transaction.deleteColumn(store, row, value.getName());
                }
            }
        }
    }
    
    
    
    
    
}

    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    