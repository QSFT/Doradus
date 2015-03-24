/*
 * Copyright (C) 2014 Dell, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dell.doradus.service.spider;

import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.dell.doradus.common.CommonDefs;
import com.dell.doradus.common.DBObject;
import com.dell.doradus.common.FieldDefinition;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.common.Utils;
import com.dell.doradus.service.db.DBService;
import com.dell.doradus.service.db.DBTransaction;
import com.dell.doradus.service.db.Tenant;

/**
 * Represents an update transaction for the Spider storage service. SpiderTransaction
 * provides methods for common operations such as adding columns to an object's primary
 * row, adding and removing indexing terms, updating an "all objects" row, etc. All 
 * updates are translated into row/column updates to the appropriate ColumnFamilies and
 * stored in a {@link DBTransaction}. Updates are committed when {@link #commit()} is
 * called, at which time the DBTransaction is committed and cleared. Additional updates
 * can be added to the same SpiderTransaction object, allowing them to be committed with
 * the same timestamp as previous commits.
 */
public class SpiderTransaction {
    /**
     * The key of the "all objects" record that resides in each Terms table.
     */
    public static final String ALL_OBJECTS_ROW_KEY = "_";
    
    /**
     * Key of "current shards" row in sharded table Terms tables.
     */
    public static final String SHARDS_ROW_KEY = "_shards";

    /**
     * Row key for the "field registry" row in the Terms table.
     */
    public static final String FIELD_REGISTRY_ROW_KEY = "_fields";
    
    /**
     * Row key prefix for "term registry" rows in the Terms table.
     */
    public static final String TERMS_REGISTRY_ROW_PREFIX = "_terms";
    
    // The keyspace and the embedded DBTransaction that holds column/row updates:
    private final Tenant m_tenant;
    private DBTransaction m_dbTran;
    
    // This map holds table/term references. It prevents duplicate updates in the same
    // transaction. It's format is <table> -> Set<field name>
    private final Map<TableDefinition, Set<String>> m_tableFieldRefMap = new HashMap<>();

    // This map holds terms referenced for specific tables/shards/fields. The map's keys are:
    //      <table> -> <shard number> -> <field name> -> <term>
    // Non-sharded objects belong to shard number 0. It prevents duplicate updates.
    private final Map<TableDefinition, Map<Integer, Map<String, Set<String>>>> m_tableTermRefMap =
        new HashMap<TableDefinition, Map<Integer, Map<String, Set<String>>>>();
    
    /**
     * Create a new SpiderTransaction object, which starts a new transaction with "now"
     * as the timestamp.
     */
    public SpiderTransaction(Tenant tenant) {
        m_tenant = tenant;
        m_dbTran  = DBService.instance().startTransaction(tenant);
    }
    
    /**
     * Clear this transaction's updates without committing them.
     */
    public void clear() {
        m_dbTran.clear();
        m_tableFieldRefMap.clear();
    }   // clear
    
    /**
     * Commit this SpiderTransaction's updates, causing them to be cleared as well. The
     * object can be used for additional updates, which will be committed with the same
     * timestamp.
     */
    public void commit() {
        try {
            DBService.instance().commit(m_dbTran);
            // Re-create the transaction to renew its timestamp.
        	m_dbTran = DBService.instance().startTransaction(m_tenant);
        } finally {
            clear();
        }
    }   // commit
    
    /**
     * Get the total number of updates (column updates/deletes and row deletes) queued
     * in this transaction so far.
     * 
     * @return  Total number of updates queued in this transaction so far.
     */
    public int getUpdateCount() {
        return m_dbTran.getUpdateCount();
    }   // getUpdateCount

    ///// Update methods
    
    /**
     * Add an "all objects" column for an object in the given table with the given ID. The
     * "all objects" lives in the Terms store and its row key is "_" for objects residing
     * in shard 0, "{shard number}/_" for objects residing in other shards. The column
     * added is named with the object's ID but has no value.
     * 
     * @param tableDef  {@link TableDefinition} of table that owns object.
     * @param objID     ID of object being added.
     * @param shardNo   Shard number if owing table is sharded.
     */
    public void addAllObjectsColumn(TableDefinition tableDef, String objID, int shardNo) {
    	String rowKey = ALL_OBJECTS_ROW_KEY;
    	if (shardNo > 0) {
    		rowKey = shardNo + "/" + ALL_OBJECTS_ROW_KEY;
    	}
        m_dbTran.addColumn(SpiderService.termsStoreName(tableDef), rowKey, objID);
    }   // addAllObjectsColumns

    /**
     * Similar to {@link #addScalarValueColumn(DBObject, String, String)} but specialized
     * for the _ID field. Adds the _ID column for the object's primary store. 
     * 
     * @param tableDef  {@link TableDefinition} of owning table.
     * @param objID     ID of object whose _ID value to set.
     * @see             #addScalarValueColumn(DBObject, String, String)
     */
    public void addIDValueColumn(TableDefinition tableDef, String objID) {
        m_dbTran.addColumn(SpiderService.objectsStoreName(tableDef),
                           objID,
                           CommonDefs.ID_FIELD,
                           Utils.toBytes(objID));
    }   // addIDValueColumn
    
    /**
     * Add a column to the "_fields" row belonging to the given table for the given scalar
     * field names. This row serves as a registry of scalar fields actually used by objects 
     * in the table. Field references go in the Terms table using the format:
     * <pre>
     *      _fields = {{field 1}:null, {field 2}:null, ...}
     * </pre>
     * Updates are added to the current transaction only for new scalar field names that
     * have not yet been referenced.
     * 
     * @param tableDef      Table that owns referenced fields.
     * @param fieldNames    Scalar field names that received an update in the current
     *                      transaction. 
     */
    public void addFieldReferences(TableDefinition tableDef, Collection<String> fieldNames) {
        if (fieldNames.size() == 0) {
            return;
        }
        
        // Only add fields we haven't added yet in this transaction.
        Set<String> currFieldNames = m_tableFieldRefMap.get(tableDef);
        if (currFieldNames == null) {
            currFieldNames = new HashSet<String>();
            m_tableFieldRefMap.put(tableDef, currFieldNames);
        }
        for (String fieldName : fieldNames) {
            if (currFieldNames.add(fieldName)) {
                m_dbTran.addColumn(SpiderService.termsStoreName(tableDef), FIELD_REGISTRY_ROW_KEY, fieldName);
            }
        }
    }   // addFieldReferences

    /**
     * Add a link value column to the objects store of the given object ID.
     *  
     * @param ownerObjID    Object ID of object owns the link field.
     * @param linkDef       {@link FieldDefinition} of the link field.
     * @param targetObjID   Referenced (target) object ID.
     */
    public void addLinkValue(String ownerObjID, FieldDefinition linkDef, String targetObjID) {
        m_dbTran.addColumn(SpiderService.objectsStoreName(linkDef.getTableDef()),
                           ownerObjID,
                           SpiderService.linkColumnName(linkDef, targetObjID));
    }   // addLinkValue

    /**
     * Add the column needed to add or replace the given scalar field belonging to the
     * object with the given ID in the given table.
     * 
     * @param tableDef      {@link TableDefinition} of table that owns object.
     * @param objID         ID of object.
     * @param fieldName     Name of scalar field being added.
     * @param fieldValue    Value being added in string form.
     */
    public void addScalarValueColumn(TableDefinition tableDef, String objID, String fieldName, String fieldValue) {
        m_dbTran.addColumn(SpiderService.objectsStoreName(tableDef),
                           objID,
                           fieldName,
                           SpiderService.scalarValueToBinary(tableDef, fieldName, fieldValue));
    }   // addScalarValueColumn
    
    /**
     * Add a link value column on behalf of the given owner object, referencing the given
     * target object ID. This is used when a link is sharded and the owner's shard number
     * is > 0. The link value column is added to a special term record.
     *  
     * @param ownerObjID    Object ID of object owns the link field.
     * @param linkDef       {@link FieldDefinition} of the link field.
     * @param targetObjID   Referenced (target) object ID (owning table is sharded).
     * @param targetShardNo Shard number of the target object. Must be > 0.
     */
    public void addShardedLinkValue(String ownerObjID, FieldDefinition linkDef, String targetObjID, int targetShardNo) {
        assert linkDef.isSharded();
        assert targetShardNo > 0;
        m_dbTran.addColumn(SpiderService.termsStoreName(linkDef.getTableDef()),
                           SpiderService.shardedLinkTermRowKey(linkDef, ownerObjID, targetShardNo),
                           targetObjID);
    }   // addShardedLinkValue

    /**
     * Add a column to the "_shards" row for the given shard number and date, indicating
     * that this shard is being used. The "_shards" row lives in the table's Terms store
     * and uses the format:
     * <pre>
     *      _shards = {{shard 1}:{date 1}, {shard 1}:{date 2}, ...}
     * </pre>
     * Dates are stored as {@link Date#getTime()} values, converted to strings via
     * {@link Long#toString()}.
     * 
     * @param tableDef      {@link TableDefinition} of a sharded table.
     * @param shardNumber   Number of a new shard (must be > 0).
     * @param startDate     Date that marks the beginning of object timestamp values that
     *                      reside in the shard.
     */
    public void addShardStart(TableDefinition tableDef, int shardNumber, Date startDate) {
        assert tableDef.isSharded() && shardNumber > 0;
        m_dbTran.addColumn(SpiderService.termsStoreName(tableDef),
                           SHARDS_ROW_KEY,
                           Integer.toString(shardNumber),
                           Utils.toBytes(Long.toString(startDate.getTime())));
    }   // addShardStart

    /**
     * Index the given term by adding a Terms column for the given DBObject, field name,
     * and term. Non-sharded format:
     * <pre>
     *      [field name]/[field value]> = {[object ID]:null}
     * </pre>
     * Term record for a sharded object:
     * <pre>
     *      [shard number]/[field name]/[field value] = {[object ID]:null}
     * </pre>
     * 
     * @param dbObj     DBObject that owns field.
     * @param fieldName Field name.
     * @param term      Term being indexed.
     */
    public void addTermIndexColumn(TableDefinition tableDef, DBObject dbObj, String fieldName, String term) {
        m_dbTran.addColumn(SpiderService.termsStoreName(tableDef),
                           SpiderService.termIndexRowKey(tableDef, dbObj, fieldName, term),
                           dbObj.getObjectID());
    }   // addTermIndexColumn
    
    /**
     * Add term references for the given field-to-term map. This map indicates terms used
     * by fields belonging to the given table that are indexed in the Terms table.
     * <br>
     * Term references are "sharding aware", which means the references represent objects
     * that belong to a specific shard. The format of the field references row for shard
     * number 0 is:
     * <pre>
     *      _terms/{field name} = {{term 1}:null, {term 2}:null, ...}
     * </pre>
     * For shard numbers > 0, the row looks like this:
     * <pre>
     *      {shard}/_terms/{field name} = {{term 1}:null, {term 2}:null, ...}
     * </pre>
     *  
     * @param tableDef          Table that owns all field/term references in the map.
     * @param shardNumber       Shard number of object that owns the given field values.
     * @param fieldTermsRefMap  Map of field names to terms referenced.
     */
    public void addTermReferences(TableDefinition tableDef, int shardNumber,
                                  Map<String, Set<String>> fieldTermsRefMap) {
        if (fieldTermsRefMap.size() == 0) {
            return;
        }
        
        Map<Integer, Map<String, Set<String>>> currShardMap = m_tableTermRefMap.get(tableDef);
        if (currShardMap == null) {
            currShardMap = new HashMap<Integer, Map<String, Set<String>>>();
            m_tableTermRefMap.put(tableDef, currShardMap);
        }
        
        Map<String, Set<String>> currFieldTermMap = currShardMap.get(shardNumber);
        if (currFieldTermMap == null) {
            currFieldTermMap = new HashMap<String, Set<String>>();
            currShardMap.put(shardNumber, currFieldTermMap);
        }
        
        for (String fieldName : fieldTermsRefMap.keySet()) {
            Set<String> currTermSet = currFieldTermMap.get(fieldName);
            if (currTermSet == null) {
                currTermSet = new HashSet<String>();
                currFieldTermMap.put(fieldName, currTermSet);
            }
            for (String term : fieldTermsRefMap.get(fieldName)) {
                if (currTermSet.add(term)) {
                    StringBuilder rowKey = new StringBuilder();
                    if (shardNumber > 0) {
                        rowKey.append(shardNumber);
                        rowKey.append("/");
                    }
                    rowKey.append(TERMS_REGISTRY_ROW_PREFIX);
                    rowKey.append("/");
                    rowKey.append(fieldName);
                    m_dbTran.addColumn(SpiderService.termsStoreName(tableDef), rowKey.toString(), term);
                }
            }
        }
    }   // addTermReferences
    
    /**
     * Delete the "all objects" column with the given object ID from the given table.
     * 
     * @param tableDef  {@link TableDefinition} of object's owning table.
     * @param objID     ID of object being deleted.
     * @param shardNo   Shard number of object being deleted.
     * @see             #addAllObjectsColumn(TableDefinition, String, int)
     */
    public void deleteAllObjectsColumn(TableDefinition tableDef, String objID, int shardNo) {
    	String rowKey = ALL_OBJECTS_ROW_KEY;
    	if (shardNo > 0) {
    		rowKey = shardNo + "/" + ALL_OBJECTS_ROW_KEY;
    	}
        m_dbTran.deleteColumn(SpiderService.termsStoreName(tableDef),
                              rowKey,
                              objID);
    }   // deleteAllObjectsColumn

    /**
     * Delete the primary field storage row for the given object. This usually called
     * when the object is being deleted.
     * 
     * @param tableDef  {@link TableDefinition} that owns object.
     * @param objID     ID of object whose "objects" row is to be deleted.
     */
    public void deleteObjectRow(TableDefinition tableDef, String objID) {
        m_dbTran.deleteRow(SpiderService.objectsStoreName(tableDef), objID);
    }   // deleteObjectRow

    /**
     * Delete a scalar value column with the given field name for the given object ID
     * from the given table.
     * 
     * @param tableDef  {@link TableDefinition} of scalar field.
     * @param objID     ID of object.
     * @param fieldName Scalar field name.
     */
    public void deleteScalarValueColumn(TableDefinition tableDef, String objID, String fieldName) {
        m_dbTran.deleteColumn(SpiderService.objectsStoreName(tableDef),
                              objID,
                              fieldName);
    }   // deleteScalarValueColumn

    /**
     * Un-index the given term by deleting the Terms column for the given DBObject, field
     * name, and term.
     * 
     * @param tableDef  {@link TableDefinition} of table that owns object.
     * @param dbObj     DBObject that owns field.
     * @param fieldName Field name.
     * @param term      Term being un-indexed.
     */
    public void deleteTermIndexColumn(TableDefinition tableDef, DBObject dbObj, String fieldName, String term) {
        m_dbTran.deleteColumn(SpiderService.termsStoreName(tableDef),
                              SpiderService.termIndexRowKey(tableDef, dbObj, fieldName, term),
                              dbObj.getObjectID());
    }   // deleteTermIndexColumn
    
    /**
     * Delete a link value column in the object table of the given owning object.
     * 
     * @param ownerObjID    Object ID of object that owns the link field.
     * @param linkDef       {@link FieldDefinition} of the link field.
     * @param targetObjID   Referenced (target) object ID.
     */
    public void deleteLinkValue(String ownerObjID, FieldDefinition linkDef, String targetObjID) {
        m_dbTran.deleteColumn(SpiderService.objectsStoreName(linkDef.getTableDef()),
                              ownerObjID,
                              SpiderService.linkColumnName(linkDef, targetObjID));
    }   // deleteLinkValue

    /**
     * Delete the shard row for the given sharded link and shard number.
     * 
     * @param linkDef       {@link FieldDefinition} of a sharded link.
     * @param owningObjID   ID of object that owns the link.
     * @param shardNumber   Shard number of row to be deleted. Must be > 0.
     * @see                 #addShardedLinkValue(String, FieldDefinition, DBObject, int)
     * @see                 SpiderService#shardedLinkTermRowKey(FieldDefinition, String, int)
     */
    public void deleteShardedLinkRow(FieldDefinition linkDef, String owningObjID, int shardNumber) {
        assert linkDef.isSharded();
        assert shardNumber > 0;
        m_dbTran.deleteRow(SpiderService.termsStoreName(linkDef.getTableDef()),
                           SpiderService.shardedLinkTermRowKey(linkDef, owningObjID, shardNumber));
    }   // deleteShardedLinkRow
    
    /**
     * Delete a link value column in the Terms store for a sharded link.
     * 
     * @param objID         ID of object that owns the link field.
     * @param linkDef       {@link FieldDefinition} of (sharded) link field.
     * @param targetObjID   ID of referenced object.
     * @param shardNo       Shard number. Must be > 0.
     */
    public void deleteShardedLinkValue(String objID, FieldDefinition linkDef, String targetObjID, int shardNo) {
        assert linkDef.isSharded();
        assert shardNo > 0;
        m_dbTran.deleteColumn(SpiderService.termsStoreName(linkDef.getTableDef()),
                              SpiderService.shardedLinkTermRowKey(linkDef, objID, shardNo),
                              targetObjID);
    }   // deleteShardedLinkValue

}   // class SpiderTransaction
