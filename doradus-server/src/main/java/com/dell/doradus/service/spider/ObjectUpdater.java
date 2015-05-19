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

import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dell.doradus.common.DBObject;
import com.dell.doradus.common.FieldDefinition;
import com.dell.doradus.common.ObjectResult;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.common.Utils;
import com.dell.doradus.core.IDGenerator;

/**
 * Performs updates (add, update, or delete) for a single {@link DBObject}. The update
 * methods are: {@link #addNewObject()}, {@link #deleteObject()}, and
 * {@link #updateObject()}. These methods are passed a parent {@link SpiderTransaction},
 * which is updated with the row/column changes of the update method if the it succeeds
 * and actually has updates.
 */
public class ObjectUpdater {
    private final SpiderTransaction m_dbTran = new SpiderTransaction();
    private final TableDefinition   m_tableDef;
    
    // Holds target object shard numbers for sharded link field updates.
    // Map is <table name> -> <object ID> -> <shard number>
    private Map<String, Map<String, Integer>> m_targetObjectShardNoMap;

    // Logging interface:
    private static Logger m_logger = LoggerFactory.getLogger(ObjectUpdater.class.getSimpleName());
    
    /**
     * Create an ObjectUpdater that perform updates for the given table.
     * 
     * @param tableDef {@link TableDefinition} of table to update.
     */
    public ObjectUpdater(TableDefinition tableDef) {
        m_tableDef = tableDef;
    }   // constructor

    /**
     * Get the {@link TableDefinition} of the table on which this ObjectUpdater executes.
     * 
     * @return  This ObjectUpdater's table definition.
     */
    public TableDefinition getTableDef() {
        return m_tableDef;
    }   // getTableDef

    /**
     * Get the {@link SpiderTransaction} to which update mutations are being added for
     * this ObjectUpdater.
     * 
     * @return  The SpiderTransaction to which updates are being added.
     */
    public SpiderTransaction getTransaction() {
        return m_dbTran;
    }

    /**
     * Get the target object shard number assigned by {@link #setTargetObjectShardNumbers(Map)}.
     * 
     * @return  Map of {table name} -> {object ID} -> {shard number}, representing target
     *          object shard numbers.
     */
    public Map<String, Map<String, Integer>> getTargetObjectShardNos() {
        return m_targetObjectShardNoMap;
    }   // getTargetObjectShardNos
    
    /**
     * Set the target object shard number map for this ObjectUpdater. This 2-level map is
     * {table name} -> {object ID} -> {shard number}, where {object ID} is an object
     * referenced by a sharded link in the current update batch, and {table name} is
     * the table in which it resides. A target object's shard number is only known when
     * the object has already been created and has a value for its sharding-field.
     *  
     * @param targObjShardNos   Map of {table name} -> {object ID} -> {shard number},
     *                          representing target object shard numbers.
     */
    public void setTargetObjectShardNumbers(Map<String, Map<String, Integer>> targObjShardNos) {
        m_targetObjectShardNoMap = targObjShardNos;
    }   // setTargetObjectShardNumbers
    
    /**
     * Add the given DBObject to the database as a new object. No check is made to see if
     * an object with the same ID already exists. If the update is successful, updates are
     * merged to the given parent SpiderTransaction.
     * 
     * @param parentTran    Parent {@link SpiderTransaction} to which updates are applied
     *                      if the add is successful.
     * @param dbObj         DBObject to be added to the database.
     * @return              {@link ObjectResult} representing the results of the update.
     */
    public ObjectResult addNewObject(SpiderTransaction parentTran, DBObject dbObj) {
        ObjectResult result = new ObjectResult();
        try {
            addBrandNewObject(dbObj);
            result.setObjectID(dbObj.getObjectID());
            result.setUpdated(true);
            parentTran.mergeSubTransaction(m_dbTran);
            m_logger.trace("addNewObject(): Object added/updated for ID={}", dbObj.getObjectID());
        } catch (Throwable ex) {
            buildErrorStatus(result, dbObj.getObjectID(), ex);
        }
        return result;
    }   // addNewObject
    
    /**
     * Delete the object with the given object ID. Because of idempotent update semantics,
     * it is not an error if the object does not exist. If an object is actually deleted,
     * updates are merged to the given parent SpiderTransaction.
     * 
     * @param parentTran    Parent {@link SpiderTransaction} to which updates are applied
     *                      if the delete is successful.
     * @param objID         ID of object to be deleted.
     * @return              {@link ObjectResult} representing the results of the delete.
     *                      Includes a comment if the object was not found.
     */
    public ObjectResult deleteObject(SpiderTransaction parentTran, String objID) {
        ObjectResult result = new ObjectResult(objID);
        try {
            result.setObjectID(objID);
            DBObject dbObj = SpiderService.instance().getObject(m_tableDef, objID);
            if (dbObj != null) {
                deleteObject(dbObj);
                result.setUpdated(true);
                parentTran.mergeSubTransaction(m_dbTran);
                m_logger.trace("deleteObject(): object deleted with ID={}", objID);
            } else {
                result.setComment("Object not found");
                m_logger.trace("deleteObject(): no object with ID={}", objID);
            }
        } catch (Throwable ex) {
            buildErrorStatus(result, objID, ex);
        }
        return result;
    }   // deleteObject
    
    /**
     * Update the given object, whose current values have been pre-fetched. The object
     * must exist, and the values in the given DBObject are compared to the given current
     * values to determine which ones are being added or updated. If an update is actually
     * made, updates are merged to the given parent SpiderTransaction.
     * 
     * @param parentTran    Parent {@link SpiderTransaction} to which updates are applied
     *                      if the delete is successful.
     * @param dbObj         DBObject to be updated.
     * @param currScalarMap Map of the existing objects current scalar values for fields
     *                      being updated. 
     * @return              {@link ObjectResult} representing the results of the update.
     *                      Includes a comment if no updates were made.
     */
    public ObjectResult updateObject(SpiderTransaction   parentTran,
                                     DBObject            dbObj,
                                     Map<String, String> currScalarMap) {
        ObjectResult result = new ObjectResult();
        try {
            result.setObjectID(dbObj.getObjectID());
            boolean bUpdated = updateExistingObject(dbObj, currScalarMap);
            result.setUpdated(bUpdated);
            if (bUpdated) {
                m_logger.trace("updateObject(): object updated for ID={}", dbObj.getObjectID());
                parentTran.mergeSubTransaction(m_dbTran);
            } else {
                result.setComment("No updates made");
                m_logger.trace("updateObject(): no updates made for ID={}", dbObj.getObjectID());
            }
        } catch (Throwable ex) {
            buildErrorStatus(result, dbObj.getObjectID(), ex);
        }
        return result;
    }   // updateObject
    
    ///// Private methods
    
    // Add new object to the database.
    private boolean addBrandNewObject(DBObject dbObj) {
        if (Utils.isEmpty(dbObj.getObjectID())) {
            dbObj.setObjectID(Utils.base64FromBinary(IDGenerator.nextID()));
        }
        checkForNewShard(dbObj);
        for (String fieldName : dbObj.getUpdatedFieldNames()) {
            FieldUpdater fieldUpdater = FieldUpdater.createFieldUpdater(this, dbObj, fieldName);
            fieldUpdater.addValuesForField();
        }
        return true;
    }   // addBrandNewObject
    
    // Add error fields to the given ObjectResult due to the given exception.
    private void buildErrorStatus(ObjectResult result, String objID, Throwable ex) {
        result.setStatus(ObjectResult.Status.ERROR);
        result.setErrorMessage(ex.getLocalizedMessage());
        if (!Utils.isEmpty(objID)) {
            result.setObjectID(objID);
        }
        if (ex instanceof IllegalArgumentException) {
            m_logger.debug("Object update error: {}", ex.toString());
        } else {
            result.setStackTrace(Utils.getStackTrace(ex));
            m_logger.debug("Object update error: {} stacktrace: {}",
                           ex.toString(), Utils.getStackTrace(ex));
        }
    }   // buildErrorStatus
    
    // If object is sharded, see if we need to add a new column to the "current shards" row.
    private void checkForNewShard(DBObject dbObj) {
        int shardNumber = m_tableDef.getShardNumber(dbObj);
        if (shardNumber > 0) {
            SpiderService.instance().verifyShard(m_tableDef, shardNumber);
        }
    }   // checkForNewShard
    
    // Return true if this is an implicitly-created object whose shard number was
    // previously undetermined but is not being assigned to a shard.
    private void checkNewlySharded(DBObject dbObj, Map<String, String> currScalarMap) {
        if (!m_tableDef.isSharded()) {
            return;
        }
        String shardingFieldName = m_tableDef.getShardingField().getName();
        if (!currScalarMap.containsKey(shardingFieldName)) {
            m_dbTran.addAllObjectsColumn(m_tableDef, dbObj.getObjectID(), m_tableDef.getShardNumber(dbObj));
        }
    }   // checkNewlySharded

    // Delete term columns and inverses for field values and the object's primary row.
    private void deleteObject(DBObject dbObj) {
        for (String fieldName : dbObj.getUpdatedFieldNames()) {
            FieldUpdater fieldUpdater = FieldUpdater.createFieldUpdater(this, dbObj, fieldName);
            fieldUpdater.deleteValuesForField();
        }
        m_dbTran.deleteObjectRow(m_tableDef, dbObj.getObjectID());
    }   // deleteObject

    // Update the given object using the appropriate strategy.
    private boolean updateExistingObject(DBObject dbObj, Map<String, String> currScalarMap) {
        if (objectIsChangingShards(dbObj, currScalarMap)) {
            return updateShardedObjectMove(dbObj);
        } else {
            return updateObjectSameShard(dbObj, currScalarMap);
        }
    }   // updateExistingObject
    
    // Update the given object "in place", meaning in the same shard. Most updates come
    // through here.
    private boolean updateObjectSameShard(DBObject dbObj, Map<String, String> currScalarMap) {
        boolean bUpdated = false;
        checkForNewShard(dbObj);
        checkNewlySharded(dbObj, currScalarMap);
        for (String fieldName : dbObj.getUpdatedFieldNames()) {
            FieldUpdater fieldUpdater = FieldUpdater.createFieldUpdater(this, dbObj, fieldName);
            bUpdated |= fieldUpdater.updateValuesForField(currScalarMap.get(fieldName));
        }
        return bUpdated;
    }   // updateObjectSameShard
    
    // Update the given object, which is changing shards. For this rare case, we must
    // delete the current object, merge the new fields into it, and re-add it.
    private boolean updateShardedObjectMove(DBObject dbObj) {
        DBObject currDBObj = SpiderService.instance().getObject(m_tableDef, dbObj.getObjectID());
        m_logger.debug("Update forcing move of object {} from shard {} to shard {}",
                       new Object[]{dbObj.getObjectID(),
                                    m_tableDef.getShardNumber(currDBObj),
                                    m_tableDef.getShardNumber(dbObj)});
        
        deleteObject(currDBObj);
        mergeAllFieldValues(dbObj, currDBObj);
        addBrandNewObject(currDBObj);
        return true;
    }   // updateShardedObjectMove

    // Merge all field values in the given source object into the given target object. SV
    // fields are replaced. MV fields acquire the results of add/remove processing.
    private void mergeAllFieldValues(DBObject srcDBObj, DBObject tgtDBObj) {
        for (String fieldName : srcDBObj.getUpdatedFieldNames()) {
            FieldDefinition fieldDef = m_tableDef.getFieldDef(fieldName);
            if (fieldDef != null && (fieldDef.isCollection() || fieldDef.isLinkField())) {
                Set<String> newValueSet =
                    ScalarFieldUpdater.mergeMVFieldValues(tgtDBObj.getFieldValues(fieldName),
                                                          srcDBObj.getRemoveValues(fieldName),
                                                          srcDBObj.getFieldValues(fieldName));
                tgtDBObj.clearValues(fieldName);
                tgtDBObj.addFieldValues(fieldName, newValueSet);
            } else {
                tgtDBObj.clearValues(fieldName);
                tgtDBObj.addFieldValue(fieldName, srcDBObj.getFieldValue(fieldName));
            }
        }
    }   // mergeAllFieldValues
    
    // Return true if, based on the values in the given current-scalar map, the given object
    // currently lives in one shard but, based the values in the given DBObject, must be
    // moved to a new shard.
    private boolean objectIsChangingShards(DBObject dbObj, Map<String, String> currScalarMap) {
        if (!m_tableDef.isSharded()) {
            return false;
        }
        
        // If object had no prior sharding-field value, it is considered in shard 0.
        int oldShardNumber = 0;
        String shardingFieldName = m_tableDef.getShardingField().getName();
        String currShardFieldValue = currScalarMap.get(shardingFieldName);
        if (currShardFieldValue != null) {
            oldShardNumber = m_tableDef.computeShardNumber(Utils.dateFromString(currShardFieldValue));
        }
        
        // Propagate old sharding-field value if not being updated now.
        int newShardNumber = m_tableDef.getShardNumber(dbObj);
        String newShardFieldValue = dbObj.getFieldValue(shardingFieldName);
        if (newShardFieldValue == null && currShardFieldValue != null) {
            dbObj.addFieldValue(shardingFieldName, currShardFieldValue);
            return false;
        }
        
        // May or may not be moving to a new shard.
        return oldShardNumber != newShardNumber;
    }   // objectIsChangingShards

}   // class ObjectUpdater
