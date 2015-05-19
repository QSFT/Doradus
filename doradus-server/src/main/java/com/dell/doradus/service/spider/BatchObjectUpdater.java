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

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dell.doradus.common.BatchResult;
import com.dell.doradus.common.DBObject;
import com.dell.doradus.common.DBObjectBatch;
import com.dell.doradus.common.FieldDefinition;
import com.dell.doradus.common.ObjectResult;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.common.Utils;
import com.dell.doradus.core.ServerConfig;
import com.dell.doradus.service.db.DBService;
import com.dell.doradus.service.db.DBTransaction;
import com.dell.doradus.service.db.Tenant;

/**
 * Performs updates for a batch of objects in a specific table. All updates are accummulated
 * in a {@link SpiderTransaction} and committed together. However, as objects are
 * processed, if the number of updates exceeds the configured limit
 * {@link ServerConfig#batch_mutation_threshold}, the current sub-batch is committed and
 * the DBTransaction is cleared. Further updates will be committed separately but with the
 * same transaction timestamp.
 */
public class BatchObjectUpdater {
    // Members:
    private final SpiderTransaction m_parentTran = new SpiderTransaction();
    private final TableDefinition   m_tableDef;

    // Logging interface:
    private static Logger m_logger = LoggerFactory.getLogger(BatchObjectUpdater.class.getSimpleName());
    
    /**
     * Create a BatchObjectUpdater that can process updates for the given table.
     * 
     * @param tableDef  {@link TableDefinition} of table.
     */
    public BatchObjectUpdater(TableDefinition tableDef) {
        m_tableDef = tableDef;
    }   // constructor
    
    /**
     * Add the given batch of objects to the database. All objects must belong to the same
     * table as given by {@link DBObjectBatch#getTableDef()}. Due to idempotent update
     * semantics, some or all objects with assigned object IDs may already exist. Such
     * objects are processed as a updates.
     * 
     * @param dbObjBatch    Batch of objects to add. 
     * @return              A {@link BatchResult} representing the results of the update.
     *                      An {@link ObjectResult} is added to the BatchResult for each
     *                      object to reflect it's outcome. 
     */
    public BatchResult addBatch(DBObjectBatch dbObjBatch) {
        Utils.require(dbObjBatch.getObjectCount() > 0, "Batch cannot be empty");
        BatchResult batchResult = new BatchResult();
        try {
            if (addOrUpdateBatch(dbObjBatch, batchResult)) {
                m_logger.debug("addBatch(): processed batch of {} objects", dbObjBatch.getObjectCount());
            } else {
                batchResult.setComment("No updates made");
                m_logger.debug("addBatch(): no updates made for {} objects", dbObjBatch.getObjectCount());
            }
        } catch (Throwable ex) {
            buildErrorStatus(batchResult, ex);
        }
        return batchResult;
    }   // addBatch
    
    /**
     * Update the database with given object batch. All objects must belong to the same
     * table as given by {@link DBObjectBatch#getTableDef()}. Each object must have an
     * assigned object ID and must already exist in the database. The SV fields in each
     * DBObject are replaced; MV fields use add/remove semantics. An {@link ObjectResult}
     * is added to the {@link BatchResult} returned to reflect the outcome of each object.
     *   
     * @param dbObjBatch    {@link DBObjectBatch} of {@link DBObject}s to update.
     * @return              {@link BatchResult} representing the results of each object
     *                      update and the batch as a whole.
     */
    public BatchResult updateBatch(DBObjectBatch dbObjBatch) {
        Utils.require(dbObjBatch.getObjectCount() > 0, "Batch cannot be empty");
        BatchResult batchResult = new BatchResult();
        try {
            if (updateBatch(dbObjBatch, batchResult)) {
                m_logger.debug("updateBatch(): processed batch of {} objects", dbObjBatch.getObjectCount());
            } else {
                batchResult.setComment("No updates made");
                m_logger.debug("updateBatch(): no updates made for {} objects", dbObjBatch.getObjectCount());
            }
        } catch (Throwable ex) {
            buildErrorStatus(batchResult, ex);
        }
        return batchResult;
    }   // updateBatch
    
    /**
     * Delete the objects with the given IDs. The {@link BatchResult} returned has an
     * {@link ObjectResult} for each object, indicating if the object was actually deleted
     * or not.  
     * 
     * @param objIDSet  Set of object IDs to delete. It is not an error if an object does
     *                  not exist.
     * @return
     */
    public BatchResult deleteBatch(Set<String> objIDSet) {
        BatchResult batchResult = new BatchResult();
        try {
            for (String objID : objIDSet) {
                checkCommit();
                ObjectUpdater objUpdater = new ObjectUpdater(m_tableDef);
                ObjectResult objResult = objUpdater.deleteObject(m_parentTran, objID);
                batchResult.addObjectResult(objResult);
            }
            commitTransaction();
        } catch (Throwable ex) {
            buildErrorStatus(batchResult, ex);
        }
        return batchResult;
    }   // deleteBatch
    
    ///// Private methods

    // Post all updates in the parent transaction to the database.
    private void commitTransaction() {
        DBTransaction dbTran = DBService.instance().startTransaction(Tenant.getTenant(m_tableDef));
        m_parentTran.applyUpdates(dbTran);
        DBService.instance().commit(dbTran);
    }

    // Add or update each object in the given batch as appropriate, updating BatchResult
    // accordingly.
    private boolean addOrUpdateBatch(DBObjectBatch dbObjBatch, BatchResult batchResult) throws IOException {
        Map<String, Map<String, String>> objCurrScalarMap = getCurrentScalars(dbObjBatch);
        Map<String, Map<String, Integer>> targObjShardNos = getLinkTargetShardNumbers(dbObjBatch);
        for (DBObject dbObj : dbObjBatch.getObjects()) {
            checkCommit();
            Map<String, String> currScalarMap = objCurrScalarMap.get(dbObj.getObjectID());  // ok if ID is null
            ObjectResult objResult = addOrUpdateObject(dbObj, currScalarMap, targObjShardNos);
            batchResult.addObjectResult(objResult);
        }
        commitTransaction();
        return true;
    }   // addOrUpdateBatch
    
    // Update each object in the given batch, updating BatchResult accordingly.
    private boolean updateBatch(DBObjectBatch dbObjBatch, BatchResult batchResult) throws IOException {
        Map<String, Map<String, String>> objCurrScalarMap = getCurrentScalars(dbObjBatch);
        Map<String, Map<String, Integer>> targObjShardNos = getLinkTargetShardNumbers(dbObjBatch);
        for (DBObject dbObj : dbObjBatch.getObjects()) {
            checkCommit();
            Map<String, String> currScalarMap = objCurrScalarMap.get(dbObj.getObjectID());
            ObjectResult objResult = updateObject(dbObj, currScalarMap, targObjShardNos);
            batchResult.addObjectResult(objResult);
        }
        commitTransaction();
        return batchResult.hasUpdates();
    }   // updateBatch
    
    // Update the given object, which must exist, using the given set of current scalar values.
    private ObjectResult updateObject(DBObject                          dbObj,
                                      Map<String, String>               currScalarMap,
                                      Map<String, Map<String, Integer>> targObjShardNos) {
        ObjectResult objResult = null;
        if (Utils.isEmpty(dbObj.getObjectID())) {
            objResult = ObjectResult.newErrorResult("Object ID is required", null);
        } else if (currScalarMap == null) {
            objResult = ObjectResult.newErrorResult("No object found", dbObj.getObjectID());
        } else {
            ObjectUpdater objUpdater = new ObjectUpdater(m_tableDef);
            if (targObjShardNos.size() > 0) {
                objUpdater.setTargetObjectShardNumbers(targObjShardNos);
            }
            objResult = objUpdater.updateObject(m_parentTran, dbObj, currScalarMap);
        }
        return objResult;
    }   // updateObject
    
    // Add the given object if its current-value map is null, otherwise update the object.
    private ObjectResult addOrUpdateObject(DBObject                          dbObj,
                                           Map<String, String>               currScalarMap,
                                           Map<String, Map<String, Integer>> targObjShardNos)
            throws IOException {
        ObjectUpdater objUpdater = new ObjectUpdater(m_tableDef);
        if (targObjShardNos.size() > 0) {
            objUpdater.setTargetObjectShardNumbers(targObjShardNos);
        }
        if (currScalarMap == null) {
            return objUpdater.addNewObject(m_parentTran, dbObj);
        } else {
            return objUpdater.updateObject(m_parentTran, dbObj, currScalarMap);
        }
    }   // addOrUpdateObject
    
    // Add error fields to the given BatchResult due to the given exception.
    private void buildErrorStatus(BatchResult result, Throwable ex) {
        result.setStatus(BatchResult.Status.ERROR);
        result.setErrorMessage(ex.getLocalizedMessage());
        if (ex instanceof IllegalArgumentException) {
            m_logger.debug("Batch update error: {}", ex.toString());
        } else {
            result.setStackTrace(Utils.getStackTrace(ex));
            m_logger.debug("Batch update error: {} stacktrace: {}",
                           ex.toString(), Utils.getStackTrace(ex));
        }
    }   // buildErrorStatus
    
    // Commit all mutations if we've exceeded the threshold.
    private void checkCommit() throws IOException {
        if (m_parentTran.getUpdateCount() >= ServerConfig.getInstance().batch_mutation_threshold) {
            commitTransaction();
        }
    }   // checkCommit
    
    // Fetch current scalar fields for all objects that have assigned object IDs.
    // Watch and complain about updates to the same ID.
    private Map<String, Map<String, String>> getCurrentScalars(DBObjectBatch dbObjBatch) throws IOException {
        Set<String> objIDSet = new HashSet<>();
        Set<String> fieldNameSet = new HashSet<String>();
        for (DBObject dbObj : dbObjBatch.getObjects()) {
            if (!Utils.isEmpty(dbObj.getObjectID())) {
                Utils.require(objIDSet.add(dbObj.getObjectID()),
                              "Cannot update the same object ID twice: " + dbObj.getObjectID());
                fieldNameSet.addAll(dbObj.getUpdatedScalarFieldNames(m_tableDef));
            }
        }
        if (m_tableDef.isSharded()) {
            fieldNameSet.add(m_tableDef.getShardingField().getName());
        }
        return SpiderService.instance().getObjectScalars(m_tableDef, objIDSet, fieldNameSet);
    }   // getCurrentScalars

    // Get the shard number of sharded link target objects in the given batch. The map is
    // <table name> -> <object ID> -> <shard number>. The map will be blank for target
    // objects that have no sharding-field value.
    private Map<String, Map<String, Integer>> getLinkTargetShardNumbers(DBObjectBatch dbObjBatch) {
        Map<String, Map<String, Integer>> result = new HashMap<>();
        Map<String, Set<String>> tableTargetObjIDMap = getAllLinkTargetObjIDs(dbObjBatch);
        for (String targetTableName : tableTargetObjIDMap.keySet()) {
            Map<String, Integer> shardNoMap =
                getShardNumbers(targetTableName, tableTargetObjIDMap.get(targetTableName));
            if (shardNoMap.size() > 0) {
                Map<String, Integer> tableShardNoMap = result.get(targetTableName);
                if (tableShardNoMap == null) {
                    tableShardNoMap = new HashMap<>();
                    result.put(targetTableName, tableShardNoMap);
                }
                tableShardNoMap.putAll(shardNoMap);
            }
        }
        return result;
    }   // getLinkTargetShardNumbers

    // Get a Map of table names to object IDs of all link values that reference a sharded
    // table in the given batch.
    private Map<String, Set<String>> getAllLinkTargetObjIDs(DBObjectBatch dbObjBatch) {
        Map<String, Set<String>> resultMap = new HashMap<>();
        for (FieldDefinition fieldDef : m_tableDef.getFieldDefinitions()) {
            if (!fieldDef.isLinkField() || !fieldDef.getInverseTableDef().isSharded()) {
                continue;
            }
            Set<String> targObjIDs = getLinkTargetObjIDs(fieldDef, dbObjBatch);
            if (targObjIDs.size() == 0) {
                continue;   // no assignments to this sharded link in the batch
            }
            
            String targetTableName = fieldDef.getInverseTableDef().getTableName();
            Set<String> tableObjIDs = resultMap.get(targetTableName);
            if (tableObjIDs == null) {
                tableObjIDs = new HashSet<>();
                resultMap.put(targetTableName, tableObjIDs);
            }
            tableObjIDs.addAll(targObjIDs);
        }
        return resultMap;
    }   // getAllLinkTargetObjIDs
    
    // Get the set of target object IDs referenced via the given link in the given batch.
    // Target object IDs can be in the add or remove set.
    private Set<String> getLinkTargetObjIDs(FieldDefinition linkDef, DBObjectBatch dbObjBatch) {
        Set<String> targObjIDs = new HashSet<>();
        for (DBObject dbObj : dbObjBatch.getObjects()) {
            List<String> objIDs = dbObj.getFieldValues(linkDef.getName());
            if (objIDs != null) {
                targObjIDs.addAll(objIDs);
            }
            Set<String> removeIDs = dbObj.getRemoveValues(linkDef.getName());
            if (removeIDs != null) {
                targObjIDs.addAll(removeIDs);
            }
        }
        return targObjIDs;
    }   // getLinkTargetObjIDs
    
    // Get the shard numbers of the given objects belonging to the given table. If an object
    // has no value for its sharding-field, leave the map empty for that object ID.
    private Map<String, Integer> getShardNumbers(String tableName, Set<String> targObjIDs) {
        TableDefinition tableDef = m_tableDef.getAppDef().getTableDef(tableName);
        FieldDefinition shardField = tableDef.getShardingField();
        Map<String, String> shardFieldMap =
           SpiderService.instance().getObjectScalar(tableDef, targObjIDs, shardField.getName());
        Map<String, Integer> shardNoMap = new HashMap<>();
        for (String objID : shardFieldMap.keySet()) {
            Date shardingFieldDate = Utils.dateFromString(shardFieldMap.get(objID));
            int shardNo = tableDef.computeShardNumber(shardingFieldDate);
            shardNoMap.put(objID, shardNo);
        }
        return shardNoMap;
    }   // getShardNumbers

}   // class BatchObjectUpdater
