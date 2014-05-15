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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.dell.doradus.common.DBObject;
import com.dell.doradus.common.FieldDefinition;
import com.dell.doradus.common.TableDefinition;

/**
 * Performs updates for a link field. Handles regular and "sharded" links.
 */
public class LinkFieldUpdater extends FieldUpdater {
    // Extra metadata we need for a link field:
    private final FieldDefinition m_linkDef;
    private final TableDefinition m_invTableDef;
    private final FieldDefinition m_invLinkDef;
    
    // Map of target object IDs to shard numbers for this link's extent table:
    private final Map<String, Integer> m_targetObjShardNos;

    // Save metadata for link and its inverse.
    protected LinkFieldUpdater(SpiderTransaction dbTran, ObjectUpdater objUpdater, DBObject dbObj, String fieldName) {
        super(dbTran, objUpdater, dbObj, fieldName);
        m_linkDef = m_tableDef.getFieldDef(m_fieldName);
        m_invTableDef = m_linkDef.getInverseTableDef();
        m_invLinkDef = m_linkDef.getInverseLinkDef();
        if (m_invTableDef.isSharded()) {
            Map<String, Map<String, Integer>> tableShardNoMap = objUpdater.getTargetObjectShardNos();
            if (tableShardNoMap != null) {
                m_targetObjShardNos = tableShardNoMap.get(m_invTableDef.getTableName());
            } else {
                m_targetObjShardNos = null;
            }
        } else {
            m_targetObjShardNos = null;
        }
    }   // constructor

    @Override
    public void addValuesForField() {
        updateLinkAddValues();
    }   // addValuesForField

    @Override
    public boolean updateValuesForField(String currentValue) {
        boolean bUpdated = false;
        bUpdated |= updateLinkRemoveValues();
        bUpdated |= updateLinkAddValues();
        return bUpdated;
    }   // updateValuesForField
    
    // Clean-up inverses and sharded term rows.
    @Override
    public void deleteValuesForField() {
        for (String targetObjID : m_dbObj.getFieldValues(m_fieldName)) {
            deleteLinkInverseValue(targetObjID);
        }
        if (m_linkDef.isSharded()) {
            deleteShardedLinkTermRows();
        }
    }   // deleteValuesForField

    ///// Private methods
    
    // For a reference through this link to the given target object ID, add the mutations
    // for the inverse reference.
    private void addInverseLinkValue(String targetObjID) {
        int shardNo = m_tableDef.getShardNumber(m_dbObj);
        if (shardNo > 0 && m_invLinkDef.isSharded()) {
            m_dbTran.addShardedLinkValue(targetObjID, m_invLinkDef, m_dbObj.getObjectID(), shardNo);
        } else {
            m_dbTran.addLinkValue(targetObjID, m_invLinkDef, m_dbObj.getObjectID());
        }
        
        // Add the "_ID" field to the inverse object's primary record.
        m_dbTran.addIDValueColumn(m_invTableDef, targetObjID);
        
        // Add the target object's ID to its all-objects row.
        int targetShardNo = getTargetObjectShardNumber(targetObjID);
        m_dbTran.addAllObjectsColumn(m_invTableDef, targetObjID, targetShardNo);
    }   // addInverseLinkValue
    
    // Add mutations to add a reference via our link to the given target object ID.
    private void addLinkValue(String targetObjID) {
        int targetShardNo = getTargetObjectShardNumber(targetObjID);
        if (targetShardNo > 0 && m_linkDef.isSharded()) {
            m_dbTran.addShardedLinkValue(m_dbObj.getObjectID(), m_linkDef, targetObjID, targetShardNo);
        } else {
            m_dbTran.addLinkValue(m_dbObj.getObjectID(), m_linkDef, targetObjID);
        }
        addInverseLinkValue(targetObjID);
    }   // addLinkValue
    
    // Get the shard number of the target object ID, if cached.
    private int getTargetObjectShardNumber(String targetObjID) {
        int targetShardNo = 0;
        if (m_invTableDef.isSharded() &&
            m_targetObjShardNos != null &&
            m_targetObjShardNos.containsKey(targetObjID)) {
            targetShardNo = m_targetObjShardNos.get(targetObjID);
        }
        return targetShardNo;
    }   // getTargetObjectShardNumber    

    // Delete the inverse reference to the given target object ID.
    private void deleteLinkInverseValue(String targetObjID) {
        int shardNo = m_tableDef.getShardNumber(m_dbObj);
        if (shardNo > 0 && m_invLinkDef.isSharded()) {
            m_dbTran.deleteShardedLinkValue(targetObjID, m_invLinkDef, m_dbObj.getObjectID(), shardNo);
        } else {
            m_dbTran.deleteLinkValue(targetObjID, m_invLinkDef, m_dbObj.getObjectID());
        }
    }   // deleteLinkInverseValue

    // Delete the reference via our link to the given target object ID and its inverse.
    private void deleteLinkValue(String targetObjID) {
        int targetShardNo = getTargetObjectShardNumber(targetObjID);
        if (targetShardNo > 0 && m_linkDef.isSharded()) {
            m_dbTran.deleteShardedLinkValue(targetObjID, m_linkDef, m_dbObj.getObjectID(), targetShardNo);
        } else {
            m_dbTran.deleteLinkValue(m_dbObj.getObjectID(), m_linkDef, targetObjID);
        }
        deleteLinkInverseValue(targetObjID);
    }   // deleteLinkValue
    
    // Delete all possible term rows that might exist for our sharded link.
    private void deleteShardedLinkTermRows() {
        // When link is shard, its sharded extent table is also sharded.
        Set<Integer> shardNums = SpiderService.instance().getShards(m_invTableDef).keySet();
        for (Integer shardNumber : shardNums) {
            m_dbTran.deleteShardedLinkRow(m_linkDef, m_dbObj.getObjectID(), shardNumber);
        }
    }   // deleteShardedLinkTermRows

    // Add new values for our link. Return true if at least one update made.
    private boolean updateLinkAddValues() {
        List<String> linkValues = m_dbObj.getFieldValues(m_linkDef.getName());
        if (linkValues == null || linkValues.size() == 0) {
            return false;
        }
        Set<String> linkValueSet = new HashSet<>(linkValues);   // remove duplicates
        for (String targetObjID : linkValueSet) {
            addLinkValue(targetObjID);
        }
        return true;
    }   // updateLinkAddValues

    // Process removes for our link. Return true if at least one update made.
    private boolean updateLinkRemoveValues() {
        Set<String> removeSet = m_dbObj.getRemoveValues(m_fieldName);
        List<String> addSet = m_dbObj.getFieldValues(m_fieldName);
        if (removeSet != null && addSet != null) {
            removeSet.removeAll(addSet);    // add+remove of same ID negates the remove
        }
        if (removeSet == null || removeSet.size() == 0) {
            return false;
        }
        
        for (String removeObjID : removeSet) {
            deleteLinkValue(removeObjID);
        }
        return true;
    }   // updateLinkRemoveValues

}   // class LinkFieldUpdater
