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

import com.dell.doradus.common.DBObject;
import com.dell.doradus.common.Utils;

/**
 * Manages updates for an object's _ID field. The _ID field is stored in the object's
 * primary object table row. It is also stored in the "all objects" row of the Terms
 * table.
 */
public class IDFieldUpdater extends ScalarFieldUpdater {

    protected IDFieldUpdater(ObjectUpdater objUpdater, DBObject dbObj) {
        super(objUpdater, dbObj, "_ID");
    }   // constructor

    // Overrides ScalarFieldUpdater.addValuesForField(); uses "_ID" for the column name
    // and the encoded object ID as the value.
    @Override
    public void addValuesForField() {
        String objID = m_dbObj.getObjectID();
        assert !Utils.isEmpty(objID);
        m_dbTran.addIDValueColumn(m_tableDef, objID);
        m_dbTran.addAllObjectsColumn(m_tableDef, objID, m_tableDef.getShardNumber(m_dbObj));
    }   // addMutationsForField

    // Object is being deleted. Just delete column in "all objects" row. 
    @Override
    public void deleteValuesForField() {
        int shardNo = m_tableDef.getShardNumber(m_dbObj);
        m_dbTran.deleteAllObjectsColumn(m_tableDef, m_dbObj.getObjectID(), shardNo);
    }   // deleteValuesForField

    // Object is being updated, but _ID field cannot be changed.
    @Override
    public boolean updateValuesForField(String currentValue) {
        Utils.require(m_dbObj.getObjectID().equals(currentValue), "Object ID cannot be changed");
        return false;
    }   // updateValuesForField

}   // class IDFieldUpdater
