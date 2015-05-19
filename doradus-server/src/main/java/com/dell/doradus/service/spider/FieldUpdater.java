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

import com.dell.doradus.common.CommonDefs;
import com.dell.doradus.common.DBObject;
import com.dell.doradus.common.FieldDefinition;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.common.Utils;

/**
 * Base class for an object that handles all updates for a single field on behalf of a
 * specific {@link DBObject}. Provides methods to:
 * <ul>
 * <li>Add new values for the field when the object is being added,</li>
 * <li>Perform adds and/or removes for the field when the object already exists and is
 *     being updated, and</li>
 * <li>Delete all values when the object is being deleted.</li>
 * </ul>
 * Concrete objects are created by calling the manufacturing method: 
 * {@link FieldUpdater#createFieldUpdater(ObjectUpdater, DBObject, String)}.
 */
public abstract class FieldUpdater {
    protected final SpiderTransaction   m_dbTran;
    protected final DBObject            m_dbObj;
    protected final String              m_fieldName;
    protected final TableDefinition     m_tableDef;

    /**
     * Create a FieldUpdater that will handle updates for the given object and field.
     * 
     * @param tableDef      {@link TableDefinition} of table in which object resides.
     * @param dbObj         {@link DBObject} of object being added, updated, or deleted.
     * @param fieldName     Name of field the new FieldUpdater will handle. Can be the
     *                      _ID field, a declared or undeclared scalar field, or a link
     *                      field.
     * @return              A FieldUpdater that can perform updates for the given object
     *                      and field.
     */
    public static FieldUpdater createFieldUpdater(ObjectUpdater objUpdater, DBObject dbObj, String fieldName) {
        if (fieldName.charAt(0) == '_') {
            if (fieldName.equals(CommonDefs.ID_FIELD)) {
                return new IDFieldUpdater(objUpdater, dbObj);
            } else {
                // Allow but skip all other system fields (e.g., "_table")
                return new NullFieldUpdater(objUpdater, dbObj, fieldName);
            }
        }
        TableDefinition tableDef = objUpdater.getTableDef();
        if (tableDef.isLinkField(fieldName)) {
            return new LinkFieldUpdater(objUpdater, dbObj, fieldName);
        } else {
            Utils.require(FieldDefinition.isValidFieldName(fieldName), "Invalid field name: %s", fieldName);
            return new ScalarFieldUpdater(objUpdater, dbObj, fieldName);
        }
    }   // createFieldUpdater
    
    /**
     * Create a FieldUpdater with the given parameters. This constructor is called by
     * concrete object constructors.
     * 
     * @param objUpdater    {@link ObjectUpdater} for which we are updatng a field.
     * @param dbObj         DBObject of object being added, updated, or deleted.
     * @param fieldName     Name of field handled by this FieldUpdater.
     */
    protected FieldUpdater(ObjectUpdater objUpdater, DBObject dbObj, String fieldName) {
        m_dbTran = objUpdater.getTransaction();
        m_tableDef = objUpdater.getTableDef();
        m_dbObj = dbObj;
        m_fieldName = fieldName;
    }   // constructor
    
    /**
     * Add field values for a new object being added. Because the object is being added,
     * we only add new columns and don't process any "field remove" actions.
     */
    public abstract void addValuesForField();

    /**
     * Update field values for an object being updated. An SV field uses "replacement"
     * semantics, whereas MV fields process separate "add" and "remove" actions. For
     * scalar fields, the existing object's current field value, if any, is passed. 
     * 
     * @param  currentValue - Current object's value if this field is a scalar and has a
     *                        current value.
     * @return True if an update was actually made. False means no updates were made.
     */
    public abstract boolean updateValuesForField(String currentValue);
    
    /**
     * Cleanup field values for an object being deleted. Delete assumes that the owning
     * object record's row is being deleted separately, so there's no need to generate
     * "delete column" mutations for columns residing in that row.
     */
    public abstract void deleteValuesForField();
    
}   // class FieldUpdater
