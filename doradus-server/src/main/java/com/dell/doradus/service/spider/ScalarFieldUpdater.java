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

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.dell.doradus.common.CommonDefs;
import com.dell.doradus.common.DBObject;
import com.dell.doradus.common.FieldDefinition;
import com.dell.doradus.common.Utils;
import com.dell.doradus.fieldanalyzer.FieldAnalyzer;

/**
 * Manages updates for an SV or MV scalar field. 
 */
public class ScalarFieldUpdater extends FieldUpdater {

    protected ScalarFieldUpdater(ObjectUpdater objUpdater, DBObject dbObj, String fieldName) {
        super(objUpdater, dbObj, fieldName);
    }   // constructor

    // Add scalar value to object record; add term columns for indexed tokens. 
    @Override
    public void addValuesForField() {
        FieldDefinition fieldDef = m_tableDef.getFieldDef(m_fieldName);
        if (fieldDef == null || !fieldDef.isCollection()) {
            addSVScalar();
        } else {
            addMVScalar();
        }
    }   // addMutationsForField

    @Override
    public boolean updateValuesForField(String currentValue) {
        boolean bUpdated = false;
        FieldDefinition fieldDef = m_tableDef.getFieldDef(m_fieldName);
        if (fieldDef == null || !fieldDef.isCollection()) {
            bUpdated = updateSVScalar(currentValue);
        } else {
            bUpdated = updateMVScalar(currentValue);
        }
        return bUpdated;
    }   // updateValuesForField
    
    // Delete term columns for indexed tokens for this scalar's fields. 
    @Override
    public void deleteValuesForField() {
        deleteTermColumns(m_dbObj.getFieldValue(m_fieldName));
    }   // deleteValuesForField
    
    // Merge the given current, remove, and new MV field values into a new set.
    public static Set<String> mergeMVFieldValues(Collection<String>  currValueSet,
                                                 Collection<String>  removeValueSet,
                                                 Collection<String>  newValueSet) {
        Set<String> resultSet = new HashSet<>();
        if (currValueSet != null) {
            resultSet.addAll(currValueSet);
        }
        if (removeValueSet != null) {
            resultSet.removeAll(removeValueSet);
        }
        if (newValueSet != null) {
            resultSet.addAll(newValueSet);
        }
        return resultSet;
    }   // mergeMVFieldValues

    ///// Private methods
    
    // Add a reference to this field's name.
    private void addFieldReference() {
        m_dbTran.addFieldReferences(m_tableDef, Arrays.asList(new String[]{m_fieldName}));
    }   // addFieldReference
    
    // Add references to the given terms for used for this field.
    private void addFieldTermReferences(Set<String> termSet) {
        Map<String, Set<String>> fieldTermRefsMap = new HashMap<String, Set<String>>();
        fieldTermRefsMap.put(m_fieldName, termSet);
        m_dbTran.addTermReferences(m_tableDef, m_tableDef.getShardNumber(m_dbObj), fieldTermRefsMap);
    }   // addFieldTermReferences
    
    // Add new MV scalar field.
    private void addMVScalar() {
        Set<String> values = new HashSet<>(m_dbObj.getFieldValues(m_fieldName));
        String fieldValue = Utils.concatenate(values, CommonDefs.MV_SCALAR_SEP_CHAR);
        m_dbTran.addScalarValueColumn(m_tableDef, m_dbObj.getObjectID(), m_fieldName, fieldValue);
        addTermColumns(fieldValue);
    }   // addMVScalar 

    // Add new SV scalar field.
    private void addSVScalar() {
        String fieldValue = m_dbObj.getFieldValue(m_fieldName);
        m_dbTran.addScalarValueColumn(m_tableDef, m_dbObj.getObjectID(), m_fieldName, fieldValue);
        addTermColumns(fieldValue);
    }   // addSVScalar
    
    // Add all Terms columns needed for our scalar field.
    private void addTermColumns(String fieldValue) {
        Set<String> termSet = tokenize(fieldValue);
        indexTerms(termSet);
        addFieldTermReferences(termSet);
        addFieldReference();
    }   // addTermColumns

    // Tokenize the given field with the appropriate analyzer and add Terms columns for each term.
    private void indexTerms(Set<String> termSet) {
        for (String term : termSet) {
            m_dbTran.addTermIndexColumn(m_tableDef, m_dbObj, m_fieldName, term);
        }
    }   // indexTerms
    
    private void deleteTermColumns(String fieldValue) {
        unindexTerms(fieldValue);
        // Nothing to do for field or term references
    }   // deleteTermColumns

    // Delete all Terms columns for terms belonging to the given field value.
    private void unindexTerms(String fieldValue) {
        for (String term : tokenize(fieldValue)) {
            unindexTerm(term);
        }
    }   // unindexTerms

    // Delete the Terms column that indexes the given term.
    private void unindexTerm(String term) {
        m_dbTran.deleteTermIndexColumn(m_tableDef, m_dbObj, m_fieldName, term);
    }   // unindexTerm

    // Tokenize the given field value with the appropriate analyzer.
    private Set<String> tokenize(String fieldValue) {
        FieldAnalyzer analyzer = FieldAnalyzer.findAnalyzer(m_tableDef, m_fieldName);
        return analyzer.extractTerms(fieldValue);
    }   // tokenize
    
    // Replace our SV scalar's value.
    private boolean updateSVScalar(String currentValue) {
        String newValue = m_dbObj.getFieldValue(m_fieldName);
        boolean bUpdated = false;
        if (Utils.isEmpty(newValue)) {
            if (!Utils.isEmpty(currentValue)) {
                m_dbTran.deleteScalarValueColumn(m_tableDef, m_dbObj.getObjectID(), m_fieldName);
                unindexTerms(currentValue);
                bUpdated = true;
            }
        } else if (!newValue.equals(currentValue)) {
            updateScalarReplaceValue(currentValue, newValue);
            bUpdated = true;
        }
        return bUpdated;
    }   // updateSVScalar

    // Process add- and remove-values for our MV scalar. The current value is in the form:
    //    <value>~<value~...~<value>
    // where ~ is the MV value separator.
    private boolean updateMVScalar(String currentValue) {
        boolean bUpdated = false;
        Set<String> currentValues = Utils.split(currentValue, CommonDefs.MV_SCALAR_SEP_CHAR);
        Set<String> newValueSet = mergeMVFieldValues(currentValues,
                                                     m_dbObj.getRemoveValues(m_fieldName),
                                                     m_dbObj.getFieldValues(m_fieldName));
        String newValue = Utils.concatenate(newValueSet, CommonDefs.MV_SCALAR_SEP_CHAR);
        if (!newValue.equals(currentValue)) {
            if (newValue.length() == 0) {
                m_dbTran.deleteScalarValueColumn(m_tableDef, m_dbObj.getObjectID(), m_fieldName);
                unindexTerms(currentValue);
            } else {
                updateScalarReplaceValue(currentValue, newValue);
            }
            bUpdated = true;
        }
        return bUpdated;
    }   // updateMVScalar

    // Replace our scalar's value with the given one, adding and removing terms as needed.
    // This works for both SV and MV scalar fields
    private void updateScalarReplaceValue(String currentValue, String newValue) {
        m_dbTran.addScalarValueColumn(m_tableDef, m_dbObj.getObjectID(), m_fieldName, newValue);
        Set<String> currTermSet = tokenize(Utils.isEmpty(currentValue) ? "" : currentValue);
        Set<String> newTermSet = tokenize(newValue);
        for (String term : currTermSet) {
            if (!newTermSet.remove(term)) {
                unindexTerm(term);
            }
        }
        indexTerms(newTermSet);
        addFieldTermReferences(newTermSet);
        if (Utils.isEmpty(currentValue)) {
            addFieldReference();
        }
    }   // updateScalarReplaceValue
    
}   // class ScalarFieldUpdater
