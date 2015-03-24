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

import java.util.GregorianCalendar;
import java.util.HashSet;
import java.util.Set;

import com.dell.doradus.common.BatchResult;
import com.dell.doradus.common.CommonDefs;
import com.dell.doradus.common.FieldDefinition;
import com.dell.doradus.common.RetentionAge;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.common.Utils;
import com.dell.doradus.search.SearchResult;
import com.dell.doradus.search.SearchResultList;
import com.dell.doradus.service.taskmanager.TaskExecutor;

/**
 * Perform data-aging task execution for a Spider application table. 
 */
public class SpiderDataAger extends TaskExecutor {
    private static final int QUERY_PAGE_SIZE = 1000;
    
    private TableDefinition m_tableDef;
    private FieldDefinition m_agingFieldDef;
    private RetentionAge    m_retentionAge;

    @Override
    public void execute() {
        setTableParams();
        checkTable();
    }   // execute

    private void setTableParams() {
        String taskID = m_taskRecord.getTaskID();
        String[] idParts = taskID.split("/");
        String tableName = idParts[1];
        m_tableDef = m_appDef.getTableDef(tableName);
        
        String fieldName = m_tableDef.getOption(CommonDefs.OPT_AGING_FIELD);
        m_agingFieldDef = m_tableDef.getFieldDef(fieldName);
        
        m_retentionAge = new RetentionAge(m_tableDef.getOption(CommonDefs.OPT_RETENTION_AGE));
    }   // setTableParams
    
    // Scan the given table for expired objects relative to the given date.
    private void checkTable() {
        m_logger.info("Checking expired objects for: {}", m_tableDef.getPath());
        GregorianCalendar checkDate = new GregorianCalendar(Utils.UTC_TIMEZONE);
        GregorianCalendar expireDate = m_retentionAge.getExpiredDate(checkDate);
        int objsExpired = 0;

        String fixedQuery = buildFixedQuery(expireDate);
        String contToken = null;
        StringBuilder uriParam = new StringBuilder();
        do {
            uriParam.setLength(0);
            uriParam.append(fixedQuery);
            if (!Utils.isEmpty(contToken)) {
                uriParam.append("&g=");
                uriParam.append(contToken);
            }
            SearchResultList resultList =
                SpiderService.instance().objectQueryURI(m_tableDef, uriParam.toString());
            Set<String> objIDSet = new HashSet<>();
            for (SearchResult result : resultList.results) {
                objIDSet.add(result.id());
            }
            if (deleteBatch(objIDSet)) {
                contToken = resultList.continuation_token;
            } else {
                contToken = null;
            }
            objsExpired += objIDSet.size();
        } while (!Utils.isEmpty(contToken));
        
        m_logger.info("Deleted {} objects for {}", objsExpired, m_tableDef.getPath());
    }   // checkTable
    
    // Build the fixed part of the query that fetches a batch of object IDs.
    private String buildFixedQuery(GregorianCalendar expireDate) {
        // Query: '{aging field} <= "{expire date}"', fetching the _ID and
        // aging field, up to a batch full at a time.
        StringBuilder fixedParams = new StringBuilder();
        fixedParams.append("q=");
        fixedParams.append(m_agingFieldDef.getName());
        fixedParams.append(" <= \"");
        fixedParams.append(Utils.formatDate(expireDate));
        fixedParams.append("\"");
        
        // Fields: _ID.
        fixedParams.append("&f=_ID");
        
        // Size: QUERY_PAGE_SIZE
        fixedParams.append("&s=");
        fixedParams.append(QUERY_PAGE_SIZE);
        return fixedParams.toString();
    }   // buildFixedQuery
    
    // Delete a batch of objects with the given object IDs. Return false if the
    // update failed or we didn't execute an update.
    private boolean deleteBatch(Set<String> objIDSet) {
        if (objIDSet.size() == 0) {
            return false;
        }
        m_logger.debug("Deleting batch of {} objects from {}", objIDSet.size(), m_tableDef.getPath());
        BatchObjectUpdater batchUpdater = new BatchObjectUpdater(m_tableDef);
        BatchResult batchResult = batchUpdater.deleteBatch(objIDSet);
        if (batchResult.isFailed()) {
            m_logger.error("Batch query failed: {}", batchResult.getErrorMessage());
            return false;
        }
        return true;
    }   // deleteBatch
    
}   // class SpiderDataAger
