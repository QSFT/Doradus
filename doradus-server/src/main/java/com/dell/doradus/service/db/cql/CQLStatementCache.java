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

package com.dell.doradus.service.db.cql;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.PreparedStatement;
import com.dell.doradus.common.Utils;
import com.dell.doradus.service.db.DBService;
import com.dell.doradus.service.db.Tenant;

/**
 * Creates and caches prepared query and update statements. The first time a {@link Query}
 * or {@link Update} is called for a given keyspace.table name, a prepared statement is
 * parsed and cached for that combination. Thereafter, the prepared statement is reused,
 * which is faster than ad-hoc CQL queries.
 */
public class CQLStatementCache {
    // Members:
    private final Logger m_logger = LoggerFactory.getLogger(getClass().getSimpleName());
    private final Tenant m_tenant;
    private final String m_keyspace;
    
    // Key is table name.
    private final Map<String, Map<Query, PreparedStatement>> m_prepQueryMap = new HashMap<>();
    private final Map<String, Map<Update, PreparedStatement>> m_prepUpdateMap = new HashMap<>();
    
    /**
     * Create a new empty statement cache.
     */
    public CQLStatementCache(Tenant tenant) {
        m_tenant = tenant;
        if (Utils.isEmpty(m_tenant.getNamespace())) {
            m_keyspace = CQLService.storeToCQLName(m_tenant.getName());
        } else {
            m_keyspace = CQLService.storeToCQLName(m_tenant.getNamespace());
        }
    }
    
    /**
     * Prepared query statement types.
     */
    public enum Query {
        SELECT_1_ROW_1_COLUMN,
        SELECT_1_ROW_COLUMN_RANGE,
        SELECT_1_ROW_COLUMN_SET,
        SELECT_ROWS_RANGE,
    }

    /**
     * Prepared update statement types.
     */
    public enum Update {
        INSERT_ROW,
        INSERT_ROW_TS,
        DELETE_COLUMN,
        DELETE_COLUMN_TS,
        DELETE_ROW,
        DELETE_ROW_TS,
    }
    
    /**
     * Get the given prepared statement for the given table and query. Upon
     * first invocation for a given combo, the query is parsed and cached.
     *
     * @param tableName Name of table to customize query for.
     * @param query     Inquiry {@link Query}.
     * @return          PreparedStatement for given combo.
     */
    public PreparedStatement getPreparedQuery(String tableName, Query query) {
        synchronized (m_prepQueryMap) {
            Map<Query, PreparedStatement> statementMap = m_prepQueryMap.get(tableName);
            if (statementMap == null) {
                statementMap = new HashMap<>();
                m_prepQueryMap.put(tableName, statementMap);
            }
            PreparedStatement prepState = statementMap.get(query);
            if (prepState == null) {
                prepState = prepareQuery(tableName, query);
                statementMap.put(query, prepState);
            }
            return prepState;
        }
    }   // getPreparedQuery

    /**
     * Get the given prepared statement for the given table and update. Upon
     * first invocation for a given combo, the query is parsed and cached.
     * 
     * @param tableName Name of table to customize update for.
     * @param update    Inquiry {@link Update}.
     * @return          PreparedStatement for given combo.
     */
    public PreparedStatement getPreparedUpdate(String tableName, Update update) {
        synchronized (m_prepUpdateMap) {
            Map<Update, PreparedStatement> statementMap = m_prepUpdateMap.get(tableName);
            if (statementMap == null) {
                statementMap = new HashMap<>();
                m_prepUpdateMap.put(tableName, statementMap);
            }
            PreparedStatement prepState = statementMap.get(update);
            if (prepState == null) {
                prepState = prepareUpdate(tableName, update);
                statementMap.put(update, prepState);
            }
            return prepState;
        }
    }   // getPreparedUpdate

    /**
     * Purge all cached statements.
     */
    public void clear() {
        synchronized (m_prepQueryMap) {
            m_prepQueryMap.clear();
        }
        
        synchronized (m_prepUpdateMap) {
            m_prepUpdateMap.clear();
        }
    }

    //----- Private methods
    
    // Create a prepared statement for the given query/table combo.
    private PreparedStatement prepareQuery(String tableName, Query query) {
        // All queries start with SELECT * FROM <keyspace>.<table>
        StringBuilder cql = new StringBuilder("SELECT * FROM ");
        cql.append(m_keyspace);
        cql.append(".");
        cql.append(tableName);
        
        switch (query) {
        case SELECT_1_ROW_1_COLUMN:
            cql.append(" WHERE key = ? AND column1 = ?;");
            break;
        case SELECT_1_ROW_COLUMN_RANGE:
            cql.append(" WHERE key = ? AND column1 >= ? AND column1 < ? LIMIT ?;");
            break;
        case SELECT_1_ROW_COLUMN_SET:
            cql.append(" WHERE key = ? AND column1 IN ?;");
            break;
        case SELECT_ROWS_RANGE:
            //unfortunately I didn't find how to get first column of each row in CQL.
            cql.append(" ;");
            break;
        default: 
            throw new RuntimeException("Not supported: " + query);
        }
        m_logger.debug("Preparing query statement: {}", cql);
        return ((CQLService)DBService.instance(m_tenant)).getSession().prepare(cql.toString());
    }   // prepareQuery

    // Create a prepared statement for the given query/table combo.
    private PreparedStatement prepareUpdate(String tableName, Update update) {
        StringBuilder cql = new StringBuilder();
        switch (update) {
        case INSERT_ROW:
            // INSERT INTO <keyspace>.<table> (key,column1,value) VALUES (<key>, <colname>, <colvalue>);
            cql.append("INSERT INTO ");
            cql.append(m_keyspace);
            cql.append(".");
            cql.append(tableName);
            cql.append(" (key,column1,value) VALUES (?, ?, ?);");
            break;
        case INSERT_ROW_TS:
            // INSERT INTO <keyspace>.<table> (key,column1,value) VALUES (<key>, <colname>, <colvalue>) USING TIMESTAMP <timestamp>;
            // Note timestamp is the last parameter
            cql.append("INSERT INTO ");
            cql.append(m_keyspace);
            cql.append(".");
            cql.append(tableName);
            cql.append(" (key,column1,value) VALUES (?, ?, ?) USING TIMESTAMP ?;");
            break;
        case DELETE_COLUMN:
            // DELETE FROM <keyspace>.<table> WHERE key=<key> AND column1=<column name>;
            cql.append("DELETE FROM ");
            cql.append(m_keyspace);
            cql.append(".");
            cql.append(tableName);
            cql.append(" WHERE key=? AND column1=?;");
            break;
        case DELETE_COLUMN_TS:
            // DELETE FROM <keyspace>.<table> USING TIMESTAMP <timestamp> WHERE key=<key> AND column1=<column name>;
            // Note timestamp is the first parameter
            cql.append("DELETE FROM ");
            cql.append(m_keyspace);
            cql.append(".");
            cql.append(tableName);
            cql.append(" USING TIMESTAMP ? WHERE key=? AND column1=?;");
            break;
        case DELETE_ROW:
            // DELETE FROM <keyspace>.<table> WHERE key='key';
            cql.append("DELETE FROM ");
            cql.append(m_keyspace);
            cql.append(".");
            cql.append(tableName);
            cql.append(" WHERE key=?;");
            break;
        case DELETE_ROW_TS:
            // DELETE FROM <keyspace>.<table> USING TIMESTAMP <timestamp> WHERE key='key';
            // Note timestamp is the first parameter
            cql.append("DELETE FROM ");
            cql.append(m_keyspace);
            cql.append(".");
            cql.append(tableName);
            cql.append(" USING TIMESTAMP ? WHERE key=?;");
            break;
        }
        m_logger.debug("Preparing update statement: {}", cql);
        return ((CQLService)DBService.instance(m_tenant)).getSession().prepare(cql.toString());
    }   // prepareUpdate

}   // class CQLStatementCache
