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
import java.util.Iterator;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.PreparedStatement;

/**
 * Creates and caches prepared query and update statements. The first time a {@link Query}
 * or {@link Update} is called for a given keyspace.table name, a prepared statement is
 * parsed and cached for that combination. Thereafter, the prepared statement is reused,
 * which is faster than ad-hoc CQL queries.
 */
public class CQLStatementCache {
    // Members:
    private final Logger m_logger = LoggerFactory.getLogger(getClass().getSimpleName());
    
    // Key is "<keyspace>.<table>"
    private final Map<String, Map<Query, PreparedStatement>> m_prepQueryMap = new HashMap<>();
    private final Map<String, Map<Update, PreparedStatement>> m_prepUpdateMap = new HashMap<>();
    
    /**
     * Create a new empty statement cache.
     */
    public CQLStatementCache() { }
    
    /**
     * Prepared query statement types.
     */
    public enum Query {
        SELECT_1_ROW_1_COLUMN,
        SELECT_1_ROW_COLUMN_RANGE,
        SELECT_1_ROW_COLUMN_RANGE_DESC,
        SELECT_1_ROW_ALL_COLUMNS,
        SELECT_ROW_SET_COLUMN_SET,
        SELECT_ROW_SET_COLUMN_RANGE,
        SELECT_ROW_SET_COLUMN_RANGE_DESC,
        SELECT_ROW_SET_ALL_COLUMNS,
        SELECT_ALL_ROWS_ALL_COLUMNS,
    }   // enum Query

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
    }   // enum Update
    
    /**
     * Get the given prepared statement for the given keyspace, table, and query. Upon
     * first invocation for a given combo, the query is parsed and cached.
     *
     * @param keyspace  Name of keyspace that owns table (should be quoted).
     * @param tableName Name of table to customize query for.
     * @param query     Inquiry {@link Query}.
     * @return          PreparedStatement for given combo.
     */
    public PreparedStatement getPreparedQuery(String keyspace, String tableName, Query query) {
        String mapKey = createMapKey(keyspace, tableName);
        synchronized (m_prepQueryMap) {
            Map<Query, PreparedStatement> statementMap = m_prepQueryMap.get(mapKey);
            if (statementMap == null) {
                statementMap = new HashMap<>();
                m_prepQueryMap.put(mapKey, statementMap);
            }
            PreparedStatement prepState = statementMap.get(query);
            if (prepState == null) {
                prepState = prepareQuery(keyspace, tableName, query);
                statementMap.put(query, prepState);
            }
            return prepState;
        }
    }   // getPreparedQuery

    /**
     * Get the given prepared statement for the given keyspace, table, and update. Upon
     * first invocation for a given combo, the query is parsed and cached.
     * 
     * @param keyspace  Name of keyspace that owns table (should be quoted).
     * @param tableName Name of table to customize update for.
     * @param update    Inquiry {@link Update}.
     * @return          PreparedStatement for given combo.
     */
    public PreparedStatement getPreparedUpdate(String keyspace, String tableName, Update update) {
        String mapKey = createMapKey(keyspace, tableName);
        synchronized (m_prepUpdateMap) {
            Map<Update, PreparedStatement> statementMap = m_prepUpdateMap.get(mapKey);
            if (statementMap == null) {
                statementMap = new HashMap<>();
                m_prepUpdateMap.put(mapKey, statementMap);
            }
            PreparedStatement prepState = statementMap.get(update);
            if (prepState == null) {
                prepState = prepareUpdate(keyspace, tableName, update);
                statementMap.put(update, prepState);
            }
            return prepState;
        }
    }   // getPreparedUpdate

    /**
     * Purge all cached statements for the given keyspace.
     * 
     * @param keyspace  Quoted CQL keyspace name.
     */
    public void purgeKeyspace(String keyspace) {
        assert keyspace.charAt(0) == '"';
        String prefix = keyspace + ".";
        synchronized (m_prepQueryMap) {
            Iterator<String> iter = m_prepQueryMap.keySet().iterator();
            while (iter.hasNext()) {
                if (iter.next().startsWith(prefix)) {
                    iter.remove();
                }
            }
        }
        
        synchronized (m_prepUpdateMap) {
            Iterator<String> iter = m_prepUpdateMap.keySet().iterator();
            while (iter.hasNext()) {
                if (iter.next().startsWith(prefix)) {
                    iter.remove();
                }
            }
        }
    }

    //----- Private methods
    
    // Create a prepared statement for the given query/table combo.
    private PreparedStatement prepareQuery(String keyspace, String tableName, Query query) {
        assert keyspace.charAt(0) == '"';
        // All queries start with SELECT * FROM <keyspace>.<table>
        StringBuilder cql = new StringBuilder("SELECT * FROM ");
        cql.append(createMapKey(keyspace, tableName));
        
        switch (query) {
        case SELECT_1_ROW_1_COLUMN:
            cql.append(" WHERE key= ? AND column1 = ?;");
            break;
        case SELECT_1_ROW_COLUMN_RANGE:
            cql.append(" WHERE key= ? AND column1 >= ? AND column1 <= ?;");
            break;
        case SELECT_1_ROW_COLUMN_RANGE_DESC:
            cql.append(" WHERE key= ? AND column1 >= ? AND column1 <= ? ORDER BY column1 DESC;");
            break;
        case SELECT_1_ROW_ALL_COLUMNS:
            cql.append(" WHERE key= ?;");
            break;
        case SELECT_ROW_SET_COLUMN_SET:
            cql.append(" WHERE key IN ? AND column1 IN ?;");
            break;
        case SELECT_ROW_SET_COLUMN_RANGE:
            cql.append(" WHERE key IN ? AND column1 >= ? AND column1 <= ?");
            break;
        case SELECT_ROW_SET_COLUMN_RANGE_DESC:
            cql.append(" WHERE key IN ? AND column1 >= ? AND column1 <= ? ORDER BY column1 DESC");
            break;
        case SELECT_ROW_SET_ALL_COLUMNS:
            cql.append(" WHERE key IN ?;");
            break;
        case SELECT_ALL_ROWS_ALL_COLUMNS:
            cql.append(" LIMIT ");
            cql.append(Integer.MAX_VALUE);
            cql.append(" ALLOW FILTERING;");
            break;
        }
        m_logger.debug("Preparing query statement: {}", cql);
        return CQLService.instance().getSession().prepare(cql.toString());
    }   // prepareQuery

    // Create a prepared statement for the given query/table combo.
    private PreparedStatement prepareUpdate(String keyspace, String tableName, Update update) {
        StringBuilder cql = new StringBuilder();
        switch (update) {
        case INSERT_ROW:
            // INSERT INTO <keyspace>.<table> (key,column1,value) VALUES (<key>, <colname>, <colvalue>);
            cql.append("INSERT INTO ");
            cql.append(createMapKey(keyspace, tableName));
            cql.append(" (key,column1,value) VALUES (?, ?, ?);");
            break;
        case INSERT_ROW_TS:
            // INSERT INTO <keyspace>.<table> (key,column1,value) VALUES (<key>, <colname>, <colvalue>) USING TIMESTAMP <timestamp>;
            // Note timestamp is the last parameter
            cql.append("INSERT INTO ");
            cql.append(createMapKey(keyspace, tableName));
            cql.append(" (key,column1,value) VALUES (?, ?, ?) USING TIMESTAMP ?;");
            break;
        case DELETE_COLUMN:
            // DELETE FROM <keyspace>.<table> WHERE key=<key> AND column1=<column name>;
            cql.append("DELETE FROM ");
            cql.append(createMapKey(keyspace, tableName));
            cql.append(" WHERE key=? AND column1=?;");
            break;
        case DELETE_COLUMN_TS:
            // DELETE FROM <keyspace>.<table> USING TIMESTAMP <timestamp> WHERE key=<key> AND column1=<column name>;
            // Note timestamp is the first parameter
            cql.append("DELETE FROM ");
            cql.append(createMapKey(keyspace, tableName));
            cql.append(" USING TIMESTAMP ? WHERE key=? AND column1=?;");
            break;
        case DELETE_ROW:
            // DELETE FROM <keyspace>.<table> WHERE key='key';
            cql.append("DELETE FROM ");
            cql.append(createMapKey(keyspace, tableName));
            cql.append(" WHERE key=?;");
            break;
        case DELETE_ROW_TS:
            // DELETE FROM <keyspace>.<table> USING TIMESTAMP <timestamp> WHERE key='key';
            // Note timestamp is the first parameter
            cql.append("DELETE FROM ");
            cql.append(createMapKey(keyspace, tableName));
            cql.append(" USING TIMESTAMP ? WHERE key=?;");
            break;
        }
        m_logger.debug("Preparing update statement: {}", cql);
        return CQLService.instance().getSession().prepare(cql.toString());
    }   // prepareUpdate

    private String createMapKey(String keyspace, String tableName) {
        assert keyspace.charAt(0) == '"';
        return keyspace + "." + tableName;
    }

}   // class CQLStatementCache
