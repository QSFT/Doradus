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
import com.datastax.driver.core.Session;

/**
 * Creates and caches prepared query and update statements. The first time a {@link Query}
 * or {@link Update} is called for a given table name, a prepared statement is parsed and
 * cached for that combination. Thereafter, the prepared statement is reused, which is
 * faster than ad-hoc CQL queries.
 */
public class CQLStatementCache {
    // Members:
    private final Logger m_logger = LoggerFactory.getLogger(getClass().getSimpleName());
    private final Map<String, Map<Query, PreparedStatement>> m_prepQueryMap = new HashMap<>();
    private final Map<String, Map<Update, PreparedStatement>> m_prepUpdateMap = new HashMap<>();
    private final Session m_session;
    
    /**
     * Create an object that uses the given session to parse and cache statements.
     * 
     * @param session   Active CQL session connected to the appropriate keyspace.
     */
    public CQLStatementCache(Session session) {
        m_session = session;
    }
    
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
     * Get the given prepared statement for the given query/table combo. Upon first
     * invocation for a given query/table combo, the query is parsed and cached.
     * 
     * @param query     Inquiry {@link Query}.
     * @param tableName Name of table to customize query for.
     * @return          PreparedStatement for given query/table combo.
     */
    public PreparedStatement getPreparedQuery(Query query, String tableName) {
        synchronized (m_prepQueryMap) {
            Map<Query, PreparedStatement> tableMap = m_prepQueryMap.get(tableName);
            if (tableMap == null) {
                tableMap = new HashMap<>();
                m_prepQueryMap.put(tableName, tableMap);
            }
            PreparedStatement prepState = tableMap.get(query);
            if (prepState == null) {
                prepState = prepareQuery(query, tableName);
                tableMap.put(query, prepState);
            }
            return prepState;
        }
    }   // getPreparedQuery

    /**
     * Get the given prepared statement for the given update/table combo. Upon first
     * invocation for a given update/table combo, the query is parsed and cached.
     * 
     * @param update    Inquiry {@link Update}.
     * @param tableName Name of table to customize update for.
     * @return          PreparedStatement for given update/table combo.
     */
    public PreparedStatement getPreparedUpdate(Update update, String tableName) {
        synchronized (m_prepUpdateMap) {
            Map<Update, PreparedStatement> tableMap = m_prepUpdateMap.get(tableName);
            if (tableMap == null) {
                tableMap = new HashMap<>();
                m_prepUpdateMap.put(tableName, tableMap);
            }
            PreparedStatement prepState = tableMap.get(update);
            if (prepState == null) {
                prepState = prepareUpdate(update, tableName);
                tableMap.put(update, prepState);
            }
            return prepState;
        }
    }   // getPreparedUpdate

    //----- Private methods
    
    // Create a prepared statement for the given query/table combo.
    private PreparedStatement prepareQuery(Query query, String tableName) {
        // All queries start with SELECT * FROM <table>
        StringBuilder cql = new StringBuilder("SELECT * FROM ");
        cql.append(tableName);
        
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
        return m_session.prepare(cql.toString());
    }   // prepareQuery

    // Create a prepared statement for the given query/table combo.
    private PreparedStatement prepareUpdate(Update update, String tableName) {
        StringBuilder cql = new StringBuilder();
        switch (update) {
        case INSERT_ROW:
            // INSERT INTO "Foo" (key,column1,value) VALUES (<key>, <colname>, <colvalue>);
            cql.append("INSERT INTO ");
            cql.append(tableName);
            cql.append(" (key,column1,value) VALUES (?, ?, ?);");
            break;
        case INSERT_ROW_TS:
            // INSERT INTO "Foo" (key,column1,value) VALUES (<key>, <colname>, <colvalue>) USING TIMESTAMP <timestamp>;
            // Note timestamp is the last parameter
            cql.append("INSERT INTO ");
            cql.append(tableName);
            cql.append(" (key,column1,value) VALUES (?, ?, ?) USING TIMESTAMP ?;");
            break;
        case DELETE_COLUMN:
            // DELETE FROM "Foo" WHERE key=<key> AND column1=<column name>;
            cql.append("DELETE FROM ");
            cql.append(tableName);
            cql.append(" WHERE key=? AND column1=?;");
            break;
        case DELETE_COLUMN_TS:
            // DELETE FROM "Foo" USING TIMESTAMP <timestamp> WHERE key=<key> AND column1=<column name>;
            // Note timestamp is the first parameter
            cql.append("DELETE FROM ");
            cql.append(tableName);
            cql.append(" USING TIMESTAMP ? WHERE key=? AND column1=?;");
            break;
        case DELETE_ROW:
            // DELETE FROM "Foo" WHERE key='key';
            cql.append("DELETE FROM ");
            cql.append(tableName);
            cql.append(" WHERE key=?;");
            break;
        case DELETE_ROW_TS:
            // DELETE FROM "Foo" USING TIMESTAMP <timestamp> WHERE key='key';
            // Note timestamp is the first parameter
            cql.append("DELETE FROM ");
            cql.append(tableName);
            cql.append(" USING TIMESTAMP ? WHERE key=?;");
            break;
        }
        m_logger.debug("Preparing update statement: {}", cql);
        return m_session.prepare(cql.toString());
    }   // prepareUpdate

}   // class CQLStatementCache
