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

import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.common.UNode;
import com.dell.doradus.common.Utils;
import com.dell.doradus.core.ServerParams;
import com.dell.doradus.search.QueryExecutor;
import com.dell.doradus.search.SearchResultList;

/**
 * ObjectQuery encapsulates the parameters that can be provided for an object query. It
 * provides methods that allow parameters to be parsed from a query URI, XML message, or
 * JSON message.
 */
public class ObjectQuery {
    // Perspective table of query:
    private final TableDefinition m_tableDef;
    
    // Members and their default values.
    private String  m_text;             // required
    private int     m_pageSize = -1;
    private String  m_continueID;
    private boolean m_bContinueAfter;
    private String  m_fieldSet;
    private int		m_skip = 0;
    private String  m_sortOrder;
    private boolean m_l2rEnabled = ServerParams.instance().getModuleParamBoolean("DoradusServer", "l2r_enable");

    /**
     * Create an object that uses the given perspective table and extracts query
     * parameters from the given "search" UNode.
     * 
     * @param  tableDef      {@link TableDefinition} of perspective table.
     * @param  searchNode    Root node of a "search" request.
     * @throws IllegalArgumentException
     *                      If the request contains an unknown parameter or is missing a
     *                      required parameter.
     */
    public ObjectQuery(TableDefinition tableDef, UNode searchNode)
            throws IllegalArgumentException {
        assert tableDef != null;
        assert searchNode != null;
        
        // Root object should be called "search".
        m_tableDef = tableDef;
        Utils.require(searchNode.getName().equals("search"),
                      "Root node should be called 'search': " + searchNode);

        // Parse child nodes.
        for (UNode childNode : searchNode.getMemberList()) {
            // All expected child nodes should be values.
            Utils.require(childNode.isValue(),
                          "'search' parameter value must be text: " + childNode);
            String paramName = childNode.getName();
            String paramValue = childNode.getValue();   // might be null
            
            // continue-after
            if (paramName.equals("continue-after")) {
                Utils.require(m_continueID == null,
                              "Parameter can only be specified once: " + paramName);
                m_continueID = paramValue;
                m_bContinueAfter = true;
                
            // continue-at
            } else if (paramName.equals("continue-at")) {
                Utils.require(m_continueID == null,
                              "Parameter can only be specified once: " + paramName);
                m_continueID = paramValue;
                m_bContinueAfter = false;
                
            // fields
            } else if (paramName.equals("fields")) {
                Utils.require(m_fieldSet == null,
                              "Parameter can only be specified once: " + paramName);
                m_fieldSet = paramValue;

            // query
            } else if (paramName.equals("query")) {
                Utils.require(m_text == null,
                              "Parameter can only be specified once: " + paramName);
                m_text = paramValue;
                Utils.require(m_text != null && m_text.length() > 0,
                              "'query' parameter cannot be empty");
            
            // skip
            } else if (paramName.equals("skip")) {
                Utils.require(m_skip == 0,
                              "Parameter can only be specified once: " + paramName);
                try {
                    m_skip = Integer.parseInt(paramValue);
                    if (m_skip < 0) {
                        throw new Exception();
                    }
                } catch (Exception e) {
                    Utils.require(false, "Invalid 'skip' value: " + paramValue);
                }
                
            // sort order
            } else if (paramName.equals("order")) {
                Utils.require(m_sortOrder == null,
                              "Parameter can only be specified once: " + paramName);
                m_sortOrder = paramValue;
                Utils.require(m_sortOrder != null && m_sortOrder.length() > 0,
                              "'order' parameter cannot be empty");
            
            // size
            } else if (paramName.equals("size")) {
                Utils.require(m_pageSize == -1,
                              "Parameter can only be specified once: " + paramName);
                try {
                    m_pageSize = Integer.parseInt(paramValue);
                    if (m_pageSize <= 0) {
                        throw new Exception();
                    }
                } catch (Exception e) {
                    Utils.require(false, "Invalid 'size' value: " + paramValue);
                }
                
            // unrecognized
            } else {
                Utils.require(false, "Unknown 'search' parameter: " + paramName);
            }
        }
        
        // Make sure we got the query text, which is the only required parameter.
        Utils.require(m_text != null, "'query' parameter is required in 'search' request");
        if (m_skip > 0) {
            // 'skip' parameter is allowed for sorted results only
        	Utils.require(m_sortOrder != null, "'skip' parameter is only allowed for 'ordered' requests");
        	// 'k' parameter is not allowed for 'continue' requests
        	Utils.require(m_continueID == null, "'skip' parameter is not allowed for 'continuation' requests");
        }
    }   // constructor
    
    /**
     * Create an object for the given perspective table that extracts query parameters
     * from the given URI query string. The given queryParam is expected to be the still-
     * encoded value passed after the '?' in a query URI. This function splits the
     * parameter into its components, applies URI decoding to each part, and then
     * validates and stores each part.
     * 
     * @param tableDef                  {@link TableDefinition} of perspective table.
     * @param queryParam                Query parameter that follows the '?' in a query URI.
     * @throws IllegalArgumentException If a parameter is unrecognized or a required
     *                                  parameter is missing.
     */
    public ObjectQuery(TableDefinition tableDef, String queryParam)
            throws IllegalArgumentException {
        assert tableDef != null;
        assert queryParam != null && queryParam.length() > 0;
        
        // Split and decode the URI parts.
        m_tableDef = tableDef;
        Map<String, String> paramMap = Utils.parseURIQuery(queryParam);
        for (Map.Entry<String, String> mapEntry : paramMap.entrySet()) {
            String name = mapEntry.getKey();
            String value = mapEntry.getValue();
            
            // q=<query text>
            if (name.equals("q")) {
                Utils.require(m_text == null, "Query parameter can only be specified once: " + name);
                m_text = value;
                
            // s=<page size>
            } else if (name.equals("s")) {
                Utils.require(m_pageSize == -1, "Query parameter can only be specified once: " + name);
                try {
                    m_pageSize = Integer.parseInt(value);
                    if (m_pageSize < 0) {
                        throw new NumberFormatException();
                    }
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Invalid pagesize value: " + name);
                }
                
            // e=<continue at ID>
            } else if (name.equals("e")) {
                Utils.require(m_continueID == null, "Query parameter can only be specified once: " + name);
                m_continueID = value;
                m_bContinueAfter = false;
                
            // g=<continue after ID>
            } else if (name.equals("g")) {
                Utils.require(m_continueID == null, "Query parameter can only be specified once: " + name);
                m_continueID = value;
                m_bContinueAfter = true;
                
            // f=<field list>
            } else if (name.equals("f")) {
                Utils.require(m_fieldSet == null, "Query parameter can only be specified once: " + name);
                m_fieldSet = value;

            // k=<skip count>
            } else if (name.equals("k")) {
                Utils.require(m_skip == 0, "Query parameter can only be specified once: " + name);
                try {
                    m_skip = Integer.parseInt(value);
                    if (m_skip < 0) {
                        throw new NumberFormatException();
                    }
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Invalid skip value: " + name);
                }
                
            // o=<sort order>
            } else if (name.equals("o")) {
                Utils.require(m_sortOrder == null, "Query parameter can only be specified once: " + name);
                m_sortOrder = value;

            // l2r=[true | false]
            } else if (name.equals("l2r")) {
                m_l2rEnabled = "true".equals(value);
                
            // Unknown or not-yet-supported parameter.
            } else {
                throw new IllegalArgumentException("Unrecognized query parameter: " + name);
            }
        }
        
        // Make sure we got the query text, which is the only required parameter.
        Utils.require(m_text != null, "Query missing required parameter: 'q'");
        if (m_skip > 0) {
            // 'k' parameter is allowed for sorted (&o) results only
        	Utils.require(m_sortOrder != null, "'k' parameter is only allowed for 'ordered' requests");
        	// 'k' parameter is not allowed for 'continue' requests
        	Utils.require(m_continueID == null, "'k' parameter is not allowed for 'continuation' requests");
        }
    }   // constructor

    ///// Getters
    
    public TableDefinition getTableDef() {
        return m_tableDef;
    }   // getTableDef

    public String getContinueID() {
        return m_continueID;
    }   // getContinueID
    
    public String getFieldSet() {
        return m_fieldSet;
    }   // getFieldSet

    public int getPageSize() {
        return m_pageSize;
    }   // getPageSize

    public String getText() {
        return m_text;
    }   // getText

    public String getSortOrder() {
        return m_sortOrder;
    }   // getSortOrder
    
    public int getSkip() {
    	return m_skip;
    }	// getSkip
    
    public boolean isContinueAfter() {
        return m_bContinueAfter;
    }   // isContinueAfter

    public boolean isL2REnabled() {
        return m_l2rEnabled;
    }   // isL2REnabled
    
    public SearchResultList query() {
        QueryExecutor query = new QueryExecutor(getTableDef()); 
        if (getPageSize() >= 0) {
            query.setPageSize(getPageSize());
        }
        if (getSkip() > 0) {
            query.setSkip(getSkip());
        }
        if (getContinueID() != null) {
            if (isContinueAfter()) {
                query.setContinueAfterID(getContinueID());
            } else {
                query.setContinueBeforeID(getContinueID());
            }
        }
        query.setL2rEnabled(isL2REnabled());
        
        // Execute the query.
        return query.search(getText(), getFieldSet(), getSortOrder());
    }

}   // class ObjectQuery
