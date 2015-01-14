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

package com.dell.doradus.olap;

import java.util.List;

import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.UNode;
import com.dell.doradus.common.Utils;
import com.dell.doradus.core.ServerConfig;

/**
 * OlapQuery encapsulates the parameters that can be provided for an OLAP search query. It
 * provides methods that allow parameters to be parsed from a query URI or UNode document.
 */
public class OlapQuery {
    private String  m_query;                // &q parameter
    private int     m_pageSize = -1;        // &s parameter
    private int     m_skip = 0;        		// &k parameter
    private String  m_fields;               // &f parameter
    private String  m_sortOrder;            // &o parameter
    private String m_shards;   				// &shards parameter
    private String m_shardsRange;			// &range parameter
    private String m_pair;					// &pair parameter
    private String m_xshards;  				// &xshards parameter
    private String m_xshardsRange;			// &xrange parameter
    private String  m_continueAt;			// &e parameter
    private String  m_continueAfter;		// &g parameter
    
    /**
     * Create an OlapQuery with query parameters extracted from the given "search" UNode.
     * 
     * @param  searchNode   Root node of a "search" request.
     */
    public OlapQuery(UNode searchNode) {
        assert searchNode != null;
        ParsedQuery parsedQuery = new ParsedQuery("search", searchNode);
        m_query = parsedQuery.get("query");
        m_pageSize = parsedQuery.getInt("size", -1);
        m_skip = parsedQuery.getInt("skip", 0);
        m_fields = parsedQuery.get("fields");
        m_sortOrder = parsedQuery.get("order");
        m_shards = parsedQuery.get("shards");
        m_shardsRange = parsedQuery.get("shards-range");
        m_pair = parsedQuery.get("pair");
        m_xshards = parsedQuery.get("x-shards");
        m_xshardsRange = parsedQuery.get("x-shards-range");
        m_continueAt = parsedQuery.get("continue-at");
        m_continueAfter = parsedQuery.get("continue-after");
        parsedQuery.checkInvalidParameters();
        checkDefaults();
    }
    
    /**
     * Create an OlapQuery that extracts query parameters from the given URI query string.
     * The given queryParam is expected to be the still-encoded value passed after the '?'
     * in a query URI. This parameter is split into its components, and URI decoding is
     * applied to each part. The parameters are then validated and stored in this object.
     * 
     * @param  queryParam               Query parameter that follows the '?' in a query URI.
     * @throws IllegalArgumentException If a parameter is unrecognized or a required
     *                                  parameter is missing.
     */
    public OlapQuery(String queryParam) throws IllegalArgumentException {
        assert queryParam != null;
        ParsedQuery parsedQuery = new ParsedQuery(queryParam);
        m_query = parsedQuery.get("q");
        m_pageSize = parsedQuery.getInt("s", -1);
        m_skip = parsedQuery.getInt("k", 0);
        m_fields = parsedQuery.get("f");
        m_sortOrder = parsedQuery.get("o");
        m_shards = parsedQuery.get("shards");
        m_shardsRange = parsedQuery.get("range");
        m_pair = parsedQuery.get("pair");
        m_xshards = parsedQuery.get("xshards");
        m_xshardsRange = parsedQuery.get("xrange");
        m_continueAt = parsedQuery.get("e");
        m_continueAfter = parsedQuery.get("g");
        parsedQuery.checkInvalidParameters();
        checkDefaults();
    }
    
    
    /**
     * Create an OlapQuery with the parameters specified
     */
    public OlapQuery(String shards, String query) {
    	m_shards = shards;
    	m_query = query;
        checkDefaults();
    }

    public void fixPairParameter() {
    	if(m_pair == null) return;
    	if(m_query == null) return;
		if(!m_query.contains("_pair.")) return;
    	
		String[] pairs = Utils.split(m_pair, ',').toArray(new String[2]);
		if(pairs.length != 2) throw new IllegalArgumentException("pair must contain two fields");
		pairs[0] = pairs[0].trim();
		pairs[1] = pairs[1].trim();
		String first = m_query.replace("_pair.first", pairs[0]).replace("_pair.second", pairs[1]);
		String second = m_query.replace("_pair.first", pairs[1]).replace("_pair.second", pairs[0]);
		m_query = "(" + first + ") OR (" + second + ")";
    }
    
    public String getFieldSet() { return m_fields; }
    public int getPageSize() { return m_pageSize; }
    public int getSkip() { return m_skip; }
    public String getQuery() { return m_query; }
    public String getSortOrder() { return m_sortOrder; }
    public String getContinueAt() { return m_continueAt; }
    public String getContinueAfter() { return m_continueAfter; }
    
    public List<String> getShards(ApplicationDefinition appDef, Olap olap) {
    	return olap.getShardsList(appDef, m_shards, m_shardsRange);
    }

    public List<String> getXShards(ApplicationDefinition appDef, Olap olap) {
    	return olap.getShardsList(appDef, m_xshards, m_xshardsRange);
    }
    
    public int getPageSizeWithSkip() {
    	if(m_pageSize < 0) return m_skip + ServerConfig.getInstance().search_default_page_size;
    	else if(m_pageSize == 0) return Integer.MAX_VALUE;
    	else return m_pageSize + m_skip;
    }
    
    
    // Check required parameters and set default values.
    private void checkDefaults() {
        Utils.require(m_shards != null || m_shardsRange != null, "shards or range parameter is not set");
        Utils.require(m_shards == null || m_shardsRange == null, "shards and range parameters cannot be both set");
        
        Utils.require(m_continueAt == null || m_continueAfter == null, "Both continue-at and continue-after parameters cannot be set");
        Utils.require((m_continueAt == null && m_continueAfter == null) || m_sortOrder == null, "continuation oarameters cannot be set if sort order is set");
        
        Utils.require(m_xshards == null || m_xshardsRange == null, "xshards and xrange parameters cannot be both set");
        if(m_xshards == null && m_xshardsRange == null) {
        	m_xshards = m_shards;
        	m_xshardsRange = m_shardsRange;
        }
        
        if (m_query == null) m_query = "*";
        if (m_pageSize == -1) m_pageSize = ServerConfig.getInstance().search_default_page_size;
    }
    
}
