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

import com.dell.doradus.common.UNode;
import com.dell.doradus.common.Utils;
import com.dell.doradus.olap.aggregate.AggregationRequestData;

/**
 * OlapAggregate encapsulates the parameters that can be provided for an OLAP aggregation query. It
 * provides methods that allow parameters to be parsed from a query URI or UNode document.
 */
public class OlapAggregate {
    private String  m_query;
    private String  m_fields;
    private String  m_metrics;
    private String m_shards;
    private String m_shardsRange;
    private String m_pair;
    private String m_xshards;  				// &xshards parameter
    private String m_xshardsRange;			// &xrange parameter
    
    /**
     * Create an OlapAggregate with query parameters extracted from the given "search" UNode.
     * 
     * @param  searchNode   Root node of a "search" request.
     */
    public OlapAggregate(UNode searchNode) {
        assert searchNode != null;
        ParsedQuery parsedQuery = new ParsedQuery("aggregate-search", searchNode);
        m_query = parsedQuery.get("query");
        m_fields = parsedQuery.get("grouping-fields");
        Utils.require(parsedQuery.get("composite-fields") == null,
        		"OLAP queries cannot use composite grouping composite-fields parameter");
        m_metrics = parsedQuery.get("metric");
        m_shards = parsedQuery.get("shards");
        m_shardsRange = parsedQuery.get("shards-range");
        m_pair = parsedQuery.get("pair");
        m_xshards = parsedQuery.get("x-shards");
        m_xshardsRange = parsedQuery.get("x-shards-range");
        parsedQuery.checkInvalidParameters();
        checkDefaults();
    }
    
    /**
     * Create an OlapAggregate that extracts query parameters from the given URI query string.
     * The given queryParam is expected to be the still-encoded value passed after the '?'
     * in a query URI. This parameter is split into its components, and URI decoding is
     * applied to each part. The parameters are then validated and stored in this object.
     * 
     * @param  queryParam               Query parameter that follows the '?' in a query URI.
     * @throws IllegalArgumentException If a parameter is unrecognized or a required
     *                                  parameter is missing.
     */
    public OlapAggregate(String queryParam) throws IllegalArgumentException {
        assert queryParam != null;
        ParsedQuery parsedQuery = new ParsedQuery(queryParam);
        m_query = parsedQuery.get("q");
        m_fields = parsedQuery.get("f");
        Utils.require(parsedQuery.get("cf") == null,
        		"OLAP queries cannot use composite grouping composite-fields parameter");
        m_metrics = parsedQuery.get("m");
        m_shards = parsedQuery.get("shards");
        m_shardsRange = parsedQuery.get("range");
        m_pair = parsedQuery.get("pair");
        m_xshards = parsedQuery.get("xshards");
        m_xshardsRange = parsedQuery.get("xrange");
        parsedQuery.checkInvalidParameters();
        checkDefaults();
    }

    /**
     * Create an OlapAggregate with the parameters specified
     */
    public OlapAggregate(String shards, String query, String fields, String metrics, String pair) {
    	m_shards = shards;
    	m_query = query;
    	m_fields = fields;
    	m_metrics = metrics;
        checkDefaults();
    }
    
    public String getQuery() { return m_query; }
    public String getFields() { return m_fields; }
    public String getMetrics() { return m_metrics; }
    
	public AggregationRequestData createRequestData(Olap olap, String application, String table) {
		AggregationRequestData requestData = new AggregationRequestData();
		requestData.application = application;
		requestData.shards = olap.getShardsList(application, m_shards, m_shardsRange);
		requestData.xshards = olap.getShardsList(application, m_xshards, m_xshardsRange);
		requestData.table = table;
		requestData.metrics = m_metrics;
		
		if(m_pair == null) {
			requestData.parts = new AggregationRequestData.Part[1];
			requestData.parts[0] = new AggregationRequestData.Part();
			requestData.parts[0].query = m_query;
			requestData.parts[0].field = m_fields;
			return requestData;
		}
		
		String[] pairs = Utils.split(m_pair, ',').toArray(new String[0]);
		Utils.require(pairs.length == 2, "_pair must contain two fields");
		pairs[0] = pairs[0].trim();
		pairs[1] = pairs[1].trim();
		
		requestData.parts = new AggregationRequestData.Part[2];
		requestData.parts[0] = new AggregationRequestData.Part();
		requestData.parts[1] = new AggregationRequestData.Part();
		
		if(m_query != null) { 
			requestData.parts[0].query = m_query.replace("_pair.first", pairs[0]).replace("_pair.second", pairs[1]);
			requestData.parts[1].query = m_query.replace("_pair.first", pairs[1]).replace("_pair.second", pairs[0]); 
		}
		if(m_fields != null) {
			requestData.parts[0].field = m_fields.replace("_pair.first", pairs[0]).replace("_pair.second", pairs[1]);
			requestData.parts[1].field = m_fields.replace("_pair.first", pairs[1]).replace("_pair.second", pairs[0]); 
		}
		
		return requestData;
	}
    
    // Check required parameters and set default values.
    private void checkDefaults() {
        Utils.require(m_shards != null || m_shardsRange != null, "shards or range parameter is not set");
        Utils.require(m_shards == null || m_shardsRange == null, "shards and range parameters cannot be both set");
        
        Utils.require(m_xshards == null || m_xshardsRange == null, "xshards and xrange parameters cannot be both set");
        if(m_xshards == null && m_xshardsRange == null) {
        	m_xshards = m_shards;
        	m_xshardsRange = m_shardsRange;
        }
        
        if (m_query == null) m_query = "*";
        if(m_metrics == null) m_metrics = "COUNT(*)";
    }
    
}
