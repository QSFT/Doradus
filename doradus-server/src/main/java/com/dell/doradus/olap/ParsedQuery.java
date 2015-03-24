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

import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.dell.doradus.common.UNode;
import com.dell.doradus.common.Utils;

/**
 * ParsedQuery provides convenient access to the query parameters that can be given in 
 * either http request parameters or UNode in case of query via POST method  
 */
public class ParsedQuery {
	// parameter names that have ever been requested
	// so that the map could be checked for invalid parameters afterwards 
	private Set<String> requestedKeys; 
	private Map<String, String> map;
    /**
     * Create a ParsedQuery with query parameters extracted from the given UNode
     * 
     * @param  requiredNodeName   	How the root node should be called.
     * @param  node   				Root node of a request.
     */
    public ParsedQuery(String requiredNodeName, UNode node) {
    	assert requiredNodeName != null;
        assert node != null;
        map = new HashMap<String, String>();
        // Root object Should be a "map" called as in 'requiredNodeName'.
        Utils.require(node.isMap() && requiredNodeName.equals(node.getName()),
                      "Root node should be a map called '"+requiredNodeName+"': " + node);
        // Parse child nodes.
        for (UNode childNode : node.getMemberList()) {
            // All expected child nodes should be values.
            Utils.require(childNode.isValue(), childNode + " parameter value must be text");
            String paramName = childNode.getName();
            String paramValue = childNode.getValue();   // might be null
            Utils.require(!map.containsKey(paramName), paramName + " can only be specified once");
            map.put(paramName, paramValue);
        }
    }
    
    /**
     * Create a ParsedQuery that extracts query parameters from the given URI query string.
     * The given queryParam is expected to be the still-encoded value passed after the '?'
     * in a query URI. This parameter is split into its components, and URI decoding is
     * applied to each part. The parameters are then validated and stored in this object.
     * 
     * 
     * @param  queryParam               Query parameter that follows the '?' in a query URI.
     * @throws IllegalArgumentException If a parameter is unrecognized or a required
     *                                  parameter is missing or specified more than once.
     */
    public ParsedQuery(String queryParam) throws IllegalArgumentException {
    	map = Utils.parseURIQuery(queryParam);
    }
    
    
    /**
     * Gets a value by the key specified. Returns null if the key does not exist. 
     */
    public String get(String key) {
    	if(requestedKeys == null) requestedKeys = new HashSet<String>();
    	String value = map.get(key);
    	requestedKeys.add(key);
    	return value;
    }
    
    /**
     * Gets a value by the key specified. Unlike 'get', checks that the parameter is not null 
     */
    public String getString(String key) {
    	String value = get(key);
    	Utils.require(value != null, key + " parameter is not set");
    	return value;
    }

    /**
     * Returns a Date by the key specified or null.
     * Date format can be yyyy-MM-dd or yyyy-DD-mm hh:mm:ss
     * See Utils.dateFromString for full list of allowed date formats
     */
    public Date getDate(String key) {
    	String value = get(key);
        Date result = Utils.isEmpty(value)  ? null : Utils.dateFromString(value);
    	return result;
    }
    
    /**
     * Returns an integer value by the key specified and throws exception if it was not specified 
     */
    public int getInt(String key) {
    	String value = get(key);
    	Utils.require(value != null, key + " parameter is not set");
    	try {
    		return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(key + " parameter should be a number");
        }
    }

    /**
     * Returns an integer value by the key specified or defaultValue if the key was not provided  
     */
    public int getInt(String key, int defaultValue) {
    	String value = get(key);
    	if(value == null) return defaultValue;
    	try {
    		return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(key + " parameter should be a number");
        }
    }

    /**
     * Returns a boolean value by the key specified or defaultValue if the key was not provided  
     */
    public boolean getBoolean(String key, boolean defaultValue) {
    	String value = get(key);
    	if(value == null) return defaultValue;
   		return XType.getBoolean(value);
    }
    

    /**
     * Checks that there are no more parameters than those that have ever been requested.
     * Call this after you processed all parameters you need if you want
     * an IllegalArgumentException to be thrown if there are more parameters   
     */
    public void checkInvalidParameters() {
    	for(String key: map.keySet()) {
    		boolean wasRequested = requestedKeys != null && requestedKeys.contains(key);
    		if(!wasRequested) throw new IllegalArgumentException("Unknown parameter " + key);
    		
    	}
    }
    
}
