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

package com.dell.doradus.common;

/**
 * Represents a statistic declaration, which is a persitent metric calculation owned by a
 * specific table.
 */
final public class StatisticDefinition {
    // Member variables:
    private final String    m_tableName;        // soft link to table that owns statistic
    private String          m_statName;         // required: name of statistic
    private String          m_metricParam;      // required: metric parameter in string form
    private String          m_groupParam;       // optional group parameter in string form
    private String          m_queryParam;       // optional query parameter in string form

    /**
     * Create a statistic definition that belongs to the given table.
     * 
     * @param tableName Name of table to which statistic belongs.
     */
    public StatisticDefinition(String tableName) {
        assert tableName != null && tableName.length() > 0;
        m_tableName = tableName;
    }   // constructor
    
    /**
     * Indicate if the given statistic name is valid. Statistic names must begin with a
     * letter and consist of all letters, digits, and underscores.
     * 
     * @param statName  Candidate statistic name.
     * @return          True if the given name is valid for statistics.
     */
    public static boolean isValidStatName(String statName) {
        return statName != null &&
               statName.length() > 0 &&
               Utils.isLetter(statName.charAt(0)) &&
               Utils.allAlphaNumUnderscore(statName);
    }   // isValidStatName
    
    ///// Getters
    
    /**
     * Get this statistic's table name. This is a soft link to the TableDefinition that
     * owns it.
     * 
     * @return The name of the table that owns this statistic.
     */
    public String getTableName() {
        return m_tableName;
    }   // getTableName
    
    /**
     * Get this statistic's name.
     * 
     * @return This statistic's name.
     */
    public String getStatName() {
        return m_statName;
    }   // getStatName
    
    /**
     * Get this statistic's metric parameter.
     * 
     * @return  This statistic's metric parameter.
     */
    public String getMetricParam() {
        return m_metricParam;
    }   // getMetricParam
    
    /**
     * Get this statistic's group parameter or null if there is no group parameter.
     * @return  This statistic's group parameter or null if there is no group parameter.
     */
    public String getGroupParam() {
        return m_groupParam;
    }   // getGroupParam
    
    /**
     * Get this statistic's query parameter or null if there is no query parameter.
     * @return  This statistic's query parameter or null if there is no query parameter.
     */
    public String getQueryParam() {
        return m_queryParam;
    }   // getQueryParam
    
    /**
     * Return true if this is a global statistic, meaning it has no group parameter.
     * 
     * @return True if this is a global statistic.
     */
    public boolean isGlobalStat() {
        return m_groupParam == null || m_groupParam.length() == 0;
    }   // isGlobalStat

    /**
     * Serialize this statistic definition into a {@link UNode} tree and return the root
     * node.
     * 
     * @return  The root node of a {@link UNode} tree representing this statistic.
     */
    public UNode toDoc() {
        // The root node is a MAP whose name is the statistic name. We set its tag name to
        // "statistic" for XML.
        UNode statNode = UNode.createMapNode(m_statName, "statistic");
        
        // Add the metric parametes always, marked as an attribute.
        statNode.addValueNode("metric", getMetricParam(), true);
        
        // Add group and query if non-empty, marked as attributes.
        if (m_groupParam != null && m_groupParam.length() > 0) {
            statNode.addValueNode("group", getGroupParam(), true);
        }
        if (m_queryParam != null && m_queryParam.length() > 0) {
            statNode.addValueNode("query", getQueryParam(), true);
        }
        return statNode;
    }   // toDoc

    /**
     * For debugging, the string "Statistic: <name>".
     */
    @Override
    public String toString() {
        return "Statistic '" + m_statName + "'";
    }   // toString
    
    ///// Setters
    
    /**
     * Parse the statistic definition rooted at the given UNode and place the attributes
     * in this object. The given UNode is the statistics node, so its name is used as the
     * statistic name. It must be a map of simple values, so its child nodes are the
     * statistic's attributes.
     * 
     * @param  statNode                 {@link UNode} that defines a statistic.
     * @throws IllegalArgumentException If the definition is invalid.
     */
    public void parse(UNode statNode) throws IllegalArgumentException {
        assert statNode != null;
        
        // Node must be a map.
        Utils.require(statNode.isMap(),
                      "'statistic' definition must be a map of unique names: " + statNode);
        
        // Ensure the statistic name is valid and save it.
        setName(statNode.getName());
        
        // Parse the child nodes.
        for (String childName : statNode.getMemberNames()) {
            // All child nodes must be values.
            UNode childNode = statNode.getMember(childName);
            Utils.require(childNode.isValue(),
                          "Value of statistic attribute must be a string: " + childNode);
            
            // "metric"
            if (childName.equals("metric")) {
                Utils.require(m_metricParam == null,
                              "'metric' can only be specified once");
                setMetricParam(childNode.getValue());
                
            // "group"
            } else if (childName.equals("group")) {
                Utils.require(m_groupParam == null,
                              "'group' can only be specified once");
                setGroupParam(childNode.getValue());
                
            // "query"
            } else if (childName.equals("query")) {
                Utils.require(m_queryParam == null,
                              "'query' can only be specified once");
                setQueryParam(childNode.getValue());
                
            // Unrecognized.
            } else {
                Utils.require(false, "Unrecognized statistic attribute: " + childName);
            }
        }

        // Ensure name and metric were specified.
        Utils.require(m_statName != null, "Statistic definition missing 'name'");
        Utils.require(m_metricParam != null, "Statistic definition missing 'metric'");
    }   // parse

    /**
     * Set this statistic's name to the given value. The name must be valid (see
     * {@link #isValidStatName(String)} or an exception is thrown.
     * 
     * @param statName                  New name for this statistic.
     * @throws IllegalArgumentException If the given name is invalid.
     */
    public void setName(String statName) throws IllegalArgumentException {
        Utils.require(isValidStatName(statName),
                      "Invalid statistic name: " + statName);
        m_statName = statName;
    }   // setName
    
    /**
     * Set this statistic's metric parameter. The parameter must be non-empty or an
     * exception is thrown.
     * 
     * @param  metricParam              Metric parameter for this statistic.
     * @throws IllegalArgumentException If the given name is empty.
     */
    public void setMetricParam(String metricParam) throws IllegalArgumentException {
        Utils.require(metricParam != null && metricParam.length() > 0,
                      "Metric parameter cannot be empty");
        m_metricParam = metricParam;
    }   // setMetricParam
    
    /**
     * Set this statistic's group parameter. The parameter must be non-empty or an
     * exception is thrown.
     * 
     * @param  groupParam               Group parameter for this statistic.
     * @throws IllegalArgumentException If the given parameter is empty.
     */
    public void setGroupParam(String groupParam) throws IllegalArgumentException {
        Utils.require(groupParam != null && groupParam.length() > 0,
                      "Group parameter cannot be empty");
        m_groupParam = groupParam;
    }   // setGroupParam
    
    /**
     * Set this statistic's query parameter. The parameter must be non-empty or an
     * exception is thrown.
     * 
     * @param  queryParam               Query parameter for this statistic.
     * @throws IllegalArgumentException If the given parameter is empty.
     */
    public void setQueryParam(String queryParam) throws IllegalArgumentException {
        Utils.require(queryParam != null && queryParam.length() > 0,
                      "Query parameter cannot be empty");
        m_queryParam = queryParam;
    }   // setQueryParam

    /**
     * Update this statistic definition to match the given new version, copying over its
     * parameters. The new statistic must have the same and belong to the same table.
     * 
     * @param newStatDef    {@link StatisticDefinition} to match.
     */
    public void update(StatisticDefinition newStatDef) {
        // Prerequisites:
        assert newStatDef != null;
        assert newStatDef.getStatName().equals(this.getStatName());
        assert newStatDef.getTableName().equals(this.getTableName());
        
        // Note that we don't use the setXxx() methods because they assert that parameters
        // can't be null.
        m_metricParam = newStatDef.m_metricParam;
        m_groupParam = newStatDef.m_groupParam;
        m_queryParam = newStatDef.m_queryParam;
    }   // update
    
}   // class StatisticDefinition
