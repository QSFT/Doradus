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

import java.util.ArrayList;
import java.util.List;

/**
 * Holds the results of an statistic query, which can be serialized into a UNode tree and
 * thereafter JSON or XML. This class can also deserialize an XML/JSON statistic result
 * back into an StatResult object. When a StatResult holds a failed statistic query
 * received from the server, it holds an error response.
 * <p>
 * A StatGroup stores the {@link StatisticDefinition} of the statistic for which it holds
 * a result. It also holds a global metric or summary value, and it may hold groups, each 
 * of which represents a specific field name and value. Groups may be nested depending on
 * the number of grouping fields in the statistic definition. See the {@link StatGroup}
 * class and method descriptions for more details.   
 */
public class StatResult {
    // Members: 
    private final StatisticDefinition   m_statDef;
    private String                      m_globalValue;
    private List<StatGroup>             m_groupList;
    private String                      m_errMsg;

    /**
     * Create a StatResult that will hold the query results for the given statistic. The
     * object will have no content or error message until "set" or "parse" methods are
     * called.
     * 
     * @param statDef   {@link StatisticDefinition} of a statistic.
     */
    public StatResult(StatisticDefinition statDef) {
        m_statDef = statDef;
    }   // constructor
    
    /**
     * Holds the values of an inner or outer group. Each StatGroup object holds the field
     * name and value for the group it represents. It also holds a group-level computation,
     * which is the metric value for a leaf group or the summary value for a non-leaf
     * group. Non-leaf groups hold one or more nested groups, recursively. 
     */
    public static class StatGroup {
        // Members belonging to this group.
        private String          m_fieldName;
        private String          m_fieldValue;
        private String          m_groupValue;
        private List<StatGroup> m_groupList;
        
        // Location construction only.
        private StatGroup() {}
        
        ///// getters
        
        /**
         * The field name for which this group holds computation(s).
         */
        public String getFieldName() {
            return m_fieldName;
        }   // getFieldName
        
        /**
         * The field value for which this group holds computation(s). The value is
         * returned as a string but may contain a numeric, timestamp, or boolean value.
         */
        public String getFieldValue() {
            return m_fieldValue;
        }   // getFieldValue
        
        /**
         * The metric computation value for this group. For a leaf group, it is the metric
         * value for the corresponding field name and value. For a non-leaf group, it is
         * the group-level summary value.
         */
        public String getGroupValue() {
            return m_groupValue;
        }   // getGroupValue
        
        /**
         * The number of child groups nested within this group. 0 if none.
         */
        public int getEmbeddedGroupCount() {
            return m_groupList == null ? 0 : m_groupList.size();
        }   // getEmbeddedGroupCount
        
        /**
         * An Iterable object that provides access to nested groups. If there are no
         * nested groups, the Iterable object will return nothing.
         */
        public Iterable<StatGroup> getGroups() {
            if (m_groupList == null) {
                return (new ArrayList<StatGroup>());
            } else {
                return m_groupList;
            }
        }   // getGroups
        
        ///// setters
        
        /**
         * Create and return a new StatGroup that is nested within this group.
         * 
         * @return New StatGroup object owned by (nested in) this group.
         */
        public StatGroup addGroup() {
            if (m_groupList == null) {
                m_groupList = new ArrayList<StatGroup>();
            }
            StatGroup group = new StatGroup();
            m_groupList.add(group);
            return group;
        }   // addGroup
        
        /**
         * Set the name of the field name to which this group pertains.
         * 
         * @param fieldName     New value for this group's field name.
         */
        public void setFieldName(String fieldName) {
            m_fieldName = fieldName;
        }   // setFieldName
        
        /**
         * Set the field value to which this group pertains.
         * 
         * @param fieldValue    New value for this group's field value.
         */
        public void setFieldValue(String fieldValue) {
            m_fieldValue = fieldValue;
        }   // setFieldValue
        
        /**
         * Set the group-level computation value. For leaf groups, this is the metric
         * value of the group. For non-leaf groups, it is the summary value of the group.
         * 
         * @param groupValue    New group value for this group.
         */
        public void setGroupValue(String groupValue) {
            m_groupValue = groupValue;
        }   // setGroupValue
        
        ///// Private methods
        
        // Serialize this StatGroup into a "group" UNode tree and return the root.
        private UNode toDoc() {
            UNode groupNode = UNode.createMapNode("group");
            
            // Add field node with alternate JSON formatting.
            UNode fieldNode = groupNode.addValueNode(m_fieldName, m_fieldValue, "field");
            fieldNode.setAltFormat(true);
            
            // Recurse to nested groups, if any.
            if (m_groupList != null) {
                UNode groupsNode = groupNode.addArrayNode("groups");
                for (StatGroup group : m_groupList) {
                    groupsNode.addChildNode(group.toDoc());
                }
            }
            
            // Add group-value, if any, as "metric" for leaf groups or "summary" for
            // non-leaf groups.
            if (m_groupValue != null) {
                if (m_groupList == null) {
                    groupNode.addValueNode("metric", m_groupValue);
                } else {
                    groupNode.addValueNode("summary", m_groupValue);
                }
            }
            return groupNode;
        }   // toDoc

        // Parse the parameters of the given "group" node.
        private void parse(UNode groupNode) {
            Utils.require(groupNode.getName().equals("group"),
                          "'group' expected: " + groupNode.getName());
            Utils.require(groupNode.isMap(), "Map expected: " + groupNode);
            for (String childName : groupNode.getMemberNames()) {
                UNode childNode = groupNode.getMember(childName);
                
                // metric or summary
                if (childName.equals("metric") || childName.equals("summary")) {
                    Utils.require(childNode.isValue(), "Value expected: " + childNode);
                    setGroupValue(childNode.getValue());
                    
                // groups
                } else if (childName.equals("groups")) {
                    Utils.require(childNode.isCollection(), "Array expected: " + childNode);
                    for (UNode nestedGroupNode : childNode.getMemberList()) {
                        StatGroup nestedGroup = addGroup();
                        nestedGroup.parse(nestedGroupNode);
                    }
                    
                // field
                } else {
                    // Unfortunate compromise kludge. In XML, this parameter looks like:
                    //      <field name="foo">bar</field>
                    // But in JSON, it uses an "alternate value format" that looks like:
                    //      "field": {"foo": "bar"}
                    // So we have two possible UNode structures.
                    if (childNode.isValue() && "field".equals(childNode.getTagName())) {
                        // Parsed from XML
                        setFieldName(childName);
                        setFieldValue(childNode.getValue());
                    } else if (childNode.isMap() && childNode.getName().equals("field")) {
                        // Parsed from JSON
                        Utils.require(childNode.getMemberCount() == 1,
                                      "One child node of 'field' expected: " + childNode);
                        UNode grandChildNode = childNode.getMember(0);
                        Utils.require(grandChildNode.isValue(), "Value expected: " + grandChildNode);
                        setFieldName(grandChildNode.getName());
                        setFieldValue(grandChildNode.getValue());
                    } else {
                        // Unrecognized
                        Utils.require(false, "Unrecognized group option: " + childName);
                    }
                }
            }
        }   // parse

    }   // static class StatGroup
    
    ///// Getters
    
    /**
     * Get the global value for this StatResult. For global statistics, which are not
     * grouped, it is the global metric computation. For grouped statistics, this is the
     * global summary value.
     *  
     * @return  The global metric or summary value of the statistic.
     */
    public String getGlobalValue() {
        return m_globalValue;
    }   // getGlobalValue
    
    /**
     * Get the {@link StatisticDefinition} of the statistic for which this StatResult
     * holds a query result.
     * 
     * @return  This StatResult's StatisticDefinition.
     */
    public StatisticDefinition getStatDef() {
        return m_statDef;
    }   // getGroupingParam
    
    /**
     * Return the number of top-level groups within this query result. For each group, a
     * {@link StatGroup} will exist to hold that group's results. There may be fewer
     * groups returned than actually calculated for the statistic if the query filtered
     * the groups being returned. For a global statistic, there will be no groups.
     * 
     * @return  The number of top-level groups within this StatResult, if any.
     */
    public int getGroupCount() {
        return m_groupList == null ? 0 : m_groupList.size(); 
    }   // getGroupCount
    
    /**
     * Return the list of {@link StatGroup}s owned by this StatResult as an Iterable
     * object. If there are no StatGroups, the Iterable will return nothing.
     * 
     * @return  A list of StatGroups owned by this StatResult as an Iterable.
     */
    public Iterable<StatGroup> getGroups() {
        if (m_groupList == null) {
            return new ArrayList<StatGroup>();
        } else {
            return m_groupList;
        }
    }   // getGroups
    
    /**
     * Return this StatResult's error message, if any. An error message is present if
     * {@link #isFailed()} returns true.
     * 
     * @return  This StatResult's error message, if any.
     */
    public String getErrorMessage() {
        return m_errMsg;
    }   // getErrorMessage
    
    /**
     * Return true if this StatResult represents a failed message. If true,
     * {@link #getErrorMessage()} can be called to get the error message.
     * 
     * @return  True if this result represents a failed statistic query.
     */
    public boolean isFailed() {
        return !Utils.isEmpty(m_errMsg);
    }   // isFailed
    
    /**
     * Serialize this StatResult object into a UNode tree and return the root node.
     * The root node is "results".
     * 
     * @return  Root node of a UNode tree representing this StatResult object.
     */
    public UNode toDoc() {
        // Root "results" node.
        UNode resultsNode = UNode.createMapNode("results");
        
        // "statistic" node.
        UNode statNode = resultsNode.addMapNode("statistic");
        statNode.addValueNode("name", m_statDef.getStatName(), true);
        String metric = m_statDef.getMetricParam();
        statNode.addValueNode("metric", metric, true);
        if (!Utils.isEmpty(m_statDef.getGroupParam())) {
            statNode.addValueNode("group", m_statDef.getGroupParam(), true);
        }
        if (!Utils.isEmpty(m_statDef.getQueryParam())) {
            statNode.addValueNode("query", m_statDef.getQueryParam(), true);
        }

        // Add groups, if any, to the parent node.
        if (m_groupList != null) {
            UNode groupsNode = resultsNode.addArrayNode("groups");
            for (StatGroup statGroup : m_groupList) {
                groupsNode.addChildNode(statGroup.toDoc());
            }
        }
        
        // Add global or summary value if present.
        if (m_globalValue != null) {
            if (m_groupList == null) {
                resultsNode.addChildNode(UNode.createValueNode("value", m_globalValue));
            } else {
                resultsNode.addChildNode(UNode.createValueNode("summary", m_globalValue));
            }
        }
        return resultsNode;
    }   // toDoc

    ///// Update methods
    
    /**
     * Parse the statistic query results structured as a UNode tree rooted at the given
     * node. The results should be those generated by {@link #toDoc()}, e.g., sent by the
     * server as XML or JSON and parsed by a client back into a UNode tree. This method
     * can be called to reconstruct the same StatResult set. Values parsed from the UNode
     * tree are mapped into this object.
     *   
     * @param resultsNode   Root node of a UNode tree. Should be a UNode MAP node with the
     *                      name "results".
     */
    public void parse(UNode resultsNode) {
        // Root must be a "results" node.
        Utils.require("results".equals(resultsNode.getName()),
                      "'results' expected: " + resultsNode.getName());
        Utils.require(resultsNode.isMap(),
                      "Map node expected: " + resultsNode);
        
        // Parse child nodes
        for (String childName : resultsNode.getMemberNames()) {
            UNode childNode = resultsNode.getMember(childName);
            
            // statistic (allow but ignore)
            if (childName.equals("statistic")) {
                Utils.require(childNode.isMap(), "Map node expected: " + childNode);
                
            // global "value" or "summary"
            } else if (childName.equals("value") || childName.equals("summary")) {
                Utils.require(childNode.isValue(), "Value expected: " + childNode);
                m_globalValue = childNode.getValue();
                
            // groups
            } else if (childName.equals("groups")) {
                Utils.require(childNode.isCollection(), "Array expected: " + childNode);
                m_groupList = new ArrayList<>();
                for (UNode groupNode : childNode.getMemberList()) {
                    StatGroup statGroup = new StatGroup();
                    statGroup.parse(groupNode);
                    m_groupList.add(statGroup);
                }
                
            // Unrecognized
            } else {
                Utils.require(false,  "Unrecognized option: " + childName);
            }
        }
    }   // parse
    
    /**
     * Mark this StatResult as a failed query result using the given message. Calling
     * this method with a non-empty error message causes {@link #isFailed()} to return
     * true.
     * 
     * @param errMsg    Error message of a failed statistic query.
     */
    public void setErrorMessage(String errMsg) {
        m_errMsg = errMsg;
    }   // setErrorMessage
    
    /**
     * Set this StatResult's global value. This is the metric value for a global
     * vaue or the summary value for grouped statistics.
     * 
     * @param globalValue   New value for this StatResult's global value.
     */
    public void setGlobalValue(String globalValue) {
        m_globalValue = globalValue;
    }   // setGlobalValue
    
    /**
     * Create a return a top-level {@link StatGroup} that will be owned by this
     * StatResult.
     * 
     * @return  A new top-level {@link StatGroup} owned by this StatResult.
     */
    public StatGroup addStatGroup() {
        StatGroup statGroup = new StatGroup();
        if (m_groupList == null) {
            m_groupList = new ArrayList<StatGroup>();
        }
        m_groupList.add(statGroup);
        return statGroup;
    }   // addStatGroup
    
}   // class StatResult
