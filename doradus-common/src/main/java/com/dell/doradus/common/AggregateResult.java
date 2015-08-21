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
 * Holds the results of an aggregate query, which can be serialized into a UNode tree and
 * thereafter JSON or XML. This class can also deserialize an XML/JSON aggregate result
 * back into an AggregateResult object, hence it can be used by a client app. For clients, 
 * this class can hold an error response from a failed aggregate query.
 * <p>
 * The general structure of an aggregate result is depicted below:
 * <pre>
 *  results:
 *      metric-param: String
 *      grouping-param: String
 *      query-param: String
 *      global-value: String
 *      total-objects: long
 *      *groupset:
 *          grouping-param: String
 *          metric-param: String
 *          groupset-value: String
 *          total-groups: long
 *          *group:
 *              composite: Boolean
 *              field-name: String
 *              field-value: String
 *              group-value: String
 *              total-groups: long
 *              *group (recursive)
 * </pre>
 * Objects marked with "*" are multi-valued. Rules for setting AggregateResult fields are
 * summarized below:
 * <H2>Global properties</H2>
 * <dl>
 * <dt>metric-param</dt>
 *  <dd>This is always set to the aggregate's "metric" ({@literal &}m) parameter. It can be a list
 *      for multi-metric queries (e.g., "COUNT(*),MAX(Sise),MIN(SendDate)").</dd>
 *      
 * <dt>grouping-param</dt>
 *  <dd>This is null for global aggregates, otherwise it holds the "grouping" ({@literal &}f)
 *      parameter.</dd>
 *      
 * <dt>query-param</dt>
 *  <dd>This holds the aggregate's "query" ({@literal &}q) parameter and may be null.</dd>
 *  
 * <dt>global-value</dt>
 *  <dd>This holds the metric value for a global aggregate or the summary value for single
 *      groupset aggregates. It is null for multi-groupset aggregates.</dd>
 *      
 * <dt>total-objects</dt>
 *  <dd>This holds the total number of objects counted for queries that compute this
 *      value.</dd>
 * </dl>
 * <H2>Groupset properties</H2>
 * <dl>
 * <dt>groupset</dt>
 *  <dd>A single groupset is used for grouped aggregates that have a single tree of groups
 *      (single- or multi-level). Multiple groupset objects are used for multi-tree
 *      (compound) and multi-metric aggregates: one groupset is used for each GROUP listed
 *      in the grouping parameter and/or for each metric function listd in the metric
 *      parameter.</dd>
 *      
 * <dt>grouping-param</dt>
 *  <dd>A groupset's grouping-param holds the grouping value that the groupset represents.
 *      For example, if an aggregate's grouping parameter is "GROUP(x),GROUP(y)", the
 *      grouping-param is "GROUP(x)" for one groupset and "GROUP(y)" for the other.</dd>
 *      
 * <dt>metric-param</dt>
 *  <dd>A groupset's metric-param holds the metric value that the groupset represents.
 *      For example, in a multi-metric query, if the aggregate's metric parameter is
 *      "COUNT(*),MAX(Size)", the metric-param is "COUNT(*)" for one groupset and
 *      "MAX(Size)" for the other.</dd>
 *      
 * <dt>groupset-value</dt>
 *  <dd>A groupset's groupset-value holds a global metric value when the groupset is a
 *      global metric value (i.e., "GROUP(*)"). For a groupset in a multi-tree aggregate,
 *      it holds the summary value for the groupset. For a multi-metric query without
 *      grouping, it is the groupset's value. For a single-tree groupset, it is null.</dd>
 *      
 * <dt>total-groups</dt>
 *  <dd>A groupset's total-groups is used when the groupset uses a TOP or BOTTOM function.
 *      It holds the total number of groups found independent of the limit imposed by the
 *      TOP/BOTTOM function.</dd>
 * </dl>
 * <H2>Group properties</H2>
 * <dl>
 * <dt>group</dt>
 *  <dd>Each groupset has one or more groups. A group can be a "leaf" group and hold a
 *      simple value, or it can be a parent group and hold lower-level groups.</dd>
 *      
 * <dt>composite</dt>
 *  <dd>If this flag is set for a group, the group is a composite group, and an extra
 *      "composite=true" value is generated for it.</dd>
 *      
 * <dt>field-name</dt>
 *  <dd>This holds the name of the grouping field represented by the group. It is set for
 *      both leaf and parent groups.</dd>
 *      
 * <dt>field-value</dt>
 *  <dd>This holds the value of the grouping field represented by the group. It is set
 *      for both leaf and parent groups.</dd>
 *      
 * <dt>group-value</dt>
 *  <dd>For leaf groups, group-value holds the metric value of the group. For parent
 *      groups, it holds the summary value of the group.</dd>
 *      
 * <dt>total-groups</dt>
 *  <dd>A group's total-groups is used when the group represents a non-leaf group whose
 *      grouping parameter uses a TOP or BOTTOM function. It holds the total number of
 *      groups found for this grouping level independent of the limit imposed by the
 *      TOP/BOTTOM function.</dd>
 * </dl>
 * To summarize, the basic aggregate structures are represented as follows:
 * <ul>
 * <li>A global aggregate has no groupset objects.
 * <li>A single-tree aggregate is represented by a single groupset object with no
 *     grouping-param, metric-param, or groupset-value properties. The groupset may have a
 *     total-groups value.
 * <li>A multi-tree (compound) aggregate is represented by one or more groupset objects,
 *     each with groupset-value (summary) and optionally grouping-param and/or total-groups
 *     values. However, metric-param is null (since it is specified at the global level).
 * <li>A multi-metric aggregate is represented by two or more groupset objects each with a
 *     metric-param and groupset-value (summary) and optionally grouping-param and/or
 *     total-groups values.
 * </ul>
 */
public class AggregateResult {
    // Members that hold global aggregate results: 
    private String              m_metricParam;
    private String              m_queryParam;
    private String              m_groupingParam;
    private String              m_globalValue;
    private long                m_totalObjects = -1;    // -1 == not set
    private List<AggGroupSet>   m_groupSetList;

    // When used at the client, we can hold a failed query error message:
    private String m_errMsg;
    
    /**
     * Abstract interface used by AggGroup and AggGroupSet, providing methods used by
     * both classes.
     */
    public interface IAggGroupList {
        /**
         * Create a new nested group that is owned by this object.
         * 
         * @return New AggGroup object owned by (nested in) this object.
         */
        public AggGroup addGroup();
    }
    
    /**
     * Holds the contents of an implict or explicit "groupset". Global results don't have
     * a groupset; single-tree grouped metrics have one implicit groupset; multi-tree
     * (compound) and multi-metric queries have multiple groupset objects.
     */
    public static class AggGroupSet implements IAggGroupList {
        // Members immediate to the groupset:
        private String          m_groupsetValue;
        private String          m_groupingParam;
        private String          m_metricParam;
        private long            m_totalGroups = -1;     // -1 == not set
        private List<AggGroup>  m_groupList;
        
        // Only AggregateResult.addGroupSet() can created objects.
        private AggGroupSet() {}
        
        ///// getters
        
        /**
         * This group set's computed metric value.
         * @return Computed metric value.
         */
        public String getGroupsetValue() {
            return m_groupsetValue;
        }   // getGroupsetValue
        
        /**
         * This group set's grouping parameter, if used.
         * @return Grouping parameter.
         */
        public String getGroupingParam() {
            return m_groupingParam; 
        }   // getGroupingParam
        
        /**
         * This group set's metric parameter.
         * @return Metric parameter.
         */
        public String getMetricParam() {
            return m_metricParam; 
        }   // getMetricParam
        
        /**
         * The total number of groups found for this group set when a TOP or BOTTOM was
         * used to limit the number of groups returned.
         * @return Total number of groups found.
         */
        public long getTotalGroups() {
            return m_totalGroups;
        }   // getTotalGroups

        /**
         * The number of groups contained by this group set. 0 if none.
         * @return Embedded group count.
         */
        public int getEmbeddedGroupCount() {
            return m_groupList == null ? 0 : m_groupList.size();
        }   // getEmbeddedGroupCount
        
        /**
         * An Iterable object that returns this group sets embedded groups. If there are
         * no embedded groups, the iterator will return nothing.
         * @return Iterable {@link AggGroup}.
         */
        public Iterable<AggGroup> getGroups() {
            if (m_groupList == null) {
                return (new ArrayList<AggGroup>());
            } else {
                return m_groupList;
            }
        }   // getGroups
        
        ///// setters
        
        /**
         * Create and return a new {@link AggGroup} object that is owned by this groupset.
         *  
         * @return  New AggGroup owned by this AggGroupSet.
         */
        public AggGroup addGroup() {
            if (m_groupList == null) {
                m_groupList = new ArrayList<AggGroup>();
            }
            AggGroup group = new AggGroup();
            m_groupList.add(group);
            return group;
        }   // addGroup
        
        /**
         * Set this groupset's grouping-param to the given value. A groupset's
         * grouping-param holds the grouping value that the groupset represents. For
         * example, if an aggregate's grouping parameter is "GROUP(x),GROUP(y)", the
         * grouping-param is "GROUP(x)" for one groupset and "GROUP(y)" for the other.
         * 
         * @param groupParam    New value for this groupset's grouping-param.
         */
        public void setGroupingParam(String groupParam) {
            m_groupingParam = groupParam;
        }   // setGroupingParam

        /**
         * Set this groupset's metric-param to the given value. A groupset's metric-param
         * holds the metric value that the groupset represents. For example, in a
         * multi-metric query, if the aggregate's metric parameter is "COUNT(*),MAX(Size)",
         * the metric-param is "COUNT(*)" for one groupset and "MAX(Size)" for the other.
         * 
         * @param metricParam   New value for this groupset's metric-param.
         */
        public void setMetricParam(String metricParam) {
            m_metricParam = metricParam;
        }   // setMetricParam
        
        /**
         * Set this groupset's groupset-value to the given value. A groupset's
         * groupset-value holds a global metric value when the groupset is a global metric
         * value (i.e., "GROUP(*)"). For a groupset in a multi-tree aggregate, it holds the
         * summary value for the groupset. For a multi-metric query without grouping, it is
         * the groupset's value. For a single-tree groupset, it is null. 
         * 
         * @param groupsetValue New value for this groupset's groupset-value. If null is
         *                      passed, the groupset-value is set to an empty string.
         */
        public void setGroupsetValue(String groupsetValue) {
            m_groupsetValue = groupsetValue == null ? "" : groupsetValue;
        }   // setGroupSetValue
        
        /**
         * Set this groupset's total-groups value. A groupset's total-groups is used when
         * the groupset uses a TOP or BOTTOM function. It holds the total number of groups
         * found independent of the limit imposed by the TOP/BOTTOM function. 
         * 
         * @param totalGroups   New value for this groupset's total-groups.
         */
        public void setTotalGroups(long totalGroups) {
            m_totalGroups = totalGroups;
        }   // setTotalGroups

        ///// Private method
        
        // Add the UNode tree for this groupset to the given parent UNode.
        private void addDoc(UNode parentNode, boolean bUseGroupSetsNode) {
            // Create "groupset" node if requested. 
            if (bUseGroupSetsNode) {
                UNode groupSetNode = parentNode.addMapNode("groupset");
                if (m_groupingParam == null && m_groupList == null && m_metricParam == null) {
                    // Special case: groupset wraps a GROUP(*) global metric.
                    // Add groupset value as a "value" node, and that's it.
                    groupSetNode.addValueNode("value", m_groupsetValue);
                    return;
                }
                
                // Most cases: "groupset" becomes the new parent node.
                parentNode = groupSetNode;
                
                // Add groupset grouping parameter, if present.
                if (m_groupingParam != null) {
                    parentNode.addValueNode("group", m_groupingParam, true);
                }
                
                // Add groupset metric parameter, if present.
                if (m_metricParam != null) {
                    parentNode.addValueNode("metric", m_metricParam, true);
                }
            }
            
            // Add groupset "summary" or "value" as appropriate.
            if (m_groupsetValue != null) {
                if (m_groupList == null) {
                    parentNode.addValueNode("value", m_groupsetValue);
                } else {
                    parentNode.addValueNode("summary", m_groupsetValue);
                }
            }
            
            // Add groupset "totalGroups" if present.
            if (m_totalGroups >= 0) {
                parentNode.addValueNode("totalgroups", Long.toString(m_totalGroups));
            }
            
            // Add "groups" node to parent with underlying "group" children.
            if (m_groupList != null) {
                UNode groupsNode = parentNode.addArrayNode("groups");
                for (AggGroup group : m_groupList) {
                    group.addDoc(groupsNode);
                }
            }
        }   // addDoc
        
        // Parse the given (explicit) groupset node. 
        private void parse(UNode groupsetNode) {
            assert groupsetNode.getName().equals("groupset");
            for (UNode childNode : groupsetNode.getMemberList()) {
                parseOption(childNode);
            }
        }   // parse

        // Parse an option that belongs to an implicit or explicit groupset node.
        private void parseOption(UNode optNode) {
            String optName = optNode.getName();
            
            // group
            if (optName.equals("group")) {
                Utils.require(optNode.isValue(), "Value expected: " + optNode);
                setGroupingParam(optNode.getValue());
                
            // metric
            } else if (optName.equals("metric")) {
                Utils.require(optNode.isValue(), "Value expected: " + optNode);
                setMetricParam(optNode.getValue());
                
            // summary
            } else if (optNode.getName().equals("summary")) {
                Utils.require(optNode.isValue(), "Value expected: " + optNode);
                setGroupsetValue(optNode.getValue());
            
            // totalgroups
            } else if (optName.equals("totalgroups")) {
                Utils.require(optNode.isValue(), "Value expected: " + optNode);
                Utils.require(Utils.allDigits(optNode.getValue()),
                              "Number expected: " + optNode.getValue());
                setTotalGroups(Long.parseLong(optNode.getValue()));
                
            // groups
            } else if (optName.equals("groups")) {
                Utils.require(optNode.isCollection(), "Array expected: " + optNode);
                for (UNode groupNode : optNode.getMemberList()) {
                    AggGroup aggGroup = addGroup();
                    aggGroup.parse(groupNode);
                }
                
            // unrecognized
            } else {
                Utils.require(false, "Unrecognized groupset option: " + optName);
            }
        }   // parseOption
    }   // static class AggGroupSet
    
    /**
     * Holds the values of an inner or outer group.
     */
    public static class AggGroup implements IAggGroupList {
        // Members belonging to this group.
        private boolean         m_bComposite;
        private String          m_fieldName;
        private String          m_fieldValue;
        private String          m_groupValue;
        private long            m_totalGroups = -1;     // -1 == not set 
        private List<AggGroup>  m_groupList;
        
        // Only AggGroupSet.addGroup() and AggGroup.addGroup() can create objects.
        private AggGroup() {}
        
        /**
         * Create a new nested group that is owned by this group.
         * 
         * @return New AggGroup object owned by (nested in) this group.
         */
        public AggGroup addGroup() {
            if (m_groupList == null) {
                m_groupList = new ArrayList<AggGroup>();
            }
            AggGroup group = new AggGroup();
            m_groupList.add(group);
            return group;
        }   // addGroup
        
        ///// getters
        
        /**
         * The field name for which this group pertains.
         * @return Field name.
         */
        public String getFieldName() {
            return m_fieldName;
        }   // getFieldName
        
        /**
         * The field value to which this group pertains.
         * @return Field value.
         */
        public String getFieldValue() {
            return m_fieldValue;
        }   // getFieldValue
        
        /**
         * The computed metric value for this group.
         * @return Metric value.
         */
        public String getGroupValue() {
            return m_groupValue;
        }   // getGroupValue
        
        /**
         * If the number of child groups computed for this group when the number of child
         * groups was limited due to a TOP or BOTTOM function.
         * @return Total groups.
         */
        public long getTotalGroups() {
            return m_totalGroups;
        }   // getTotalGroups

        /**
         * The number of child groups nested within this group. 0 if none.
         * @return Embedded group count.
         */
        public int getEmbeddedGroupCount() {
            return m_groupList == null ? 0 : m_groupList.size();
        }   // getEmbeddedGroupCount
        
        /**
         * An Iterable object that provides access to nested groups. If there are no
         * nested groups, the object will return nothing.
         * @return Nested groups as an Iterable {@link AggGroup}.
         */
        public Iterable<AggGroup> getGroups() {
            if (m_groupList == null) {
                return (new ArrayList<AggGroup>());
            } else {
                return m_groupList;
            }
        }   // getGroups
        
        /**
         * Whether this group corresponds to a <i>composite</i> group.
         * @return True if this is a composite group.
         */
        public boolean isComposite() {
            return m_bComposite;
        }   // isComposite
        
        ///// setters
        
        /**
         * Set the composite flag for this group to the given value.
         * 
         * @param bComposite    New value for this group's composite flag.
         */
        public void setComposite(boolean bComposite) {
            m_bComposite = bComposite;
        }   // setComposite
        
        /**
         * Set the field-name of this group. This holds the name of the grouping field
         * represented by the group. It is set for both leaf and parent groups.
         * 
         * @param fieldName     New value for this group's field-name.
         */
        public void setFieldName(String fieldName) {
            m_fieldName = fieldName;
        }   // setFieldName
        
        /**
         * Set the field-value of this group. This holds the value of the grouping field
         * represented by the group. It is set for both leaf and parent groups.
         * 
         * @param fieldValue    New value for this group's field-value.
         */
        public void setFieldValue(String fieldValue) {
            m_fieldValue = fieldValue;
        }   // setFieldValue
        
        /**
         * Set the group-value of this group. For leaf groups, group-value holds the metric
         * value of the group. For parent groups, it holds the summary value of the group.
         * 
         * @param groupValue    New group-value for this group. If null is passed, the
         *                      group-value is set to an empty string.
         */
        public void setGroupValue(String groupValue) {
            m_groupValue = groupValue == null ? "" : groupValue;
        }   // setGroupValue
        
        /**
         * Set the total-groups value of this group. A group's total-groups is used when
         * the group represents a non-leaf group whose grouping parameter uses a TOP or
         * BOTTOM function. It holds the total number of groups found for this grouping
         * level independent of the limit imposed by the TOP/BOTTOM function.
         * 
         * @param totalGroups   New total-groups value for this group.
         */
        public void setTotalGroups(long totalGroups) {
            m_totalGroups = totalGroups;
        }   // setTotalGroups
        
        ///// Private methods
        
        // Add the UNode tree for this group to the given parent node.
        private void addDoc(UNode parentNode) {
            // Create group node.
            UNode groupNode = parentNode.addMapNode("group");
            
            // Add "composite" flag if needed.
            if (m_bComposite) {
                groupNode.addValueNode("composite", "true", true);
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
            
            // Add field node with alternate JSON formatting.
            UNode fieldNode = groupNode.addValueNode(m_fieldName, m_fieldValue, "field");
            fieldNode.setAltFormat(true);
            
            // Add totalgroups value if any.
            if (m_totalGroups >= 0) {
                groupNode.addValueNode("totalgroups", Long.toString(m_totalGroups));
            }
            
            // Recurse to nested groups, if any.
            if (m_groupList != null) {
                UNode groupsNode = groupNode.addArrayNode("groups");
                for (AggGroup group : m_groupList) {
                    group.addDoc(groupsNode);
                }
            }
        }   // toDoc

        // Parse the parameters of the given "group" node.
        private void parse(UNode groupNode) {
            Utils.require(groupNode.getName().equals("group"),
                          "'group' expected: " + groupNode.getName());
            for (String childName : groupNode.getMemberNames()) {
                UNode childNode = groupNode.getMember(childName);
                
                // composite
                if (childName.equals("composite")) {
                    Utils.require(childNode.isValue(), "Value expected: " + childNode);
                    setComposite(Boolean.parseBoolean(childNode.getValue()));
                    
                // metric or summary
                } else if (childName.equals("metric") || childName.equals("summary")) {
                    Utils.require(childNode.isValue(), "Value expected: " + childNode);
                    setGroupValue(childNode.getValue());
                    
                // totalgroups 
                } else if (childName.equals("totalgroups")) {
                    Utils.require(childNode.isValue(), "Value expected: " + childNode);
                    Utils.require(Utils.allDigits(childNode.getValue()), "Number expected: " + childNode);
                    setTotalGroups(Long.parseLong(childNode.getValue()));
                    
                // groups
                } else if (childName.equals("groups")) {
                    Utils.require(childNode.isCollection(), "Array expected: " + childNode);
                    for (UNode nestedGroupNode : childNode.getMemberList()) {
                        AggGroup nestedGroup = addGroup();
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

    }   // static class AggGroup
    
    ///// Getters
    
    public String getGlobalValue() {
        return m_globalValue;
    }   // getGlobalValue
    
    public String getGroupingParam() {
        return m_groupingParam;
    }   // getGroupingParam
    
    public String getMetricParam() {
        return m_metricParam;
    }   // getMetricParam
    
    public String getQueryParam() {
        return m_queryParam;
    }   // getMetricParam
    
    public long getTotalObjects() {
        return m_totalObjects;
    }   // getTotalObjects
    
    public int getGroupsetCount() {
        return m_groupSetList == null ? 0 : m_groupSetList.size(); 
    }   // getGroupsetCount
    
    public Iterable<AggGroupSet> getGroupsets() {
        if (m_groupSetList == null) {
            return new ArrayList<AggGroupSet>();
        } else {
            return m_groupSetList;
        }
    }   // getGroupsets
    
    /**
     * Return this AggregateResult's error message, if any. An error message is present if
     * {@link #isFailed()} returns true.
     * 
     * @return  This AggregateResult's error message, if any.
     */
    public String getErrorMessage() {
        return m_errMsg;
    }   // getErrorMessage
    
    /**
     * Return true if this AggregateResult represents a failed message. If true,
     * {@link #getErrorMessage()} can be called to get the error message.
     * 
     * @return  True if this result represents a failed aggregate query.
     */
    public boolean isFailed() {
        return !Utils.isEmpty(m_errMsg);
    }   // isFailed
    
    /**
     * Serialize this AggregateResult object into a UNode tree and return the root node.
     * The root node is "results".
     * 
     * @return  Root node of a UNode tree representing this AggregateResult object.
     */
    public UNode toDoc() {
        // Root "results" node.
        UNode resultsNode = UNode.createMapNode("results");
        
        // "aggregate" node.
        UNode aggNode = resultsNode.addMapNode("aggregate");
        aggNode.addValueNode("metric", m_metricParam, true);
        if (m_queryParam != null){
            aggNode.addValueNode("query", m_queryParam, true);
        }
        if (m_groupingParam != null) {
            aggNode.addValueNode("group", m_groupingParam, true);
        }
        
        // Add "totalobjects" if used.
        if (m_totalObjects >= 0) {
            resultsNode.addValueNode("totalobjects", Long.toString(m_totalObjects));
        }
        
        // Add global or summary value if present.
        if (m_globalValue != null) {
            if (m_groupSetList == null) {
                resultsNode.addChildNode(UNode.createValueNode("value", m_globalValue));
            } else {
                resultsNode.addChildNode(UNode.createValueNode("summary", m_globalValue));
            }
        }

        // If needed, the "groupsets" node becomes the new parent. 
        UNode parentNode = resultsNode;
        boolean bGroupSetsNode = false;
        if (m_groupSetList != null && m_groupSetList.size() > 1) {
            bGroupSetsNode = true;
            UNode groupSetsNode = parentNode.addArrayNode("groupsets");
            parentNode = groupSetsNode;
        }

        // Add groupsets, if any, to the parent node.
        if (m_groupSetList != null) {
            for (AggGroupSet groupSet : m_groupSetList) {
                groupSet.addDoc(parentNode, bGroupSetsNode);
            }
        }
        return resultsNode;
    }   // toDoc

    ///// Update methods
    
    /**
     * Parse the aggregate results structured as a UNode tree rooted at the given node.
     * The results should be those generated by {@link #toDoc()}, e.g., sent by the server
     * as XML or JSON and parsed by a client back into a UNode tree. This method can be
     * called to reconstruct the same AggregateResult set. Values parsed from the UNode
     * tree are mapped into this object.
     *   
     * @param resultsNode   Root node of a UNode tree. Should be a UNode MAP node with the
     *                      name "results".
     * @throws IllegalArgumentException If the UNode tree is malformed.
     */
    public void parse(UNode resultsNode) throws IllegalArgumentException {
        // Root must be a "results" node.
        Utils.require("results".equals(resultsNode.getName()),
                      "'results' expected: " + resultsNode.getName());
        
        // Parse "groupsets" first if present.
        UNode groupsetsNode = resultsNode.getMember("groupsets");
        if (groupsetsNode != null) {
            for (UNode groupsetNode : groupsetsNode.getMemberList()) {
                // Children should only be "groupset" Map.
                Utils.require(groupsetNode.getName().equals("groupset"),
                              "'groupset' expected: " + groupsetNode);
                
                // Add an AggGroupSet child and parse child nodes into it.
                AggGroupSet groupset = addGroupSet();
                groupset.parse(groupsetNode);
            }
        }
        
        // If a "groupsets" node was not found, and this is not a global aggregate query,
        // we'll create a single AggGroupSet object to hold the single-tree results using
        // this variable:
        AggGroupSet singleGroupSet = null;
        
        // Parse child nodes
        for (String childName : resultsNode.getMemberNames()) {
            UNode childNode = resultsNode.getMember(childName);
            
            // aggregate
            if (childName.equals("aggregate")) {
                for (String paramName : childNode.getMemberNames()) {
                    UNode paramNode = childNode.getMember(paramName);
                    Utils.require(paramNode.isValue(), "Value node expected: " + paramNode);
                    if (paramName.equals("metric")) {
                        m_metricParam = paramNode.getValue();
                    } else if (paramName.equals("query")) {
                        m_queryParam = paramNode.getValue();
                    } else if (paramName.equals("group")) {
                        m_groupingParam = paramNode.getValue();
                    } else {
                        Utils.require(false, "Unrecognized aggregate parameter: " + paramName);
                    }
                }
                
            // totalobjects
            } else if (childName.equals("totalobjects")) {
                Utils.require(childNode.isValue(), "Value expected: " + childNode);
                m_totalObjects = Long.parseLong(childNode.getValue());
                
            // global "value" or "summary"
            } else if (childName.equals("value") || childName.equals("summary")) {
                Utils.require(childNode.isValue(), "Value expected: " + childNode);
                m_globalValue = childNode.getValue();
                
            // groupsets
            } else if (childName.equals("groupsets")) {
                // Already parsed above.
                assert groupsetsNode != null;
            
            // groups
            } else if (childName.equals("groups")) {
                // Create and/or update the implicit groupset object. 
                Utils.require(groupsetsNode == null, "Outer 'groups' not allowed with 'groupsets'");
                if (singleGroupSet == null) {
                    singleGroupSet = addGroupSet();
                }
                singleGroupSet.parseOption(childNode);
                
            // totalgroups
            } else if (childName.equals("totalgroups")) {
                // Create and/or update the implicit groupset object. 
                Utils.require(groupsetsNode == null, "Outer 'totalgroups' not allowed with 'groupsets'");
                if (singleGroupSet == null) {
                    singleGroupSet = addGroupSet();
                }
                singleGroupSet.parseOption(childNode);

            // Unrecognized
            } else {
                Utils.require(false,  "Unrecognized option: " + childName);
            }
        }
    }   // parse
    
    /**
     * Mark this AggregateResult as a failed query result using the given message. Calling
     * this method with a non-empty error message causes {@link #isFailed()} to return true.
     * 
     * @param errMsg    Error message of a failed aggregate query.
     */
    public void setErrorMessage(String errMsg) {
        m_errMsg = errMsg;
    }   // setErrorMessage
    
    /**
     * Set this AggregateResult's global-value. This holds the metric value for a global
     * aggregate or the summary value for single groupset aggregates. It is null for
     * multi-groupset aggregates.
     * 
     * @param globalValue   New value for this AggregateResult's global-value. If null is
     *                      passed, the global-value is set to an empty string.
     */
    public void setGlobalValue(String globalValue) {
        m_globalValue = globalValue == null ? "" : globalValue;
    }   // setGlobalValue
    
    /**
     * Set this AggregateResult's grouping-param. This is null for global aggregates,
     * otherwise it holds the "grouping" ({@literal &}f) parameter.
     * 
     * @param groupingParam New value for this AggregateResult's grouping-param.
     */
    public void setGroupingParam(String groupingParam) {
        m_groupingParam = groupingParam;
    }   // setGroupingParam
    
    /**
     * Set this AggregateResult's metric-param. This is always set to the aggregate's
     * "metric" ({@literal &}m) parameter. It can be a list for multi-metric queries (e.g.,
     * "COUNT(*),MAX(Sise),MIN(SendDate)).
     * 
     * @param metricParam   New value for this AggregateResult's metric-param.
     */
    public void setMetricParam(String metricParam) {
        m_metricParam = metricParam;
    }   // setMetricParam

    /**
     * Set this AggregateResult's query-param. This holds the aggregate's "query" ({@literal &}q)
     * parameter and may be null.
     *  
     * @param queryParam    New value for this AggregateResult's query-param.
     */
    public void setQueryParam(String queryParam) {
        m_queryParam = queryParam;
    }   // setqueryParam

    /**
     * Set this AggregateResult's total-objects. This holds the total number of objects
     * counted for queries that compute this value.
     * 
     * @param totalObjects  New value for this AggregateResult's total-objects.
     */
    public void setTotalObjects(long totalObjects) {
        m_totalObjects = totalObjects;
    }   // setTotalObjects
    
    /**
     * Create and return a new {@link AggGroupSet} that belongs to this AggregateResult.
     * 
     * @return  A new {@link AggGroupSet} that belongs to this AggregateResult.
     */
    public AggGroupSet addGroupSet() {
        AggGroupSet aggGroupSet = new AggGroupSet();
        if (m_groupSetList == null) {
            m_groupSetList = new ArrayList<AggGroupSet>();
        }
        m_groupSetList.add(aggGroupSet);
        return aggGroupSet;
    }   // addGroupSet
    
}   // class AggregateResult
