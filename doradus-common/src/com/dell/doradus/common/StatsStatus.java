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

import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Holds the results of a get statistics refresh status command. The object is basically a
 * 2-level map of table names to statistic names to refresh status, which reflects the
 * recalculation status of a set of statistics. It can then be serialized to/from XML or
 * JSON via the {@link UNode} class.
 */
public class StatsStatus {
    // Map of table name to statistic name-to-status map:
    private final SortedMap<String, SortedMap<String, String>> m_statMap;

    /**
     * Create an empty StatsStatus object.
     */
    public StatsStatus() {
        m_statMap = new TreeMap<String, SortedMap<String, String>>();
    }   // constructor
    
    /**
     * Add the given status description for the given statistic to this object.
     * 
     * @param statDef   {@link StatisticDefinition} of a statistic.
     * @param status    Recalculation/status of the statistic as a string.
     */
    public void addStatus(StatisticDefinition statDef, String status) {
        // Find or create table map.
        SortedMap<String, String> tableMap = m_statMap.get(statDef.getTableName());
        if (tableMap == null) {
            tableMap = new TreeMap<String, String>();
            m_statMap.put(statDef.getTableName(), tableMap);
        }
        
        // Wrap the status values in a StatStatus and add to the table map indexed by
        // statistic name.
        tableMap.put(statDef.getStatName(), status);
    }   // addStatus

    /**
     * Get the table names for which this StatsStatus has statistic refresh information.
     * 
     * @return  The table names for which this StatsStatus has statistic refresh
     *          information. Normally, a StatsStatus has information for a single table.
     */
    public Set<String> getTableNames() {
        return m_statMap.keySet();
    }   // getTableNames
    
    /**
     * Get a map of statistic refresh statuses for the given table. Null is returned if
     * there are no statistic refresh statuses for the given table. Otherwise, each key
     * in the map is a statistic name and its value is the refresh status of the
     * corresponding statistic.
     * 
     * @param tableName Name of a table.
     * @return          Statistic name-to-status map of statistics owned by the given
     *                  table. Null if there is are statistics or refresh information
     *                  for the table.
     */
    public Map<String, String> getTableStats(String tableName) {
        return m_statMap.get(tableName);
    }   // getTableStats
    
    /**
     * Serialize this StatsStatus as a UNode tree.
     * @return  This StatsStatus serialized as a UNode tree.
     */
    public UNode toDoc() {
        UNode result = UNode.createMapNode("stats-status");
        UNode tablesNode = result.addMapNode("tables");
        for (String tableName : m_statMap.keySet()) {
            UNode tableNode = tablesNode.addMapNode(tableName, "table");
            UNode statsNode = tableNode.addMapNode("statistics");
            SortedMap<String, String> tableStatMap = m_statMap.get(tableName); 
            for (String statName : tableStatMap.keySet()) {
                statsNode.addValueNode(statName, tableStatMap.get(statName), "statistic");
            }
        }
        return result;
    }   // toDoc

    /**
     * Deserialize a UNode tree into a StatsStatus object. This method expects the root
     * node of the UNode tree created by {@link #toDoc()}.
     * 
     * @param rootNode  Root node of a {@link UNode} tree created via {@link #toDoc()}.
     */
    public void parse(UNode rootNode) {
        Utils.require(rootNode != null, "rootNode");
        Utils.require(rootNode.isMap(), "Root node must be a map");
        Utils.require("stats-status".equals(rootNode.getName()),
                      "Root node must be 'stats-status': %s", rootNode.getName());
        Utils.require(rootNode.getMemberCount() == 1,
                      "Single child node expected for root node 'stats-status'");
        
        UNode tablesNode = rootNode.getMember(0);
        Utils.require(tablesNode.isMap() && tablesNode.getName().equals("tables"),
                      "'tables' node expected: %s", tablesNode.getName());
        for (UNode tableNode : tablesNode.getMemberList()) {
            String tableName = tableNode.getName();
            Utils.require(tablesNode.getMemberCount() == 1,
                          "Single child node expected for table node: %s", tableName);
            SortedMap<String, String> tableStatMap = new TreeMap<>();
            m_statMap.put(tableName, tableStatMap);
            
            UNode statsNode = tableNode.getMember(0);
            Utils.require(statsNode.getName().equals("statistics"),
                          "Child of table node should be 'statistics': %s", statsNode.getName());
            for (UNode statNode : statsNode.getMemberList()) {
                Utils.require(statNode.isValue(), "Value node expected: %s", statNode.getName());
                tableStatMap.put(statNode.getName(), statNode.getValue());
            }
        }
    }   // parse
    
}   // class StatsStatus
