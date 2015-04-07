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

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Holds the definition of an application, including its options, tables, and their
 * fields.
 */
final public class ApplicationDefinition {
    // Application name (case-sensitive):
    private String m_appName;
    
    // Optional application key:
    private String m_key;
    
    // Map of options used by this application. Option names are case-sensitive.
    private final Map<String, String> m_optionMap =
        new HashMap<String, String>();
    
    // Map of table names (case-sensitive) to TableDefinitions:
    private final SortedMap<String, TableDefinition> m_tableMap =
        new TreeMap<String, TableDefinition>();

    /**
     * Test the given string for validity as an application name. A valid application
     * name must begin with a letter and contain only letters, digits, and underscores.
     * 
     * @param  appName  Candidate application name.
     * @return          True if name is syntactically valid.
     */
    public static boolean isValidName(String appName) {
        return appName != null &&
               appName.length() > 0 &&
               Utils.isLetter(appName.charAt(0)) &&
               Utils.allAlphaNumUnderscore(appName);
    }   // isValidName
    
    /**
     * Create a new empty ApplicationDefinition object. The object is not valid until
     * members have been set from parsing or set methods.
     */
    public ApplicationDefinition() {
    }   // constructor

    /**
     * Parse the application definition rooted at given UNode tree and copy its contents
     * into this object. The root node is the "application" object, so its name is the
     * application name and its child nodes are definitions such as "key", "options",
     * "fields", etc. An exception is thrown if the definition contains an error.
     *  
     * @param appNode   Root of a UNode tree that defines an application.
     */
    public void parse(UNode appNode) {
        assert appNode != null;
        
        // Verify application name and save it.
        Utils.require(isValidName(appNode.getName()),
                      "Invalid application name: " + appNode.getName());
        m_appName = appNode.getName();
        
        // Create an external link map, which will hold links defined in each table whose
        // extent is some other table. After parsing all tables, we'll verify these.
        Map<String, Map<String, FieldDefinition>> externalLinkMap =
            new HashMap<String, Map<String, FieldDefinition>>();
        
        // Iterate through the application object's members.
        for (String childName : appNode.getMemberNames()) {
            // See if we recognize this member.
            UNode childNode = appNode.getMember(childName);
            
            // "key"
            if (childName.equals("key")) {
                // Must be a value.
                Utils.require(childNode.isValue(),
                              "'key' value must be a string: " + childNode);
                Utils.require(m_key == null,
                              "'key' can be specified only once");
                m_key = childNode.getValue();
                
            // "options"
            } else if (childName.equals("options")) {
                // Each name in the map is an option name.
                for (String optName : childNode.getMemberNames()) {
                    // Option must be a value.
                    UNode optNode = childNode.getMember(optName);
                    Utils.require(optNode.isValue(),
                                  "'option' must be a value: " + optNode);
                    setOption(optNode.getName(), optNode.getValue());
                }
                
            // "tables"
            } else if (childName.equals("tables")) {
                // Should be specified only once.
                Utils.require(m_tableMap.size() == 0,
                              "'tables' can be specified only once");
                
                // Parse the table definitions, adding them to this app def and building
                // the external link map as we go.
                for (UNode tableNode : childNode.getMemberList()) {
                    // This will throw if the table definition has an error.
                    TableDefinition tableDef = new TableDefinition(this);
                    tableDef.parse(tableNode, externalLinkMap);
                    addTable(tableDef);
                }
                
            // "schedules"
            } else if (childName.equals("schedules")) {
                for (UNode schedNode : childNode.getMemberList()) {
                    parseSchedule(schedNode);
                }
                
            // Unrecognized
            } else {
                Utils.require(false, "Unrecognized 'application' element: " + childName);
            }
        }
        
        // Finalize this application definition, including external link validation.
        finalizeDefinition(externalLinkMap);
    }   // parse(UNode)

    ///// Getters

    /**
     * Return this application's name.
     * 
     * @return  Application name (case-sensitive).
     */
    public String getAppName() {
        return m_appName;
    }   // getName
    
    /**
     * Get this application's key. If no key has been defined for the application, null is
     * returned.
     * 
     * @return This application's key, if any.
     */
    public String getKey() {
        return m_key;
    }   // getKey
    
    /**
     * Indicate whether or not this application tables to be implicitly created.
     * 
     * @return  True if this application tables to be implicitly created.
     */
    public boolean allowsAutoTables() {
        String optValue = getOption(CommonDefs.AUTO_TABLES);
        return optValue != null && optValue.equalsIgnoreCase("true");
    }   // allowsAutoTables
    
    /**
     * Get the value of the given option such as "AutoTables". Null is returned if the
     * option has not been set.
     *  
     * @param optName   Option name (case-sensitive).
     * @return          Option value or null if no value has been set.
     */
    public String getOption(String optName) {
        return m_optionMap.get(optName);
    }   // getOption
    
    /**
     * Get a Set<String> of all option names currently defined for this application. For
     * each option name in the returned set, {@link #getOption(String)} can be called to
     * get the value of that option.
     * 
     * @return  Set of all option names currently defined for this table. The set will be
     *          empty if no options are defined.
     */
    public Set<String> getOptionNames() {
        return m_optionMap.keySet();
    }   // getOptionNames
    
    /**
     * Get the resource path for this application.
     * 
     * @return  String in the form /Tenant:{tenant}/Application:{application}
     */
    public String getPath() {
        if (!m_optionMap.containsKey(CommonDefs.OPT_TENANT)) {
            return "/Tenant:?/Application:" + m_appName;
        } else {
            return "/Tenant:" + m_optionMap.get(CommonDefs.OPT_TENANT) + "/Application:" + m_appName;
        }
    }   // getPath
    
    /**
     * Get the StorageService option for this application. Null is returned if the option
     * has not been set.
     * 
     * @return  The StorageService option for this application.
     */
    public String getStorageService() {
        return getOption(CommonDefs.OPT_STORAGE_SERVICE);
    }   // getStorageService
    
    /**
     * Return the {@link TableDefinition} for the table with the given name or null if
     * this application does not know about such a table.
     * 
     * @param tableName Name of table.
     * @return          {@link TableDefinition} of corresponding table, or null if
     *                  unknown to this application.
     */
    public TableDefinition getTableDef(String tableName) {
        return m_tableMap.get(tableName);
    }   // getTableDef
    
    /**
     * Get the map of all {@link TableDefinition}s owned by this application indexed by
     * table name. This map is not copied so the caller must only read!
     * 
     * @return  The map of all {@link TableDefinition}s owned by this application.
     */
    public Map<String, TableDefinition> getTableDefinitions() {
        return m_tableMap;
    }   // getTableDefinitions

    /**
     * Get this application's definition including tables, schedules, etc. as a
     * {@link UNode} tree.
     * 
     * @return  The root of a {@link UNode} tree.
     */
    public UNode toDoc() {
        // The root node is always a MAP whose name is the application's name. In case it
        // is serialized to XML, we set this node's tag name to "application".
        UNode appNode = UNode.createMapNode(m_appName, "application");
        
        // Add the application's key.
        if (!Utils.isEmpty(m_key)) {
            appNode.addValueNode("key", m_key);
        }
        
        // Add options, if any, in a MAP node.
        if (m_optionMap.size() > 0) {
            UNode optsNode = appNode.addMapNode("options");
            for (String optName : m_optionMap.keySet()) {
                if (!optName.equals(CommonDefs.OPT_TENANT)) {    // don't include for now
                    // Set each option's tag name to "option" for XML's sake.
                    optsNode.addValueNode(optName, m_optionMap.get(optName), "option");
                }
            }
        }
        
        // Add tables, if any, in a MAP node.
        if (m_tableMap.size() > 0) {
            UNode tablesNode = appNode.addMapNode("tables");
            for (TableDefinition tableDef : m_tableMap.values()) {
                tablesNode.addChildNode(tableDef.toDoc());
            }
        }
        
        return appNode;
    }   // toDoc
    
    // For debugging:
    @Override
    public String toString() {
        return "Application '" + m_appName + "'";
    }   // toString()
    
    ///// Setters
    
    /**
     * Set this ApplicationDefinition's application name to the given value. An exception
     * is thrown if the name is invalid. This method should only be used when building-up
     * an application definition: it does not change the name of an existing application.
     * 
     * @param appName   Application name for this definition.
     */
    public void setAppName(String appName) {
        Utils.require(isValidName(appName), "Invalid application name: " + appName);
        m_appName = appName;
    }   // setAppName
    
    /**
     * Set this ApplicationDefinition's key to the given value. This method should only be
     * used when building-up an application definition: it does not change the key of an
     * existing application.
     * 
     * @param key   Application key for this definition.
     */
    public void setKey(String key) {
        m_key = key;
    }   // setKey
    
    /**
     * Set the option with the given name to the given value. If the option value is null,
     * the option is "unset". If the option has an existing value, it is replaced.
     * 
     * @param optName   Name of option to set (case-sensitive).
     * @param optValue  New value of option or null to "unset".
     */
    public void setOption(String optName, String optValue) {
        if (optValue == null) {
            m_optionMap.remove(optName);
        } else {
            m_optionMap.put(optName, optValue.trim());
        }
    }   // setOption
    
    /**
     * Add the given table definition to this application. This method assumes that the
     * table definition has been validated and the corresponding database table has been
     * (or will be) created. It throws an IllegalArgumentException if the table definition
     * currently belongs to an application with a different name or its name is not unique
     * within the already-defined tables. If the table definition looks OK, it is
     * transferred to this application by adding it to the table map and by setting the
     * table's application definition to us. 
     * 
     * @param  tableDef {@link TableDefinition} of a new table.
     */
    public void addTable(TableDefinition tableDef) {
        // Ensure this table was constructed with us as the target.
        if (!tableDef.getAppDef().getAppName().equals(this.getAppName())) {
            throw new IllegalArgumentException("Attempt to define table in wrong application: " +
                                               tableDef.getTableName());
        }
        
        // Ensure this table is unique.
        if (m_tableMap.containsKey(tableDef.getTableName())) {
            throw new IllegalArgumentException("Attempt to add duplicate table: " +
                                               tableDef.getTableName());
        }
        
        // Looks fine. Transfer to us.
        tableDef.setApplication(this);
        m_tableMap.put(tableDef.getTableName(), tableDef);
    }   // addTable

    /**
     * Remove the given table from this application, presumably because it has been
     * deleted.
     * 
     * @param tableDef  {@link TableDefinition} to be deleted.
     */
    public void removeTableDef(TableDefinition tableDef) {
        assert tableDef != null;
        assert tableDef.getAppDef() == this;
        
        m_tableMap.remove(tableDef.getTableName());
    }   // removeTableDef

    
    ///// Private Methods
    
    // Support legacy <schedule> for a while. We only support application- and table-level
    // data-aging, which we convert into application- and table-level options.
    // TODO: Delete this when <schedule> is no longer parsed in schemas.
    private void parseSchedule(UNode schedNode) {
        String tableName = null;
        String taskType = null;
        String schedule = null;
        for (String attrName : schedNode.getMemberNames()) {
            UNode attrNode = schedNode.getMember(attrName);
            Utils.require(attrNode.isValue(),
                          "Schedule attribute value must be a string: " + attrNode);
            String attrValue = attrNode.getValue();
            switch (attrName.toLowerCase()) {
            case "type":
                Utils.require(taskType == null, "'type' attribute can be specified only once");
                Utils.require(attrValue.equals("data-aging"), "Unrecognized 'type': " + attrValue);
                taskType = attrValue;
                break;

            case "table":
                Utils.require(tableName == null, "'table' can be specified only once");
                tableName = attrValue;
                break;
                
            case "value":
                Utils.require(schedule == null, "'value' can be specified only once");
                schedule = attrValue;
                break;
                
            default:
                Utils.require(false, "Unrecognized attribute: " + attrName);
            }
        }

        Utils.require(taskType != null, "Schedule is missing 'type': " + schedNode);
        Utils.require(schedule != null, "Schedule is missing 'value': " + schedNode);
        String agingFreq = convertCronValue(schedule);
        if (Utils.isEmpty(tableName)) {
            setOption(CommonDefs.OPT_AGING_CHECK_FREQ, agingFreq);
        } else {
            TableDefinition tableDef = getTableDef(tableName);
            Utils.require(tableDef != null, "Unknown table in 'schedule': " + tableName);
            tableDef.setOption(CommonDefs.OPT_AGING_CHECK_FREQ, agingFreq);
        }
    }   // parseSchedule

    // Convert the given cron expression into a simple frequency value such as "5 MINUTES".
    // The cron expression has the following general format:
    //      <minutes> <hours> <month day> <month> <week day>
    // For now, we're only going for rough conversions. So:
    //      "* ..."     becomes "1 MINUTE"
    //      "5 ..."     becomes "5 MINUTES"
    //      "0 * ..."   becomes "1 HOUR"
    //      "0 3 ..."   becomes "3 HOURS"
    //      "0 0 * ..." becomes "1 DAY"
    // And so forth.
    // TODO: Delete this when <schedule> is no longer parsed in schemas.
    private String convertCronValue(String cronExpr) {
        if (Utils.isEmpty(cronExpr)) {
            return "1 DAY";
        }
        String[] parts = cronExpr.split(" ");
        String minutes = convertCronPart(parts, 0);
        String hours = convertCronPart(parts, 1);
        String days = convertCronPart(parts, 2);
        String months = convertCronPart(parts, 3);
        if (!minutes.equals("0")) {
            return minutes.equals("*") ? "1 MINUTE" : minutes + " MINUTES";
        }
        if (!hours.equals("0")) {
            return hours.equals("*") ? "1 HOUR" : hours + " HOURS";
        }
        if (!days.equals("0")) {
            return days.equals("*") ? "1 DAY" : days + " DAYS";
        }
        if (!months.equals("0")) {
            return months.equals("*") ? "1 MONTH" : months + " MONTHS";
        }
        return "1 DAY"; // no idea what it was
    }
    
    // Convert the given cron part to a number or "*".
    private String convertCronPart(String[] parts, int index) {
        if (index >= parts.length) {
            return "*";
        }
        if (isAllDigits(parts[index])) {
            return parts[index];
        }
        return "0";
    }   // converCronParts
    
    // Return true if all chars in the string are digits.
    private boolean isAllDigits(String str) {
        for (int index = 0; index < str.length(); index++) {
            if (!Utils.isDigit(str.charAt(index))) {
                return false;
            }
        }
        return true;
    }   // isAllDigits

    // Make final application fix-ups and integrity checks.
    private void finalizeDefinition(Map<String, Map<String, FieldDefinition>> externalLinkMap) {
        // Verify all external link references.
        processExternalLinks(externalLinkMap);
    }   // finalizeDefinition

    // Verify all external links found, if any, while parsing an application definition.
    // We implicitly add tables and inverse links mentioned by cross-table table links,
    // and we look for contradictions.
    private void processExternalLinks(Map<String, Map<String, FieldDefinition>> externalLinkMap) {
        // Examine externally-referenced links in order of the table in which they were
        // mentioned.
        for (String tableName : externalLinkMap.keySet()) {
            // See if this table was defined in this application definition.
            TableDefinition tableDef = getTableDef(tableName);
            if (tableDef == null) {
                // Create an implicit table definition and add it to the application.
                tableDef = new TableDefinition(this, tableName);
                addTable(tableDef);
            }
            
            // Check this table to see if it defines each forward-referenced link field.
            Map<String, FieldDefinition> forwardLinks = externalLinkMap.get(tableName);
            for (String fieldName : forwardLinks.keySet()) {
                // inverseDef is the explicitly-defined link (in some other table) that
                // inferred this link field.
                FieldDefinition inverseDef = forwardLinks.get(fieldName);
                FieldDefinition linkDef = tableDef.getFieldDef(fieldName);
                if (linkDef == null) {
                    // Link was not been explicitly defined, so implicitly define it.
                    linkDef = new FieldDefinition(tableDef);
                    linkDef.setType(inverseDef.getType());  // link or xlink
                    linkDef.setName(fieldName);
                    linkDef.setLinkInverse(inverseDef.getName());
                    linkDef.setLinkExtent(inverseDef.getTableName());
                    linkDef.setCollection(true);
                    if (inverseDef.getType() == FieldType.XLINK) {
                        linkDef.setXLinkJunction("_ID");
                    }
                    tableDef.addFieldDefinition(linkDef);
                } else {
                    // The link was explicitly defined. Ensure it matches the inverse link
                    // that inferred it.
                    if (!linkDef.getLinkExtent().equals(inverseDef.getTableName()) ||
                        !linkDef.getLinkInverse().equals(inverseDef.getName()) ||
                        linkDef.getType() != inverseDef.getType()) {
                        throw new IllegalArgumentException(
                            "Link '" + linkDef.getName() +  "' in table '" + tableName +
                            "' conflicts with inverse link '" + inverseDef.getName() +
                            "' in table '" + inverseDef.getTableName() + "'");
                    }
                }
            }   // for link fields
        }   // for table names
    }   // processExternalLinks
    
    /**
     * Replaces of all occurences of aliases defined with this table, by their expressions.
     * Now a simple string.replace is used. 
     * 
     * @param str string to replace
     * @return string with replaced aliases. If there were no aliases, the string is unchanged.
     */
	public String replaceAliaces(String str) {
		if(str == null) return str;
		// for performance
		if(str.indexOf(CommonDefs.ALIAS_FIRST_CHAR) < 0) return str;
		
		PriorityQueue<AliasDefinition> aliasQueue = new PriorityQueue<AliasDefinition>(11, new Comparator<AliasDefinition>() {
			public int compare(AliasDefinition alias1, AliasDefinition alias2) {
				return alias2.getName().length() - alias1.getName().length();
			}
		});
		
		for(TableDefinition tableDef : getTableDefinitions().values()) {
			for(AliasDefinition aliasDef : tableDef.getAliasDefinitions()) {
				aliasQueue.add(aliasDef);					
			}
		}
		while(true) {
			String newstring = str; 
			 while (aliasQueue.size() != 0) {			        
				AliasDefinition aliasDef = aliasQueue.poll();
		        newstring = newstring.replace(aliasDef.getName(), aliasDef.getExpression());
			}	 
			if(newstring.equals(str)) break;
			str = newstring;
		}
		return str;
	}

}   // class ApplicationDefinition
