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

import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Holds the definition of an object table.
 */
final public class TableDefinition {
    // Milliseconds within various units:
    private static final long MILLIS_IN_HOUR = 1000 * 60 * 60;      // 3,600,000
    private static final long MILLIS_IN_DAY = MILLIS_IN_HOUR * 24;  // 86,400,000
    private static final long MILLIS_IN_WEEK = MILLIS_IN_DAY * 7;   // 604,800,000
    
    // Application to which we belong:
    private ApplicationDefinition m_appDef;
    
    // This table's logical (Doradus) application, unique to its owning application:
    private String m_tableName;
    
    // Map of options used by this table. Option names are stored down-cased.
    private final Map<String, String> m_optionMap =
        new HashMap<String, String>();
    
    // Sharding start date and granularity from parsed options:
    private GregorianCalendar   m_shardingStartDate;
    private ShardingGranularity m_shardingGranularity;
    
    // Map of fields that this table owns. The map is final, but the field definitions
    // can be updated.
    private final SortedMap<String, FieldDefinition> m_fieldDefMap =
        new TreeMap<String, FieldDefinition>();
    
    // Map of statistics that this table owns:
    private final SortedMap<String, StatisticDefinition> m_statDefMap =
        new TreeMap<String, StatisticDefinition>();

    // Map of alias definitions sorted by name:
    private final SortedMap<String, AliasDefinition> m_aliasDefMap =
        new TreeMap<String, AliasDefinition>();
    
    // The values we support for sharding-granularity:
    public enum ShardingGranularity {
        HOUR,
        DAY,
        WEEK,
        MONTH;
        
        /**
         * Return the {@link ShardingGranularity} for the given granularity in string form.
         * If the value is unrecognized, null is returned. This method can be used to
         * validate granularity values.
         * 
         * @param  value    Sharding granularity as a string (case-insensitive): e.g., DAY,
         *                  WEEK, or MONTH.
         * @return          {@link ShardingGranularity} for given string or null if the
         *                  value is unrecognized.
         */
        public static ShardingGranularity fromString(String value) {
            // Since we only have a few values, just search.
            for (ShardingGranularity gran : values()) {
                if (gran.toString().equalsIgnoreCase(value)) {
                    return gran;
                }
            }
            return null;
        }   // fromString
        
    }   // enum ShardingGranularity
    
    /**
     * Create a new empty TableDefinition that belongs to the given application. It will
     * not have a name or any fields until they parsed or added.
     * 
     * @param appDef    Application to which table belongs.
     */
    public TableDefinition(ApplicationDefinition appDef) {
        assert appDef != null;
        m_appDef = appDef;
    }   // constructor

    /**
     * Create a new TableDefinition with the given name and belonging to the given
     * application but with all default options and no initial fields. This constructor
     * is used by implicitly-defined tables.
     * 
     * @param appDef    Application to which table belongs.
     * @param tableName Unique table name.
     */
    public TableDefinition(ApplicationDefinition appDef, String tableName) {
        assert appDef != null;
        assert tableName != null && tableName.length() > 0;
        
        // Validate table name.
        if (!isValidTableName(tableName)) {
            throw new IllegalArgumentException("Invalid table name: " + tableName);
        }
        
        // Save parameters
        m_appDef = appDef;
        m_tableName = tableName;
    }   // constructor
    
    /**
     * Parse a table definition rooted at the given UNode. The given node is the table
     * definition, hence its name is the table name. The node must be a MAP whose child
     * nodes are table definitions such as "options" and "fields". This method is used
     * when the table definition is not part of a larger application definition that may
     * contain link forward-references.
     * 
     * @param tableNode         {@link UNode} whose name is the table's name and whose
     *                          child nodes define the tables features.
     */
    public void parse(UNode tableNode) {
        parse(tableNode, null);
    }   // parse
    
    /**
     * Parse a table definition in rooted at the given UNode. The given node is the table
     * definition, hence its name is the table name. The node must be a MAP whose child
     * nodes are table definitions such as "options" and "fields".
     * 
     * @param tableNode         {@link UNode} whose name is the table's name and whose
     *                          child nodes define the tables features.
     * @param externalLinkMap   Optional map of external link references. If this map is
     *                          null, we do not attempt to generate or validate external
     *                          link references.
     * <p> 
     *                          If this map is not null, it may have values when we're
     *                          called, and we may add it it. The map is <table name>-to-
     *                          <link name>-to-<field definition> where <table name>
     *                          appears to own the <link name>, which was defined as the
     *                          'inverse' of the link <field definition>. The caller must
     *                          verify that each <table name> and <link name> is valid
     *                          and/or implicitly defined.
     */
    public void parse(UNode                                     tableNode,
                      Map<String, Map<String, FieldDefinition>> externalLinkMap) {
        assert tableNode != null;
        assert m_appDef != null;
        assert m_fieldDefMap.isEmpty();
        assert m_statDefMap.isEmpty();
        assert m_optionMap.isEmpty();
        assert m_tableName == null;
        
        // Node must be a map or a VALUE with a name only (empty table def).
        Utils.require(tableNode.isMap() || (tableNode.isValue() && Utils.isEmpty(tableNode.getValue())),
                      "'table' definition must be a map of unique names: " + tableNode);
        
        // Verify table name and save it.
        Utils.require(isValidTableName(tableNode.getName()),
                      "Invalid table name: " + tableNode.getName());
        m_tableName = tableNode.getName();
        
        // Examine table node's children.
        for (String childName : tableNode.getMemberNames()) {
            // See if we recognize it.
            UNode childNode = tableNode.getMember(childName);
            
            // "fields"
            if (childName.equals("fields")) {
                // Value must be a map.
                Utils.require(childNode.isMap(),
                              "'fields' must be a map of unique names: " + childNode);
                
                // Process field definitions.
                for (String fieldName : childNode.getMemberNames()) {
                    // Create a FieldDefinition and parse the node's value into it.
                    // This will throw if the definition is invalid.
                    FieldDefinition fieldDef = new FieldDefinition(this);
                    fieldDef.parse(childNode.getMember(fieldName));
                    
                    // Ensure field name is unique and add it to the table's field map.
                    addFieldDefinition(fieldDef);
                    
                    // If the field is a group, verify that all nested field names are
                    // unique and add them to the field definition map.
                    if (fieldDef.isGroupField()) {
                        validateNestedFields(fieldDef);
                    }
                }
                
            // "statistics"
            } else if (childName.equals("statistics")) {
                // Value must be a map.
                Utils.require(childNode.isMap(),
                              "'statistics' must be a map of unique names: " + childNode);
                
                // Parse the each statistic definition.
                for (String statName : childNode.getMemberNames()) {
                    // Create a stat def and parse the details into it.
                    StatisticDefinition statDef = new StatisticDefinition(m_tableName);
                    statDef.parse(childNode.getMember(statName));
                    
                    // Ensure statistic names are unique.
                    Utils.require(!m_statDefMap.containsKey(statDef.getStatName()),
                                  "Statistic names must be unique: " + statDef.getStatName());
                    addStatDefinition(statDef);
                }
                
            // "options"
            } else if (childName.equals("options")) {
                // Value should be a map.
                Utils.require(childNode.isMap(),
                              "'options' must be a map of unique names: " + childNode);
                
                // Examine each option.
                for (String optName : childNode.getMemberNames()) {
                    // Each option must be a simple value and specified only once.
                    UNode optNode = childNode.getMember(optName);
                    Utils.require(optNode.isValue(),
                                  "'option' must be a value: " + optNode);
                    Utils.require(getOption(optName) == null,
                                  "Option '" + optName + "' can only be specified once");
                    
                    // Add option to option map, which performs some immediate validation.
                    setOption(optName, optNode.getValue());
                }
                
            // "aliases"
            } else if (childName.equals("aliases")) {
                // Value should be a map.
                Utils.require(childNode.isMap(),
                              "'aliases' must be a map of unique names: " + childNode);
                
                // Parse and add each AliasDefinition.
                for (String aliasName : childNode.getMemberNames()) {
                    AliasDefinition aliasDef = new AliasDefinition(m_tableName);
                    aliasDef.parse(childNode.getMember(aliasName));
                    addAliasDefinition(aliasDef);
                }
                
            // Unrecognized
            } else {
                Utils.require(false, "Unrecognized 'table' element: " + childName);
            }
        }
        
        // Finialize this table definition, including external link validation.
        finalizeTableDefinition(externalLinkMap);
    }   // parse

    /**
     * Indicate if the given string is a valid table name. Table names must begin with a
     * letter and consist of all letters, digits, and underscores. The special names
     * "Counters", "Links", and "Statistics" are also not allowed.
     * 
     * @param tableName Candidate table name.
     * @return          True if the name is not null, not empty, starts with a letter, and
     *                  consists of only letters, digits, and underscores.
     */
    public static boolean isValidTableName(String tableName) {
        return tableName != null &&
               tableName.length() > 0 &&
               Utils.isLetter(tableName.charAt(0)) &&
               Utils.allAlphaNumUnderscore(tableName) &&
               !tableName.equals("Counters") &&
               !tableName.equals("Links") &&
               !tableName.equals("Statistics");
    }   // isValidTableName

    ///// Getters

    /**
     * Compute and return the date at which the shard with the given number starts based
     * on this table's sharding options. For example, if sharding-granularity is MONTH
     * and the sharding-start is 2012-10-15, then shard 2 starts on 2012-11-01.
     * 
     * @param shardNumber   Shard number (> 0).
     * @return              Date on which the given shard starts. It will >= this table's
     *                      sharding-start option.
     */
    public Date computeShardStart(int shardNumber) {
        assert isSharded();
        assert shardNumber > 0;
        assert m_shardingStartDate != null;
        
        // Shard #1 always starts on the sharding-start date.
        Date result = null;
        if (shardNumber == 1) {
            result = m_shardingStartDate.getTime();
        } else {
            // Clone m_shardingStartDate and adjust by shard number.
            GregorianCalendar shardDate = (GregorianCalendar)m_shardingStartDate.clone();
            switch (m_shardingGranularity) {
            case HOUR:
                // Increment start date HOUR by shard number - 1.
                shardDate.add(Calendar.HOUR_OF_DAY, shardNumber - 1);
                break;
                
            case DAY:
                // Increment start date DAY by shard number - 1.
                shardDate.add(Calendar.DAY_OF_MONTH, shardNumber - 1);
                break;
                
            case WEEK:
                // Round the sharding-start date down to the MONDAY of the same week.
                // Then increment it's DAY by (shard number - 1) * 7.
                shardDate = Utils.truncateToWeek(m_shardingStartDate);
                shardDate.add(Calendar.DAY_OF_MONTH, (shardNumber - 1) * 7);
                break;
            
            case MONTH:
                // Increment start date MONTH by shard number - 1, but the day is always 1.
                shardDate.add(Calendar.MONTH, shardNumber - 1);
                shardDate.set(Calendar.DAY_OF_MONTH, 1);
                break;
            }
            result = shardDate.getTime();
        }
        assert computeShardNumber(result) == shardNumber;
        return result;
    }   // computeShardStart
    
    /**
     * Compute the shard number of an object belong to this table with the given
     * sharding-field value. This method should only be called on a sharded table for
     * which the sharding-field, sharding-granularity, and sharding-start options have
     * been set. The value returned will be 0 if the given date falls before the
     * sharding-start date. Otherwise it will be >= 1, representing the shard in which
     * the object should reside.
     *  
     * @param shardingFieldValue    Value of an object's sharding-field as a Date in the
     *                              UTC time zone.
     * @return                      Shard number representing the shard in which the
     *                              object should reside.
     */
    public int computeShardNumber(Date shardingFieldValue) {
        assert shardingFieldValue != null;
        assert isSharded();
        assert m_shardingStartDate != null;
        
        // Convert the sharding field value into a calendar object. Note that this value
        // will have non-zero time elements.
        GregorianCalendar objectDate = new GregorianCalendar(Utils.UTC_TIMEZONE);
        objectDate.setTime(shardingFieldValue);
        
        // Since the start date has no time elements, if the result is negative, the object's
        // date is before the start date. 
        if (objectDate.getTimeInMillis() < m_shardingStartDate.getTimeInMillis()) {
            // Object date occurs before sharding start. Shard number -> 0.
            return 0;
        }

        // Determine the shard number based on the granularity.
        int shardNumber = 1;
        switch (m_shardingGranularity) {
        case HOUR:
            // Increment shard number by difference in millis-per-hours.
            shardNumber += (objectDate.getTimeInMillis() - m_shardingStartDate.getTimeInMillis()) / MILLIS_IN_HOUR;
            break;
            
        case DAY:
            // Since the dates are in UTC, which doesn't have DST, the difference in days
            // is simply the difference in whole number of millis-per-day.
            shardNumber += (objectDate.getTimeInMillis() - m_shardingStartDate.getTimeInMillis()) / MILLIS_IN_DAY;
            break;
            
        case WEEK:
            // Truncate the sharding-start date to MONDAY. The difference in weeks is then
            // the shard increment.
            GregorianCalendar shard1week = Utils.truncateToWeek(m_shardingStartDate);
            shardNumber += (objectDate.getTimeInMillis() - shard1week.getTimeInMillis()) / MILLIS_IN_WEEK;
            break;
            
        case MONTH:
            // Difference in months is 12 * (difference in years) + (difference in months)
            int diffInMonths = ((objectDate.get(Calendar.YEAR) - m_shardingStartDate.get(Calendar.YEAR)) * 12) +
                               (objectDate.get(Calendar.MONTH) - m_shardingStartDate.get(Calendar.MONTH));
            shardNumber += diffInMonths;
            break;
            
        default:
            Utils.require(false, "Unknown sharding-granularity: " + m_shardingGranularity);
        }
        return shardNumber;
    }   // computeShardNumber
    
    /**
     * Get the {@link ApplicationDefinition} to which this table definition applies. Note
     * that while parsing table definitions, a TableDefinition object may not yet be known
     * to the ApplicationDefinition to which it points.
     * 
     * @return {@link ApplicationDefinition} to which this table definition applies.
     */
    public ApplicationDefinition getAppDef() {
        return m_appDef;
    }   // getAppDef
    
    /**
     * Get the name of the table represented by this table definition. This is the logical
     * or "REST name", which is unique among tables owned by the application but not
     * necessarily between applications. Also, not that the database table name will be
     * different than this name.
     * 
     * @return  This table's name.
     */
    public String getTableName() {
        return m_tableName;
    }   // getTableName
    
    /**
     * Get the field definitions for this table as an Collection&lt;{@link FieldDefinition}&gt;
     * object. All outer and nested group, link, and scalar fields are returned. To expand
     * group fields without traversing the same {@link FieldDefinition} twice, call this
     * method and skip fields for which {@link FieldDefinition#isNestedField()} is true.
     * Then, for each field for which {@link FieldDefinition#isGroupField()} is true, call
     * {@link FieldDefinition#getNestedFields()} to iterate its immediate nested fields.
     * <p>
     * NOTE: The field definitions returned by this method's iterator are not copied, so
     * be careful not to modify them!
     * 
     * @return  An iterator that returns all {@link FieldDefinition}s owned by this table.
     */
    public Collection<FieldDefinition> getFieldDefinitions() {
        return m_fieldDefMap.values();
    }   // getFieldDefinitions

    /**
     * Get the {@link FieldDefinition} for the field with the given name, used by this
     * table. If there is no such field known, null is returned.
     * 
     * @param   fieldName   Candidate field name.
     * @return              {@link FieldDefinition} of corresponding field if known to
     *                      this table, otherwise null.
     */
    public FieldDefinition getFieldDef(String fieldName) {
        return m_fieldDefMap.get(fieldName);
    }   // getFieldDef

    /**
     * Return the value of the option with the given name or null if there is no such
     * option stored. This method does not verify that the option name is actually
     * valid.
     * 
     * @param optionName    Name of option (case-insensitive).
     * @return              Value of current value for option or null if there isn't one.
     */
    public String getOption(String optionName) {
        return m_optionMap.get(optionName.toLowerCase());
    }   // getOption
    
    /**
     * Get a Set<String> of all option names currently defined for this table. For each
     * option name in the returned set, {@link #getOption(String)} can be called to get
     * the value of that option.
     * 
     * @return  Set of all option names currently defined for this table. The set will be
     *          empty if no options are defined.
     */
    public Set<String> getOptionNames() {
        return m_optionMap.keySet();
    }   // getOptionNames
    
    /**
     * Get the {@link FieldDefinition} of the sharding-field for this table. If this table
     * is not sharded, the sharding-field option has not yet been set, or the sharding
     * field has not yet been defined, null is returned.
     *  
     * @return  The {@link FieldDefinition} of the sharding-field for this table or null
     *          if the table is not sharded or the sharding-field has not yet been defined.
     */
    public FieldDefinition getShardingField() {
        if (!isSharded()) {
            return null;
        }
        String shardingFieldNme = getOption(CommonDefs.OPT_SHARDING_FIELD);
        if (shardingFieldNme == null) {
            return null;
        }
        return getFieldDef(shardingFieldNme);   // may return null
    }   // getShardingField

    /**
     * Determine the shard number of the given object for this table. If this table is
     * not sharded or the object has no value for its sharding field
     * (see {@link #getShardingField()}), the shard number is 0. Otherwise, the
     * sharding-field value is used to determine the shard number based on the table's
     * sharding start and granularity.
     * <p>
     * Note: The caller must ensure that if a value exists for the sharding-field, it is
     * loaded into the given DBObject. Otherwise, this method will incorrectly assume the
     * sharding value has not been set and therefore imply shard 0.
     * 
     * @param dbObj     {@link DBObject} to determine shard number.
     * @return          0 if the object's owning table is not sharded, the object has no
     *                  sharding-field value, or the object's sharding field value places
     *                  it before sharding was started. Otherwise, the value is > 0.
     */
    public int getShardNumber(DBObject dbObj) {
        if (!isSharded()) {
            return 0;
        }
        
        String value = dbObj.getFieldValue(getShardingField().getName());
        if (value == null) {
            return 0;
        }
        Date shardingFieldValue = Utils.dateFromString(value);
        return computeShardNumber(shardingFieldValue);
    }   // getShardNumber

    /**
     * Get the alias definitions owned by this table as an Iterable object. NOTE: The
     * definitions are not copied, hence the caller must be careful not to modify it!
     * 
     * @return AliasDefinitions owne by this table as an Iterable.
     */
    public Iterable<AliasDefinition> getAliasDefinitions() {
        return m_aliasDefMap.values();
    }   // getAliasDefinitions
    
    /**
     * Get the {@link AliasDefinition} belonging to this table with the given name, or
     * null if this table does not own such an alias.
     * 
     * @param aliasName Name of alias owned by this table.
     * @return          {@link AliasDefinition} of alias or null if unknown.
     */
    public AliasDefinition getAliasDef(String aliasName) {
        return m_aliasDefMap.get(aliasName);
    }   // getAliasDef
    
    /**
     * Get the statistic definitions for statistics owned by this table as an Iterable
     * object. NOTE: The statistic definitions are not copied, hence the caller must be
     * careful not to modify it!
     * 
     * @return StatisticDefinitions owned by this table as an Iterable.
     */
    public Iterable<StatisticDefinition> getStatDefinitions() {
        return m_statDefMap.values();
    }   // getStatDefinitions
    
    /**
     * Get statistics names.
     * 
     * @return Set of statistics names
     */
    public Set<String> getStatDefNames() {
    	return m_statDefMap.keySet();
    }

    /**
     * Get the {@link StatisticDefinition} belonging to this table with the given name, or
     * null if this table does not own such a statistic.
     * 
     * @param statName  Name of statistic owned by this table.
     * @return          {@link StatisticDefinition} of statistic or null if unknown.
     */
    public StatisticDefinition getStatDef(String statName) {
        return m_statDefMap.get(statName);
    }   // getStatDef
    
    /**
     * Return true if the given field name is an MV scalar field. Since scalar fields
     * must be declared as MV, only predefined fields can be MV.
     * 
     * @param   fieldName   Candidate field name.
     * @return              True if the name is a known MV scalar field, otherwise false.
     */
    public boolean isCollection(String fieldName) {
        FieldDefinition fieldDef = m_fieldDefMap.get(fieldName);
        return fieldDef != null && fieldDef.isScalarField() && fieldDef.isCollection();
    }   // isCollection
    
    /**
     * Return true if this table definition possesses a FieldDefinition with the given name
     * that defines a Link field.
     * 
     * @param   fieldName   Candidate field name.
     * @return              True if the name is a known Link field, otherwise false.
     */
    public boolean isLinkField(String fieldName) {
        FieldDefinition fieldDef = m_fieldDefMap.get(fieldName);
        return fieldDef != null && fieldDef.isLinkField();
    }   // isLinkField

    /**
     * Return true if the option with the given name has been defined and its value is
     * "true".
     * 
     * @param  optName  Option name (case-insensitive).
     * @return          True if the option has been defined as "true"; false if not set or
     *                  explicitly set to something other than "true" (case-insensitive).
     */
    public boolean isOptionSet(String optName) {
        String optValue = getOption(optName);
        return optValue != null && optValue.equalsIgnoreCase("true");
    }   // isOptionSet
    
    /**
     * Checks whether the table is assigned for data aging.
     * @return	true if data aging is properly defined for the table, false otherwise.
     */
    public boolean isSetForAging() {
        if (getOption(CommonDefs.OPT_AGING_FIELD) == null || getOption(CommonDefs.OPT_RETENTION_AGE) == null) {
            return false;
        }
        RetentionAge retAge = new RetentionAge(getOption(CommonDefs.OPT_RETENTION_AGE));
        return retAge.getValue() > 0;
    }	// isSetForAging
    
    /**
     * Return true if this table definition possesses a FieldDefinition with the given name
     * that defines a scalar field or if the field is undefined and therefore considered a
     * text field.
     * 
     * @param   fieldName   Candidate field name.
     * @return              True if the name is a known scalar field, otherwise false.
     */
    public boolean isScalarField(String fieldName) {
        FieldDefinition fieldDef = m_fieldDefMap.get(fieldName);
        return fieldDef == null || fieldDef.isScalarField();
    }   // isScalarField
    
    /**
     * Return true if this table is sharded, meaning the sharding-field option has been
     * defined. Note, however, that we don't check to see if the sharding field itself
     * has been defined and is of the appropriate type.
     * 
     * @return  True if this table is sharded.
     */
    public boolean isSharded() {
        return getOption(CommonDefs.OPT_SHARDING_FIELD) != null;
    }   // isSharded
    
    /**
     * Serialize this table definition into a {@link UNode} tree and return the root node.
     * 
     * @return  The root node of a {@link UNode} tree for this table definition.
     */
    public UNode toDoc() {
        // The root node is a MAP, but we set its tag name to "table" for XML.
        UNode tableNode = UNode.createMapNode(m_tableName, "table");
        
        // Add options, if any.
        if (getOptionNames().size() > 0) {
            // Options node is a MAP. 
            UNode optsNode = tableNode.addMapNode("options");
            for (String optName : getOptionNames()) {
                // Set the tag name of each option to "option" for XML.
                optsNode.addValueNode(optName, getOption(optName), "option");
            }
        }
        
        // Add fields, if any.
        if (m_fieldDefMap.size() > 0) {
            // Fields node is a MAP. 
            UNode fieldsNode = tableNode.addMapNode("fields");
            
            // Add definitions of outer (non-nested) fields only. Each group field will
            // recurse to its nested fields.
            for (FieldDefinition fieldDef : m_fieldDefMap.values()) {
                if (!fieldDef.isNestedField()) {
                    fieldsNode.addChildNode(fieldDef.toDoc());
                }
            }
        }

        // Add "aliases", if any.
        if (m_aliasDefMap.size() > 0) {
            // Aliases node is a MAP. 
            UNode aliasesNode = tableNode.addMapNode("aliases");
            for (AliasDefinition aliasDef : m_aliasDefMap.values()) {
                aliasesNode.addChildNode(aliasDef.toDoc());
            }
        }
        
        // Add a "statistics", if any.
        if (m_statDefMap.size() > 0) {
            // Statistics node is a MAP. 
            UNode statsNode = tableNode.addMapNode("statistics");
            for (StatisticDefinition statDef : m_statDefMap.values()) {
                statsNode.addChildNode(statDef.toDoc());
            }
        }
        
        return tableNode;
    }   // toDoc

    // Return "Table 'foo'"
    @Override
    public String toString() {
        return "Table '" + m_tableName + "'";
    }   // toString
    
    ///// Setters
    
    /**
     * Transfer this table definition to the given {@link ApplicationDefinition} by
     * re-assigning its ApplicationDefinition pointer to the given one.
     * 
     * @param appDef    New {@link ApplicationDefinition} owner of this table.
     */
    public void setApplication(ApplicationDefinition appDef) {
        assert appDef != null;
        m_appDef = appDef;
    }   // setApplication
    
    /**
     * Set this table's name to the given value. If the name is not a valid table name,
     * an exception is thrown. This method should only be used when building a new
     * TableDefinition -- it does not change the name of an existing table.
     * 
     * @param tableName New name of this table.
     */
    public void setTableName(String tableName) {
        if (!isValidTableName(tableName)) {
            throw new IllegalArgumentException("Invalid table name: " + tableName);
        }
        m_tableName = tableName;
    }   // setTableName
    
    /**
     * Set the option with the given name to the given value. This method does not
     * validate that the given option name and value are valid since options are
     * storage service-specific.
     *  
     * @param optionName    Option name. This is down-cased when stored.
     * @param optionValue   Option value. Cannot be null.
     */
    public void setOption(String optionName, String optionValue) {
        // Ensure option value is not empty and trim excess whitespace.
        Utils.require(optionName != null, "optionName");
        Utils.require(optionValue != null && optionValue.trim().length() > 0,
                      "Value for option '" + optionName + "' can not be empty");
        optionValue = optionValue.trim();
        m_optionMap.put(optionName.toLowerCase(), optionValue);
        
        // sharding-granularity and sharding-start are validated here since we must set
        // local members when the table's definition is parsed.
        if (optionName.equalsIgnoreCase(CommonDefs.OPT_SHARDING_GRANULARITY)) {
            m_shardingGranularity = ShardingGranularity.fromString(optionValue);
            Utils.require(m_shardingGranularity != null,
                          "Unrecognized 'sharding-granularity' value: " + optionValue);
        } else if (optionName.equalsIgnoreCase(CommonDefs.OPT_SHARDING_START)) {
            Utils.require(isValidShardDate(optionValue),
                          "'sharding-start' must be YYYY-MM-DD: " + optionValue);
            m_shardingStartDate = new GregorianCalendar(Utils.UTC_TIMEZONE);
            m_shardingStartDate.setTime(Utils.dateFromString(optionValue));
        }
    }   // setOption

    /**
     * Add the given {@link FieldDefinition} object to this table's list of known fields.
     * The field should already be validated. If the field is a Link field, it is assigned
     * the next available link ID for this table.
     * 
     * @param fieldDef  New {@link FieldDefinition} to add to this table.
     */
    public void addFieldDefinition(FieldDefinition fieldDef) {
        // Assure field name is unique and add the definition to the map.
        Utils.require(!m_fieldDefMap.containsKey(fieldDef.getName()),
                      "Field names must be unique: " + fieldDef.getName());
        m_fieldDefMap.put(fieldDef.getName(), fieldDef);
    }   // addFieldDefinition

    /**
     * Get the TableDefinition of the "extent" table for the given link field.
     * 
     * @param  linkDef  {@link FieldDefinition} for a link field.
     * @return          {@link TableDefinition} for link's extent (target) table.
     */
    public TableDefinition getLinkExtentTableDef(FieldDefinition linkDef) {
        assert linkDef != null;
        assert linkDef.isLinkType();
        
        TableDefinition tableDef = m_appDef.getTableDef(linkDef.getLinkExtent());
        assert tableDef != null;
        return tableDef;
    }   // getLinkExtentTableDef
    
    /**
     * Examine the given column name and, if it represents an MV link value, add it to the
     * given MV link value map. If a link value is successfully extracted, true is returned.
     * If the column name is not in the format used for MV link values, false is returned.
     * 
     * @param colName        Column name from an object record belonging to this table (in
     *                       string form).
     * @param mvLinkValueMap Link value map to be updated if the column represents a valid
     *                       link value.
     * @return               True if a link value is extracted and added to the map; false
     *                       means the column name was not a link value.
     */
    public boolean extractLinkValue(String colName, Map<String, Set<String>> mvLinkValueMap) {
        // Link column names always begin with '~'.
        if (colName.length() == 0 || colName.charAt(0) != '~') {
            return false;
        }
        
        // A '/' should separate the field name and object value.
        int slashInx = colName.indexOf('/');
        if (slashInx < 0) {
            return false;
        }
        
        // Extract the field name and ensure we know about this field.
        String fieldName = colName.substring(1, slashInx);
        
        // Extract the link value's target object ID and add it to the value set for the field.
        String linkValue = colName.substring(slashInx + 1);
        Set<String> valueSet = mvLinkValueMap.get(fieldName);
        if (valueSet == null) {
            // First value for this field.
            valueSet = new HashSet<String>();
            mvLinkValueMap.put(fieldName, valueSet);
        }
        valueSet.add(linkValue);
        return true;
    }   // extractLinkValue
    
    ///// Private methods
    
    // Add the given alias definition to this table definition.
    private void addAliasDefinition(AliasDefinition aliasDef) {
        // Prerequisites:
        assert aliasDef != null;
        assert !m_aliasDefMap.containsKey(aliasDef.getName());
        assert aliasDef.getTableName().equals(this.getTableName());
        
        m_aliasDefMap.put(aliasDef.getName(), aliasDef);
    }   // addAliasDefinition

    // Add the given statistic definition to this table definition.
    private void addStatDefinition(StatisticDefinition newStatDef) {
        // Prerequisites:
        assert newStatDef != null;
        assert !m_statDefMap.containsKey(newStatDef.getStatName());
        assert newStatDef.getTableName().equals(this.getTableName());
        
        m_statDefMap.put(newStatDef.getStatName(), newStatDef);
    }   // addStatDefinition

    // Perform final validation checks for the table definition we just pased.
    private void finalizeTableDefinition(Map<String, Map<String, FieldDefinition>> externalLinkMap) {
        // Options are validated by the assigned storage manager.
        
        // Validate implicit inverse links, if any.
        if (externalLinkMap != null) {
            // Examine all Link fields and make sure each 'inverse' is specified.
            validateInverseLinks(externalLinkMap);
        }
    }   // finalizeTableDefinition
    
    // Verify that the given shard-starting date is in the format YYYY-MM-DD. If the format
    // is bad, just return false.
    private boolean isValidShardDate(String shardDate) {
        try {
            // If the format is invalid, a ParseException is thrown.
            Utils.dateFromString(shardDate);
            return true;
        } catch (IllegalArgumentException ex) {
            return false;
        }
    }   // isValidShardDate

    // Examine all links defined in this table to ensure they are complete. If a link's
    // inverse is in this table but not explicitly defined, add it to the table's field
    // definitions. If a link's inverse is in another table, add it to the given
    // external link map. In the latter case, we also ensure that two different links are
    // trying to declare the same link as their inverse.
    private void validateInverseLinks(Map<String, Map<String, FieldDefinition>> externalLinkMap) {
        // Take a snapshot of field names so we can add fields as iterate.
        Set<String> fieldNames = new HashSet<>(m_fieldDefMap.keySet());
        for (String fieldName : fieldNames) {
            // Skip non-Link fields.
            FieldDefinition fieldDef = m_fieldDefMap.get(fieldName);
            if (!fieldDef.isLinkType()) {
                continue;
            }
            
            // If the link is an xlink, validate the junction field.
            FieldType linkType = fieldDef.getType();
            if (linkType == FieldType.XLINK) {
                verifyJunctionField(fieldDef);
            }
            
            // See if link inverse is in this table or another.
            String linkInverse = fieldDef.getLinkInverse();
            assert linkInverse != null;
            String linkExtent = fieldDef.getLinkExtent();
            assert linkExtent != null;
            if (linkExtent.equals(m_tableName)) {
                // Inverse is in this table. See if the inverse field was explicitly defined.
                FieldDefinition inverseFieldDef = m_fieldDefMap.get(linkInverse);
                if (inverseFieldDef == null) {
                    inverseFieldDef = new FieldDefinition(this);
                    inverseFieldDef.setType(linkType);
                    inverseFieldDef.setName(linkInverse);
                    inverseFieldDef.setLinkInverse(fieldName);
                    inverseFieldDef.setLinkExtent(m_tableName);
                    // by default, links are multi-valued
                    inverseFieldDef.setCollection(true);
                    addFieldDefinition(inverseFieldDef);
                } else {
                    // Inverse was explicitly defined. Ensure it points back to this link
                    // and this table.
                    Utils.require(inverseFieldDef.getLinkInverse().equals(fieldName),
                                  "Conflicting 'inverse' clauses for Link fields '" + fieldName +
                                  "' and '" + linkInverse + "'");
                    Utils.require(inverseFieldDef.getLinkExtent().equals(m_tableName), 
                                  "Conflicting 'table' options for Link fields '" + fieldName +
                                  "' and '" + linkInverse + "'");
                }
            } else {
                // Another table is the target for this link. See if the other table is
                // already externally-referenced.
                Map<String, FieldDefinition> forwardLinks = externalLinkMap.get(linkExtent);
                if (forwardLinks == null) {
                    // First external reference to this table.
                    forwardLinks = new HashMap<String, FieldDefinition>();
                    externalLinkMap.put(linkExtent, forwardLinks);
                }
                
                // See if the extent table already has a forward reference to this link.
                // If it does, this means two links both name the same link as their inverse!
                Utils.require(!forwardLinks.containsKey(linkInverse),
                              "Only one link field can define '" + linkInverse + "' in table '" +
                              linkExtent + "' as its 'inverse'");
                
                // Map the inverse link's name to us as its (eventual) inverse.
                forwardLinks.put(linkInverse, fieldDef);
            }
        }
    }   // validateInverseLinks
    
    // Verify that the given xlink's junction field is either _ID or an SV text field.
    private void verifyJunctionField(FieldDefinition xlinkDef) {
        String juncField = xlinkDef.getXLinkJunction();
        if (!"_ID".equals(juncField)) {
            FieldDefinition juncFieldDef = m_fieldDefMap.get(juncField);
            Utils.require(juncFieldDef != null,
                            String.format("Junction field for xlink '%s' has not been defined: %s",
                                          xlinkDef.getName(), xlinkDef.getXLinkJunction()));
            //Utils.require(juncFieldDef.getType() == FieldType.TEXT && !juncFieldDef.isCollection(),
            //                String.format("Junction field for xlink '%s' must be an SV text field: ",
            //                              xlinkDef.getName(), xlinkDef.getXLinkJunction()));
            Utils.require(juncFieldDef.getType() == FieldType.TEXT,
                    String.format("Junction field for xlink '%s' must be a text field: ",
                                  xlinkDef.getName(), xlinkDef.getXLinkJunction()));
        }
    }   // verifyJunctionField

    // Verify that this group field's nested fields are uniquely named among all fields in
    // this table, and add each nested field to the global field map. This allows us to
    // treat nested fields individually as well as qualified via their group fields.
    private void validateNestedFields(FieldDefinition groupFieldDef) {
        assert groupFieldDef.isGroupField();
        
        for (FieldDefinition nestedFieldDef : groupFieldDef.getNestedFields()) {
            // addFieldDefinition() ensures that the field name is unique, adds the nested
            // field to the field map and, if it is a link, adds it to the link map if the
            // link has a field number.
            addFieldDefinition(nestedFieldDef);

            // If this nested field is a group, recurse to it.
            if (nestedFieldDef.isGroupField()) {
                validateNestedFields(nestedFieldDef);
            }
        }
    }   // validateNestedFields

    /**
     * Replaces of all occurences of aliases defined with this table, by their expressions.
     * Now a simple string.replace is used. 
     * 
     * @param str string to replace
     * @return string with replaced aliases. If there were no aliases, the string is unchanged.
     */
	public String replaceAliaces(String str) {
		return getAppDef().replaceAliaces(str);
	}
    
    
}   // class TableDefinition
