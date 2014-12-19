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

import it.sauronsoftware.cron4j.SchedulingPattern;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.dell.doradus.management.CronUtils;
import com.dell.doradus.management.TaskSettings;

/**
 * Represents a schedule definition for a task. The enum {@link SchedType} defines the
 * types of schedules we recognize.
 */
public class ScheduleDefinition {
	/**
	 * Default schedules that are assigned to automatically created data-aging
	 * and stat-refresh tasks (every day at midnight). 
	 */
	public static final String DEFAULT_AGING_SCHEDULE = "0 0 * * *";
	public static final String DEFAULT_STATREFRESH_SCHEDULE	= "0 0 * * *";
	
    /**
     * Represents the types of {@link ScheduleDefinition}s currently recognized.
     */
    public enum SchedType {
        APP_DEFAULT("app-default", null, null) {
        	@Override
        	public void validate(ScheduleDefinition schedDef, String serviceName) {
        		super.validate(schedDef, serviceName);
        		Utils.require(schedDef.m_schedSpec != null, "Value should be defined for " + getName());
                Utils.require(schedDef.m_tableName == null, "'table' attribute not valid for " + getName());
                Utils.require(schedDef.m_taskDeclaration == null, "'statistic' attribute not valid for " + getName());
        	}
        },
        TABLE_DEFAULT("table-default", null, null) {
        	@Override
        	public void validate(ScheduleDefinition schedDef, String serviceName) {
        		super.validate(schedDef, serviceName);
        		Utils.require(schedDef.m_schedSpec != null, "Value should be defined for " + getName());
                Utils.require(schedDef.m_tableName != null, "'table' attribute should be defined for " + getName());
                Utils.require(schedDef.m_appDef.getTableDef(schedDef.m_tableName) != null, "Table " + schedDef.m_tableName + " not defined for " + getName());
                Utils.require(schedDef.m_taskDeclaration == null, "'statistic' attribute not valid for " + getName());
        	}
        },
        RE_INDEX("re-index", "com.dell.doradus.tasks.ReIndexFieldData", new String[0]),
        DELETE_SCALAR("delete-scalar", "com.dell.doradus.tasks.DeleteScalarFieldData", new String[0]),
        DELETE_LINK("delete-link", "com.dell.doradus.tasks.DeleteLinkFieldData", new String[0]),
        STAT_REFRESH("stat-refresh", "com.dell.doradus.service.statistic.StatRefresher",
        		new String[] {"SpiderService"}) {
        			@Override
                	public void validate(ScheduleDefinition schedDef, String serviceName) {
                		super.validate(schedDef, serviceName);
                        Utils.require(schedDef.m_tableName != null, "'table' attribute should be defined for " + getName());
                        TableDefinition tabDef = schedDef.m_appDef.getTableDef(schedDef.m_tableName);
                        Utils.require(tabDef != null, "Table " + schedDef.m_tableName + " not defined for " + getName());
                        Utils.require(tabDef.getStatDefinitions().iterator().hasNext(), 
                        		"No statistics defined for the " + schedDef.m_tableName + " table");
                        if (!Utils.isEmpty(schedDef.m_taskDeclaration)) {
	                        Utils.require(tabDef.getStatDefNames().contains(schedDef.m_taskDeclaration),
	                        		"Statistic " + schedDef.m_taskDeclaration + " is not defined for " + schedDef.m_tableName + " table");
                        }
                	}
                },
        DATA_AGING("data-aging", "com.dell.doradus.tasks.DataAger",
        		new String[] {"SpiderService", "OLAPService"}) {
        			@Override
                	public void validate(ScheduleDefinition schedDef, String serviceName) {
                		super.validate(schedDef, serviceName);
                		if ("OLAPService".equals(serviceName)) {
                            Utils.require(schedDef.m_tableName == null, "'table' attribute not valid for " + getName());
                		} else if (schedDef.m_tableName != null) {
                			String[] tableNames = schedDef.m_tableName.split(",");
                			for (String tableName : tableNames) {
                				TableDefinition tabDef = schedDef.m_appDef.getTableDef(tableName.trim());
                				Utils.require(tabDef != null, "Table " + schedDef.m_tableName + " not defined for " + getName());
                			}
                		}
                        Utils.require(schedDef.m_taskDeclaration == null, "'statistic' attribute not valid for " + getName());
                	}
                };

        /**
         * This SchedType's friendly name (e.g., "app-default").
         * 
         * @return  This SchedType's friendly name (e.g., "app-default").
         */
        public String getName() {
            return m_name;
        }   // getName
        
        /**
         * This SchedType's task implementation class name.
         * 
         * @return  This SchedType's task implementation class name.
         * 	null, if no task assigned to this SchedType.
         */
        public String getClassName() {
            return m_className;
        }   // getClassName
        
        public static List<String> getTaskNames() {
        	List<String> list = new ArrayList<String>();
        	list.add(STAT_REFRESH.getName());
        	list.add(DATA_AGING.getName());
        	return list;
        }
        
        // Friendly name for display purposes.
        final private String m_name;
        
        // Server implementation task class name
        final private String m_className;
        
        // Table of acceptable services for the task. Null if all are acceptable.
        final private List<String> m_services;
        
        // Constructor
        private SchedType(String name, String className, String[] services) {
            m_name = name;
            m_className = className;
            	m_services = services == null ? null : Arrays.asList(services);
        }   // constructor
        
        // Map of friendly names to enum objects for easy look-up:
        private static Map<String, SchedType> g_schedNameMap = new HashMap<String, SchedType>();
        static {
            for (SchedType schedType : values()) {
                g_schedNameMap.put(schedType.m_name, schedType);
            }
        }   // static initializer
        
        public boolean accepts(String serviceName) {
        	return m_services == null || m_services.contains(serviceName);
        }
        
        /**
         * Get the SchedType for the given friendly name, or null if it is unrecognized.
         * 
         * @param  name Friendly (display) name of a {@link SchedType} (case-sensitive).
         * @return      {@link SchedType} with the given name, or null if it is unrecognized.
         */
        public static SchedType getByName(String name) {
            return g_schedNameMap.get(name);
        }   // getByName
        
        /**
         * Implements specific semantic validation for schedule of a given type.
         * Checks schedule value validness by default.
         * @param schedDef      {@link ScheduleDefinition} of schedule to validate.
         * @param serviceName   Name of service to which schedule belongs.
         */
        public void validate(ScheduleDefinition schedDef, String serviceName) {
            String scheduleValue = schedDef.m_schedSpec;
            
            // Ensure we got a valid schedule spec or no spec at all
            if (scheduleValue != null && scheduleValue.isEmpty()) {
            	scheduleValue = schedDef.m_schedSpec = null;
            }
            if (scheduleValue != null && !TaskSettings.NEVER.equals(scheduleValue) && !SchedulingPattern.validate(scheduleValue)) {
            	// Quartz cron expression?
            	String converted = CronUtils.packSchedule(scheduleValue.trim());
            	if (SchedulingPattern.validate(converted)) {
            		scheduleValue = schedDef.m_schedSpec = converted;
            	}
            }
            Utils.require(
            		scheduleValue == null ||
            		TaskSettings.NEVER.equals(scheduleValue) || 
            		SchedulingPattern.validate(scheduleValue),
                          "Schedule specification is invalid in <schedule> definition: " + scheduleValue);
        }
    }   // enum SchedType
    
    // Required member variables (always valid):
    private final ApplicationDefinition m_appDef;
    private SchedType                   m_schedType;
    private String                      m_schedSpec;
    
    // Table name required for all but APP_DEFAULT schedules:
    private String m_tableName;
    
    // Statistic name required for STAT_REFRESH schedules:
    private String m_taskDeclaration;
    
    /**
     * Create an empty ScheduleDefinition that belongs to the given
     * {@link ApplicationDefinition}. The schedule will not be valid until its
     * members are set via a parse or set method.
     *  
     * @param appDef    {@link ApplicationDefinition} to which this statistic definition
     *                  belongs.
     */
    public ScheduleDefinition(ApplicationDefinition appDef) {
        m_appDef = appDef;
    }   // constructor
    
    public ScheduleDefinition(ApplicationDefinition appDef, SchedType schedType, String schedSpec, String tableName, String taskDeclaration) {
        if ("".equals(taskDeclaration)) {
        	taskDeclaration = null;
        }
    	m_appDef = appDef;
    	m_schedType = schedType;
    	m_schedSpec = schedSpec;
    	m_tableName = tableName;
    	m_taskDeclaration = taskDeclaration;
    }   // constructor

    ///// Getters

    /**
     * Return a hierarchical "name" that uniquely identifies this schedule. The name
     * always includes the schedule's type and owning application, plus table name and/or
     * statistic name if applicable. Hence the format is always one of these:
     * <pre>
     *      app-default/{application name}
     *      table-default/{application name}/{table name}
     *      data-aging/{application name}/{table name}
     *      stat-refresh/{application name}/{table name}/{statistic name}
     * </pre>
     * Examples:
     * <pre>
     *      app-default/Magellan
     *      table-default/Magellan/Stars
     *      data-aging/Magellan/Stars
     *      stat-refresh/Magellan/Stars/AverageLuminescence
     * </pre>
     */
    @SuppressWarnings("incomplete-switch")
	public String getName() {
        // Start by adding the schedule type and application name.
        StringBuilder buffer = new StringBuilder();
        buffer.append(m_schedType.m_name);
        buffer.append("/");
        buffer.append(m_appDef.getAppName());
        
        // Add additional parameters, if needed.
        switch (m_schedType) {
        case TABLE_DEFAULT:
        case DATA_AGING:
            // Add "/{table name}/*" for these.
           	buffer.append("/").append(m_tableName != null ? m_tableName : "*");
            break;

        case STAT_REFRESH:
            // Add "/{table name}/{statistic name}" for this one.
            buffer.append("/");
            buffer.append(m_tableName);
            buffer.append("/");
            buffer.append(m_taskDeclaration);
            break;

        }
        return buffer.toString();
    }   // getName
    
    /**
     * Get the name of the task to which this schedule definition applies. A name
     * will be returned only if this schedule definition's type is
     * {@link SchedType#STAT_REFRESH}, otherwise null is returned.
     * 
     * @return  The name of the statistic to which this schedule definition applies, or
     *          null if not applicable.
     */
	public String getTaskDeclaration() 	
	{
		return m_taskDeclaration;
	}

    public String getStatisticName() {
        return m_taskDeclaration;
    }   // getStatisticName
    
    /**
     * Get the name of the table to which this schedule definition applies, if applicable.
     * If this schedule definition's type is not specific to a table, the value will be
     * null.
     * 
     * @return  The name of the table to which this schedule definition applies, or null
     *          if not applicable.
     */
    public String getTableName() {
        return m_tableName;
    }   // getTableName
    
    /**
     * Set table name to null. Actually it makes the schedule definition's name changed,
     * so be careful not to use the method for the definitions that is already
     * included into the schedules map.
     */
    public void clearTableName() {
    	m_tableName = null;
    }	// clearTableName
    
    /**
     * Get this schedule definition's type, which is a {@link SchedType}.
     * 
     * @return  This schedule definition's type as a {@link SchedType}.
     */
    public SchedType getType() {
        return m_schedType;
    }   // getType
    
    /**
     * Get this schedule specification.
     * 
     * @return  This schedule specification.
     */
    public String getSchedSpec() {
        return m_schedSpec;
    }   // geSchedSpec
    
    /**
     * Sets new schedule specification
     *  
     * @param newSchedSpec
     */
    public void setSchedSpec(String newSchedSpec) {
    	m_schedSpec = newSchedSpec;
    }  // setSchedSpec

    /**
     * Serialize this schedule definition into a {@link UNode} tree and return the root node.
     * 
     * @return  The root node of a {@link UNode} tree representing this schedule definition.
     */
    @SuppressWarnings("incomplete-switch")
	public UNode toDoc() {
        // The schedule node is a MAP named "schedule".
        UNode schedNode = UNode.createMapNode("schedule");
        
        // Add the "type" and "value" attributes, also marked as XML attributes.
        schedNode.addValueNode("type", m_schedType.getName(), true);
        if (m_schedSpec != null) {
        	schedNode.addValueNode("value", m_schedSpec, true);
        }
        
        // Add additional attributes, if any, specific to schedule type.
        switch (m_schedType) {
        case TABLE_DEFAULT:
        case DATA_AGING:
            // These have optional table name.
            if (!Utils.isEmpty(m_tableName)) {
                schedNode.addValueNode("table", m_tableName, true);
            }
            break;
        
        case STAT_REFRESH:
            // This needs table and optional statistics names.
            schedNode.addValueNode("table", m_tableName, true);
            if (!Utils.isEmpty(m_taskDeclaration)) {
            	schedNode.addValueNode("statistic", m_taskDeclaration, true);
            }
            break;
        }
        return schedNode;
    }   // toDoc

    /**
     * Returns the same value as {@link #getName()}.
     */
    @Override
    public String toString() {
        return getName();
    }   // toString

    ///// Setters
    
    /**
     * Parse the schedule definition rooted at the given UNode and map the corresponding
     * attributes into this object. An IllegalArgumentException is thrown if the
     * definition is invalid.
     * 
     * @param schedNode                 Root {@link UNode} of a schedule definition
     * @throws IllegalArgumentException If the definition is invalid.
     */
    public void parse(UNode schedNode) throws IllegalArgumentException {
        assert schedNode != null;
        
        // Schedule must be a map of unique names.
        Utils.require(schedNode.isMap(),
                      "'schedule' definition must be a map of unique names: " + schedNode);
        
        // Parse attributes, each of which should be a simple value.
        for (String attrName : schedNode.getMemberNames()) {
            // All attribute values must be a string.
            UNode attrNode = schedNode.getMember(attrName);
            Utils.require(attrNode.isValue(),
                          "Schedule attribute value must be a string: " + attrNode);
            String attrValue = attrNode.getValue();
            
            // See if we recognize attribute name.
            // 'type'
            if (attrName.equals("type")) {
                // Must be a recognizable type mnemonic (case-sensitive).
                Utils.require(m_schedType == null,
                              "'type' attribute can be specified only once");
                m_schedType = SchedType.getByName(attrValue);
                Utils.require(m_schedType != null,
                              "Unrecognized 'type': " + m_tableName);
                
            // 'table'
            } else if (attrName.equals("table")) {
                Utils.require(m_tableName == null,
                              "'table' can be specified only once");
                m_tableName = attrValue;
                
            // 'statistic'
            } else if (attrName.equals("statistic")) {
                Utils.require(m_taskDeclaration == null,
                              "'statistic' can be specified only once");
                m_taskDeclaration = attrValue;
                
            // 'value'
            } else if (attrName.equals("value")) {
                Utils.require(m_schedSpec == null,
                              "'value' can be specified only once");
                m_schedSpec = attrValue;
                
            // 'checktype'
            } else if (attrName.equals("checktype")) {
                // Ignore the obsolete attribute "checktype"
                
            } else {
                // Don't recognize any other attributes.
                Utils.require(false, "Unrecognized attribute: " + attrName);
            }
        }
    }   // parse
    
    /**
     * Structure validation of this ScheduleDefinition by checking that items 
     * contain all the necessary components. An IllegalArgumentException is thrown if
     * anything is amiss.
     *   
     * @throws IllegalArgumentException If this schedule definition is not valid.
     */
    public void validate(String serviceName) throws IllegalArgumentException {
        // Schedule type should be set by now.
        Utils.require(m_schedType != null, "Schedule 'type' has not been set");
        Utils.require(m_schedType.accepts(serviceName), 
        		"The task type %s is not acceptable for scheduling in %s applications",
        		m_schedType.getName(), serviceName);
        m_schedType.validate(this, serviceName);
    }   // validate

}   // class ScheduleDefinition
