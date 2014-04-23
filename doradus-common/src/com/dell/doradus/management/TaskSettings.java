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

package com.dell.doradus.management;

import java.beans.ConstructorProperties;
import java.util.Map;

/**
 * The TasksSettings class defines a suite of settings of background tasks, implements 
 * the settings inheritance model, and provides keys for identification of tasks and 
 * groups of tasks.
 * <p>
 * Each TaskSettings object contains either values of settings of a task (-"task-settings") 
 * or default values of the settings for all tasks in some group (-"group-settings"). 
 * The exact mission of each object is fixed at construction time and depends on constructor 
 * you choose. 
 * <p>
 * We distinguish between two categories of tasks: 1) "table-task" is aimed at one 
 * of tables of an application, and 2) "application-task" is aimed at the application 
 * at whole. The tasks are organized in a group hierarchy. The top-level group includes 
 * all tasks of all applications. On next level, the tasks are divided in groups each of 
 * which includes all tasks of one of applications. The low-level comprises the groups 
 * each of which includes all tasks for one of tables of the corresponding application. 
 * We call the TaskSettings objects which represent these groups as "global default 
 * settings", "application default settings", and "table default settings", respectively.
 * <p>
 * If a task have undefined value of some setting, we get value of the setting 
 * from lowest level group that includes this task. If the value also isn't defined, 
 * we look for in group on the next higher level, and etc. We call it "settings 
 * inheritance".  Generally, to get a concrete value of some setting of a task/group  
 * (- schedule, for example) we must have in hands the settings specified for each 
 * group that includes this task/group directly or indirectly. Such collections of the 
 * TaskSettings objects can be got via ITaskManager interface for each application
 * (see: ITaskManager.getAppSettings). Consumers of these collections are 'resolve'
 * methods which together implement "settings inheritance" model. The TaskSettings 
 * class provides such method ("resolveXXX") for each setting, in addition to usual 
 * getter and setter.
 * <p>
 * Within collections which are provided by the ITaskManager.getAppSetting method, 
 * each TaskSettings object is identified by an unique key which at the same time plays 
 * a role of identifier of task/group represented by the object. The key value getter 
 * is getKey() method of the object.  Besides, for supporting the child-parent relations 
 * in the TaskSettings collections, each TaskSettings object provides the key of group 
 * that directly includes task/group represented by this object (see: getDefaultSettingsKey).
 * <p>
 * The syntax of the TaskSettings's keys restricts space of valid table names and task 
 * names which can be used with the TeskSettings constructors. Valid name of table 
 * is any nonempty string which doesn't equal to "*" and doesn't contain '/' characters. 
 * Valid name of task is any nonempty string that doesn't equal to "*" and doesn't end 
 * with "/*" string.
 * <p>
  * Currently, the tasks have only one setting, it is specification of the task schedule 
  * (see: getSchedule, setSchedule, and resolveSchedule methods). I think, in future, 
  * we can want to expand this list.
 */
public class TaskSettings {
	
	public static final String KEY_ALL = "*";
	public static final String KEY_SEP = "/";
	public static final String NEVER = "never";
	

	/**
	 * Represents the taxonomy of TaskSettings objects.
	 */
	public enum Mission {
		/** Default values of settings for all tasks. */
		GLOBAL_DEFAULT_SETTINGS, 
		/** Default values of settings for all tasks belonging to one of applications. */
		APPLICATION_DEFAULT_SETTINGS, 
		/** Default values of settings for all tasks belonging to one of tables of one application. */
		TABLE_DEFAULT_SETTINGS, 
		/** Values of settings of an application-task. */
		APPLICATION_TASK_SETTINGS, 
		/** Values of settings of an table-task. */
		TABLE_TASK_SETTINGS
	}

	/**
	 * Creates new TaskSettings instance that is intended to contain default
	 * settings for all tasks in all applications.
	 */
	public TaskSettings() {
		this(true, "Global default settings", null, null, NEVER);
	}

	/**
	 * Creates new TaskSettings instance that is intended to contain default
	 * settings for all tasks in an application.
	 * 
	 * @param description String or null.
	 */
	public TaskSettings(String description) {
		this(false, description, null, null, null);
	}

	/**
	 * Creates new TaskSettings instance that is intended to contain default
	 * settings for group of tasks aimed at named table.
	 * 
	 * @param description 
	 * 		String or null.
	 * @param tableName 
	 * 		Nonempty string that doesn't equal to "*" and doesn't contain '/' characters.
	 * @exception java.lang.IllegalArgumentException when tableName is invalid.
	 */
	public TaskSettings(String description, String tableName) {
		this(false, description, tableName, null, null);
		
		if(tableName == null) {
			throw new IllegalArgumentException("tableName shan't be null.");
		}
	}

	/**
	 * Creates new TaskSettings instance that is intended to contain settings of either table-task or application-task in dependency on the tableName value.
	 * 
	 * @param description 
	 * 		String or null.
	 * @param tableName 
	 * 		The null, for application-task, or nonempty string that doesn't equal "*" and doesn't contain '/' characters.
	 * @param taskName 
	 * 		Nonempty string that doesn't equal to "*" and doesn't end with "/*" string.
	 * @exception java.lang.IllegalArgumentException when tableName or taskName is invalid.
	 */
	public TaskSettings(String description, String tableName, String taskName) {
		this(false, description, tableName, taskName, null);

		if(taskName == null) {
			throw new IllegalArgumentException("taskName shan't be null.");
		}
	}

	/**
	 * This constructor is not intended for direct usage. It is required to JMX
	 * for (de)serialization of the TaskSettings objects.
	 */
	@ConstructorProperties({ "globalDefaultSettings", "description",
			"tableName", "taskName", "schedule" })
	public TaskSettings(boolean globalDefaultSettings, String description,
			String tableName, String taskName, String schedule) {

		validate(tableName, taskName);

		this.globalDefaultSettings = globalDefaultSettings;
		this.description = description;
		this.tableName = tableName;
		this.taskName = taskName;
		this.schedule = schedule;
		
		key = getKeyImpl();
		defaultSettingsKey = getDefaultSettingsKeyImpl();
		
		if(this.description == null) {
			this.description = getDefaultDescription();
		}
	}

	/**
	 * Creates new TaskSettings instance the mission of which is defined by 
	 * the given key. If the mission is not GLOBAL_DEFAULT_SETTINGS 
	 * the values of settings within this instance will be set to undefined state.
	 * 
	 * @param key	
	 * 		The null or string with syntax supported by the getKey() method.
	 * @exception 
	 * 		java.lang.IllegalArgumentException when key format is invalid.
	 * @return 
	 * 		TaskSettings or null, if key is null.
	 */
	public static TaskSettings createByKey(String key) {
		if(key == null) {
			return null;
		}
		
		
		if(KEY_ALL.equals(key)) {
			return new TaskSettings();
		
		} else if((KEY_ALL + KEY_SEP + KEY_ALL).equals(key)) {
			return new TaskSettings(null);
			
		} else if(key.endsWith(KEY_SEP + KEY_ALL)) {
			int i = key.lastIndexOf(KEY_SEP);
			if(i > 0) {
				String tableName = key.substring(0, i);
				return new TaskSettings(null, tableName);
			}			
		} else if(key.startsWith(KEY_ALL + KEY_SEP)) {
			int i = key.indexOf(KEY_SEP);
			if( i < key.length() - 1) {
				String taskName = key.substring(i + 1);
				return new TaskSettings(null, null, taskName);
			}
		} else {		
			int i = key.indexOf(KEY_SEP);
			if(i > 0 && i < key.length() - 1) {
				String tableName = key.substring(0, i);
				String taskName = key.substring(i + 1);
				return new TaskSettings(null, tableName, taskName);
			}
		}
				
		throw new IllegalArgumentException("Invalid key: \"" + key + "\".");		
	}
	
	/**
	 * Returns the true if argument is key of global default settings (- group of all tasks in all apps).
	 */
	public static boolean isGlobalDefaultSettingsKey(String key) {
		return KEY_ALL.equals(key);
	}
	
	/**
	 * Returns the true if argument is key of some default settings (- a group of tasks, not one task).
	 */
	public static boolean isDefaultSettingsKey(String key) {
		try {
			TaskSettings s = createByKey(key);
			return s != null && s.isDefaultSettings();
		} catch(IllegalArgumentException ex) {
			return false;
		}
	}
	
	/**
	 * Returns the true if argument is a valid key of a task or group of tasks.
	 */
	public static boolean isValidKey(String key) {
		try {
			return createByKey(key) != null;
		} catch(IllegalArgumentException ex) {
			return false;
		}
	}
	
	

	/**
	 * Reports the mission of this instance.
	 * 
	 * @return TaskSettingsMission
	 */
	public Mission getMission() {
		return getMissionImpl();
	}

	/**
	 * Gets a readable description (display name) of the represented task/group.
	 * @return String or null.
	 */
	public String getDescription() {
		return description;
	}
	
	/**
	 * @param description String or null.
	 */
	public void setDescription(String description) {
		this.description = description;
	}

	/**
	 * If this instance represents a table-task or group of table-tasks then returns 
	 * name of the table. Otherwise, returns null.
	 * 
	 * @return String or null.
	 */
	public String getTableName() {
		return tableName;
	}

	/**
	 * If this instance represents a task then returns name of the task.
	 * Otherwise, returns null.
	 * <p>
	 * Usually the task name will comprise the task type identifier and the task
	 * parameters.
	 * 
	 * @return String or null, if this is default settings.
	 */
	public String getTaskName() {
		return taskName;
	}

	/**
	 * Returns a string key that uniquely identifies this TaskSettings object,
	 * and also the task/group which it represents, among others belonging
	 * to the same application.
	 * 
	 * <p>The key syntax:
	 * <p>
	 * <li> <all>                                    -- GLOBAL_DEFAULT_SETTINGS</li>
	 * <li> <all>/<all>                           -- APPLICATION_DEFAULT_SETTINGS</li>
	 * <li> <table-name>/<all>              -- TABLE_DEFAULT_SETTINGS</li>
	 * <li> <all>/<task-name>               -- APPLICATION_TASK_SETTINGS</li>
	 * <li> <table-name>/<task-name>  -- TABLE_TASK_SETTINGS</li>
	 * </p>
	 * where: 
	 * <p>
	 * <li> <all> -- the "*" string. </li>
	 * <li> <table-name> -- nonempty string that doesn't equal to "*" and doesn't contain '/' characters.. </li>
	 * <li> <task-name> -- nonempty string that doesn't equal to "*" and doesn't end with "/*" string. </li>
	 * </p>
	 * @return String (not null).
	 */
	public String getKey() {
		return key;
	}

	/**
	 * Returns the key of TaskSettings object that contains default settings values
	 * for task/group represented by this instance. That is, this key identifies 
	 * group that includes this task/group directly.
	 * 
	 * @return String or null, if this instance contains global default settings.
	 */
	public String getDefaultSettingsKey() {
		return defaultSettingsKey;
	}

	/**
	 * Returns the true if this instance represents a group of tasks.
	 */
	public boolean isDefaultSettings() {
		return getTaskName() == null;
	}

	/**
	 * Returns the true if this instance represents the defaults for all tasks in all applications.
	 */
	public boolean isGlobalDefaultSettings() {
		return globalDefaultSettings;
	}
	
	/**
	 * Returns the true if this instance contains a setting (or several settings) with undefined value.
	 * It means that a context with defaults is required for usage of this instance (see: resolveXXX).
	 */
	public boolean containsUndefinedValues() {
		return schedule == null;
	}
	
	/**
	 * Returns the true if this instance contains a setting (or several settings) with defined value.
	 * In particular, it means that this instance is a subject for long-term saving.
	 */
	public boolean containsDefinedValues() {
		return schedule != null;
	}

	/**
	 * Get the current schedule setting for task/group represented by this instance.
	 * 
	 * @return <cron-expression> -OR- "never" -OR- null (see: setSchedule).
	 */
	public String getSchedule() {
		return schedule;
	}

	/**
	 * Sets the current schedule setting for task/group represented by this instance.
	 * Legal value of this setting is a string specification with the following syntax:
	 * 
	 *<p><schedule-spec> ::= <cron-expression> -OR- <never> -OR- null
	 *<p>
	 *<p>where:
	 *<p> 
	 *<p><cron-expression> 
	 *<p>		- represents a recurrence interval and date/time when to run 
	 *<p>		the task(s), in cron's format (see Quartz docs).
	 *<p>
	 *<p><never> 
	 *<p>   	- the "never" keyword that indicates not schedulable task(s).
	 *<p>
	 *<p>null 
	 *<p>   	- not defined value that says to use the default schedule which 
	 *<p>		is specified for group that directly includes the task/group represented 
	 *<p>		by this instance (see: getDefaultSettingsKey).
	 * 
	 * @param schedule Schedule specification string. 
	 */
	public void setSchedule(String schedule) {
		this.schedule = schedule;
	}

	/**
	 * If the current schedule specification is not defined (see: setSchedule) then 
	 * returns default value inherited from groups represented by given collection 
	 * of TaskSettings objects. Otherwise, returns the raw schedule specification 
	 * (see: getSchedule).
	 * 
	 * @param settingsMap
	 *            All settings belonging to application (see: ITaskManager.getAppSettings).
	 * @param defaultSchedule
	 *            Global default schedule (see: ITaskManager.getGlobalDefaultSettings).
	 * 
	 * @return <cron-expression> -OR- "never" (see: setSchedule)
	 */
	public String resolveSchedule(Map<String, TaskSettings> settingsMap, String defaultSchedule) {
		
		TaskSettings s = this;
		
		while(s.schedule == null) {
			String k = s.getDefaultSettingsKey();
			
			if(k == null) {
				return defaultSchedule;
			}
			
			if(settingsMap.containsKey(k)) {
				s = settingsMap.get(k);
				
			} else {
				s = createByKey(k);
				if(s.isGlobalDefaultSettings()) 
					return defaultSchedule;
			}
		}

		return s.schedule;
	}
	
	/**
	 * Creates new TaskSettings instance the mission of which is the same as this instance 
	 * but values of all settings are resolved in given context.
	 * @param settingsMap
	 * @param globalDefaults
	 * @return New TaskSettings instance.
	 */
	public TaskSettings resolveAll(Map<String, TaskSettings> settingsMap, TaskSettings globalDefaults) {
		TaskSettings s = createByKey(key);
		s.description = description;
		s.schedule = resolveSchedule(settingsMap, globalDefaults != null ? globalDefaults.schedule : NEVER);
		return s;
	}
	
	/**
     * Compares this instance to the specified object.  The result is {@code
     * true} if and only if the argument is not {@code null} and is a {@code
     * TaskSettings} object that has the same key and values of settings as 
     * this object.
     * <p>
     * WARN: This method ignores the description string. 
	 */
	@Override
	public boolean equals(Object obj) {
		if(this == obj)
			return true;
		
		if(obj instanceof TaskSettings) {
			TaskSettings other = (TaskSettings)obj;
			if(key.equals(other.key)) {
				return (schedule == null && other.schedule == null) 
						|| (schedule != null && schedule.equals(other.schedule));
			}
		}
		return false;
	}
	
	@Override
	public int hashCode() {
		int hash = key.hashCode();
		if (schedule != null) {
			hash ^= schedule.hashCode();
		}
		return hash;
	}

	/**
	 */
	protected Mission getMissionImpl() {
		if (globalDefaultSettings)
			return Mission.GLOBAL_DEFAULT_SETTINGS;

		if (isDefaultSettings()) {
			if (getTableName() == null)
				return Mission.APPLICATION_DEFAULT_SETTINGS;
			return Mission.TABLE_DEFAULT_SETTINGS;
		}

		if (getTableName() == null)
			return Mission.APPLICATION_TASK_SETTINGS;
		return Mission.TABLE_TASK_SETTINGS;
	}

	/**
	 */
	protected String getKeyImpl() {
		return globalDefaultSettings ? KEY_ALL : getKeyImpl(tableName, taskName);
	}

	/**
	 */
	protected String getKeyImpl(String tableName, String taskName) {
		if (taskName == null) {
			return (tableName == null ? KEY_ALL : tableName) + KEY_SEP + KEY_ALL;
		}

		return (tableName == null ? KEY_ALL : tableName) + KEY_SEP + taskName;
	}

	/**
	 */
	@SuppressWarnings("incomplete-switch")
	protected String getDefaultSettingsKeyImpl() {
		Mission m = getMissionImpl();

		switch (m) {
		case APPLICATION_DEFAULT_SETTINGS:
			return "*";
		case TABLE_DEFAULT_SETTINGS:
			return getKeyImpl(null, null);
		case APPLICATION_TASK_SETTINGS:
			return getKeyImpl(null, null);
		case TABLE_TASK_SETTINGS:
			return getKeyImpl(getTableName(), null);
		}

		// GLOBAL_DEFAULT_SETTINGS
		return null;
	}


	/**
	 */
	@SuppressWarnings("incomplete-switch")
	protected String getDefaultDescription() {
		Mission m = getMissionImpl();

		switch (m) {
		case APPLICATION_DEFAULT_SETTINGS:
			return "Default settings for all tasks belonging to application";
		case TABLE_DEFAULT_SETTINGS:
			return "Default settings for all tasks belonging to table";
		case APPLICATION_TASK_SETTINGS:
			return "Settings of application-task";
		case TABLE_TASK_SETTINGS:
			return "Settings of table-task";
		}

		// GLOBAL_DEFAULT_SETTINGS
		return "Default settings for all tasks";
	}
	
	/**
	 */
	protected void validate(String tableName, String taskName) {
		if(tableName != null && (tableName.isEmpty() || tableName.equals(KEY_ALL) || tableName.indexOf(KEY_SEP) >= 0)) {
			throw new IllegalArgumentException("Illegal tableName: \"" + tableName + "\".");
		}

		if(taskName != null && (taskName.isEmpty() || taskName.equals(KEY_ALL) || taskName.endsWith(KEY_SEP + KEY_ALL))) {
			throw new IllegalArgumentException("Illegal taskName: \"" + taskName + "\".");
		}
	}

	protected boolean globalDefaultSettings;
	protected String description;
	protected String tableName;
	protected String taskName;
	protected String key;
	protected String defaultSettingsKey;
	protected String schedule;
	
	public String toStr() {
		StringBuilder b = new StringBuilder();
		b.append("key:                   " + key + "\n");
		b.append("defaultSettingsKey:    " + defaultSettingsKey + "\n");
		b.append("globalDefaultSettings: " + globalDefaultSettings + "\n");
		b.append("description:           " + description + "\n");
		b.append("tableName:             " + tableName + "\n");
		b.append("taskName:              " + taskName + "\n");
		b.append("schedule:              " + schedule + "\n");
		return b.toString();
	}
}
