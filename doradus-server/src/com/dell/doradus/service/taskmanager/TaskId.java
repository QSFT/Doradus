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

package com.dell.doradus.service.taskmanager;

import java.util.ArrayList;
import java.util.List;

import com.dell.doradus.common.Utils;

/**
 * Background task identifying characteristics.
 */
public class TaskId {
	private String m_appName;	// Application (mandatory)
	private String m_tableName;	// Table (optional)
	private String m_taskType;	// Task type (mandatory)
	private String m_param;		// Additional parameters string (optional)
	
	/**
	 * Private constructor prevents from creating "ad-hoc" tasks.
	 */
	private TaskId() {}
	
	// Access functions (read only)
	public String getAppName() { return m_appName; }
	public String getTableName() { return m_tableName; }
	public String getTaskType() { return m_taskType; }
	public String getParam() { return m_param; }
	
	/**
	 * Creating a task identifier from the task key defined by ScheduleDefinition.
	 * 
	 * @param scheduleId	Schedule definition identifying key
	 * @return				Newly created task ID
	 */
	public static TaskId fromScheduleId(String scheduleId) {
		TaskId task = new TaskId();
		String[] nameParts = scheduleId.split("/");
		task.m_appName = nameParts[1];	// mandatory
		task.m_tableName = "*".equals(nameParts[2]) ? null : nameParts[2];
		task.m_taskType = nameParts[0];	// mandatory
		if (nameParts.length > 3) {
			task.m_param = nameParts[3];
		}
		return task;
	}
	
	/**
	 * Creating a task from a key defined by TaskManager.
	 * 
	 * @param taskTableId	TaskManager identifying key
	 * @return				Newly created task ID
	 */
	public static TaskId fromTaskTableId(String taskTableId) {
		TaskId task = new TaskId();
		String[] nameParts = taskTableId.split("/");
		task.m_appName = nameParts[0];	// mandatory
		task.m_tableName = "*".equals(nameParts[1]) ? null : nameParts[1];
		task.m_taskType = nameParts[2];	// mandatory
		if (nameParts.length > 3) {
			task.m_param = nameParts[3];
		}
		return task;
	}
	
	/**
	 * Returns an identifying key that ScheduleDefinition uses.
	 * 
	 * @return	ScheduleDefinition identifying key
	 */
	public String getScheduleId() {
		String tableName = m_tableName == null ? "*" : m_tableName; 
		List<String> nameParts = new ArrayList<>();
		nameParts.add(m_taskType);
		nameParts.add(m_appName);
		nameParts.add(tableName);
		if (m_param != null) {
			nameParts.add(m_param);
		}
		return Utils.concatenate(nameParts, "/");
	}
	
	/**
	 * Returns an identifying key that TaskManager uses.
	 * 
	 * @return	TaskManager identifying key
	 */
	public String getTaskTableId() {
		String tableName = m_tableName == null ? "*" : m_tableName; 
		List<String> nameParts = new ArrayList<>();
		nameParts.add(m_appName);
		nameParts.add(tableName);
		nameParts.add(m_taskType);
		if (m_param != null) {
			nameParts.add(m_param);
		}
		return Utils.concatenate(nameParts, "/");
	}
	
	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || !(o instanceof TaskId)) return false;
		TaskId obj = (TaskId)o;
		boolean equals = m_appName.equals(obj.m_appName) && m_taskType.equals(obj.m_taskType);
		if (m_tableName == null) {
			equals &= obj.m_tableName == null;
		} else {
			equals &= m_tableName.equals(obj.m_tableName);
		}
		if (m_param == null) {
			equals &= obj.m_param == null;
		} else {
			equals &= m_param.equals(obj.m_param);
		}
		return equals;
	}
	
	@Override
	public int hashCode() {
		int hash = m_appName.hashCode() ^ m_taskType.hashCode();
		if (m_tableName != null) hash ^= m_tableName.hashCode();
		if (m_param != null) hash ^= m_param.hashCode();
		return hash;
	}
	
	@Override
	public String toString() {
		return getTaskTableId();
	}
}
