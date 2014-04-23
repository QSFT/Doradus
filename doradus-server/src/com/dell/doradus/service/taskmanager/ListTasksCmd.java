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

import java.util.Date;
import java.util.List;
import java.util.Map;

import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.HttpCode;
import com.dell.doradus.common.RESTResponse;
import com.dell.doradus.common.ScheduleDefinition;
import com.dell.doradus.common.UNode;
import com.dell.doradus.common.ScheduleDefinition.SchedType;
import com.dell.doradus.management.TaskStatus;
import com.dell.doradus.service.rest.RESTCallback;
import com.dell.doradus.service.schema.SchemaService;

public class ListTasksCmd extends RESTCallback {

	@Override
	protected RESTResponse invoke() {
		// Check service availability
		TaskManagerService tmService = TaskManagerService.instance();
		if (!tmService.isInitialized()) {
			return new RESTResponse(
					HttpCode.SERVICE_UNAVAILABLE, 
					"Task management service is unavailable. Please try later");
		}
		
		// Process request parameters
		Map<String, String> variables = m_request.getVariables();
		String appNamePar = variables.get("application");
		if (appNamePar == null) appNamePar = "*";
		String tableNamePar = variables.get("table");
		if (tableNamePar == null) tableNamePar = "*";
		String taskTypePar = variables.get("task");
		if (taskTypePar == null) taskTypePar = "*";
		String taskIdPattern = tableNamePar + "/" + taskTypePar;
		TaskMatcher taskMatcher = new TaskMatcher(appNamePar, taskIdPattern);
		
		// Peek up task statuses and form the result
		UNode response = UNode.createMapNode("tasks");
		List<ApplicationDefinition> applications = SchemaService.instance().getAllApplications();
		for (ApplicationDefinition appDef : applications) {
			String appName = appDef.getAppName();
			Map<String, ScheduleDefinition> schedDefinitions = appDef.getSchedules();
			for(ScheduleDefinition definition : schedDefinitions.values()) {
				SchedType taskType = definition.getType();
				if (taskType == SchedType.APP_DEFAULT ||
					taskType == SchedType.TABLE_DEFAULT) {
					continue;
				}
				String taskName = taskType.getName();
				if (definition.getTaskDeclaration() != null) {
					taskName += "/" + definition.getTaskDeclaration();
				}
				String tableName = definition.getTableName();
				if (tableName == null) {
					tableName = "*";
				}
				String taskId = tableName + "/" + taskName;
				if (!taskMatcher.match(appName, taskId)) {
					continue;
				}
				UNode taskNode = response.addMapNode(appName + "/" + taskId, "task");
				String schedule = definition.getSchedSpec();
				if (schedule != null) {
					taskNode.addValueNode("schedule", schedule);
				}
				TaskStatus status = TaskDBUtils.getTaskStatus(appName, taskId);
				if (status != null) {
					addStatus(taskNode, status);
				}
			}
			
			for (TaskId taskId : TaskDBUtils.getLiveTasks(appName)) {
				if (!schedDefinitions.containsKey(taskId.getScheduleId())) {
					String tableTaskId = taskId.getTaskTableId();
					if (response.getMember(tableTaskId) == null) {
					    UNode taskNode = response.addMapNode(tableTaskId, "task");
					    TaskStatus status = TaskDBUtils.getTaskStatus(
					        appName, tableTaskId.substring(appName.length() + 1));
					    if (status != null) {
					        addStatus(taskNode, status);
					    }
					}
				}
			}
		}
		
		
		return new RESTResponse(
				HttpCode.OK, 
				response.toString(m_request.getOutputContentType()), 
				m_request.getOutputContentType());
	}

	private static void addStatus(UNode taskNode, TaskStatus status) {
		taskNode.addValueNode("state", status.getLastRunState().name());
		if (status.getLastRunScheduledStartTime() > 0) {
			taskNode.addValueNode("last-scheduled-time", new Date(status.getLastRunScheduledStartTime()).toString());
		}
		if (status.getLastRunActualStartTime() > 0) {
			taskNode.addValueNode("last-started-time", new Date(status.getLastRunActualStartTime()).toString());
		}
		if (status.getLastRunFinishTime() > 0) {
			taskNode.addValueNode("last-finished-time", new Date(status.getLastRunFinishTime()).toString());
		}
		if (status.isSchedulingSuspended()) {
			taskNode.addValueNode("suspended", null);
		}
	}
}
