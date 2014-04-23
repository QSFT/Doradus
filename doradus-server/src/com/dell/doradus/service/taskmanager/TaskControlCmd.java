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

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.HttpCode;
import com.dell.doradus.common.RESTResponse;
import com.dell.doradus.common.ScheduleDefinition;
import com.dell.doradus.common.Utils;
import com.dell.doradus.common.ScheduleDefinition.SchedType;
import com.dell.doradus.service.rest.RESTCallback;
import com.dell.doradus.service.schema.SchemaService;

public class TaskControlCmd extends RESTCallback {

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
		String appNamePar = m_request.getVariableDecoded("application");
		if (appNamePar == null) appNamePar = "*";
		String tableNamePar = m_request.getVariableDecoded("table");
		if (tableNamePar == null) tableNamePar = "*";
		String taskTypePar = m_request.getVariableDecoded("task");
		if (taskTypePar == null) taskTypePar = "*";
		String taskIdPattern = tableNamePar + "/" + taskTypePar;
		String taskParam = m_request.getVariableDecoded("param");
		if (taskParam != null) {
			taskIdPattern += "/" + taskParam;
		}
		String command = m_request.getVariableDecoded("command");
		
		if (Utils.isEmpty(command)) {
			return new RESTResponse(HttpCode.BAD_REQUEST, "Empty task command");
		}

		Set<String> tasksFailed = new HashSet<>();
		
		switch (command.toLowerCase()) {
		case "interrupt":
		case "stop":
			if (!tmService.interrupt(appNamePar, taskIdPattern)) {
				return new RESTResponse(
						HttpCode.NOT_FOUND,
						String.format("No task %s/%s found", appNamePar, taskIdPattern));
			}
			break;
		default:
			boolean taskFound = false;
			List<ApplicationDefinition> applications = SchemaService.instance().getAllApplications();
			for (ApplicationDefinition appDef : applications) {
				String appName = appDef.getAppName();
				for(ScheduleDefinition definition : appDef.getSchedules().values()) {
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
					TaskMatcher taskMatcher = new TaskMatcher(appNamePar, taskIdPattern);
					if (!taskMatcher.match(appName, taskId)) {
						continue;
					}
					taskFound = true;
					switch (command.toLowerCase()) {
					case "start":
						if (!tmService.startImmediately(appName, taskId)) {
							tasksFailed.add(appName + "/" + taskId);
						}
						break;
					case "suspend":
						tmService.suspend(appName, taskId);
						break;
					case "resume":
						tmService.resume(appName, taskId);
						break;
					default:
						return new RESTResponse(HttpCode.BAD_REQUEST, "Unknown command: " + command);
					}
				}
			}
			if (!taskFound) {
				return new RESTResponse(
						HttpCode.NOT_FOUND,
						String.format("No task %s/%s found", appNamePar, taskIdPattern));
			}
		}
		
		return tasksFailed.isEmpty() ? 
				new RESTResponse(HttpCode.OK, "Command delivered") :
				new RESTResponse(
						HttpCode.BAD_REQUEST,
						"Failed to start task(s): " + Arrays.toString(tasksFailed.toArray(new String[tasksFailed.size()])));
	}

}
