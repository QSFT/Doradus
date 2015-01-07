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

package com.dell.doradus.service.spider;

import java.util.Map;

import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.FieldDefinition;
import com.dell.doradus.common.HttpCode;
import com.dell.doradus.common.RESTResponse;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.common.Utils;
import com.dell.doradus.common.ScheduleDefinition.SchedType;
import com.dell.doradus.service.StorageService;
import com.dell.doradus.service.rest.NotFoundException;
import com.dell.doradus.service.rest.RESTCallback;
import com.dell.doradus.service.schema.SchemaService;
import com.dell.doradus.service.taskmanager.TaskManagerService;
import com.dell.doradus.tasks.DeleteLinkFieldData;
import com.dell.doradus.tasks.DoradusTask;

/**
 * Processes the following REST commands:
 * <pre>
 *      POST   /_tasks/{application}/{table}/{task-type}/{field}
 *      POST   /_tasks/{application}/{table}/{task-type}/{field}?{param}
 * </pre>
 */
public class FixDataCmd extends RESTCallback {

	@Override
	protected RESTResponse invoke() {
        String application = m_request.getVariableDecoded("application");
        ApplicationDefinition appDef = SchemaService.instance().getApplication(application);
        if (appDef == null) {
            throw new NotFoundException("Unknown application: " + application);
        }
        
        StorageService storageService = SchemaService.instance().getStorageService(appDef);
        Utils.require(storageService.getClass().getSimpleName().equals(SpiderService.class.getSimpleName()),
                      "Only SpiderService applications support this request: %s", application);
        
        String table = m_request.getVariableDecoded("table");
        TableDefinition tableDef = appDef.getTableDef(table);
        Utils.require(tableDef != null, "Unknown table for application '%s': %s", application, table);

        String taskType = m_request.getVariableDecoded("task-type");
        SchedType schedType = SchedType.getByName(taskType);
        Utils.require(schedType != null, "Unrecognized task type: %s", taskType);
        
        String field = m_request.getVariableDecoded("field");
        if (schedType == SchedType.RE_INDEX) {
        	FieldDefinition fieldDef = tableDef.getFieldDef(field);
	        Utils.require(fieldDef != null && fieldDef.isScalarField(),
	        		"Unknown scalar field for table '%s': %s", table, field);
        }
        
        String param = m_request.getVariable("param");
        
        String taskId = table + "/" + taskType + "/" + field;
        
        DoradusTask task = DoradusTask.createTask(application, taskId);
        Utils.require(task != null, "Couldn't start a task: %s/%s", application, taskId);
        if (schedType == SchedType.DELETE_LINK && param != null) {
        	Map<String, String> extraParams = Utils.parseURIQuery(param);
        	String tableParam = extraParams.get(DeleteLinkFieldData.TABLE_PARAM);
        	Utils.require(tableParam != null,
        			"Parameter %s not found among request parameters", DeleteLinkFieldData.TABLE_PARAM);
        	String inverseParam = extraParams.get(DeleteLinkFieldData.INVERSE_PARAM);
        	Utils.require(inverseParam != null,
        			"Parameter %s not found among request parameters", DeleteLinkFieldData.INVERSE_PARAM);
        	task.setExtraParam(DeleteLinkFieldData.TABLE_PARAM, tableParam);
        	task.setExtraParam(DeleteLinkFieldData.INVERSE_PARAM, inverseParam);
        }
        TaskManagerService.instance().startTask(task);
        return new RESTResponse(HttpCode.OK, "Task added");
	}

}
