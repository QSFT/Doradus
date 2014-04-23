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


package com.dell.doradus.olap;

import java.util.HashMap;
import java.util.Map;

import com.dell.doradus.common.UNode;
import com.dell.doradus.common.Utils;
import com.dell.doradus.management.TaskRunState;
import com.dell.doradus.management.TaskStatus;

public class OlapTasks {
	private Map<String, TaskStatus> tasks = new HashMap<String, TaskStatus>();
	
	public UNode toDoc() {
	    UNode root = UNode.createArrayNode("tasks");
	    
	    for (Map.Entry<String, TaskStatus> task : tasks.entrySet()) {
	        UNode taskNode = root.addMapNode("task");
	        String taskId = task.getKey();
	        TaskStatus taskStatus = task.getValue();
	        taskNode.addValueNode("id", taskId);
	        taskNode.addValueNode("status", taskStatus.getLastRunState().toString());
	        taskNode.addValueNode("lastPerformed", Long.toString(taskStatus.getLastRunActualStartTime()));
	        taskNode.addValueNode("finishTime", Long.toString(taskStatus.getLastRunFinishTime()));
	        taskNode.addValueNode("elapsedTimeSeconds", Utils.formatElapsedTime(taskStatus.getLastRunFinishTime() - taskStatus.getLastRunActualStartTime()));
	    }
	    return root;
	}
	
	public void parse(UNode root) {
	    tasks.clear();
	    // { tasks:
	    assert root.getName().equals("tasks");
	    assert root.isArray();
	    //     [ {
	    for (UNode taskNode : root.getMemberList()) {
	        //      task: {
	        assert taskNode.getName().equals("task");
	        assert taskNode.isMap();
	        TaskStatus status = new TaskStatus();
	        for (UNode childNode : taskNode.getMemberList()) {
	            assert childNode.isValue();
	            String name = childNode.getName();
	            String value = childNode.getValue();
	            if ("id".equals(name)) {
	                //   "id": "*/data-aging",
	                tasks.put(value, status);
	            } else if ("status".equals(name)) {
	                //   "status": "Succeeded",
	                status.setLastRunState(TaskRunState.statusFromString(value));
	            } else if ("lastPerformed".equals(name)) {
	                //   "lastPerformed": "1326700000",
	                status.setLastRunActualStartTime(Long.parseLong(value));
	            } else if ("finishTime".equals(name)) {
	                //   "finishTime": "1326700020",
	                status.setLastRunFinishTime(Long.parseLong(value));
	            }
	        }
	        // }
	    }
	    // ] }
	}
	
	public TaskStatus getTaskStatus(String taskId) {
		return tasks.get(taskId);
	}
	
	public void setTaskStatus(String taskId, TaskStatus status) {
		tasks.put(taskId,  status);
	}
	
	public Map<String, TaskStatus> getTasks() {
		return tasks;
	}
}
