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

import java.util.HashMap;
import java.util.Map;

/**
 * Represents the execution stages of task.
 */
public enum TaskRunState {
	
	/**
	 * Indicates the task wasn't executed at all. For example, it will be initial 
	 * state of new task after its creation.
	 */
	Undefined, 
	
	/**
	 * Indicates the task execution is complete for the unknown reasons
	 * (- server has been killed, for example). 
	 * <p>
	 * The task manager must detect such situations during initialization, 
	 * when server is restarted. A condition example: a saved TaskStatus  
	 * says the task is executing just now (see TaskStatus.isExecuting).
	 */
	Unknown,
	
	/** 
	 * The task execution is started but has not yet completed. 
	 */
	Started, 
	
	/** 
	 * The task execution has been completed successfully. 
	 */
	Succeeded, 
	
	/** 
	 * The task execution has been completed due to an exception. 
	 */
	Failed, 
	
	/** 
	 * The "interrupt execution" signal has been received by task from 
	 * environment (when the task is in Started state) but execution has 
	 * not yet completed (see ITaskManager.interrupt).
	 */
	Interrupting, 
	
	/** 
	 * The task execution has been interrupted in response to "interrupt 
	 * execution" signal.
	 */
	Interrupted;
    // Map of all known states by up-cased name (even though enum names are usually
    // up-cased by convention).
    private static final Map<String, TaskRunState> STATE_NAMES =
        new HashMap<String, TaskRunState>();
    
    // The static initializer is called after all enum objects are constructed. Hence,
    // we can use the static values() method to iterate them an build the map. 
    static {
        for (TaskRunState method : values()) {
        	STATE_NAMES.put(method.toString().toUpperCase(), method);
        }
    }
    /**
     * Return the TaskRunState object associated with the given case-insensitive enum
     * name or null if the given name is unknown.
     * 
     * @param   statusName Case-insensitive name of an Task status (e.g. "Started").
     * @return             Corresponding TaskRunState enum, if recognized, else null.
     */
    public static TaskRunState statusFromString(String statusName) {
        return STATE_NAMES.get(statusName.toUpperCase());
    }   // methodFromString

}
