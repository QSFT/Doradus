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

/**
 * Interface provides a filtering mechanism to filter tasks by some criteria
 * based on task's application name and/or task ID. It may be used as a callback
 * for the functions that must select some tasks for processing.
 */
public abstract class TaskFilter {
    // TODO: Need to add tenant to filter.
	public final static TaskFilter NO_FILTER = new TaskFilter() {
		@Override
		public boolean filter(String appName, String taskId) {
			return true;
		}
	};
	
	public abstract boolean filter(String appName, String taskId);
}
