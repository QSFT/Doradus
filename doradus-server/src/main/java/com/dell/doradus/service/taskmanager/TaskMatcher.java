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

public class TaskMatcher {
	final String m_appNamePattern;
	final String m_taskIdPattern;
	
	public TaskMatcher(String appNamePattern, String taskIdPattern) {
		m_appNamePattern = appNamePattern;
		m_taskIdPattern = taskIdPattern;
	}
	
	public boolean match(final String appName, final String taskId) {
		String[] patternParts = m_taskIdPattern.split("/");
		String[] idParts = taskId.split("/");
		// application names matches?
		if (!"*".equals(m_appNamePattern) && !appName.equals(m_appNamePattern)) {
			return false;
		}
		// table names matches?
		if (!"*".equals(patternParts[0]) && !idParts[0].equals(patternParts[0])) {
			return false;
		}
		// task type matches?
		if (!"*".equals(patternParts[1]) && !idParts[1].equals(patternParts[1])) {
			return false;
		}
		// task parameters matches?
		if (patternParts.length == 3 && !"*".equals(patternParts[2]) && 
				(idParts.length < 3 || !idParts[2].equals(patternParts[2]))) {
			return false;
		}
		return true;
	}
	

}
