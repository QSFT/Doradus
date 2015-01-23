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

import com.dell.doradus.common.Utils;

/**
 * Specifies a task to be performed by the {@link TaskManagerService}. Each object identifies the
 * application, table (if applicable), task type, and execution frequency of the task. 
 */
public class Task {
    // Members:
    private final String  m_appName;
    private final String  m_tableName;
    private final String  m_taskName;
    private final TaskFrequency m_taskFreq;
    private final Class<? extends TaskExecutor> m_executorClass;

    /**
     * Create a task with the given properties.
     * 
     * @param appName       Name of the application to which the task applies.
     * @param tableName     Name of the table to which the task applies. Can be empty or
     *                      null if the task is not table-specific.
     * @param taskName      Name of task. This name identifies the task type and must be
     *                      unique within task types used by a given storage manager.
     *                      Example: "data-aging".
     * @param taskFreq      {@link TaskFrequency} that describes how often the task should
     *                      be executed: "1 DAY", "30 MINUTES", etc.
     * @param executorClass Class object for {@link TaskExecutor} subclass that executes
     *                      this task when it is time to run.
     */
    public Task(String appName, String tableName, String taskName, String taskFreq, Class<? extends TaskExecutor> executorClass) {
        m_appName = appName;
        m_tableName = Utils.isEmpty(tableName) ? "*" : tableName;
        m_taskName = taskName;
        m_taskFreq = new TaskFrequency(taskFreq);
        m_executorClass = executorClass;
    }

    /**
     * Get this task's application name.
     * 
     * @return  App name as a string.
     */
    public String getAppName() {
        return m_appName;
    }
    
    /**
     * Get this task's table name, or "*" if the task is not table-specific.
     * 
     * @return  Table name or "*".
     */
    public String getTableName() {
        return m_tableName;
    }
    
    /**
     * This task's name, such as "data-aging".
     * 
     * @return  This task's type name.
     */
    public String getTaskName() {
        return m_taskName;
    }

    /**
     * Get the frequency at which this task is defined to execute.
     * 
     * @return  Task frequency as a {@link TaskFrequency} object.
     */
    public TaskFrequency getTaskFreq() {
        return m_taskFreq;
    }
    
    /**
     * Get this task's ID, which is a three part string such as:
     * <pre>
     *      foo/bar/data-aging
     * </pre>
     * 
     * @return  This task's ID.
     */
    public String getTaskID() {
        return m_appName + "/" + m_tableName + "/" + m_taskName;
    }
    
    /**
     * Get the {@link TaskExecutor} subclass that must be used to execute this task.
     * 
     * @return  TaskExecutor subclass {@link Class} object.
     */
    public Class<? extends TaskExecutor> getExecutorClass() {
        return m_executorClass;
    } 
    
    /**
     * Same as {@link #getTaskID()}.
     */
    @Override
    public String toString() {
        return getTaskID();
    }

}   // class Task
