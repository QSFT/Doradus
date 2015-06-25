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

import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;

import com.dell.doradus.common.UNode;
import com.dell.doradus.common.Utils;

/**
 * Represents the status of a task execution. A TaskRecord holds the task's ID, status,
 * and timestamps that describes the execution status. This object can be created and
 * then written to the database, or it can be created by reading a task status record
 * from the database.
 */
public class TaskRecord {
    // Current task status fields used; suffix time-value properties with "Time":
    public static final String PROP_EXECUTOR = "Executor";
    public static final String PROP_FINISH_TIME = "FinishTime";
    public static final String PROP_START_TIME = "StartTime";
    public static final String PROP_STATUS = "Status";
    public static final String PROP_PROGRESS = "Progress";
    public static final String PROP_PROGRESS_TIME = "ProgressTime";
    public static final String PROP_FAIL_REASON = "FailReason";
    
    // Members:
    private final String  m_taskID;
    private final Map<String, String> m_properties = new HashMap<>();

    /**
     * Status values that a task execution can have. 
     */
    public enum TaskStatus {
        NEVER_EXECUTED(""),
        IN_PROGRESS("In progress"),
        FAILED("Failed"),
        COMPLETED("Completed");
        
        private final String m_displayString;
        private TaskStatus(String displayString) {
            m_displayString = displayString;
        }
        
        @Override
        public String toString() {
            return m_displayString;
        }
        
        /**
         * Convert display value to corresponding enum.
         *  
         * @param displayString Value from {@link #toString()}.
         * @return              Corresponding TaskStatus or null if unknown.
         */
        public static TaskStatus findStatus(String displayString) {
            if (Utils.isEmpty(displayString)) {
                return NEVER_EXECUTED;
            }
            for (TaskStatus status : TaskStatus.values()) {
                if (status.toString().equalsIgnoreCase(displayString)) {
                    return status;
                }
            }
            return null;
        }

        /**
         * Return true if the given status is Failed or Completed.
         * 
         * @param status    TaskStatus to test.
         * @return          True if the status says the task is done.
         */
        public static boolean isCompleted(TaskStatus status) {
            return status == FAILED || status == COMPLETED;
        }
    }   // enum TaskStatus
    
    /**
     * Create a TaskRecord object using the given ID. All timestamps are null, and the
     * status is set to {@link TaskStatus#NEVER_EXECUTED}.
     * 
     * @param taskID    ID of task to which this status record applies.
     */
    public TaskRecord(String taskID) {
        m_taskID = taskID;
        setProperty(PROP_STATUS, TaskStatus.NEVER_EXECUTED.toString());
    }
    
    //----- Getters
    
    /**
     * Get this status object's task ID.
     * 
     * @return  Task ID: will not be null.
     */
    public String getTaskID() {
        return m_taskID;
    }
    
    /**
     * Get the value of a time property as a Calendar value. If the given property has
     * not been set or is not a time value, null is returned.
     * 
     * @param propName  Name of a time-valued task record property to fetch. 
     * @return          Time property value as a Calendar object in the UTC time zone.
     */
    public Calendar getTime(String propName) {
        assert propName.endsWith("Time");
        if (!m_properties.containsKey(propName)) {
            return null;
        }
        Calendar calendar = new GregorianCalendar(Utils.UTC_TIMEZONE);
        calendar.setTimeInMillis(Long.parseLong(m_properties.get(propName)));
        return calendar;
    }
    
    /**
     * Get all non-null properties for this task status object as a key/value map.
     * 
     * @return  Non-null status properties such as executor, timestamps, and status.
     */
    public Map<String, String> getProperties() {
        return new HashMap<>(m_properties);
    }   // getProperties
    
    /**
     * Get this task status object's status. It will not be null.
     * 
     * @return  {@link TaskStatus} of task execution.
     */
    public TaskStatus getStatus() {
        return TaskStatus.findStatus(m_properties.get(PROP_STATUS));
    }
    
    //----- Setters
    
    /**
     * Set the given status property to the given value. Timestamp values must be given as
     * an integer in string form, expressed as milliseconds since the epoch. The status
     * property must be given as a known {@link TaskStatus#toString()} display value.
     *  
     * @param name  Property name to add or update.
     * @param value New value for property. May be empty/null.
     */
    public void setProperty(String name, String value) {
        m_properties.put(name, value);
    }   // setProperty

    /**
     * Set this task record's status to the given value.
     * 
     * @param status    New {@link TaskStatus} value.
     */
    public void setStatus(TaskStatus status) {
        m_properties.put(PROP_STATUS, status.toString());
    }

    /**
     * Serialize this task record into a UNode tree. The root UNode is returned, which is
     * a map containing one child value node for each TaskRecord property. The map's name
     * is the task ID with a tag name of "task". Timestamp values are formatted into
     * friendly display format.
     * 
     * @return  Root of a {@link UNode} tree.
     */
    public UNode toDoc() {
        UNode rootNode = UNode.createMapNode(m_taskID, "task");
        for (String name : m_properties.keySet()) {
            String value = m_properties.get(name);
            if (name.endsWith("Time")) {
                rootNode.addValueNode(name, formatTimestamp(value));
            } else {
                rootNode.addValueNode(name, value);
            }
        }
        return rootNode;
    }

    // Format the given integer timestamp value into a date/time string.
    private String formatTimestamp(String value) {
        try {
            long longValue = Long.parseLong(value);
            return Utils.formatDate(longValue);
        } catch (NumberFormatException e) {
            return "Invalid timestamp: " + value; 
        }
    }   // formatTimestamp
    
}   // class TaskRecord
