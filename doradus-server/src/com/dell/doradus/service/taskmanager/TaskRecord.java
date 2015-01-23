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
    // Current task status fields used:
    public static final String PROP_EXECUTOR = "Executor";
    public static final String PROP_FINISH_TIME = "FinishTime";
    public static final String PROP_START_TIME = "StartTime";
    public static final String PROP_STATUS = "Status";
    
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
    }   // enum TaskStatus
    
    // Members:
    private final String  m_taskID;      
    private String        m_executor;           // IP address of Doradus node
    private String        m_finishTime;         // millisecond time in string form 
    private String        m_startTime;          // millisecond time in string form 
    private TaskStatus    m_status; 

    /**
     * Create a TaskRecord object using the given ID. All timestamps are null, and the
     * status is set to {@link TaskStatus#NEVER_EXECUTED}.
     * 
     * @param taskID    ID of task to which this status record applies.
     */
    public TaskRecord(String taskID) {
        m_taskID = taskID;
        m_status = TaskStatus.NEVER_EXECUTED;
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
     * Get the time at which this task finished, if known. If no finish-time has been set,
     * a Calendar object with minimal item (millis = 0) is returned.
     * 
     * @return  Finish time as a Calendar object in the UTC time zone.
     */
    public Calendar getFinishTime() {
        Calendar calendar = new GregorianCalendar(Utils.UTC_TIMEZONE);
        if (Utils.isEmpty(m_finishTime)) {
            calendar.setTimeInMillis(0);
        } else {
            calendar.setTimeInMillis(Long.parseLong(m_finishTime));
        }
        return calendar;
    }
    
    /**
     * Get the time at which this task started, if known. If no start-time has been set,
     * a Calendar object with minimal item (millis = 0) is returned.
     * 
     * @return  Start time as a Calendar object in the UTC time zone.
     */
    public Calendar getStartTime() {
        Calendar calendar = new GregorianCalendar(Utils.UTC_TIMEZONE);
        if (Utils.isEmpty(m_startTime)) {
            calendar.setTimeInMillis(0);
        } else {
            calendar.setTimeInMillis(Long.parseLong(m_startTime));
        }
        return calendar;
    }
    
    /**
     * Get all non-null properties for this task status object as a key/value map.
     * 
     * @return  Non-null status properties such as executor, timestamps, and status.
     */
    public Map<String, String> getProperties() {
        Map<String, String> propMap = new HashMap<>();
        if (!Utils.isEmpty(m_executor)) {
            propMap.put(PROP_EXECUTOR, m_executor);
        }
        if (!Utils.isEmpty(m_finishTime)) {
            propMap.put(PROP_FINISH_TIME, m_finishTime);
        }
        if (!Utils.isEmpty(m_startTime)) {
            propMap.put(PROP_START_TIME, m_startTime);
        }
        if (m_status != null) {
            propMap.put(PROP_STATUS, m_status.toString());
        }
        return propMap;
    }   // getProperties
    
    /**
     * Get this task status object's status. It will not be null.
     * 
     * @return  {@link TaskStatus} of task execution.
     */
    public TaskStatus getStatus() {
        return m_status;
    }
    
    //----- Setters
    
    /**
     * Set the given status property to the given value. Only properties defined in this
     * class (see public static strings) are recognized: all unknown property names are
     * ignored. Timestamp values must be given as an integer in string form, expressed as
     * milliseconds since the epoch. The status property must be given as a known
     * {@link TaskStatus} display value.
     *  
     * @param name  Property name to add or update.
     * @param value New value for property.
     */
    public void setProperty(String name, String value) {
        switch (name) {
        case PROP_EXECUTOR:
            m_executor = value;
            break;
        case PROP_FINISH_TIME:
            m_finishTime = value;
            break;
        case PROP_START_TIME:
            m_startTime = value;
            break;
        case PROP_STATUS:
            m_status = TaskStatus.findStatus(value);
        default:
            // Ignore unrecognized properties.
        }
    }   // setProperty

    /**
     * Set this task record's status to the given value.
     * 
     * @param status    New {@link TaskStatus} value.
     */
    public void setStatus(TaskStatus status) {
        m_status = status;
    }

    /**
     * Serialize this task record into a UNode tree. The root UNode is returned, which is
     * a map containing one child value node for each TaskRecord property. The map's name
     * is the task ID with a tag name of "task".
     * 
     * @return  Root of a {@link UNode} tree.
     */
    public UNode toDoc() {
        UNode rootNode = UNode.createMapNode(m_taskID, "task");
        if (!Utils.isEmpty(m_executor)) {
            rootNode.addValueNode(PROP_EXECUTOR, m_executor);
        }
        if (!Utils.isEmpty(m_finishTime)) {
            rootNode.addValueNode(PROP_FINISH_TIME, formatTimestamp(m_finishTime));
        }
        if (!Utils.isEmpty(m_startTime)) {
            rootNode.addValueNode(PROP_START_TIME, formatTimestamp(m_startTime));
        }
        if (m_status != null) {
            rootNode.addValueNode(PROP_STATUS, m_status.toString());
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
