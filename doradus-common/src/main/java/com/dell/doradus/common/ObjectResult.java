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

package com.dell.doradus.common;

//import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * ObjectResult holds the results of an add- or update-object request. It minimally has an
 * object ID and status, and it optionally has other result fields as a set of strings.
 */
public class ObjectResult {
    // Possible object-level statuses:
    public enum Status {OK, WARNING, ERROR};
    
    // All fields are stored in this simple name/value map.
    private Map<String, String> m_resultFields = new LinkedHashMap<String, String>();

    // Field keys currently used:
    private static final String COMMENT = "comment";
    private static final String ERROR_MSG = "error";
    private static final String OBJECT_ID = CommonDefs.ID_FIELD;
    private static final String STACK_TRACE = "stacktrace";
    private static final String STATUS = "status";
    private static final String OBJECT_UPDATED = "updated";
    
    /**
     * Create an ObjectResult with a status of ERROR and the given error message and
     * optional object ID.
     * 
     * @param errMsg    Error message.
     * @param objID     Optional object ID.
     */
    public static ObjectResult newErrorResult(String errMsg, String objID) {
        ObjectResult result = new ObjectResult();
        result.setStatus(Status.ERROR);
        result.setErrorMessage(errMsg);
        if (!Utils.isEmpty(objID)) {
            result.setObjectID(objID);
        }
        return result;
    }   // newErrorResult
    
    /**
     * Create an ObjectResult with an initial status of "OK". A different status and
     * alternate fields can be set with setXXX() methods.  
     */
    public ObjectResult() {
        setStatus(Status.OK);
    }   // constructor

    /**
     * Create an ObjectResult with an initial status of "OK" and the given object ID.
     * 
     * @param objID Object ID assigned to this ObjectResult.
     */
    public ObjectResult(String objID) {
        setStatus(Status.OK);
        setObjectID(objID);
    }   // constructor
    
    /**
     * Create an ObjectResult from the UNode tree rooted at the given node. The node must
     * be a MAP called "doc" and contain only simple name/value pairs.
     * 
     * @param docNode   Root UNode of an ObjectResult definition.
     */
    public ObjectResult(UNode docNode) {
        // Root node must called "doc".
        Utils.require(docNode.getName().equals("doc"),
                      "Root node must be called 'doc': " + docNode);
        for (String childName : docNode.getMemberNames()) {
            // Child node must be a simple value
            UNode childNode = docNode.getMember(childName);
            Utils.require(childNode.isValue(),
                          "'doc' node should be a value: " + childNode); 
            m_resultFields.put(childName, childNode.getValue());
        }
    }   // constructor

    ///// Getters
    
    /**
     * Indicate if this ObjectResult represents a failed update.
     * 
     * @return True if this ObjectResult represents a failed update.
     */
    public boolean isFailed() {
        return getStatus() != Status.OK;
    }   // isFailed

    /**
     * Get the comment for this response, if any
     * 
     * @return Comment for this response, if any.
     */
    public String getComment() {
        return m_resultFields.get(COMMENT);
    }   // getObjectID
    
    /**
     * Get the object ID associated with this response.
     * 
     * @return Object ID associated with this response.
     */
    public String getObjectID() {
        return m_resultFields.get(OBJECT_ID);
    }   // getObjectID
    
    /**
     * Get the error message for this response object, if one exists.
     * 
     * @return  Error message for this response or null if there isn't one.
     */
    public String getErrorMessage() {
        return m_resultFields.get(ERROR_MSG);
    }   // getErrorMessage
    
    /**
     * Get the error details for this response object, if any exists. The error details is
     * typically a stack trace or comment message. The details are returned as a map of
     * detail names to values.
     * 
     * @return  Map of error detail names to values, if any. If there are no details, the
     *          map will be empty (not null).
     */
    public Map<String, String> getErrorDetails() {
        // Add stacktrace and/or comment fields.
        Map<String, String> detailMap = new LinkedHashMap<String, String>();
        if (m_resultFields.containsKey(COMMENT)) {
            detailMap.put(COMMENT, m_resultFields.get(COMMENT));
        }
        if (m_resultFields.containsKey(STACK_TRACE)) {
            detailMap.put(STACK_TRACE, m_resultFields.get(STACK_TRACE));
        }
        return detailMap;
    }   // getErrorDetail
    
    /**
     * Get the stack trace for this response object, if one exists.
     * 
     * @return  Stack trace for this response or null if there isn't one.
     */
    public String getStackTrace() {
        return m_resultFields.get(STACK_TRACE);
    }   // getStackTrace
    
    /**
     * Get the status for this object update result. If a status has not been explicitly
     * defined, a value of OK is returned.
     * 
     * @return  Status value of this object update result.
     */
    public Status getStatus() {
        String status = m_resultFields.get(STATUS);
        if (status == null) {
            return Status.OK;
        } else {
            return Status.valueOf(status.toUpperCase());
        }
    }   // getStatus
    
    /**
     * Return true if the update reflected in this status object actually updated the
     * database. False means no update was performed either due to errors or because the
     * requested updated required no changes to the database, e.g., the object was already
     * added, updated, or deleted. See {@link #isFailed()} to see if an error occurred.
     * 
     * @return  True if the update reflected in this result actually made changes to the
     *          database.
     */
    public boolean isUpdated() {
        String objUpdated = m_resultFields.get(OBJECT_UPDATED);
        return objUpdated != null && objUpdated.equalsIgnoreCase("true");
    }   // isUpdated
    
    /**
     * Serialize this ObjectResult into a UNode tree. The root node is called "doc".
     * 
     * @return  This object serialized into a UNode tree.
     */
    public UNode toDoc() {
        // Root node is called "doc".
        UNode result = UNode.createMapNode("doc");
        
        // Each child of "doc" is a simple VALUE node.
        for (String fieldName : m_resultFields.keySet()) {
            // In XML, we want the element name to be "field" when the node name is "_ID".
            if (fieldName.equals(OBJECT_ID)) {
                result.addValueNode(fieldName, m_resultFields.get(fieldName), "field");
            } else {
                result.addValueNode(fieldName, m_resultFields.get(fieldName));
            }
        }
        return result;
    }   // toDoc

    ///// Setters
    
    /**
     * Set the comment field to the given value.
     * 
     * @param comment - New comment value.
     */
    public void setComment(String comment) {
        m_resultFields.put(COMMENT, comment);
    }   // setComment
    
    /**
     * Set the error message to the given value.
     * 
     * @param errMsg - New error message.
     */
    public void setErrorMessage(String errMsg) {
        m_resultFields.put(ERROR_MSG, errMsg);
    }   // setErrorMessage
    
    /**
     * Set the object ID for this result.
     * 
     * @param objID - New Object ID value.
     */
    public void setObjectID(String objID) {
        m_resultFields.put(OBJECT_ID, objID);
    }   // setObjectID
    
    /**
     * Set the strack trace for this result.
     * 
     * @param stackTrace - New stack trace.
     */
    public void setStackTrace(String stackTrace) {
        m_resultFields.put(STACK_TRACE, stackTrace);
    }   // setStackTrace
    
    /**
     * Set the status of this result to the given value.
     * 
     * @param status - New status value.
     */
    public void setStatus(Status status) {
        m_resultFields.put(STATUS, status.toString());
    }   // setStatus
    
    /**
     * Set the object-updated state for this status. "True" means the database was updated
     * to perform the requested update. "False" means no updates were performed either
     * because of errors or because no updates were required, e.g., the object was already
     * added, updated, or deleted.
     * 
     * @param bUpdated  New value for object-updated state.
     */
    public void setUpdated(boolean bUpdated) {
        m_resultFields.put(OBJECT_UPDATED, Boolean.toString(bUpdated));
    }   // setUpdated
    
}   // ObjectResult
