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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * BatchResult holds the results of a batch add- or update-object request.
 */
public class BatchResult {
    // Possible batch-level statuses:
    public enum Status {OK, WARNING, ERROR};
    
    // Field keys currently used:
    private static final String COMMENT = "comment";
    private static final String ERROR_MSG = "error";
    private static final String STACK_TRACE = "stacktrace";
    private static final String STATUS = "status";
    private static final String HAS_UPDATES = "has_updates";
    
    // Overall batch fields (status, error, etc.)
    private final Map<String, String> m_resultFields = new HashMap<String, String>();
    
    // ObjectResults of individually failed objects, indexed by object ID.
    private final Map<String, ObjectResult> m_objResultMap = new HashMap<String, ObjectResult>();
    
    /**
     * Create a new BatchResult with an ERROR status and the given error message.
     * 
     * @param errMsg    Error message.
     * @return          New BatchResult object with ERROR status and given error message.
     */
    public static BatchResult newErrorResult(String errMsg) {
        BatchResult result = new BatchResult();
        result.setStatus(Status.ERROR);
        result.setErrorMessage(errMsg);
        return result;
    }   // newErrorResult
    
    /**
     * Create a new BatchResult with an initial status of "OK". Additional fields can be
     * set via setXxx() methods.
     */
    public BatchResult() {
        setStatus(Status.OK);
    }   // constructor

    /**
     * Create a BatchResult object from the UNode tree rooted at the given node. The node
     * must be a map called "batch-result".
     * 
     * @param rootNode  Root node of UNode tree describing a BatchResult.
     */
    public BatchResult(UNode rootNode) {
        // Root node must be a MAP called "batch-result".
        Utils.require(rootNode.isMap() && rootNode.getName().equals("batch-result"),
                      "Root node must be a map called 'batch-result': " + rootNode);
        for (String childName : rootNode.getMemberNames()) {
            UNode childNode = rootNode.getMember(childName);
            if (childName.equals("docs")) {
                // Must be an array, each member of which is a "doc".
                Utils.require(childNode.isCollection(),
                              "'docs' node must be an array: " + childNode);
                for (UNode docNode : childNode.getMemberList()) {
                    addObjectResult(new ObjectResult(docNode));
                }
            } else if (childNode.isValue()) {
                // Put all other value nodes into "result fields" map.
                m_resultFields.put(childNode.getName(), childNode.getValue());
            } else {
                // Unknown child node of batch-result.
                Utils.require(false, "Unexpected child node of 'batch-result': " + childNode);
            }
        }
    }   // constructor

    ///// Setters
    
    /**
     * Add the given {@link ObjectResult} to this batch. If the given result has no
     * object ID, it is indexed with a unique "fake" object ID so that it will appear
     * in {@link #getFailedObjectIDs()} and {@link #getObjectResult(String)}.
     * 
     * @param objResult ObjectResult to add to this batch result.
     */
    public void addObjectResult(ObjectResult objResult) {
        String objectID = objResult.getObjectID();
        if (Utils.isEmpty(objectID)) {
            objectID = fakeObjectID();
        }
        assert !m_objResultMap.containsKey(objectID);
        m_objResultMap.put(objectID, objResult);
        
        // Elevate batch-level status to warning if needed.
        Status batchResult = getStatus();
        if (batchResult == Status.OK && objResult.getStatus() != ObjectResult.Status.OK) {
            setStatus(Status.WARNING);
        }
        if (objResult.isUpdated()) {
            setHasUpdates(true);
        }
    }   // addObjectResult 
    
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
     * Set the "has updates" state of this batch result. When true, this means that at
     * lease one update was performed by the batch. False means no updates were performed,
     * either because of failures or because no updates were necessary. This method is
     * called with a value of "true" automatically when {@link #addObjectResult(ObjectResult)}
     * is passed an ObjectResult whose isUpdated() value is true.
     * 
     * @param bHasUpdates   True to mark this batch result as having performed at least one
     *                      update.
     */
    public void setHasUpdates(boolean bHasUpdates) {
        m_resultFields.put(HAS_UPDATES, Boolean.toString(bHasUpdates));
    }   // setHasUpdates
    
    /**
     * Set the strack trace for this result.
     * 
     * @param stackTrace - New stack trace.
     */
    public void setStackTrace(String stackTrace) {
        m_resultFields.put(STACK_TRACE, stackTrace);
    }   // setStackTrace
    
    /**
     * Set the status of this batch result to the given value. When
     * {@link #addObjectResult(ObjectResult)} is called, the batch result's status is
     * automatically elevated if needed based on the status of the given ObjectResult. 
     * 
     * @param status - New status value.
     */
    public void setStatus(Status status) {
        m_resultFields.put(STATUS, status.toString());
    }   // setStatus
    
    ///// Getters
    
    /**
     * Get the comment for this response, if any
     * 
     * @return Comment for this response, if any.
     */
    public String getComment() {
        return m_resultFields.get(COMMENT);
    }   // getObjectID
    
    /**
     * Get the batch-level error message for this BatchResult. If there is no batch-level
     * error message, null is returned.
     * 
     * @return  Batch-level error message.
     */
    public String getErrorMessage() {
        return m_resultFields.get(ERROR_MSG);
    }   // getErrorMessage
    
    /**
     * Get the set of object IDs whose update failed in this batch result. There will be
     * {@link ObjectResult} objects present for each one. When an error occurs but the
     * corresponding object did not acquire an object ID (e.g., when a new object was
     * being added), a "fake" object ID is assigned to distinguish it from the results of
     * other objects.
     * 
     * @return  Set of object IDs whose update failed.
     */
    public Set<String> getFailedObjectIDs() {
        Set<String> result = new HashSet<>();
        for (String objID : m_objResultMap.keySet()) {
            if (m_objResultMap.get(objID).isFailed()) {
                result.add(objID);
            }
        }
        return result;
    }   // getFailedObjectIDs
    
    /**
     * Get the {@link ObjectResult} for the object with the given object ID. If there is
     * result for the given object ID in this match, null is returned.
     * 
     * @param objectID  Object ID of a failed object.
     * @return          Object's error message or null if there isn't one.
     */
    public ObjectResult getObjectResult(String objectID) {
        return m_objResultMap.get(objectID);
    }   // getObjectResult
    
    /**
     * Return the set of object IDs for which this BatchResult has an {@link ObjectResult}.
     * If an object result was received that did not contain an object ID, a fake, unique
     * object ID is assigned so that it will appear in this set and can be used by
     * {@link #getObjectResult(String)}. If this BatchResult has no {@link ObjectResult}s,
     * an empty set is returned.
     * 
     * @return  The set of object IDs for which this BatchResult has an {@link ObjectResult}.
     */
    public Set<String> getResultObjectIDs() {
        return m_objResultMap.keySet();
    }   // getResultObjectIDs
    
    /**
     * Get the stack trace for this result, if one exists.
     * 
     * @return  Stack trace for this result or null if there isn't one.
     */
    public String getStackTrace() {
        return m_resultFields.get(STACK_TRACE);
    }   // getStackTrace
    
    /**
     * Get the status string for this batch. If no status has been explicitly assigned,
     * the default is OK.
     * 
     * @return  Batch-level status.
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
     * Return true if this batch result has at least one object that was updated.
     *  
     * @return  True if at least one object was updated by the batch that created this
     *          result.
     */
    public boolean hasUpdates() {
        String hasUpdates = m_resultFields.get(HAS_UPDATES);
        return hasUpdates != null && Boolean.parseBoolean(hasUpdates);
    }   // hasUpdates
    
    /**
     * Indicate if the status for this BatchResult is WARNING or ERROR, which means there
     * are errors for one or more objects or for the batch as a whole.
     * 
     * @return True if this BatchResult has errors.
     */
    public boolean isFailed() {
        return getStatus() == Status.ERROR;
    }   // isFailed

    /**
     * Serialize this BatchResult result into a UNode tree. The root node is called
     * "batch-result".
     * 
     * @return  This object serialized into a UNode tree.
     */
    public UNode toDoc() {
        // Root node is map with 1 child per result field and optionally "docs".
        UNode result = UNode.createMapNode("batch-result");
        for (String fieldName : m_resultFields.keySet()) {
            result.addValueNode(fieldName, m_resultFields.get(fieldName));
        }
        if (m_objResultMap.size() > 0) {
            UNode docsNode = result.addArrayNode("docs");
            for (ObjectResult objResult : m_objResultMap.values()) {
                docsNode.addChildNode(objResult.toDoc());
            }
        }
        return result;
    }   // toDoc

    // Next fake object ID value.
    private static final AtomicInteger g_nextObjectID = new AtomicInteger(1);
    
    // Generate a fake object ID for objects where the _ID field wasn't returned.
    private static String fakeObjectID() {
        // Prefix the next fake object ID # with "#fake-objectID-"
        return "#unknown-objectID-" + g_nextObjectID.getAndIncrement();
    }   // fakeObjectID

}   // class BatchResult
