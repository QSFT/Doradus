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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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
    private final Map<String, String> m_resultFields = new LinkedHashMap<String, String>();
    
    // ObjectResults of individually failed objects, indexed by object ID.
    private final List<ObjectResult> m_objResultList = new ArrayList<ObjectResult>();
    
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
     * must be called "batch-result".
     * 
     * @param rootNode  Root node of UNode tree describing a BatchResult.
     */
    public BatchResult(UNode rootNode) {
        // Root node must be called "batch-result".
        Utils.require(rootNode.getName().equals("batch-result"),
                      "Root node must be called 'batch-result': " + rootNode);
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
     * Add the given {@link ObjectResult} to this batch.
     * 
     * @param objResult ObjectResult to add to this batch result.
     */
    public void addObjectResult(ObjectResult objResult) {
        m_objResultList.add(objResult);
        
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
     * Get the {@link ObjectResult}s for objects whose failed in this batch. When an
     * object update fails, if the original DBObject had no object ID, none will exist in
     * the corresponding ObjectResult either.
     * 
     * @return  Set of object IDs whose update failed.
     */
    public Iterable<ObjectResult> getFailedObjectResults() {
        List<ObjectResult> result = new ArrayList<>();
        for (ObjectResult objResult : m_objResultList) {
            if (objResult.isFailed()) {
                result.add(objResult);
            }
        }
        return result;
    }   // getFailedObjectResults
    
    /**
     * Return the the {@link ObjectResult}s for the batch result as an iterable object.
     * The ObjectResults are returned in the same order they were added to the original
     * {@link DBObjectBatch} that generated this result. If this BatchResult has no
     * ObjectResults, the iterator will be empty but not null.
     * 
     * @return  The {@link ObjectResult}s of this batch result as an Iterable object.
     */
    public Iterable<ObjectResult> getResultObjects() {
        return m_objResultList;
    }   // getResultObjects
    
    /**
     * Return the count of {@link ObjectResult}s in this batch results.
     * 
     * @return  Count of {@link ObjectResult}s in this batch results.
     */
    public int getResultObjectCount() {
        return m_objResultList.size();
    }

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
        if (m_objResultList.size() > 0) {
            UNode docsNode = result.addArrayNode("docs");
            for (ObjectResult objResult : m_objResultList) {
                docsNode.addChildNode(objResult.toDoc());
            }
        }
        return result;
    }   // toDoc

}   // class BatchResult
