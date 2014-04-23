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

package com.dell.doradus.service;

/**
 * Abstract root class for Doradus StorageService implementations. Defines methods that
 * must be implemented by a subclass to function as a storage service.
 */
import com.dell.doradus.common.AggregateResult;
import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.BatchResult;
import com.dell.doradus.common.DBObject;
import com.dell.doradus.common.DBObjectBatch;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.common.UNode;
import com.dell.doradus.search.SearchResultList;

/**
 * Abstract root class for Doradus storage services. Extends the {@link Service} class
 * with methods that subclasses must have to operate as a storage service. Methods are
 * provided for schema management, object queries, and object updates. The DoradusServer
 * determines which storage service manages each application and routes
 * application-specific REST commands command to the appropriate service.
 */
public abstract class StorageService extends Service {

    //----- Schema update methods
    
    /**
     * Delete all service-specific storage for the given application.
     * 
     * @param appDef    {@link ApplicationDefinition} of application to delete.
     */
    public abstract void deleteApplication(ApplicationDefinition appDef);

    /**
     * Implement storage service-specific storage changes, if any, for the given
     * application. If the application is being modified, the existing application's
     * definition is passed since the new one has already been stored.
     * 
     * @param oldAppDef {@link ApplicationDefinition} of existing application, if present.
     * @param appDef    {@link ApplicationDefinition} of a new application.
     */
    public abstract void initializeApplication(ApplicationDefinition oldAppDef,
                                               ApplicationDefinition appDef);
    
    /**
     * Perform storage service-specific checks for the given application definition. An
     * exception (usually IllegalArgumentException) should be thrown if the application is
     * not valid for any reason.
     *  
     * @param appDef    {@link ApplicationDefinition} to validate.
     */
    public abstract void validateSchema(ApplicationDefinition appDef);

    //----- Object query methods
    
    /**
     * Get all field values for the object in the given table with the given ID. If no
     * such object is found, null is returned.
     * 
     * @param tableDef  {@link TableDefinition} to which object should belong.
     * @param objID     ID of object to find.
     * @return          {@link DBObject} containing all scalar and link field values for
     *                  the given object if found, otherwise null.
     */
    public abstract DBObject getObject(TableDefinition tableDef, String objID);
    
    /**
     * Perform an object query on the given table using the given URI parameters. The URI
     * parameters come after the "?" in a query URI and should be encoded. Example:
     * <pre>
     *      .../_query?q=Foo:bar+Name:(John+Smith)&f=_ID
     * </pre>
     * In this example, params should be passed as "Foo:bar+Name:(John+Smith)&f=_ID".  
     * 
     * @param tableDef  {@link TableDefinition} of table to query.
     * @param params    Query parameters in URI format as described above.
     * @return          Object query results as a {@link SearchResultList} object.
     */
    public abstract SearchResultList objectQueryURI(TableDefinition tableDef, String params);
    
    /**
     * Perform an object query on the given table passing query parameters as a UNode tree.
     * The parameters should be parsed from an XML or JSON entity, passing the root object
     * of the tree.  
     * 
     * @param tableDef  {@link TableDefinition} of table to query.
     * @param rootNode  Root {@link UNode} of parsed query parameters.
     * @return          Object query results as a {@link SearchResultList} object.
     */
    public abstract SearchResultList objectQueryDoc(TableDefinition tableDef, UNode rootNode);
    
    /**
     * Perform an aggregate query on the given table using the given URI parameters. The
     * URI parameters come after the "?" in a query URI and should be encoded. Example:
     * <pre>
     *      .../_aggregate?m=COUNT(*)&q=Foo:bar
     * </pre>
     * In this example, params should be passed as "m=COUNT(*)&q=Foo:bar".
     * 
     * @param tableDef  {@link TableDefinition} of table to query.
     * @param params    Query parameters in URI format as described above.
     * @return          Aggregate query results as an {@link AggregateResult} object. 
     */
    public abstract AggregateResult aggregateQueryURI(TableDefinition tableDef, String params);
    
    /**
     * Perform an aggregate query on the given table passing parameters as a UNode tree.
     * The parameters should be parsed from an XML or JSON entity, passing the root object
     * of the tree.  
     * 
     * @param tableDef  {@link TableDefinition} of table to query.
     * @param rootNode  Root {@link UNode} of parsed query parameters.
     * @return          Aggregate query results as an {@link AggregateResult} object. 
     */
    public abstract AggregateResult aggregateQueryDoc(TableDefinition tableDef, UNode rootNode);
    
    //----- Object update methods
    
    /**
     * Add the given batch of objects to the given store belonging to the given
     * application.
     * 
     * @param appDef    {@link ApplicationDefinition} of application to update.
     * @param storeName Name of store to which objects should be added.
     * @param batch     {@link DBObjectBatch} of objects to add.
     * @return          {@link BatchResult} representing the results of the update.
     */
    public abstract BatchResult addBatch(ApplicationDefinition appDef, String storeName, DBObjectBatch batch);
    
    /**
     * Delete the given set of objects from the given store belonging to the given
     * application.
     * 
     * @param appDef    {@link ApplicationDefinition} of application to update.
     * @param storeName Name of store in which objects should be deleted.
     * @param batch     {@link DBObjectBatch} of objects to delete.
     * @return          {@link BatchResult} representing the results of the delete.
     */
    public abstract BatchResult deleteBatch(ApplicationDefinition appDef, String storeName, DBObjectBatch batch);
    
    /**
     * Update the given store belonging to the given application with the given batch of
     * objects. The semantics of updates are defined by the storage service.
     * 
     * @param appDef    {@link ApplicationDefinition} of application to update.
     * @param storeName Name of store in which objects should be updated.
     * @param batch     {@link DBObjectBatch} of objects to update.
     * @return          {@link BatchResult} representing the results of the update.
     */
    public abstract BatchResult updateBatch(ApplicationDefinition appDef, String storeName, DBObjectBatch batch);

}   // class StorageService
