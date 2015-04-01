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
import java.util.Collection;

import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.service.taskmanager.Task;

/**
 * Abstract root class for Doradus storage services. Extends the {@link Service} class
 * with methods that subclasses must have to operate as a storage service.
 */
public abstract class StorageService extends Service {

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

    /**
     * Return the {@link Task}s that are required for the given application, which is
     * managed by this storage manager. Subclasses do not need to override this method if
     * they do not require tasks: by default, the implementation returns null.
     * 
     * @param appDef    {@link ApplicationDefinition} managed by this storage service.
     * @return          Collection of {@link Task}s required by the application or null
     *                  if there are none.
     */
    public Collection<Task> getAppTasks(ApplicationDefinition appDef) {
        return null;
    }   // getAppTasks
    
}   // class StorageService
