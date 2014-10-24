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

package com.dell.doradus.service.rest;

import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.RESTResponse;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.service.schema.SchemaService;

/**
 * Defines a callback object that is invoked for a specific REST request. A callback
 * object is used by following these steps:
 * <ol>
 * <li>Create an object of the appropriate subclass using the nullary constructor.</li>
 * <li>Call {@link #setRequest(RESTRequest)} passing the {@link RESTRequest} that defines
 * the callback's context.</li>
 * <li>Call {@link #invoke()} to invoke the appropriate callback method.
 * </ol>
 */
public abstract class RESTCallback {
    protected RESTRequest m_request;

    /**
     * Store the given {@link RESTRequest} as the context for this callback execution.
     * This method must be called once before {@link #invoke()} is called. This approach
     * is used instead of passing the RESTRequest to the constructor to prevent all
     * subclasses from having to declare the same constructor.
     * 
     * @param request   {@link RESTRequest} that encapsulates the parameters of a REST
     *                  request and provides convenience methods. Provides context to
     *                  subclasses when {@link #invoke()} is called.
     */
    final public void setRequest(RESTRequest request) {
        m_request = request;
    }   // setRequest
    
    /**
     * Return the {@link RESTRequest} that defines the context of this REST command
     * execution.
     * 
     * @return  The {@link RESTRequest} that was passed to {@link #setRequest(RESTRequest)}. 
     */
    final public RESTRequest getRequest() {
        return m_request;
    }   // getRequest
    
    /**
     * Process the REST request represented by this callback instance by invoking the
     * overridden callback method.
     * 
     * @return The REST command response to be returned to the client, encapsulated in a
     *         {@link RESTResponse}.
     */
    protected abstract RESTResponse invoke();

    /**
     * Validate the given parameter as a valid application name. The given parameter is
     * extracted and decoded, and the corresponding {@link ApplicationDefinition} is
     * returned. A RuntimeException is thrown if the parameter name does not exist within
     * this REST request. A {@link NotFoundException} is thrown if the given application
     * does not exist.
     * 
     * @param appParamName          URI parameter name that contains an application name.
     * @return                      {@link ApplicationDefinition} of application if found.
     * @throws NotFoundException    If the given application is not defined.
     */
    protected ApplicationDefinition validateApplication(String appParamName) throws NotFoundException {
        String appName = m_request.getVariableDecoded(appParamName);
        if (appName == null) {
            throw new RuntimeException("No such parameter name: " + appParamName);
        }
        ApplicationDefinition appDef = SchemaService.instance().getApplication(appName);
        if (appDef == null) {
            throw new NotFoundException("Unknown application: " + appName);
        }
        return appDef;
    }   // validateApplication

    /**
     * Validate the given parameter as a valid table belong to the given application. A
     * RuntimeException is thrown if the parameter name does not exist within this REST
     * request. A {@link NotFoundException} is thrown if the given table name does not
     * exist for the given application.
     * 
     * @param appDef            {@link ApplicationDefinition} that owns the candidate
     *                          table name.
     * @param tableParamName    URI parameter name that holds the table name.
     * @return
     */
    protected TableDefinition validateTable(ApplicationDefinition appDef, String tableParamName) {
        String tableName = m_request.getVariableDecoded(tableParamName);
        if (tableName == null) {
            throw new RuntimeException("No such parameter name: " + tableParamName);
        }
        TableDefinition tableDef = appDef.getTableDef(tableName);
        if (tableDef == null) {
            throw new NotFoundException("Unknown table '" + tableName + "' for application '" + appDef.getAppName() + "'");
        }
        return tableDef;
    }   // validateTable
    
}   // abstract class RESTCallback
