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

import com.dell.doradus.common.RESTResponse;

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

}   // abstract class RESTCallback
