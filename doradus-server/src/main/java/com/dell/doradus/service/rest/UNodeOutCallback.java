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

import com.dell.doradus.common.HttpCode;
import com.dell.doradus.common.RESTResponse;
import com.dell.doradus.common.UNode;

/**
 * Specializes the {@link RESTCallback} class for REST commands that expect no input
 * entity but return output as a {@link UNode} tree. Since no input entity is expected,
 * an IllegalArgumentException is thrown if one is found. The caller must implement
 * {@link #invokeUNodeOut(UNode)}, which is called to process the command. It must either
 * thrown an exception or return a UNode tree, which is serialized into JSON or XML as
 * requested by the user.
 * 
 * @see UNodeInCallback
 * @see UNodeInOutCallback
 */
public abstract class UNodeOutCallback extends RESTCallback {

    // Declared "final" so subclass does not attempt to override.
    @Override
    public final RESTResponse invoke() {
        UNode outNode = invokeUNodeOut();
        assert outNode != null;
        return new RESTResponse(HttpCode.OK,
                                outNode.toString(m_request.getOutputContentType()),
                                m_request.getOutputContentType());
    }   // invokeUNodeIn

    /**
     * The subclass must implement this method and throw an exception or return a UNode
     * object.
     * 
     * @return Root of a UNode tree representing the output response.
     */
    public abstract UNode invokeUNodeOut();
    
}   // abstract class UNodeOutCallback
