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

import java.util.HashMap;
import java.util.Map;

import com.dell.doradus.common.HttpCode;
import com.dell.doradus.common.HttpDefs;
import com.dell.doradus.common.RESTResponse;
import com.dell.doradus.common.UNode;
import com.dell.doradus.common.Utils;

/**
 * Specializes the {@link UNodeInCallback} class to allow the REST response to be returned
 * as a {@link UNode} tree. Therefore, subclasses of this class process both the input and
 * output entities, if any, as UNode trees. This class formats the output UNode into a
 * {@link RESTResponse} using the requested output format. Subclasses must implement the
 * method {@link #invokeUNodeOut(UNode)}, which is called when the callback is
 * invoked. 
 */
public abstract class UNodeOutCallback extends UNodeInCallback {

    // Declared "final" so subclass does not attempt to override.
    @Override
    public final RESTResponse invokeUNodeIn(UNode inNode) {
        UNode outNode = invokeUNodeOut(inNode);
        Map<String, String> headers = new HashMap<>();
        headers.put(HttpDefs.CONTENT_TYPE, m_request.getOutputContentType().toString());
        return new RESTResponse(HttpCode.OK,
                                Utils.toBytes(outNode.toString(m_request.getOutputContentType())),
                                headers);
    }   // invokeUNodeIn

    /**
     * The subclass must implement this method, processing the REST request with the given
     * parameters.
     * 
     * @param inNode        Root node of a UNode tree created by parsing the input entity
     *                      with the specified content-type. If there is no input entity,
     *                      this parameter will be null.
     * @return              Root node of a UNode tree representing the output response.
     */
    public abstract UNode invokeUNodeOut(UNode inNode);
    
}   // abstract class UNodeOutCallback
