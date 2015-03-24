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
import com.dell.doradus.common.Utils;

/**
 * Specializes the {@link UNodeInCallback} class for REST commands that both require a
 * a {@link UNode} tree as input and produce one as output. Subclasses of this class
 * must implement {@link #invokeUNodeInOut(UNode)}. Since an input entity is expected,
 * this class throws an IllegalArgumentException if the command is missing an input
 * entity. If an exception is not thrown, this class formats the output UNode into a
 * {@link RESTResponse} using the requested output format.
 * 
 * @see UNodeOutCallback
 * @see UNodeInCallback
 */
public abstract class UNodeInOutCallback extends UNodeInCallback {

    @Override
    public final RESTResponse invokeUNodeIn(UNode inNode) {
        Utils.require(inNode != null, "An input entity is required for this command");
        UNode outNode = invokeUNodeInOut(inNode);
        return new RESTResponse(HttpCode.OK,
                                outNode.toString(m_request.getOutputContentType()),
                                m_request.getOutputContentType());
    }

    /**
     * Callback that must be implemented by subclasses. The input UNode will not be null
     * since an exception is thrown if no input entity is received. The method must either
     * thrown an exception to generated an error response, or it must return a UNode
     * object, which is serialized into the output response along with a 200 OK code.
     * 
     * @param inNode    Input entity parsed into a {@link UNode}. Won't be null.
     * @return          Output UNode tree, which is serialized into a JSON or XML message
     *                  as requested by the user.
     */
    public abstract UNode invokeUNodeInOut(UNode inNode);
    
}   // abstract class UNodeInOutCallback
