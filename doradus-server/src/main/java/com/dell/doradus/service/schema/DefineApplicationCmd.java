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

package com.dell.doradus.service.schema;

import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.HttpCode;
import com.dell.doradus.common.HttpMethod;
import com.dell.doradus.common.RESTResponse;
import com.dell.doradus.common.UNode;
import com.dell.doradus.common.Utils;
import com.dell.doradus.service.rest.UNodeInCallback;
import com.dell.doradus.service.rest.annotation.Description;

@Description(
    name = "DefineApp",
    summary = "Defines a new application or modifies an existing one. The input entity " +
              "must be a serialized ApplicationDefinition object.",
    methods = {HttpMethod.POST},
    uri = "/_applications",
    inputEntity = "ApplicationDefinition"
)
public class DefineApplicationCmd extends UNodeInCallback {

    @Override
    public RESTResponse invokeUNodeIn(UNode inNode) {
        Utils.require(inNode != null, "This command requires an input entity");
        ApplicationDefinition appDef = new ApplicationDefinition();
        appDef.parse(inNode);
        SchemaService.instance().defineApplication(m_request.getTenant(), appDef);
        return new RESTResponse(HttpCode.OK);
    }   // invokeUNodeIn

}   // class DefineApplicationCmd
