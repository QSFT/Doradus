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
import com.dell.doradus.service.rest.NotFoundException;
import com.dell.doradus.service.rest.RESTCmdDesc;
import com.dell.doradus.service.rest.UNodeInCallback;

/**
 * Handle the REST command: PUT /_applications/{application}
 */
@RESTCmdDesc(
             name = "ModifyApp",
             uri = "/_applications",
             methods = {HttpMethod.PUT},
             inputEntity = "{application}"
            )
public class ModifyApplicationCmd extends UNodeInCallback {

    @Override
    public RESTResponse invokeUNodeIn(UNode inNode) {
        Utils.require(inNode != null, "This command requires an input entity");
        String appName = m_request.getVariableDecoded("application");
        ApplicationDefinition currAppDef =
            SchemaService.instance().getApplication(m_request.getTenant(), appName);
        if (currAppDef == null) {
            throw new NotFoundException("Unknown application: " + appName);
        }
        
        ApplicationDefinition newAppDef = new ApplicationDefinition();
        newAppDef.parse(inNode);
        Utils.require(newAppDef.getAppName().equals(currAppDef.getAppName()),
                      "Application name cannot be changed: %s", newAppDef.getAppName());
        
        SchemaService.instance().defineApplication(m_request.getTenant(), newAppDef);
        return new RESTResponse(HttpCode.OK);
    }   // invokeUNodeIn

}   // class ModifyApplicationCmd
