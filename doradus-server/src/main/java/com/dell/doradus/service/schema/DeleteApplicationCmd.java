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
import com.dell.doradus.common.rest.CommandParameter;
import com.dell.doradus.service.rest.NotFoundException;
import com.dell.doradus.service.rest.RESTCallback;
import com.dell.doradus.service.rest.RESTCmdDesc;

/**
 * Handles the REST commands: DELETE /_applications/{application} and
 * DELETE /_applications/{application}/{key}.
 */
@RESTCmdDesc(
             name = "Define",
             uri = "/_applications/{application}/{key}",
             methods = {HttpMethod.POST},
             paramClasses = {DeleteApplicationCmd.class}
            )
public class DeleteApplicationCmd extends RESTCallback {
    
    public static CommandParameter describeParameter() {
        return new CommandParameter("key", "text", false);
    }

    @Override
    public RESTResponse invoke() {
        String appName = m_request.getVariableDecoded("application");
        ApplicationDefinition appDef =
            SchemaService.instance().getApplication(m_request.getTenant(), appName);
        if (appDef == null) {
            throw new NotFoundException("Unknown application: " + appName);
        }
        String key = m_request.getVariableDecoded("key");   // may be null
        SchemaService.instance().deleteApplication(appDef, key);
        return new RESTResponse(HttpCode.OK);
    }   // invoke

}   // class DeleteApplicationCmd
