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
import com.dell.doradus.common.rest.RESTParameter;
import com.dell.doradus.service.rest.NotFoundException;
import com.dell.doradus.service.rest.RESTCallback;
import com.dell.doradus.service.rest.annotation.Description;
import com.dell.doradus.service.rest.annotation.ParamDescription;

@Description(
    name = "DeleteApp",
    summary = "Deletes an existing application including all of its data.",
    methods = {HttpMethod.DELETE},
    uri = "/_applications/{application}"
)
public class DeleteApplicationCmd extends RESTCallback {
    
    @ParamDescription
    public static RESTParameter describeParameter() {
        return new RESTParameter("key", "text", false);
    }

    @Override
    public RESTResponse invoke() {
        String appName = m_request.getVariableDecoded("application");
        ApplicationDefinition appDef =
            SchemaService.instance().getApplication(m_request.getTenant(), appName);
        if (appDef == null) {
            throw new NotFoundException("Unknown application: " + appName);
        }
        SchemaService.instance().deleteApplication(appDef, null);
        return new RESTResponse(HttpCode.OK);
    }   // invoke

}   // class DeleteApplicationCmd
