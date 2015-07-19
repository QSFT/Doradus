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
import com.dell.doradus.common.HttpMethod;
import com.dell.doradus.common.UNode;
import com.dell.doradus.service.rest.NotFoundException;
import com.dell.doradus.service.rest.UNodeOutCallback;
import com.dell.doradus.service.rest.annotation.Description;

/**
 * Handle the REST command: GET /_applications/{application}
 */
@Description(
    name = "ListApp",
    summary = "Returns the schema of an existing application.",
    methods = {HttpMethod.GET},
    uri = "/_applications/{application}",
    outputEntity = "{application}"
)
public class ListApplicationCmd extends UNodeOutCallback {

    @Override
    public UNode invokeUNodeOut() {
        String appName = m_request.getVariableDecoded("application");
        ApplicationDefinition appDef =
            SchemaService.instance().getApplication(m_request.getTenant(), appName);
        if (appDef == null) {
            throw new NotFoundException("Unknown application: " + appName);
        }
        return appDef.toDoc();
    }   // invokeUNodeOut

}   // class ListApplicationCmd
