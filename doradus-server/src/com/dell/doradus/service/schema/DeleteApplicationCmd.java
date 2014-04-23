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

/**
 * Handles the REST command: DELETE /_applications/{application}/{key}
 */
package com.dell.doradus.service.schema;

import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.HttpCode;
import com.dell.doradus.common.RESTResponse;
import com.dell.doradus.common.Utils;
import com.dell.doradus.service.rest.NotFoundException;
import com.dell.doradus.service.rest.RESTCallback;

public class DeleteApplicationCmd extends RESTCallback {

    @Override
    public RESTResponse invoke() {
        String application = m_request.getVariableDecoded("application");
        String key = m_request.getVariableDecoded("key");
        ApplicationDefinition appDef = SchemaService.instance().getApplication(application);
        if (appDef == null) {
            throw new NotFoundException("Application not found: " + application);
        }
        Utils.require(appDef.getKey().equals(key), "Application key does not match: " + key);
        SchemaService.instance().deleteApplication(application);
        return new RESTResponse(HttpCode.OK);
    }   // invoke

}   // class DeleteApplicationCmd
