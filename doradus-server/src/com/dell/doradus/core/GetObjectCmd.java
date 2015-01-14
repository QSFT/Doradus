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

package com.dell.doradus.core;

import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.DBObject;
import com.dell.doradus.common.HttpCode;
import com.dell.doradus.common.RESTResponse;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.service.StorageService;
import com.dell.doradus.service.rest.NotFoundException;
import com.dell.doradus.service.rest.RESTCallback;
import com.dell.doradus.service.schema.SchemaService;

/**
 * Implements the REST command: GET /{application}/{table}/{ID}. Verifies the application
 * and table and passes the command to the application's registered storage service.
 */
public class GetObjectCmd extends RESTCallback {

    @Override
    public RESTResponse invoke() {
        ApplicationDefinition appDef = m_request.getAppDef();
        TableDefinition tableDef = m_request.getTableDef(appDef);
        String objID = m_request.getVariableDecoded("ID");
        StorageService storageService = SchemaService.instance().getStorageService(appDef);
        DBObject dbObj = storageService.getObject(tableDef, objID);
        if (dbObj == null) {
            throw new NotFoundException("No object found with ID: " + objID);
        }
        String body = dbObj.toGroupedDoc(tableDef).toString(m_request.getOutputContentType());
        return new RESTResponse(HttpCode.OK, body, m_request.getOutputContentType());
    }   // invoke

}   // class GetObjectCmd
