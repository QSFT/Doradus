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

import java.io.Reader;
import java.util.Map;

import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.BatchResult;
import com.dell.doradus.common.DBObjectBatch;
import com.dell.doradus.common.HttpCode;
import com.dell.doradus.common.RESTResponse;
import com.dell.doradus.common.UNode;
import com.dell.doradus.common.Utils;
import com.dell.doradus.service.StorageService;
import com.dell.doradus.service.rest.NotFoundException;
import com.dell.doradus.service.rest.ReaderCallback;
import com.dell.doradus.service.schema.SchemaService;

/**
 * Implements the REST command: POST /{application}/{store}[?{params}. Verifies the given
 * application and passes the command to its registered storage service.
 */
public class AddObjectsCmd extends ReaderCallback {

    @Override
    public RESTResponse invokeStreamIn(Reader reader) {
        String application = m_request.getVariableDecoded("application");
        String store = m_request.getVariableDecoded("store");
        ApplicationDefinition appDef = SchemaService.instance().getApplication(application);
        if (appDef == null) {
            throw new NotFoundException("Unknown application: " + application);
        }
        Utils.require(reader != null, "This command requires an input entity");
        
        DBObjectBatch dbObjBatch = new DBObjectBatch();
        if (m_request.getInputContentType().isJSON()) {
            dbObjBatch.parseJSON(reader);
        } else {
            UNode rootNode = UNode.parse(reader, m_request.getInputContentType());
            dbObjBatch.parse(rootNode);
        }

        Map<String, String> paramMap = Utils.parseURIQuery(m_request.getVariable("params"));
        StorageService storageService = SchemaService.instance().getStorageService(appDef);
        BatchResult batchResult = storageService.addBatch(appDef, store, dbObjBatch, paramMap);
        String body = batchResult.toDoc().toString(m_request.getOutputContentType());
        return new RESTResponse(HttpCode.CREATED, body, m_request.getOutputContentType());
    }   // invokeStreamIn

}   // class AddObjectsCmd
