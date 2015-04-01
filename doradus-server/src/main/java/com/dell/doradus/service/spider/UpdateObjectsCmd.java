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

package com.dell.doradus.service.spider;

import java.io.Reader;

import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.BatchResult;
import com.dell.doradus.common.DBObjectBatch;
import com.dell.doradus.common.HttpCode;
import com.dell.doradus.common.RESTResponse;
import com.dell.doradus.common.UNode;
import com.dell.doradus.common.Utils;
import com.dell.doradus.service.rest.ReaderCallback;

/**
 * Implements the REST commands: PUT /{application}/{table}.
 */
public class UpdateObjectsCmd extends ReaderCallback {

    @Override
    public RESTResponse invokeStreamIn(Reader reader) {
        Utils.require(reader != null, "This command requires an input entity");
        ApplicationDefinition appDef = m_request.getAppDef();
        String table = m_request.getVariableDecoded("table");
        
        DBObjectBatch dbObjBatch = new DBObjectBatch();
        if (m_request.getInputContentType().isJSON()) {
            dbObjBatch.parseJSON(reader);
        } else {
            UNode rootNode = UNode.parse(reader, m_request.getInputContentType());
            dbObjBatch.parse(rootNode);
        }

        BatchResult batchResult = SpiderService.instance().addBatch(appDef, table, dbObjBatch);
        String body = batchResult.toDoc().toString(m_request.getOutputContentType());
        return new RESTResponse(HttpCode.OK, body, m_request.getOutputContentType());
    }   // invokeStreamIn

}   // class UpdateObjectsCmd
