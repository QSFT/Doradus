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
import com.dell.doradus.common.HttpMethod;
import com.dell.doradus.common.RESTResponse;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.common.UNode;
import com.dell.doradus.common.Utils;
import com.dell.doradus.service.rest.RESTCmdDesc;
import com.dell.doradus.service.rest.ReaderCallback;

/**
 * Implements the REST command: DELETE /{application}/{table}.
 */
@RESTCmdDesc(
             name = "Delete",
             uri = "/{application}/{table}",
             methods = HttpMethod.DELETE,
             inputEntity = "batch"
            )
public class DeleteObjectsCmd extends ReaderCallback {

    @Override
    public RESTResponse invokeStreamIn(Reader reader) {
        ApplicationDefinition appDef = m_request.getAppDef();
        TableDefinition tableDef = m_request.getTableDef(appDef);
        Utils.require(reader != null, "This command requires an input entity");
        
        DBObjectBatch dbObjBatch = new DBObjectBatch();
        UNode rootNode = UNode.parse(reader, m_request.getInputContentType());
        dbObjBatch.parse(rootNode);

        BatchResult batchResult = SpiderService.instance().deleteBatch(tableDef, dbObjBatch);
        String body = batchResult.toDoc().toString(m_request.getOutputContentType());
        return new RESTResponse(HttpCode.OK, body, m_request.getOutputContentType());
    }   // invokeStreamIn

}   // class DeleteObjectsCmd
