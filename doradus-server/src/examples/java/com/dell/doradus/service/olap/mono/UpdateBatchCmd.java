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

package com.dell.doradus.service.olap.mono;

import java.io.Reader;
import java.util.Map;

import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.BatchResult;
import com.dell.doradus.common.HttpCode;
import com.dell.doradus.common.HttpMethod;
import com.dell.doradus.common.RESTResponse;
import com.dell.doradus.common.UNode;
import com.dell.doradus.common.Utils;
import com.dell.doradus.common.rest.RESTParameter;
import com.dell.doradus.olap.OlapBatch;
import com.dell.doradus.service.rest.ReaderCallback;
import com.dell.doradus.service.rest.annotation.Description;
import com.dell.doradus.service.rest.annotation.ParamDescription;

@Description(
    name = "Update",
    summary = "Adds a batch of data to the 'mono' shard. The batch can contain new, " +
              "modified, and deleted objects.",
    methods = {HttpMethod.POST, HttpMethod.PUT},
    uri = "/{application}/_data?{params}",
    inputEntity = "batch"
)
public class UpdateBatchCmd extends ReaderCallback {

    @ParamDescription
    public static RESTParameter describeParams() {
        return new RESTParameter("params", null, false).add("overwrite", "boolean");
    }
    
    @Override
    public RESTResponse invokeStreamIn(Reader reader) {
        Utils.require(reader != null, "This command requires an input entity");
        ApplicationDefinition appDef = m_request.getAppDef();
        
        OlapBatch batch = null;
        if (m_request.getInputContentType().isJSON()) {
            batch = OlapBatch.parseJSON(reader);
        } else {
            UNode rootNode = UNode.parse(reader, m_request.getInputContentType());
            batch = OlapBatch.fromUNode(rootNode);
        }

        Map<String, String> paramMap = Utils.parseURIQuery(m_request.getVariable("params"));
        BatchResult batchResult = OLAPMonoService.instance().addBatch(appDef, batch, paramMap);
        String body = batchResult.toDoc().toString(m_request.getOutputContentType());
        if (m_request.getMethod() == HttpMethod.PUT) {
            return new RESTResponse(HttpCode.CREATED, body, m_request.getOutputContentType());
        } else {
            return new RESTResponse(HttpCode.OK, body, m_request.getOutputContentType());
        }
    }

}   // class UpdateBatchCmd
