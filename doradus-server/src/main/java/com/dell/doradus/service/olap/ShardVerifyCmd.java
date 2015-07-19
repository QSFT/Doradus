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

package com.dell.doradus.service.olap;

import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.HttpCode;
import com.dell.doradus.common.HttpMethod;
import com.dell.doradus.common.RESTResponse;
import com.dell.doradus.olap.CheckDatabase;
import com.dell.doradus.service.rest.RESTCallback;
import com.dell.doradus.service.rest.annotation.Description;

/**
 * Handle the REST command: GET /{application}/_verify/{shard}
 * Verify that all segments in the shard are correct
 */
@Description(
    name = "Verify",
    summary = "Performs an integrity check on a given shard. " +
              "Results are written to the Doradus log file.",
    methods = HttpMethod.GET,
    uri = "/{application}/_verify/{shard}"
)
public class ShardVerifyCmd extends RESTCallback {

    @Override
    public RESTResponse invoke() {
        ApplicationDefinition appDef = m_request.getAppDef();
        String shard = m_request.getVariableDecoded("shard");
        CheckDatabase.checkShard(OLAPService.instance().getOlap(), appDef, shard);
        return new RESTResponse(HttpCode.OK);
    }

} 
