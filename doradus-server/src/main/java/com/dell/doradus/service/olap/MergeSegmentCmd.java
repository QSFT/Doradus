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
import com.dell.doradus.common.rest.CommandParameter;
import com.dell.doradus.olap.MergeOptions;
import com.dell.doradus.service.rest.RESTCallback;
import com.dell.doradus.service.rest.annotation.Description;
import com.dell.doradus.service.rest.annotation.ParamDescription;

/**
 * Handle the REST commands: POST /{application}/_shards/{shard} and
 * POST /{application}/_shards/{shard}?{params}
 */
@Description(
    name = "Merge",
    summary = "Merges all updates in a specific shard.",
    methods = HttpMethod.POST,
    uri = "/{application}/_shards/{shard}?{params}"
)
public class MergeSegmentCmd extends RESTCallback {
    @ParamDescription
    public static CommandParameter describeParams() {
        return new CommandParameter("params")
                        .add("expire-date", "text")
                        .add("timeout", "integer")
                        .add("force-merge", "boolean");
    }

    @Override
    public RESTResponse invoke() {
        ApplicationDefinition appDef = m_request.getAppDef();
        String shard = m_request.getVariableDecoded("shard");
        String params = m_request.getVariableDecoded("params");
        if (params == null) {
            params = "";
        }
        MergeOptions options = new MergeOptions(params);
        OLAPService.instance().mergeShard(appDef, shard, options);
        return new RESTResponse(HttpCode.OK);
    }   // invoke

}   // class MergeSegmentCmd
