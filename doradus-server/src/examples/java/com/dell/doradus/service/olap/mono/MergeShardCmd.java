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

import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.HttpCode;
import com.dell.doradus.common.HttpMethod;
import com.dell.doradus.common.RESTResponse;
import com.dell.doradus.service.olap.OLAPService;
import com.dell.doradus.service.rest.RESTCallback;
import com.dell.doradus.service.rest.annotation.Description;

@Description(
    name = "Merge",
    summary = "Merges new batch data, if any, for a specific application to the mono shard.",
    methods = HttpMethod.POST,
    uri = "/{application}/_merge"
)
public class MergeShardCmd extends RESTCallback {

    @Override
    public RESTResponse invoke() {
        ApplicationDefinition appDef = m_request.getAppDef();
        OLAPService.instance().mergeShard(appDef, OLAPMonoService.MONO_SHARD_NAME, null);
        return new RESTResponse(HttpCode.OK);
    }

}   // class MergeShardCmd
