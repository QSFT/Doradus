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

import java.util.Map;

import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.HttpMethod;
import com.dell.doradus.common.UNode;
import com.dell.doradus.common.Utils;
import com.dell.doradus.common.rest.CommandParameter;
import com.dell.doradus.service.olap.OLAPService;
import com.dell.doradus.service.rest.UNodeOutCallback;
import com.dell.doradus.service.rest.annotation.Description;
import com.dell.doradus.service.rest.annotation.ParamDescription;

/**
 * Handle the REST command: GET /{application}/_statistics?{params}
 */
@Description(
    name = "Statistics",
    summary = "Returns detailed storage statistics for the data in the 'mono' shard.",
    methods = HttpMethod.GET,
    uri = "/{application}/_statistics?{params}",
    outputEntity = "statistics"
)
public class ShardStatisticsCmd extends UNodeOutCallback {
    @ParamDescription
    public static CommandParameter describeParams() {
        return new CommandParameter("params", null, false)
                        .add("file", "text")
                        .add("sort", "text")
                        .add("mem", "boolean");
    }
    
    @Override
    public UNode invokeUNodeOut() {
        ApplicationDefinition appDef = m_request.getAppDef();
        String params = m_request.getVariableDecoded("params");
        Map<String, String> paramMap = Utils.parseURIQuery(params);
        return OLAPService.instance().getStatistics(appDef, OLAPMonoService.MONO_SHARD_NAME, paramMap);
    }

}   // class ShardStatisticsCmd
