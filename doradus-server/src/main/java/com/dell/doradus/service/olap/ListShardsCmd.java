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
import com.dell.doradus.common.UNode;
import com.dell.doradus.common.Utils;
import com.dell.doradus.service.rest.UNodeOutCallback;

/**
 * Handle the REST command: GET /{application}/_shards
 */
public class ListShardsCmd extends UNodeOutCallback {

    @Override
    public UNode invokeUNodeOut() {
        ApplicationDefinition appDef = m_request.getAppDef();
        Utils.require(OLAPService.class.getSimpleName().equals(appDef.getStorageService()),
                      "Application '%s' is not an OLAP application", appDef.getAppName());
        
        // "result": {"<app name>": {"shards": [*<shard name>]}}
        UNode resultNode = UNode.createMapNode("result");
        UNode appNode = resultNode.addMapNode(appDef.getAppName(), "application");
        UNode shardsNode = appNode.addArrayNode("shards");
        for (String shard : OLAPService.instance().listShards(appDef)) {
            shardsNode.addValueNode("value", shard);
        }
        return resultNode;
    }   // invokeUNodeOut

}   // class ListShardsCmd
