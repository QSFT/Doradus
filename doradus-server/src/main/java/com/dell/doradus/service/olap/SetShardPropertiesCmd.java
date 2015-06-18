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

import java.util.Map;

import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.HttpCode;
import com.dell.doradus.common.RESTResponse;
import com.dell.doradus.common.Utils;
import com.dell.doradus.service.rest.RESTCallback;

/**
 * Handle the REST commands: PUT /{application}/_properties/{shard}?{params}
 */
public class SetShardPropertiesCmd extends RESTCallback {

    @Override
    public RESTResponse invoke() {
        ApplicationDefinition appDef = m_request.getAppDef();
        Utils.require(OLAPService.class.getSimpleName().equals(appDef.getStorageService()),
                      "Application '%s' is not an OLAP application", appDef.getAppName());
        String shard = m_request.getVariableDecoded("shard");
        String params = m_request.getVariableDecoded("params");
        Map<String, String> paramsMap = Utils.parseURIQuery(params);
        String expireDate = paramsMap.get("expire-date");
        Utils.require(expireDate != null, "expire-date parameter expected");
        Utils.require(paramsMap.size() == 1, "Only expire-date parameter expected");
        OLAPService.instance().getOlap().setExpirationDate(appDef, shard, expireDate);
        return new RESTResponse(HttpCode.OK);
    }   // invoke

}   // class MergeSegmentCmd
