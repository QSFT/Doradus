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
import com.dell.doradus.common.RESTResponse;
import com.dell.doradus.common.Utils;
import com.dell.doradus.olap.CheckDatabase;
import com.dell.doradus.service.rest.RESTCallback;

/**
 * Handle the REST command: DELETE /{application}/_shards/{shard}/{segment}.
 */
public class DeleteSegmentCmd2 extends RESTCallback {

    @Override
    public RESTResponse invoke() {
        ApplicationDefinition appDef = m_request.getAppDef();
        Utils.require(OLAPService.class.getSimpleName().equals(appDef.getStorageService()),
                      "Application '%s' is not an OLAP application", appDef.getAppName());
        String shard = m_request.getVariableDecoded("shard");
        String segment = m_request.getVariableDecoded("segment");
        CheckDatabase.deleteSegment(OLAPService.instance().getOlap(), appDef, shard, segment);
        return new RESTResponse(HttpCode.OK);
    }   // invoke

}   // class DeleteSegmentCmd
