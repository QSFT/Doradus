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

import java.util.HashMap;
import java.util.Map;

import com.dell.doradus.common.HttpCode;
import com.dell.doradus.common.HttpDefs;
import com.dell.doradus.common.HttpMethod;
import com.dell.doradus.common.RESTResponse;
import com.dell.doradus.common.Utils;
import com.dell.doradus.common.rest.CommandParameter;
import com.dell.doradus.service.rest.RESTCallback;
import com.dell.doradus.service.rest.annotation.Description;
import com.dell.doradus.service.rest.annotation.ParamDescription;

/**
 * Handles the REST command: GET /_olapp?{params}
 * 
 * simple application that allows browse OLAP data and build simple aggregation queries
 * using simple HTML interface 
 */
@Description(
    name = "Browse",
    summary = "Provides a simple web browser interface for OLAP applications.",
    methods = HttpMethod.GET,
    uri = "GET /_olapp?{params}",
    visible = false
)
public class OlappCmd extends RESTCallback {
    @ParamDescription
    public static CommandParameter describeParams() {
        // {params} are optional but details don't need to be public.
        return new CommandParameter("params", null, false);
    }
    
    @Override
    public RESTResponse invoke() {
        String params = m_request.getVariable("params");
        Map<String, String> parameters = null;
        if (Utils.isEmpty(params)) {
            parameters = new HashMap<String, String>(0);
        } else {
            parameters = Utils.parseURIQuery(params);
        }
        
        String html = OLAPService.instance().browseOlapp(m_request.getTenant(), parameters);
        Map<String, String> headers = new HashMap<String, String>();
        headers.put(HttpDefs.CONTENT_TYPE, "text/html");
        return new RESTResponse(HttpCode.OK, Utils.toBytes(html), headers);
    }   // invoke

}   // class OlappCmd
