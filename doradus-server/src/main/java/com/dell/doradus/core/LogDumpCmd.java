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

package com.dell.doradus.core;

import java.util.Map;

import com.dell.doradus.common.HttpCode;
import com.dell.doradus.common.RESTResponse;
import com.dell.doradus.common.Utils;
import com.dell.doradus.service.rest.RESTCallback;
import com.dell.doradus.utilities.MemoryAppender;

/**
 * Reply to a REST command such as: GET /_dump. Return a stack trace of all current
 * as a plain text message.
 */
public class LogDumpCmd extends RESTCallback {

    @Override public RESTResponse invoke() {
        String params = m_request.getVariableDecoded("params");
        Map<String, String> paramMap = Utils.parseURIQuery(params);
        String level = paramMap.get("level");
        String logs = MemoryAppender.getLog(level);
        return new RESTResponse(HttpCode.OK, logs);
    }

}   // TheadDumpCmd
