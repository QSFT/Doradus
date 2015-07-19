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

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;

import com.dell.doradus.common.HttpCode;
import com.dell.doradus.common.HttpMethod;
import com.dell.doradus.common.RESTResponse;
import com.dell.doradus.common.Utils;
import com.dell.doradus.service.rest.RESTCallback;
import com.dell.doradus.service.rest.annotation.Description;

/**
 * Reply to a REST command such as: GET /_dump. Return a stack trace of all current
 * as a plain text message.
 */
@Description(
    name = "Dump",
    summary = "Performs a stack dump of all threads. The results are returned as plain text.",
    methods = {HttpMethod.GET},
    uri = "/_dump",
    privileged = true,
    outputEntity = "{text}"
)
public class ThreadDumpCmd extends RESTCallback {

    @Override
    public RESTResponse invoke() {
        StringBuilder dump = new StringBuilder();
        dump.append("Doradus Thread Dump @ ");
        dump.append(Utils.formatDate(System.currentTimeMillis()));
        dump.append("\n\n");
        
        ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
        ThreadInfo[] threadInfos = threadBean.dumpAllThreads(true, true);
        for (ThreadInfo thread : threadInfos) {
            dump.append(thread.toString());
        }
        return new RESTResponse(HttpCode.OK, dump.toString());
    }

}   // ThreadDumpCmd
