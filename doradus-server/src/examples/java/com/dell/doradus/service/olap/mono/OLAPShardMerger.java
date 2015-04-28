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

import com.dell.doradus.service.olap.OLAPService;
import com.dell.doradus.service.taskmanager.TaskExecutor;

/**
 * Perform shard-merging task execution for an OLAPMono application. 
 */
public class OLAPShardMerger extends TaskExecutor {

    @Override
    public void execute() {
        m_logger.debug("Checking shard {} for data to merge", OLAPMonoService.MONO_SHARD_NAME);
        try {
            OLAPService.instance().mergeShard(m_appDef, OLAPMonoService.MONO_SHARD_NAME, null);
        } catch (RuntimeException e) {
            m_logger.warn("Auto-merge task failed", e);
        }
    }

}   // OLAPShardMerger
