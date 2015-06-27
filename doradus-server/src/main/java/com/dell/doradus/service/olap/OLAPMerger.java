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

import java.util.List;

import org.slf4j.Logger;

import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.olap.MergeOptions;
import com.dell.doradus.olap.Olap;
import com.dell.doradus.service.taskmanager.Task;

/**
 * Perform automatic merge task execution for an OLAP application. 
 */
public class OLAPMerger extends Task {

    public OLAPMerger(ApplicationDefinition appDef, String autoMergeFreq) {
        super(appDef, null, "auto-merge", autoMergeFreq);
    }

    @Override
    public void execute() {
        doTask(m_appDef, m_logger);
    }
    
    public static void doTask(ApplicationDefinition appDef, Logger logger) {
        logger.info("Merging shards {}", appDef.getPath());
        MergeOptions options = new MergeOptions();
        Olap olap = OLAPService.instance().getOlap();
        List<String> shards = olap.listShards(appDef);
        for (String shard : shards) {
            olap.merge(appDef, shard, options);
            logger.info("Merged shard '{}/{}'", shard, appDef.getAppName());
        }
    }

}
