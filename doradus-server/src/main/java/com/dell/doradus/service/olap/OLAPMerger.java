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

import java.util.Arrays;
import java.util.List;

import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.olap.MergeOptions;
import com.dell.doradus.olap.Olap;
import com.dell.doradus.service.taskmanager.Task;

/**
 * Perform automatic merge task execution for an OLAP application. 
 */
public class OLAPMerger extends Task {
    private final List<String> shards;
    private final MergeOptions options;

    // Scheduled merge for all shards
    public OLAPMerger(ApplicationDefinition appDef, String autoMergeFreq) {
        super(appDef, null, "auto-merge", autoMergeFreq);
        shards = OLAPService.instance().listShards(appDef);
        options = new MergeOptions();
    }

    // Immediate merge of a single shard
    public OLAPMerger(ApplicationDefinition appDef, String shard, MergeOptions opts) {
        super(appDef, null, "auto-merge", null);
        shards = Arrays.asList(shard);
        options = opts;
    }
    
    @Override
    public void execute() {
        m_logger.info("Merging shards {}", m_appDef.getAppName());
        Olap olap = OLAPService.instance().getOlap();
        for (String shard : shards) {
            olap.merge(m_appDef, shard, options);
            m_logger.info("Merged shard '{}/{}'", shard, m_appDef.getAppName());
        }
    }

}
