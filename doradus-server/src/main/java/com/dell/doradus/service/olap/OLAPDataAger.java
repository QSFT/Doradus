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

import java.util.Date;
import java.util.List;

import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.service.taskmanager.Task;

/**
 * Perform data-aging task execution for an OLAP application. 
 */
public class OLAPDataAger extends Task {

    public OLAPDataAger(ApplicationDefinition appDef, String agingFreq) {
        super(appDef, null, "data-aging", agingFreq);
    }

    @Override
    public void execute() {
        m_logger.info("Checking expired shards for {}", m_appDef.getAppName());
        Date now = new Date();
        OLAPService olap = OLAPService.instance();
        List<String> shards = olap.listShards(m_appDef);
        for (String shardName : shards) {
            Date expirationDate = olap.getExpirationDate(m_appDef, shardName);
            m_logger.debug("shard '{}' has expiration date '{}'", shardName, expirationDate);
            if (expirationDate != null && expirationDate.before(now)) {
                olap.deleteShard(m_appDef, shardName);
                m_logger.info("Deleted expired shard '{}' for {}", shardName, m_appDef.getAppName());
            }
        }
    }

}   // class OLAPDataAger
