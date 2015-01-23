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

package com.dell.doradus.service.taskmanager;

import java.util.Collection;

import com.dell.doradus.common.UNode;
import com.dell.doradus.service.db.Tenant;
import com.dell.doradus.service.rest.UNodeOutCallback;

/**
 * Handle the REST command: GET /_tasks. Lists recorded tasks for all applications in the
 * perspective tenant.
 */
public class ListTasksCmd extends UNodeOutCallback {

    @Override
    public UNode invokeUNodeOut() {
        Tenant tenant = m_request.getTenant();
        Collection<TaskRecord> taskRecords = TaskManagerService.instance().getTaskRecords(tenant);
        UNode result = UNode.createMapNode("tasks");
        for (TaskRecord taskRecord : taskRecords) {
            result.addChildNode(taskRecord.toDoc());
        }
        return result;
    }

}   // class ListTasksCmd
