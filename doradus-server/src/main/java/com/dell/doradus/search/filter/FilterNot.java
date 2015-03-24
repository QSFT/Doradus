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

package com.dell.doradus.search.filter;

import java.util.Set;

import com.dell.doradus.search.aggregate.Entity;

public class FilterNot implements Filter {
    private Filter m_inner;
    
    public FilterNot(Filter inner) {
        m_inner = inner;
    }
    
    @Override public boolean check(Entity entity) {
        return !m_inner.check(entity);
    }

    @Override public void addFields(Set<String> fields) {
        m_inner.addFields(fields);
    }
    
}
