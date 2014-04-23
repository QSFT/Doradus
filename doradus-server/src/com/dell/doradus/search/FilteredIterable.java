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

package com.dell.doradus.search;

import java.util.Iterator;
import java.util.List;

import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.core.ObjectID;
import com.dell.doradus.search.aggregate.EntitySequence;
import com.dell.doradus.search.filter.Filter;

public class FilteredIterable implements Iterable<ObjectID> {
    private Filter m_filter;
    private Iterable<ObjectID> m_sequence;
    private Searcher m_searcher;
    private TableDefinition m_table;
    private List<String> m_fields;
    
    public FilteredIterable(Searcher searcher, Filter filter, Iterable<ObjectID> sequence, TableDefinition table) {
        m_filter = filter;
        m_sequence = sequence;
        m_searcher = searcher;
        m_table = table;
        m_fields = Searcher.getFields(m_filter);
    }
    
    @Override
    public Iterator<ObjectID> iterator() {
    	if(m_filter == null) return m_sequence.iterator();
    	
        EntitySequence sequence = m_searcher.getSequence(m_table, m_sequence, m_fields);
        return new FilteredIterator(m_filter, sequence.iterator());
    }
    
    public Filter filter() { return m_filter; }
    public Iterable<ObjectID> sequence() { return m_sequence; }
    
    
}
