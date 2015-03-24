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

package com.dell.doradus.search.iterator;

import java.util.Iterator;
import java.util.List;

import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.core.ObjectID;
import com.dell.doradus.fieldanalyzer.FieldAnalyzer;

public class AllIterable implements Iterable<ObjectID> {
	private TableDefinition m_table;
	private List<Integer> m_shards;
	private ObjectID m_continuation;
	private boolean m_inclusive;

    public AllIterable(TableDefinition table, List<Integer> shards, ObjectID continuation, boolean inclusive) {
    	m_table = table;
    	m_shards = shards;
    	m_continuation = continuation;
    	m_inclusive = inclusive;
    }
    
	@Override public Iterator<ObjectID> iterator() {
		TermsIterable te = new TermsIterable(m_table, m_shards, m_continuation, m_inclusive);
		te.add(FieldAnalyzer.makeAllKey());
		return te.iterator();
	}
}
