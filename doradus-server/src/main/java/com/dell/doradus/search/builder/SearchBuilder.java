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

package com.dell.doradus.search.builder;

import java.util.List;

import com.dell.doradus.common.FieldDefinition;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.core.ObjectID;
import com.dell.doradus.fieldanalyzer.FieldAnalyzer;
import com.dell.doradus.fieldanalyzer.TextAnalyzer;
import com.dell.doradus.search.FilteredIterable;
import com.dell.doradus.search.SearchParameters;
import com.dell.doradus.search.Searcher;
import com.dell.doradus.search.filter.Filter;
import com.dell.doradus.search.iterator.AllIterable;
import com.dell.doradus.search.query.Query;

public abstract class SearchBuilder {
	protected Searcher m_searcher;
	protected SearchParameters m_params;
	protected TableDefinition m_table;
	protected List<Integer> m_shards;
	
	public void set(Searcher searcher, SearchParameters params, TableDefinition tableDef) {
		m_searcher = searcher;
		m_params = params;
		m_table = tableDef;
	}
	
	public void set(List<Integer> shards) {
		m_shards = shards;
	}
	
	public FilteredIterable create(Iterable<ObjectID> sequence, Filter filter) {
		return new FilteredIterable(m_searcher, filter, sequence, m_table);
	}
	
	public AllIterable all() {
		return new AllIterable(m_table, m_shards, m_params.continuation, m_params.inclusive);
	}
	
	public FieldAnalyzer analyzer(String field) {
		FieldDefinition f = m_table.getFieldDef(field);
		if(f == null || ! f.isScalarField()) return TextAnalyzer.instance();
		else return FieldAnalyzer.findAnalyzer(f);
	}
	
	public abstract FilteredIterable search(Query query);
	public abstract Filter filter(Query query);
}
