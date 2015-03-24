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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.core.ObjectID;
import com.dell.doradus.search.aggregate.DBEntitySequenceFactory;
import com.dell.doradus.search.aggregate.EntitySequence;
import com.dell.doradus.search.builder.BuilderAll;
import com.dell.doradus.search.builder.BuilderAnd;
import com.dell.doradus.search.builder.BuilderBinary;
import com.dell.doradus.search.builder.BuilderDatePartBinary;
import com.dell.doradus.search.builder.BuilderFieldCount;
import com.dell.doradus.search.builder.BuilderFieldCountRange;
import com.dell.doradus.search.builder.BuilderId;
import com.dell.doradus.search.builder.BuilderLink;
import com.dell.doradus.search.builder.BuilderLinkCount;
import com.dell.doradus.search.builder.BuilderLinkCountRange;
import com.dell.doradus.search.builder.BuilderLinkId;
import com.dell.doradus.search.builder.BuilderLinkTransitive;
import com.dell.doradus.search.builder.BuilderMVSBinary;
import com.dell.doradus.search.builder.BuilderNot;
import com.dell.doradus.search.builder.BuilderOr;
import com.dell.doradus.search.builder.BuilderRange;
import com.dell.doradus.search.builder.SearchBuilder;
import com.dell.doradus.search.filter.Filter;
import com.dell.doradus.search.iterator.AllIterable;
import com.dell.doradus.search.query.AllQuery;
import com.dell.doradus.search.query.AndQuery;
import com.dell.doradus.search.query.BinaryQuery;
import com.dell.doradus.search.query.DatePartBinaryQuery;
import com.dell.doradus.search.query.FieldCountQuery;
import com.dell.doradus.search.query.FieldCountRangeQuery;
import com.dell.doradus.search.query.IdQuery;
import com.dell.doradus.search.query.LinkCountQuery;
import com.dell.doradus.search.query.LinkCountRangeQuery;
import com.dell.doradus.search.query.LinkIdQuery;
import com.dell.doradus.search.query.LinkQuery;
import com.dell.doradus.search.query.MVSBinaryQuery;
import com.dell.doradus.search.query.NotQuery;
import com.dell.doradus.search.query.OrQuery;
import com.dell.doradus.search.query.Query;
import com.dell.doradus.search.query.RangeQuery;
import com.dell.doradus.search.query.TransitiveLinkQuery;

public class Searcher {
    public static ArrayList<String> EMPTY_ARRAY = new ArrayList<String>(0);
    private static Map<Class<? extends Query>, Class<? extends SearchBuilder>> m_registeredBuilders = 
    		new HashMap<Class<? extends Query>, Class<? extends SearchBuilder>>();
    
    static {
    	m_registeredBuilders.put(AllQuery.class, BuilderAll.class);
    	m_registeredBuilders.put(AndQuery.class, BuilderAnd.class);
    	m_registeredBuilders.put(BinaryQuery.class, BuilderBinary.class);
    	m_registeredBuilders.put(DatePartBinaryQuery.class, BuilderDatePartBinary.class);
    	m_registeredBuilders.put(FieldCountQuery.class, BuilderFieldCount.class);
    	m_registeredBuilders.put(FieldCountRangeQuery.class, BuilderFieldCountRange.class);
    	m_registeredBuilders.put(IdQuery.class, BuilderId.class);
    	m_registeredBuilders.put(LinkQuery.class, BuilderLink.class);
    	m_registeredBuilders.put(LinkCountQuery.class, BuilderLinkCount.class);
    	m_registeredBuilders.put(LinkCountRangeQuery.class, BuilderLinkCountRange.class);
    	m_registeredBuilders.put(LinkIdQuery.class, BuilderLinkId.class);
    	m_registeredBuilders.put(TransitiveLinkQuery.class, BuilderLinkTransitive.class);
    	m_registeredBuilders.put(MVSBinaryQuery.class, BuilderMVSBinary.class);
    	m_registeredBuilders.put(NotQuery.class, BuilderNot.class);
    	m_registeredBuilders.put(OrQuery.class, BuilderOr.class);
    	m_registeredBuilders.put(RangeQuery.class, BuilderRange.class);
    }
    
    private DBEntitySequenceFactory m_factory;
    
    public Searcher() {
        m_factory = new DBEntitySequenceFactory();
    }

    public FilteredIterable search(SearchParameters params, TableDefinition tableDef, Query query) {
    	return search(params, tableDef, query, null);
    }
    
    public FilteredIterable search(SearchParameters params, TableDefinition tableDef, Query query, List<Integer> shards) {
    	Class<? extends Query> queryClass = query.getClass();
    	if(!m_registeredBuilders.containsKey(queryClass)) {
    		throw new IllegalArgumentException("query " + queryClass.getSimpleName() + " not supported");
    	}
    	SearchBuilder builder;
		try {
			builder = m_registeredBuilders.get(queryClass).newInstance();
		}catch(RuntimeException re) {
			throw re;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
    	builder.set(this, params, tableDef);
    	builder.set(shards);
    	FilteredIterable iter = builder.search(query);
    	if(iter == null) {
    		AllIterable allIter = new AllIterable(tableDef, shards, params.continuation, params.inclusive);
    		Filter filter = builder.filter(query);
    		iter = new FilteredIterable(this, filter, allIter, tableDef);
    	}
    	return iter;
    }

    public Filter filter(SearchParameters params, TableDefinition tableDef, Query query) {
    	Class<? extends Query> queryClass = query.getClass();
    	if(!m_registeredBuilders.containsKey(queryClass)) {
    		throw new IllegalArgumentException("query " + queryClass.getSimpleName() + " not supported");
    	}
    	SearchBuilder builder;
		try {
			builder = m_registeredBuilders.get(queryClass).newInstance();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
    	builder.set(this, params, tableDef);
    	return builder.filter(query);
    }
    
    public DBEntitySequenceFactory getFactory() { return m_factory; }
    
    public EntitySequence getSequence(TableDefinition table, Iterable<ObjectID> sequence) {
        return m_factory.getSequence(table, sequence, EMPTY_ARRAY);
    }

    public EntitySequence getSequence(TableDefinition table, Iterable<ObjectID> sequence, List<String> fields) {
        return m_factory.getSequence(table, sequence, fields);
    }

    public static List<String> getFields(Filter filter) {
    	if(filter == null) return EMPTY_ARRAY;
        HashSet<String> fieldsSet = new HashSet<String>();
    	filter.addFields(fieldsSet);
        ArrayList<String> fields = new ArrayList<String>();
        if(fieldsSet.contains("*")) fields.add("*");
        else fields.addAll(fieldsSet);
        return fields;
    }
}







