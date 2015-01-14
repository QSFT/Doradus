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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.core.ObjectID;
import com.dell.doradus.core.ServerConfig;
import com.dell.doradus.search.aggregate.DBEntitySequenceFactory;
import com.dell.doradus.search.aggregate.DBEntitySequenceOptions;
import com.dell.doradus.search.aggregate.EntitySequence;
import com.dell.doradus.search.aggregate.SortOrder;
import com.dell.doradus.search.filter.Filter;
import com.dell.doradus.search.parser.AggregationQueryBuilder;
import com.dell.doradus.search.parser.DoradusQueryBuilder;
import com.dell.doradus.search.query.Query;
import com.dell.doradus.utilities.TimerGroup;

/**
 * Represents a full text query, including its perspective table and parameters.
 */
public class QueryExecutor {
    protected static Logger LOG = LoggerFactory.getLogger(QueryExecutor.class.getSimpleName());

    private TableDefinition m_table;
    private int m_pageSize;
    private int m_skip = 0;
    private SearchParameters m_parameters = new SearchParameters();
    
    public QueryExecutor(TableDefinition tableDef) {
    	m_table = tableDef;
    	m_pageSize = ServerConfig.getInstance().search_default_page_size;
    }
    
    public void setPageSize(int pageSize) {
        m_pageSize = pageSize;
    }
    
    public void setSkip(int skip) {
    	m_skip = skip;
    }

    public void setContinueAfterID(String objectID) {
        m_parameters.continuation = IDHelper.createID(objectID);
        m_parameters.inclusive = false;
    }
    
    public void setContinueBeforeID(String objectID) {
        m_parameters.continuation = IDHelper.createID(objectID);
        m_parameters.inclusive = true;
    }
    
    public void setL2rEnabled(boolean l2rEnabled) {
        m_parameters.l2r = l2rEnabled;
    }
    
    public SearchResultList search(String query, String fields, String sortOrder) {
    	if(sortOrder != null && m_parameters.continuation != null) {
    		throw new IllegalArgumentException("Cannot use &g or &e parameters with &o parameter. " +
    				"To use paging please specify &k (skip) parameter instead");
    	}
        TimerGroup timers = new TimerGroup("search.timing");
        timers.start("search");
        if(m_pageSize == 0) m_pageSize = Integer.MAX_VALUE - 1;
        FieldSet fieldSet = new FieldSet(m_table, fields);
        fieldSet.limit = m_pageSize;
        try {
        	Iterable<ObjectID> iter = search(query);
            DBEntitySequenceOptions def = DBEntitySequenceOptions.defaultOptions;
            int p = m_pageSize;
            if(p < Integer.MAX_VALUE) p++;
            DBEntitySequenceOptions options = new DBEntitySequenceOptions(
            		Math.min(p, def.entityBuffer),
            		Math.min(p, def.linkBuffer),
            		Math.min(p, def.initialLinkBuffer),
            		Math.min(p, def.initialLinkBufferDimension),
            		Math.min(p, def.initialScalarBuffer));
            int cap = Math.min(p, 10000);
            DBEntitySequenceFactory factory = new DBEntitySequenceFactory(cap, cap, cap, options);
            SortOrder[] orders = AggregationQueryBuilder.BuildSortOrders(sortOrder, m_table);
            FieldSetCreator fieldSetCreator = new FieldSetCreator(fieldSet, orders);
            EntitySequence sequence = factory.getSequence(m_table, iter, fieldSetCreator.loadedFields);
            SearchResultList searchResultList = fieldSetCreator.create(sequence, m_skip);
            return searchResultList;
        }catch(Exception ex) {
//        	LOG.error("Search error: {}", ex);
        	throw ex;
        	//throw new IllegalArgumentException(ex);
        }finally {
            timers.stop("search");
            timers.log("search");
        }
    }
    
    public Iterable<ObjectID> search(String query) {
    	Query qu = DoradusQueryBuilder.Build(query, m_table);
    	return search(qu);
    }

    public Iterable<ObjectID> search(Query query) {
    	LOG.debug("query: {}", query);
    	Searcher s = new Searcher();
    	return s.search(m_parameters, m_table, query);
    }
    
    public Filter filter(String query) {
    	Query qu = DoradusQueryBuilder.Build(query, m_table);
    	return filter(qu);
    }

    public Filter filter(Query query) {
    	LOG.debug("query: {}", query);
    	Searcher s = new Searcher();
    	return s.filter(m_parameters, m_table, query);
    }
    
    
}

