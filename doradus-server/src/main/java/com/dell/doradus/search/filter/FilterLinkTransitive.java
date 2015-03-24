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

import java.util.List;
import java.util.Set;

import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.search.Searcher;
import com.dell.doradus.search.aggregate.Entity;
import com.dell.doradus.search.aggregate.EntitySequence;
import com.dell.doradus.search.query.LinkQuery;
import com.dell.doradus.search.query.TransitiveLinkQuery;

public class FilterLinkTransitive implements Filter {
	public static final int MAX_DEPTH = 20;
    enum Quantifier { ANY, NONE, ALL }
    
    private TransitiveLinkQuery m_query;
    private Filter m_inner;
    private List<String> m_innerFields;
    private Quantifier m_quantifier;
    private int m_depth;
    
    public FilterLinkTransitive(Searcher searcher, TableDefinition table, TransitiveLinkQuery query, Filter inner) {
        m_query = query;
        m_inner = inner;
        m_innerFields = Searcher.getFields(m_inner);
        m_depth = query.depth;
        if(LinkQuery.ANY.equals(m_query.quantifier)) m_quantifier = Quantifier.ANY;
        else if(LinkQuery.ALL.equals(m_query.quantifier)) m_quantifier = Quantifier.ALL;
        else if(LinkQuery.NONE.equals(m_query.quantifier)) m_quantifier = Quantifier.NONE;
        else throw new IllegalArgumentException("Unknown link quantifier: " + query.quantifier);
    }
    

    @Override public boolean check(Entity entity) {
		if(m_depth == 0) m_depth = MAX_DEPTH;
		return check(m_depth - 1, entity);
    }

    private boolean check(int level, Entity entity) {
        EntitySequence links = entity.getLinkedEntities(m_query.link, m_innerFields);
        if(m_quantifier == Quantifier.ANY) {
            for(Entity e: links) {
                if(m_inner.check(e)) return true;
				if(level > 0 && check(level - 1, e))  return true;
            }
            return false;
        }
        else if(m_quantifier == Quantifier.ALL) {
            boolean hasLinks = false;
            for(Entity e: links) {
                hasLinks = true;
                if(!m_inner.check(e)) return false;
				if(level > 0 && !check(level - 1, e)) return false;
            }
            return hasLinks;
        } 
        else if(m_quantifier == Quantifier.NONE) {
            for(Entity e: links) {
                if(m_inner.check(e)) return false;
				if(level > 0 && check(level - 1, e)) return false;
            }
            return true;
        } 
        else throw new IllegalArgumentException("Unknown quantifier: " + m_quantifier.toString());
    }


	@Override public void addFields(Set<String> fields) { }
    
    
}
