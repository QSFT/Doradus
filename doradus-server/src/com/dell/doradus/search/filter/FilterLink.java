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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.dell.doradus.common.FieldDefinition;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.search.Searcher;
import com.dell.doradus.search.aggregate.Entity;
import com.dell.doradus.search.aggregate.EntitySequence;
import com.dell.doradus.search.query.LinkQuery;

public class FilterLink implements Filter {
    enum Quantifier { ANY, NONE, ALL }
    
    private LinkQuery m_query;
    private Filter m_inner;
    private List<String> m_innerFields;
    private Quantifier m_quantifier;
    private List<String> m_links = new ArrayList<String>();
    
    public FilterLink(Searcher searcher, TableDefinition table, LinkQuery query, Filter inner) {
        m_query = query;
        m_inner = inner;
        m_innerFields = Searcher.getFields(m_inner);
        if(LinkQuery.ANY.equals(m_query.quantifier)) m_quantifier = Quantifier.ANY;
        else if(LinkQuery.ALL.equals(m_query.quantifier)) m_quantifier = Quantifier.ALL;
        else if(LinkQuery.NONE.equals(m_query.quantifier)) m_quantifier = Quantifier.NONE;
        else throw new IllegalArgumentException("Unknown link quantifier: " + query.quantifier);
        
        addGroupFields(table.getFieldDef(m_query.link));
    }
    
    private void addGroupFields(FieldDefinition fieldDef) {
    	if(fieldDef.isLinkField()) m_links.add(fieldDef.getName());
    	else if(fieldDef.isGroupField()) {
    		for(FieldDefinition child: fieldDef.getNestedFields()) {
    			addGroupFields(child);
    		}
    	}
    }

    @Override public boolean check(Entity entity) {
    	List<EntitySequence> links = new ArrayList<EntitySequence>(m_links.size());
    	for(String link: m_links) links.add(entity.getLinkedEntities(link, m_innerFields));
        if(m_quantifier == Quantifier.ANY) {
        	for(EntitySequence es: links) {
	            for(Entity e: es) {
	                if(m_inner.check(e)) return true;
	            }
        	}
            return false;
        }
        else if(m_quantifier == Quantifier.ALL) {
            boolean hasLinks = false;
        	for(EntitySequence es: links) {
	            for(Entity e: es) {
	                hasLinks = true;
	                if(!m_inner.check(e)) return false;
	            }
            }
            return hasLinks;
        } 
        else if(m_quantifier == Quantifier.NONE) {
        	for(EntitySequence es: links) {
	            for(Entity e: es) {
	            	if(m_inner.check(e)) return false;
	            }
            }
            return true;
        } 
        else throw new IllegalArgumentException("Unknown quantifier: " + m_quantifier.toString());
    }

    @Override public void addFields(Set<String> fields) { }
    
}
