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

import com.dell.doradus.common.CommonDefs;
import com.dell.doradus.common.Utils;
import com.dell.doradus.search.aggregate.Entity;
import com.dell.doradus.search.query.LinkQuery;

public class FilterMVSEquals implements Filter {
    enum Quantifier { ANY, NONE, ALL }
    
    private String m_field;
    private String m_value;
    private Quantifier m_quantifier;
    
    public FilterMVSEquals(String field, String value, String quantifier) {
        m_field = field;
        m_value = value;
        if(LinkQuery.ANY.equals(quantifier)) m_quantifier = Quantifier.ANY;
        else if(LinkQuery.ALL.equals(quantifier)) m_quantifier = Quantifier.ALL;
        else if(LinkQuery.NONE.equals(quantifier)) m_quantifier = Quantifier.NONE;
        else throw new IllegalArgumentException("Unknown MVS quantifier: " + quantifier);
    }

    @Override public boolean check(Entity entity) {
    	String fieldValue = entity.get(m_field);
    	if(fieldValue == null)return false;
        if(m_quantifier == Quantifier.ANY) {
    		for(String subvalue: Utils.split(fieldValue, CommonDefs.MV_SCALAR_SEP_CHAR)) {
    			if(FilterEquals.compare(subvalue, m_value)) return true;
            }
            return false;
        }
        else if(m_quantifier == Quantifier.ALL) {
            boolean hasValues = false;
    		for(String subvalue: Utils.split(fieldValue, CommonDefs.MV_SCALAR_SEP_CHAR)) {
                hasValues = true;
                if(!FilterEquals.compare(subvalue, m_value)) return false;
            }
            return hasValues;
        } 
        else if(m_quantifier == Quantifier.NONE) {
    		for(String subvalue: Utils.split(fieldValue, CommonDefs.MV_SCALAR_SEP_CHAR)) {
                if(FilterEquals.compare(subvalue, m_value)) return false;
            }
            return true;
        } 
        else throw new IllegalArgumentException("Unknown quantifier: " + m_quantifier.toString());
    }

    @Override public void addFields(Set<String> fields) {
        fields.add(m_field);
    }
    
}
