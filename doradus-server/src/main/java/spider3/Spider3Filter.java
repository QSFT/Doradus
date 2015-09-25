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

package spider3;

import com.dell.doradus.common.FieldDefinition;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.common.Utils;
import com.dell.doradus.search.query.AllQuery;
import com.dell.doradus.search.query.AndQuery;
import com.dell.doradus.search.query.BinaryQuery;
import com.dell.doradus.search.query.DatePartBinaryQuery;
import com.dell.doradus.search.query.FieldCountQuery;
import com.dell.doradus.search.query.FieldCountRangeQuery;
import com.dell.doradus.search.query.IdInQuery;
import com.dell.doradus.search.query.IdQuery;
import com.dell.doradus.search.query.IdRangeQuery;
import com.dell.doradus.search.query.LinkCountQuery;
import com.dell.doradus.search.query.LinkCountRangeQuery;
import com.dell.doradus.search.query.LinkIdQuery;
import com.dell.doradus.search.query.LinkQuery;
import com.dell.doradus.search.query.MVSBinaryQuery;
import com.dell.doradus.search.query.NoneQuery;
import com.dell.doradus.search.query.NotQuery;
import com.dell.doradus.search.query.OrQuery;
import com.dell.doradus.search.query.PathComparisonQuery;
import com.dell.doradus.search.query.Query;
import com.dell.doradus.search.query.RangeQuery;
import com.dell.doradus.search.query.TransitiveLinkQuery;

public class Spider3Filter {
	
	public static DocSet search(TableDefinition tableDef, Query query) {
	    
		if(query instanceof AllQuery) {
	        DocSet set = new DocSet();
		    set.fillAll(tableDef);
			return set;
        } 
		
		if(query instanceof NoneQuery) {
	        DocSet set = new DocSet();
            return set;
		} 
		
		if(query instanceof AndQuery) {
		    DocSet set = null;
			for(Query qu : ((AndQuery)query).subqueries) {
			    DocSet newset = search(tableDef, qu);
			    if(set == null) set = newset;
			    else set.and(newset);
			}
			return set;
		} 
		
		if(query instanceof OrQuery) {
            DocSet set = null;
            for(Query qu : ((AndQuery)query).subqueries) {
                DocSet newset = search(tableDef, qu);
                if(set == null) set = newset;
                else set.or(newset);
            }
		}
		
		if(query instanceof NotQuery) {
		    DocSet set = new DocSet();
		    set.fillAll(tableDef);
		    DocSet newset = search(tableDef, ((NotQuery)query).innerQuery);
			set.andNot(newset);
		}
		
		if(query instanceof IdInQuery) {
			IdInQuery iiq = (IdInQuery)query;
			DocSet set = new DocSet();
			for(String id : iiq.ids) set.addId(id);
			return set;
		}
		
		if(query instanceof BinaryQuery) {
			BinaryQuery bq = (BinaryQuery)query;
			String field = bq.field;
			String value = bq.value;
			if(value == null) value = "*";
			if(bq.operation.equals(BinaryQuery.CONTAINS)) value = "*" + value + "*";
			Utils.require(field != null && !"*".equals(field), "All-field queries not supported");
			DocSet set = new DocSet();
			FieldDefinition fieldDef = tableDef.getFieldDef(field);
            Utils.require(fieldDef != null, "Unknown field: " + field);
			set.fillField(fieldDef, value);
			return set;
		}
		
		if(query instanceof MVSBinaryQuery) {
			MVSBinaryQuery mvs = (MVSBinaryQuery)query;
			BinaryQuery bq = mvs.innerQuery;
			String field = bq.field;
			String value = bq.value;
            if(value == null) value = "*";
            if(bq.operation.equals(BinaryQuery.CONTAINS)) value = "*" + value + "*";
            Utils.require(field != null && !"*".equals(field), "All-field queries not supported");
            Utils.require(mvs.quantifier.equals(LinkQuery.ANY), "Only ANY quantifier supported");
            DocSet set = new DocSet();
            FieldDefinition fieldDef = tableDef.getFieldDef(field);
            Utils.require(fieldDef != null, "Unknown field: " + field);
            set.fillField(fieldDef, value);
            return set;
		}
		
		if(query instanceof LinkQuery) {
            LinkQuery lq = (LinkQuery)query;
            String field = lq.link;
            Utils.require(lq.quantifier.equals(LinkQuery.ANY), "Only ANY quantifier supported");
            Utils.require(lq.filter == null, "Filters not supported");
            FieldDefinition fieldDef = tableDef.getFieldDef(field);
            Utils.require(fieldDef != null && fieldDef.isLinkField(), "Unknown link: " + field);
            DocSet linkedSet = search(fieldDef.getInverseTableDef(), lq.innerQuery);
            DocSet set = new DocSet();
            set.fillLink(fieldDef, linkedSet);
            return set;
		}
		
		if(query instanceof RangeQuery) {
		    Utils.require(false, "Range query not supported");
		}
		
		if(query instanceof TransitiveLinkQuery) {
            Utils.require(false, "Transitive query not supported");
		}
		
		if(query instanceof IdQuery) {
			IdQuery iq = (IdQuery)query;
            DocSet set = new DocSet();
            set.addId(iq.id);
            return set;
		}
		
		if(query instanceof LinkIdQuery) {
			LinkIdQuery lq = (LinkIdQuery)query;
			LinkQuery linkq = new LinkQuery(lq.quantifier, lq.link, new IdQuery(lq.id));
			return search(tableDef, linkq);
		}
		
		if(query instanceof LinkCountQuery) {
            Utils.require(false, "LinkCountQuery not supported");
		}
		
		if(query instanceof LinkCountRangeQuery) {
            Utils.require(false, "LinkCountRangeQuery not supported");
		}
		
		if(query instanceof DatePartBinaryQuery) {
            Utils.require(false, "DatePartBinaryQuery not supported");
		}
		
		if(query instanceof FieldCountQuery) {
            Utils.require(false, "FieldCountQuery not supported");
		}
		
		if(query instanceof FieldCountRangeQuery) {
            Utils.require(false, "FieldCountRangeQuery not supported");
		}
		if(query instanceof IdRangeQuery) {
            Utils.require(false, "IdRangeQuery not supported");
        }
		
		if(query instanceof PathComparisonQuery) {
            Utils.require(false, "PathComparisonQuery not supported");
		} 
		
		throw new IllegalArgumentException("Query " + query.getClass().getSimpleName() + " not supported");
	}

}








