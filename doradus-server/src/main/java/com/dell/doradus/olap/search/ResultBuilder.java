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

package com.dell.doradus.olap.search;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.TimeZone;
import java.util.regex.Pattern;

import com.dell.doradus.common.FieldDefinition;
import com.dell.doradus.common.FieldType;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.common.Utils;
import com.dell.doradus.core.ServerConfig;
import com.dell.doradus.olap.aggregate.mr.MFCollectorSet;
import com.dell.doradus.olap.collections.BdLongSet;
import com.dell.doradus.olap.io.BSTR;
import com.dell.doradus.olap.store.CubeSearcher;
import com.dell.doradus.olap.store.FieldSearcher;
import com.dell.doradus.olap.store.IdSearcher;
import com.dell.doradus.olap.store.NumSearcherMV;
import com.dell.doradus.olap.store.ValueSearcher;
import com.dell.doradus.olap.xlink.XLinkContext;
import com.dell.doradus.olap.xlink.XLinkQuery;
import com.dell.doradus.search.aggregate.AggregationGroup;
import com.dell.doradus.search.filter.FilterContains;
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
import com.dell.doradus.search.query.PathCountRangeQuery;
import com.dell.doradus.search.query.Query;
import com.dell.doradus.search.query.RangeQuery;
import com.dell.doradus.search.query.TransitiveLinkQuery;
import com.dell.doradus.search.util.LRUSizeCache;

public class ResultBuilder {
	private static int queryCache = -1;
	private static LRUSizeCache<String, Result> m_cache;
	
	public static Result search(TableDefinition tableDef, Query query, CubeSearcher searcher) {
		synchronized(ResultBuilder.class) {
			if(queryCache == -1) {
				queryCache = ServerConfig.getInstance().olap_query_cache_size_mb;
				if(queryCache > 0) {
					m_cache = new LRUSizeCache<String, Result>(0, queryCache * 1024L * 1024);
				}
			}
		}
		boolean skipCache = m_cache == null || XLinkContext.isXLinkQuery(tableDef, query); 
		if(skipCache) return searchInternal(tableDef, query, searcher);
		
		String key = searcher.getId() + "/" + tableDef.getTableName() + "/" + query.toString();
		Result result = m_cache.get(key);
		if(result != null) {
			Result newResult = new Result(result);
			return newResult;
		}
		result = searchInternal(tableDef, query, searcher);
		m_cache.put(key, result, result.getBitVector().getBuffer().length + 2 * key.length() + 16);
		//return result;
		Result newResult = new Result(result);
		return newResult;
	}
	
	
	private static Result searchInternal(TableDefinition tableDef, Query query, CubeSearcher searcher) {
		Result r = new Result(searcher.getDocs(tableDef.getTableName()));
		if(query instanceof AllQuery) {
			r.not();
		} else if(query instanceof AndQuery) {
			r.not();
			for(Query qu : ((AndQuery)query).subqueries) {
				Result c = search(tableDef, qu, searcher);
				r.and(c);
				if(r.countSet() == 0) return r;
			}
		} else if(query instanceof NoneQuery) {
		} else if(query instanceof OrQuery) {
			IdInQuery iiq = IdInQuery.tryCreate((OrQuery)query);
			if(iiq != null) return searchInternal(tableDef, iiq, searcher);
			
			for(Query qu : ((OrQuery)query).subqueries) {
				Result c = search(tableDef, qu, searcher);
				r.or(c);
			}
		} else if(query instanceof NotQuery) {
			r = search(tableDef, ((NotQuery)query).innerQuery, searcher);
			r.not();
		} else if(query instanceof IdInQuery) {
			IdInQuery iiq = (IdInQuery)query;
			List<BSTR> ids = new ArrayList<BSTR>(iiq.ids.size());
			for(String id : iiq.ids) ids.add(new BSTR(id));
			Collections.sort(ids);
			IdSearcher id_searcher = searcher.getIdSearcher(tableDef.getTableName());
			id_searcher.reset();
			for(BSTR id : ids) {
				int doc = id_searcher.findNext(id);
				if(doc >= 0) r.set(doc);
			}
		} else if(query instanceof BinaryQuery) {
			BinaryQuery bq = (BinaryQuery)query;
			String field = bq.field;
			String value = bq.value;
			if(field == null && "*".equals(value)) {
				r.not();
				return r;
			}
			if(field == null || "*".equals(field)) throw new IllegalArgumentException("All-fields search not supported");
			FieldDefinition f = tableDef.getFieldDef(field);
			if(f == null) throw new IllegalArgumentException("Field '" + field + "' not found");
			
			//if(value.indexOf('?') >= 0) throw new IllegalArgumentException("'?' is not supported*");
			if(f.getType() == FieldType.TEXT) {
				if(value == null) {
					FieldSearcher field_searcher = searcher.getFieldSearcher(tableDef.getTableName(), field);
					field_searcher.fillCount(0, 1, r);
					return r;
				}
				if("*".equals(value)) {
					FieldSearcher field_searcher = searcher.getFieldSearcher(tableDef.getTableName(), field);
					field_searcher.fillCount(0, 1, r);
					r.not();
					return r;
				}
				
				value = value.toLowerCase();
				ValueSearcher vs = searcher.getValueSearcher(tableDef.getTableName(), field);
				if(bq.operation.equals(BinaryQuery.EQUALS)) {
					//first occurrence of '*' or '?'
					int idx = value.indexOf('*');
					int idx2 = value.indexOf('?');
					if(idx < 0) idx = idx2;
					else if(idx2 >= 0 && idx2 < idx) idx = idx2;
					
					if(idx >= 0 && idx != value.length() - 1) {
						Result vr = new Result(vs.size());
						for(int i = 0; i < vs.size(); i++) {
							String str = vs.getValue(i).toString();
							if(Utils.matchesPattern(str, value)) {
								vr.set(i);
							}
						}
						if(vr.countSet() == 0) return r;
						FieldSearcher field_searcher = searcher.getFieldSearcher(tableDef.getTableName(), field);
						field_searcher.fillDocs(vr, r);
					}
					else if(idx >= 0) {
						value = value.substring(0, value.length() - 1);
						BSTR term = new BSTR(value);
						int term_min = vs.find(term, false);
						term = new BSTR(value + "\uFFFF");
						int term_max = vs.find(term, false);
                        if(term_max < term_min) return r;
						FieldSearcher field_searcher = searcher.getFieldSearcher(tableDef.getTableName(), field);
						field_searcher.fill(term_min, term_max, r);
					}
					else {
						BSTR term = new BSTR(value);
						int term_no = vs.find(term, true);
						if(term_no >= 0) {
							FieldSearcher field_searcher = searcher.getFieldSearcher(tableDef.getTableName(), field);
							field_searcher.fill(term_no, r);
						}
					}
				}
				else if(bq.operation.equals(BinaryQuery.CONTAINS)) {
					Result vr = new Result(vs.size());
					for(int i = 0; i < vs.size(); i++) {
						String str = vs.getValue(i).toString();
						if(FilterContains.compare(str, value)) vr.set(i);
					}
                    if(vr.countSet() == 0) return r;
					FieldSearcher field_searcher = searcher.getFieldSearcher(tableDef.getTableName(), field);
					field_searcher.fillDocs(vr, r);
				}
				else if(bq.operation.equals(BinaryQuery.REGEXP)) {
					Result vr = new Result(vs.size());
					for(int i = 0; i < vs.size(); i++) {
						String str = vs.getValue(i).toString();
						if(Pattern.matches(value, str)) vr.set(i);
					}
                    if(vr.countSet() == 0) return r;
					FieldSearcher field_searcher = searcher.getFieldSearcher(tableDef.getTableName(), field);
					field_searcher.fillDocs(vr, r);
				}
				else throw new IllegalArgumentException(bq.operation + " is not supported");
			} else if(NumSearcherMV.isNumericType(f.getType())) {
				if(value == null) {
					NumSearcherMV num_searcher = searcher.getNumSearcher(tableDef.getTableName(), field);
					num_searcher.fillNull(r);
					return r;
				}
				if("*".equals(value)) {
					NumSearcherMV num_searcher = searcher.getNumSearcher(tableDef.getTableName(), field);
					num_searcher.fillNull(r);
					r.not();
					return r;
				}
				if(!bq.operation.equals(BinaryQuery.EQUALS)) throw new IllegalArgumentException("Contains is not supported for numeric types");
				if(value.indexOf('*') >= 0 ||value.indexOf('?') >= 0) throw new IllegalArgumentException("Wildcard search not supported for numeric types");
				NumSearcherMV num_searcher = searcher.getNumSearcher(tableDef.getTableName(), field);
				num_searcher.fill(NumSearcherMV.parse(value, f.getType()), r);
			} else throw new IllegalArgumentException("Field type '" + f.getType() + "' not supported");
		} else if(query instanceof MVSBinaryQuery) {
			MVSBinaryQuery mvs = (MVSBinaryQuery)query;
			BinaryQuery bq = mvs.innerQuery;
			String field = bq.field;
			String value = bq.value;
			if(field == null && "*".equals(value)) {
				r.not();
				return r;
			}
			if(field == null || "*".equals(field)) throw new IllegalArgumentException("All-fields search not supported");
			FieldDefinition f = tableDef.getFieldDef(field);
			if(f == null) throw new IllegalArgumentException("Field '" + field + "' not found");
			//if(value.indexOf('?') >= 0) throw new IllegalArgumentException("'?' is not supported*");
			if(f.getType() == FieldType.TEXT) {
				if(value == null) {
					FieldSearcher field_searcher = searcher.getFieldSearcher(tableDef.getTableName(), field);
					field_searcher.fillCount(0, 1, r);
					return r;
				}
				if("*".equals(value)) {
					FieldSearcher field_searcher = searcher.getFieldSearcher(tableDef.getTableName(), field);
					field_searcher.fillCount(0, 1, r);
					r.not();
					return r;
				}
				
				value = value.toLowerCase();
				ValueSearcher vs = searcher.getValueSearcher(tableDef.getTableName(), field);
				if(bq.operation.equals(BinaryQuery.EQUALS)) {
					//first occurrence of '*' or '?'
					int idx = value.indexOf('*');
					int idx2 = value.indexOf('?');
					if(idx < 0) idx = idx2;
					else if(idx2 >= 0 && idx2 < idx) idx = idx2;
					
					if(idx >= 0 && idx != value.length() - 1) {
						Result vr = new Result(vs.size());
						for(int i = 0; i < vs.size(); i++) {
							String str = vs.getValue(i).toString();
							if(Utils.matchesPattern(str, value)) vr.set(i);
						}
						if(LinkQuery.ALL.equals(mvs.quantifier)) vr.not();
						FieldSearcher field_searcher = searcher.getFieldSearcher(tableDef.getTableName(), field);
						field_searcher.fillDocs(vr, r);
						if(LinkQuery.ALL.equals(mvs.quantifier)) {
							Result noValues = new Result(r.size());
							field_searcher.fillCount(0, 1, noValues);
							r.or(noValues);
						}
					}
					else if(idx >= 0) {
						value = value.substring(0, value.length() - 1);
						BSTR term = new BSTR(value);
						int term_min = vs.find(term, false);
						term = new BSTR(value + "\uFFFF");
						int term_max = vs.find(term, false);
						FieldSearcher field_searcher = searcher.getFieldSearcher(tableDef.getTableName(), field);
						if(LinkQuery.ALL.equals(mvs.quantifier)) {
							field_searcher.fill(Integer.MIN_VALUE, term_min, r);
							field_searcher.fill(term_max, Integer.MAX_VALUE, r);
						} else {
							field_searcher.fill(term_min, term_max, r);
						}
					} else {
						BSTR term = new BSTR(value);
						int term_no = vs.find(term, true);
						if(term_no >= 0) {
							FieldSearcher field_searcher = searcher.getFieldSearcher(tableDef.getTableName(), field);
							if(LinkQuery.ALL.equals(mvs.quantifier)) {
								field_searcher.fill(Integer.MIN_VALUE, term_no, r);
								field_searcher.fill(term_no + 1, Integer.MAX_VALUE, r);
								
								Result noValues = new Result(r.size());
								field_searcher.fillCount(0, 1, r);
								r.or(noValues);
								
							} else {
								field_searcher.fill(term_no, r);
							}
						} else if(LinkQuery.ALL.equals(mvs.quantifier)) {
							r.not();
						}

					}
				}
				else if(bq.operation.equals(BinaryQuery.CONTAINS)) {
					Result vr = new Result(vs.size());
					for(int i = 0; i < vs.size(); i++) {
						String str = vs.getValue(i).toString();
						if(FilterContains.compare(str, value)) vr.set(i);
					}
					if(LinkQuery.ALL.equals(mvs.quantifier)) vr.not();
					FieldSearcher field_searcher = searcher.getFieldSearcher(tableDef.getTableName(), field);
					field_searcher.fillDocs(vr, r);
					if(LinkQuery.ALL.equals(mvs.quantifier)) {
						Result noValues = new Result(r.size());
						field_searcher.fillCount(0, 1, r);
						r.or(noValues);
					}
				}
				else if(bq.operation.equals(BinaryQuery.REGEXP)) {
					Result vr = new Result(vs.size());
					for(int i = 0; i < vs.size(); i++) {
						String str = vs.getValue(i).toString();
						if(Pattern.matches(value, str)) vr.set(i);
					}
					if(LinkQuery.ALL.equals(mvs.quantifier)) vr.not();
					FieldSearcher field_searcher = searcher.getFieldSearcher(tableDef.getTableName(), field);
					field_searcher.fillDocs(vr, r);
					if(LinkQuery.ALL.equals(mvs.quantifier)) {
						Result noValues = new Result(r.size());
						field_searcher.fillCount(0, 1, r);
						r.or(noValues);
					}
				}
				else throw new IllegalArgumentException(bq.operation + " is not supported");
			} else if(NumSearcherMV.isNumericType(f.getType())) {
				if(value == null) {
					NumSearcherMV num_searcher = searcher.getNumSearcher(tableDef.getTableName(), field);
					num_searcher.fillNull(r);
					return r;
				}
				if("*".equals(value)) {
					NumSearcherMV num_searcher = searcher.getNumSearcher(tableDef.getTableName(), field);
					num_searcher.fillNull(r);
					r.not();
					return r;
				}
				
				if(LinkQuery.ANY.equals(mvs.quantifier)) {
					return searchInternal(tableDef, mvs.innerQuery, searcher);
				}
				else if(LinkQuery.NONE.equals(mvs.quantifier)) {
					Result result = searchInternal(tableDef, mvs.innerQuery, searcher);
					result.not();
					return result;
				}
				else if(LinkQuery.ALL.equals(mvs.quantifier)) {
					Result result = searchInternal(tableDef, mvs.innerQuery, searcher);
					NumSearcherMV ns = searcher.getNumSearcher(tableDef.getTableName(), field);
					Result countOne = new Result(result.size());
					ns.fillCount(1,2,countOne);
					result.and(countOne);
					return result;
				}
			}
			else throw new IllegalArgumentException("Field type '" + f.getType() + "' not supported");
			if(!LinkQuery.ANY.equals(mvs.quantifier)) r.not();
		} else if(query instanceof LinkQuery) {
			//NONE(link: P) = NOT ANY(link: P); ALL(link: P)= NOT ANY(link: NOT P)
			// ALL(link: filter=X and inner=Y) = ANY(link: X) AND NOT ANY(link: X AND NOT Y)
			LinkQuery lq = (LinkQuery)query;
			if(lq.xlink != null) {
				((XLinkQuery)lq.xlink).search(searcher, r);
				return r;
			}
			FieldDefinition field = tableDef.getFieldDef(lq.link);
			Utils.require(!field.isGroupField(), "Group fields are not supported");
			Utils.require(field.isLinkField(), lq.link + " is not a link field");
			TableDefinition extent = tableDef.getAppDef().getTableDef(field.getLinkExtent());
			Result inner = search(extent, lq.innerQuery, searcher);
			
            if(!LinkQuery.NONE.equals(lq.quantifier) && inner.countSet() == 0) {
                return r;
            }
			
			if(LinkQuery.ALL.equals(lq.quantifier)) inner.not();
			Result filter = null;
			if(lq.filter != null) {
				filter = search(extent, lq.filter, searcher);
				inner.and(filter);
			}
			
			if(LinkQuery.ANY.equals(lq.quantifier) && inner.countSet() == 0) {
			    return r;
			}
			
			//Result inner = search(extent, lq.getInnerQuery(), searcher);
			FieldSearcher field_searcher = searcher.getFieldSearcher(field.getLinkExtent(), field.getLinkInverse());
			field_searcher.fields(inner, r);
			if(!LinkQuery.ANY.equals(lq.quantifier)) r.not();
			if(LinkQuery.ALL.equals(lq.quantifier)) {
				inner.clear();
				inner.not();
				if(filter != null) inner.and(filter);
				Result atLeastOne = new Result(r.size());
				field_searcher.fields(inner, atLeastOne);
				r.and(atLeastOne);
			}
		} else if(query instanceof RangeQuery) {
			RangeQuery rq = (RangeQuery)query;
			String field = rq.field;
			FieldDefinition f = tableDef.getFieldDef(field);
			if(f == null) throw new IllegalArgumentException("Field '" + field + "' not found");
			if(f.getType() == FieldType.TEXT) {
				String min = rq.min == null ? "" : rq.min.toLowerCase();
				if(!rq.minInclusive) min += "\u0000";
				ValueSearcher vs = searcher.getValueSearcher(tableDef.getTableName(), field);
				BSTR term = new BSTR(min);
				int term_min = vs.find(term, false);
				if(term_min < 0) term_min = vs.size();
				int term_max = vs.size();
				if(rq.max != null) {
					String max = rq.max.toLowerCase();
					if(rq.maxInclusive) max += "\u0000";
					term = new BSTR(max);
					term_max = vs.find(term, false);
				}
				FieldSearcher field_searcher = searcher.getFieldSearcher(tableDef.getTableName(), field);
				field_searcher.fill(term_min, term_max, r);
			} else if(NumSearcherMV.isNumericType(f.getType())) {
				long min = Long.MIN_VALUE;
				long max = Long.MAX_VALUE;
				if(rq.min != null) min = NumSearcherMV.parse(rq.min, f.getType());
				if(rq.max != null) max = NumSearcherMV.parse(rq.max, f.getType());
				if(!rq.minInclusive) min++;
				if(rq.maxInclusive) max++;
				NumSearcherMV num_searcher = searcher.getNumSearcher(tableDef.getTableName(), field);
				num_searcher.fill(min, max, r);
			} else throw new IllegalArgumentException("Field type '" + f.getType() + "' not supported");
		} else if(query instanceof TransitiveLinkQuery) {
			TransitiveLinkQuery lq = (TransitiveLinkQuery)query;
			if(LinkQuery.ALL.equals(lq.quantifier)) {
				// ALL(link^: filter=X and inner=Y) = ANY(link^: X) AND NOT ANY(link^: X AND NOT Y)
				//  = ANY(link: filter=X) AND NOT ANY(link^: filter=X and inner= NOT Y)
				// note that ANY(link:x) implies ANY(link^:x)
				AndQuery q = new AndQuery();
				LinkQuery clauseExists = new LinkQuery();
				clauseExists.filter = lq.filter;
				clauseExists.innerQuery = new AllQuery();
				clauseExists.link = lq.link;
				clauseExists.quantifier = LinkQuery.ANY;
				TransitiveLinkQuery clauseEveryone = new TransitiveLinkQuery();
				clauseEveryone.depth = lq.depth;
				clauseEveryone.filter = lq.filter;
				NotQuery nq = new NotQuery();
				nq.innerQuery = lq.innerQuery;
				clauseEveryone.innerQuery = nq;
				clauseEveryone.link = lq.link;
				clauseEveryone.quantifier = LinkQuery.ANY;
				NotQuery negation = new NotQuery();
				negation.innerQuery = clauseEveryone;
				q.subqueries.add(clauseExists);
				q.subqueries.add(negation);
				return searchInternal(tableDef, q, searcher);
			}
			FieldDefinition field = tableDef.getFieldDef(lq.link);
			int depth = lq.depth == 0 ? 100 : lq.depth;
			TableDefinition extent = tableDef.getAppDef().getTableDef(field.getLinkExtent());
			Result inner = search(extent, lq.getInnerQuery(), searcher);
			FieldSearcher field_searcher = searcher.getFieldSearcher(field.getLinkExtent(), field.getLinkInverse());
			
			
			Result iteration = new Result(r.size());
			Result next_iteration = new Result(r.size());
			field_searcher.fields(inner, iteration);
			r.or(iteration);
			int resultsCount = r.countSet();
			while(--depth > 0) {
				field_searcher.fields(iteration, next_iteration);
				Result tmp = iteration;
				iteration = next_iteration;
				next_iteration = tmp;
				r.or(iteration);
				int nextCount = r.countSet();
				if(resultsCount == nextCount) break;
				resultsCount = nextCount;
			}
			
			if(lq.filter != null) {
				Result filter = ResultBuilder.search(extent, lq.filter, searcher);
				r.and(filter);
			}
			return r;
			
		} else if(query instanceof IdQuery) {
			IdQuery iq = (IdQuery)query;
			if("*".equals(iq.id)) {
				r.clear();
				r.not();
				return r;
			}
			IdSearcher id_searcher = searcher.getIdSearcher(tableDef.getTableName());
			BSTR id = new BSTR(iq.id);
			int doc = id_searcher.find(id, true);
			if(doc >= 0) r.set(doc);
		} else if(query instanceof LinkIdQuery) {
			LinkIdQuery lq = (LinkIdQuery)query;
			if(lq.xlink != null) {
				((XLinkQuery)lq.xlink).search(searcher, r);
				return r;
			}
			// IS NULL
			if(lq.id == null) {
				FieldDefinition f = tableDef.getFieldDef(lq.link);
				if(f == null) throw new IllegalArgumentException("Link " + lq.link + " not found in table " + tableDef.getTableName());
				FieldSearcher field_searcher = searcher.getFieldSearcher(f.getTableName(),f.getName());
				field_searcher.fillCount(0,  1, r);
				return r;
			}
			if("*".equals(lq.id)) {
				FieldDefinition f = tableDef.getFieldDef(lq.link);
				if(f == null) throw new IllegalArgumentException("Link " + lq.link + " not found in table " + tableDef.getTableName());
				FieldSearcher field_searcher = searcher.getFieldSearcher(f.getTableName(),f.getName());
				field_searcher.fillCount(0,  1, r);
				r.not();
				return r;
			}
			LinkQuery linkq = new LinkQuery(lq.quantifier, lq.link, new IdQuery(lq.id));
			return search(tableDef, linkq, searcher);
		} else if(query instanceof LinkCountQuery) {
			LinkCountQuery q = (LinkCountQuery)query;
			if(q.xlink != null) {
				((XLinkQuery)q.xlink).search(searcher, r);
				return r;
			}
			FieldDefinition f = tableDef.getFieldDef(q.link);
			Utils.require(f != null, q.link + " not found in " + tableDef.getTableName());
			Utils.require(f.isLinkField(), q.link + " is not a link field");
			FieldSearcher field_searcher = searcher.getFieldSearcher(f.getTableName(),f.getName());
			if(q.filter != null) {
				TableDefinition extent = tableDef.getAppDef().getTableDef(f.getLinkExtent());
				Result filter = search(extent, q.filter, searcher);
				field_searcher.fillCount(q.count, q.count + 1, filter, r);
			}
			else field_searcher.fillCount(q.count, q.count + 1, r);
		} else if(query instanceof LinkCountRangeQuery) {
			LinkCountRangeQuery q = (LinkCountRangeQuery)query;
			if(q.xlink != null) {
				((XLinkQuery)q.xlink).search(searcher, r);
				return r;
			}
			FieldDefinition f = tableDef.getFieldDef(q.link);
			Utils.require(f != null, q.link + " not found in " + tableDef.getTableName());
			Utils.require(f.isLinkField(), q.link + " is not a link field");
			FieldSearcher field_searcher = searcher.getFieldSearcher(f.getTableName(),f.getName());
			int min = q.range.min == null ? Integer.MIN_VALUE : Integer.parseInt(q.range.min);
			int max = q.range.max == null ? Integer.MAX_VALUE : Integer.parseInt(q.range.max);
			if(!q.range.minInclusive) min++;
			if(q.range.maxInclusive) max++;
			if(q.filter != null) {
				TableDefinition extent = tableDef.getAppDef().getTableDef(f.getLinkExtent());
				Result filter = search(extent, q.filter, searcher);
				field_searcher.fillCount(min, max, filter, r);
			}
			else field_searcher.fillCount(min, max, r);
		} else if(query instanceof DatePartBinaryQuery) {
			DatePartBinaryQuery dpq = (DatePartBinaryQuery)query;
			int datePart = dpq.part;
			BinaryQuery bq = dpq.innerQuery;
			String field = bq.field;
			String value = bq.value;
			if(bq.operation != BinaryQuery.EQUALS) throw new IllegalArgumentException("Contains is not supported");
			FieldDefinition f = tableDef.getFieldDef(field);
			if(f == null) throw new IllegalArgumentException("Field '" + field + "' not found");
			if(f.getType() != FieldType.TIMESTAMP) throw new IllegalArgumentException("Field '" + field + "' in DatePartBinaryQuery should be timestamp");
			if(value.indexOf('*') >= 0 ||value.indexOf('?') >= 0) throw new IllegalArgumentException("Wildcard search not supported for DatePartBinaryQuery");
			int partValue = Integer.parseInt(value);
			//Months in Calendar start with 0 instead of 1
			if(datePart == Calendar.MONTH) partValue--;
			
			NumSearcherMV num_searcher = searcher.getNumSearcher(tableDef.getTableName(), field);
			Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
			for(int i = 0; i < r.size(); i++) {
				int sz = num_searcher.size(i);
				for(int j = 0; j < sz; j++) {
					long millis = num_searcher.get(i, j);
					if(millis == 0) continue;
					cal.setTimeInMillis(millis);
					if(cal.get(datePart) == partValue) r.set(i);
				}
			}
		} else if(query instanceof FieldCountQuery) {
			FieldCountQuery q = (FieldCountQuery)query;
			FieldDefinition f = tableDef.getFieldDef(q.field);
			Utils.require(f != null, q.field + " not found in " + tableDef.getTableName());
			if(NumSearcherMV.isNumericType(f.getType())) {
				NumSearcherMV num_searcher = searcher.getNumSearcher(f.getTableName(), f.getName());
				num_searcher.fillCount(q.count, q.count + 1, r);
			}
			else {
				FieldSearcher field_searcher = searcher.getFieldSearcher(f.getTableName(),f.getName());
				field_searcher.fillCount(q.count, q.count + 1, r);
			}
		} else if(query instanceof FieldCountRangeQuery) {
			FieldCountRangeQuery q = (FieldCountRangeQuery)query;
			FieldDefinition f = tableDef.getFieldDef(q.field);
			Utils.require(f != null, q.field + " not found in " + tableDef.getTableName());
			int min = q.range.min == null ? Integer.MIN_VALUE : Integer.parseInt(q.range.min);
			int max = q.range.max == null ? Integer.MAX_VALUE : Integer.parseInt(q.range.max);
			if(!q.range.minInclusive) min++;
			if(q.range.maxInclusive) max++;
			if(NumSearcherMV.isNumericType(f.getType())) {
				NumSearcherMV num_searcher = searcher.getNumSearcher(f.getTableName(), f.getName());
				num_searcher.fillCount(min, max, r);
			}
			else {
				FieldSearcher field_searcher = searcher.getFieldSearcher(f.getTableName(),f.getName());
				field_searcher.fillCount(min, max, r);
			}
		} else if(query instanceof IdRangeQuery) {
			IdRangeQuery rq = (IdRangeQuery)query;
			String min = rq.min == null ? "" : rq.min;
			if(!rq.minInclusive) min += "\u0000";
			IdSearcher idSearcher = searcher.getIdSearcher(tableDef.getTableName());
			BSTR term = new BSTR(min);
			int term_min = idSearcher.find(term, false);
			if(term_min < 0) term_min = idSearcher.size();
			int term_max = idSearcher.size();
			if(rq.max != null) {
				String max = rq.max;
				if(rq.maxInclusive) max += "\u0000";
				term = new BSTR(max);
				term_max = idSearcher.find(term, false);
			}
			for(int i = term_min; i < term_max; i++) {
				r.set(i);
			}
        } else if(query instanceof PathComparisonQuery) {
            PathComparisonQuery eq = (PathComparisonQuery)query;
            ArrayList<AggregationGroup> groups = new ArrayList<>();
            FieldDefinition f1 = eq.group1.getLastField();
            FieldDefinition f2 = eq.group2.getLastField();
            if(f1 != null && f2 != null && (!NumSearcherMV.isNumericType(f1.getType()) || NumSearcherMV.isNumericType(f2.getType()))) {
                Utils.require(f1.getTableName().equals(f2.getTableName()) && f1.getName().equals(f2.getName()), "Set operations on text/link fields are only allowed on link paths ending with the same field");
            }
            groups.add(eq.group1);
            groups.add(eq.group2);
            MFCollectorSet collector = new MFCollectorSet(searcher, groups, false);
            BdLongSet[] sets = new BdLongSet[2];
            for(int i = 0; i < sets.length; i++) {
                sets[i] = new BdLongSet(1024);
                sets[i].enableClearBuffer();
            }
            
            if("INTERSECTS".equals(eq.quantifier)) {
                for(int i = 0; i < r.size(); i++) {
                    collector.collect(i, sets);
                    if(sets[0].intersects(sets[1])) {
                        r.set(i);
                    }
                    sets[0].clear();
                    sets[1].clear();
                }
            } else if("EQUALS".equals(eq.quantifier)) {
                for(int i = 0; i < r.size(); i++) {
                    collector.collect(i, sets);
                    if(sets[0].equals(sets[1])) {
                        r.set(i);
                    }
                    sets[0].clear();
                    sets[1].clear();
                }
            } else if("DIFFERS".equals(eq.quantifier)) {
                for(int i = 0; i < r.size(); i++) {
                    collector.collect(i, sets);
                    if(sets[0].differs(sets[1])) {
                        r.set(i);
                    }
                    sets[0].clear();
                    sets[1].clear();
                }
            } else if("CONTAINS".equals(eq.quantifier)) {
                for(int i = 0; i < r.size(); i++) {
                    collector.collect(i, sets);
                    if(sets[0].contains(sets[1])) {
                        r.set(i);
                    }
                    sets[0].clear();
                    sets[1].clear();
                }
            } else if("DISJOINT".equals(eq.quantifier)) {
                for(int i = 0; i < r.size(); i++) {
                    collector.collect(i, sets);
                    if(sets[0].disjoint(sets[1])) {
                        r.set(i);
                    }
                    sets[0].clear();
                    sets[1].clear();
                }
            } else throw new IllegalArgumentException("Unknown quantifier: " + eq.quantifier);
            
        } else if(query instanceof PathCountRangeQuery) {
        	PathCountRangeQuery qu = (PathCountRangeQuery)query;
            ArrayList<AggregationGroup> groups = new ArrayList<>();
            groups.add(qu.path);
            MFCollectorSet collector = new MFCollectorSet(searcher, groups, false);
            BdLongSet[] sets = new BdLongSet[1];
            for(int i = 0; i < sets.length; i++) {
                sets[i] = new BdLongSet(1024);
                sets[i].enableClearBuffer();
            }
            
            long min = Long.MIN_VALUE;
            long max = Long.MAX_VALUE;
            if(qu.range.min != null) {
            	min = Long.parseLong(qu.range.min);
            	if(!qu.range.minInclusive) min++;
            }
            if(qu.range.max != null) {
            	max = Long.parseLong(qu.range.max);
            	if(qu.range.maxInclusive) max++;
            }
            
            for(int i = 0; i < r.size(); i++) {
                collector.collect(i, sets);
                int count = sets[0].size();
                if(min <= count && count < max) {
                    r.set(i);
                }
                sets[0].clear();
            }
            
		} else throw new IllegalArgumentException("Query " + query.getClass().getSimpleName() + " not supported");
		return r;
	}

}








