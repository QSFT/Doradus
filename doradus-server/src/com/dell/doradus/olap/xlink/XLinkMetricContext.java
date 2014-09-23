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

package com.dell.doradus.olap.xlink;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.dell.doradus.common.FieldDefinition;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.olap.aggregate.IMetricCollector;
import com.dell.doradus.olap.aggregate.IMetricValue;
import com.dell.doradus.olap.aggregate.MetricCollectorFactory;
import com.dell.doradus.olap.aggregate.MetricCounter;
import com.dell.doradus.olap.aggregate.MetricCounterFactory;
import com.dell.doradus.olap.io.BSTR;
import com.dell.doradus.olap.search.Result;
import com.dell.doradus.olap.search.ResultBuilder;
import com.dell.doradus.olap.store.CubeSearcher;
import com.dell.doradus.olap.store.FieldSearcher;
import com.dell.doradus.olap.store.IdSearcher;
import com.dell.doradus.olap.store.IntIterator;
import com.dell.doradus.olap.store.ValueSearcher;
import com.dell.doradus.search.aggregate.AggregationGroupItem;
import com.dell.doradus.search.aggregate.AggregationMetric;
import com.dell.doradus.search.aggregate.BinaryExpression;
import com.dell.doradus.search.aggregate.LongIntegerExpression;
import com.dell.doradus.search.aggregate.MetricExpression;
import com.dell.doradus.search.aggregate.NumberExpression;
import com.dell.doradus.search.query.AllQuery;
import com.dell.doradus.search.query.Query;

// class representing structures needed during the search/aggregate, if external links are present 
public class XLinkMetricContext {
	
	public static class XMetrics {
		public Map<BSTR, IMetricValue> metricsMap = new HashMap<BSTR, IMetricValue>();
	}
	
	public XLinkContext context;
	
	public XLinkMetricContext(XLinkContext context) {
		this.context = context;
	}

	
	public void setupXLinkMetric(List<MetricExpression> metrics) {
		for(MetricExpression metric : metrics) {
			setupXLinkMetric(metric);
		}
	}

	public void setupXLinkMetric(MetricExpression metric) {
		if(metric instanceof AggregationMetric) {
			setupXLinkMetric((AggregationMetric)metric);
		}
		else if(metric instanceof NumberExpression) {
		}
		else if(metric instanceof LongIntegerExpression) {
		}
		else if(metric instanceof BinaryExpression) {
			BinaryExpression be = (BinaryExpression)metric;
			setupXLinkMetric(be.first);
			setupXLinkMetric(be.second);
		}
		else throw new IllegalArgumentException("Invalid expression type: " + metric.getClass().getName());
	}

	public void setupXLinkMetric(AggregationMetric metric) {
		if(metric.filter != null) context.setupXLinkQuery(metric.tableDef, metric.filter);
		List<AggregationGroupItem> items = metric.items;
		TableDefinition tableDef = metric.tableDef;
		if(items == null) return;
		for(int i = items.size() - 1; i >= 0; i--) {
			AggregationGroupItem item = items.get(i);
			if(item.query != null) context.setupXLinkQuery(item.tableDef, item.query);
			if(!item.fieldDef.isXLinkField()) continue;
			metric.items = new ArrayList<AggregationGroupItem>();
			for(int j = i + 1; j < items.size(); j++) {
				metric.items.add(items.get(j));
			}
			metric.tableDef = item.tableDef;
			XMetrics xmetrics = new XMetrics();
			if(item.fieldDef.isXLinkDirect()) setupDirect(xmetrics, item.fieldDef, metric, item.query);
			else setupInverse(xmetrics, item.fieldDef, metric, item.query);
			item.xlinkContext = xmetrics;
			// restore the group and table definition
			metric.items = items;
			metric.tableDef = tableDef;
		}
		
	}
	
	private void setupDirect(XMetrics xmetrics, FieldDefinition fieldDef, AggregationMetric metric, Query filter) {
		if(filter == null) filter = new AllQuery();
		TableDefinition invTable = fieldDef.getInverseTableDef();
		for(String xshard : context.xshards) {
			CubeSearcher searcher = context.olap.getSearcher(context.application, xshard);
			Result bvQuery = ResultBuilder.search(invTable, filter, searcher);
			MetricCounter metricCounter = MetricCounterFactory.create(searcher, metric);
			IMetricCollector metricCollector = MetricCollectorFactory.create(searcher, metric);
			IdSearcher ids = searcher.getIdSearcher(invTable.getTableName());
			int docsCount = ids.size();
			for(int doc = 0; doc < docsCount; doc++) {
				if(!bvQuery.get(doc)) continue;
				IMetricValue value = metricCollector.get();
				metricCounter.add(doc, value);
				xmetrics.metricsMap.put(new BSTR(ids.getId(doc)), value);
			}
		}
	}

	private void setupInverse(XMetrics xmetrics, FieldDefinition fieldDef, AggregationMetric metric, Query filter) {
		if(filter == null) filter = new AllQuery();
		TableDefinition invTable = fieldDef.getInverseTableDef();
		FieldDefinition inv = fieldDef.getInverseLinkDef();
		for(String xshard : context.xshards) {
			CubeSearcher searcher = context.olap.getSearcher(context.application, xshard);
			Result bvQuery = ResultBuilder.search(invTable, filter, searcher);
			MetricCounter metricCounter = MetricCounterFactory.create(searcher, metric);
			IMetricCollector metricCollector = MetricCollectorFactory.create(searcher, metric);
			FieldSearcher fs = searcher.getFieldSearcher(inv.getTableName(), inv.getXLinkJunction());
			IntIterator iter = new IntIterator();
			int docsCount = fs.size();
			IMetricValue[] vals = new IMetricValue[fs.fields()];
			for(int doc = 0; doc < docsCount; doc++) {
				if(!bvQuery.get(doc)) continue;
				IMetricValue value = metricCollector.get();
				metricCounter.add(doc, value);
				fs.fields(doc, iter);
				for(int i = 0; i < iter.count(); i++) {
					int val = iter.get(i);
					if(vals[val] == null) vals[val] = metricCollector.get();
					vals[val].add(value);
				}
			}
			
			ValueSearcher vs = searcher.getValueSearcher(invTable.getTableName(), inv.getXLinkJunction());
			for(int v = 0; v < vs.size(); v++) {
				if(vals[v] == null) continue;
				BSTR id = vs.getValue(v);
				IMetricValue metricValue = xmetrics.metricsMap.get(id);
				if(metricValue == null) xmetrics.metricsMap.put(new BSTR(id), vals[v]);
				else metricValue.add(vals[v]);
			}
			
		}
	}
	
}
