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

package com.dell.doradus.olap.aggregate;

import java.util.List;

import com.dell.doradus.olap.search.Result;
import com.dell.doradus.olap.search.ResultBuilder;
import com.dell.doradus.olap.store.CubeSearcher;
import com.dell.doradus.olap.store.NumSearcherMV;
import com.dell.doradus.olap.xlink.DirectXLinkMetricCounter;
import com.dell.doradus.olap.xlink.InverseXLinkMetricCounter;
import com.dell.doradus.olap.xlink.XMetrics;
import com.dell.doradus.search.aggregate.AggregationGroupItem;
import com.dell.doradus.search.aggregate.AggregationMetric;
import com.dell.doradus.search.aggregate.BinaryExpression;
import com.dell.doradus.search.aggregate.LongIntegerExpression;
import com.dell.doradus.search.aggregate.MetricExpression;
import com.dell.doradus.search.aggregate.NumberExpression;

public class MetricCounterFactory {

	public static MetricCounterSet create(CubeSearcher searcher, List<MetricExpression> metrics) {
		MetricCounterSet counterSet = new MetricCounterSet();
		counterSet.counters = new MetricCounter[metrics.size()];
		for(int i = 0; i < metrics.size(); i++) {
			counterSet.counters[i] = create(searcher, metrics.get(i));
		}
		return counterSet;
	}

	public static MetricCounter create(CubeSearcher searcher, MetricExpression metric) {
		if(metric instanceof AggregationMetric) {
			return create(searcher, (AggregationMetric)metric);
		}
		else if(metric instanceof NumberExpression) {
			return new MetricCounterExpr.Constant();
		}
		else if(metric instanceof LongIntegerExpression) {
			return new MetricCounterExpr.Constant();
		}
		else if(metric instanceof BinaryExpression) {
			MetricCounterExpr.Binary b = new MetricCounterExpr.Binary();
			BinaryExpression be = (BinaryExpression)metric;
			b.first = create(searcher, be.first);
			b.second = create(searcher, be.second);
			return b;
		}
		else throw new IllegalArgumentException("Invalid expression type: " + metric.getClass().getName());
	}
	
	
	public static MetricCounter create(CubeSearcher searcher, AggregationMetric metric) {
		MetricCounter counter = null;
		if(metric.items == null || metric.items.size() == 0) counter = new MetricCounter.Count();
		else counter = create(searcher, metric, 0);
		if(metric.filter != null) {
			Result result = ResultBuilder.search(metric.tableDef, metric.filter, searcher);
			counter = new MetricCounter.FilteredCounter(result, counter);
		}
		return counter;
	}

	public static MetricCounter createPartial(CubeSearcher searcher, AggregationMetric metric, int start) {
		if(metric.items == null || metric.items.size() == 0) throw new RuntimeException("Count-star metric cannot be overlapped");
		if(metric.filter != null)  throw new RuntimeException("Metric with global filter cannot be overlapped");
		return create(searcher, metric, start);
	}
	
	private static MetricCounter create(CubeSearcher searcher, AggregationMetric metric, int index) {
		AggregationGroupItem item = metric.items.get(index);
		Result filter = null;
		if(item.query != null) {
			filter = ResultBuilder.search(item.tableDef, item.query, searcher);
		}
		if(index == metric.items.size() - 1) {
			if(NumSearcherMV.isNumericType(item.fieldDef.getType())) {
				if("MINCOUNT".equals(metric.function) || "MAXCOUNT".equals(metric.function)) {
					return new MetricCounter.NumCount(item.fieldDef, searcher);
				}
				else return new MetricCounter.Num(item.fieldDef, searcher);
			} else if(item.fieldDef.isXLinkDirect()) {
				return new DirectXLinkMetricCounter(searcher, item.fieldDef, (XMetrics)item.xlinkContext);
			} else if(item.fieldDef.isXLinkInverse()) {
				return new InverseXLinkMetricCounter(searcher, item.fieldDef, (XMetrics)item.xlinkContext);
			} else {
				if("MIN".equals(metric.function) || "MAX".equals(metric.function) || "DISTINCT".equals(metric.function)) {
					if(item.isTransitive) return new MetricCounterTransitive.TransitiveLinkValue(filter, item.fieldDef, item.transitiveDepth, searcher);
					else return new MetricCounter.FieldValue(filter, item.fieldDef, searcher);
				}
				if(item.isTransitive) return new MetricCounterTransitive.TransitiveLinkCount(filter, item.fieldDef, item.transitiveDepth, searcher);
				else return new MetricCounter.FieldCount(filter, item.fieldDef, searcher);
			}
		}
		else {
			MetricCounter inner = create(searcher, metric, index + 1);
			if(item.fieldDef.isLinkField()) {
				if(item.isTransitive) return new MetricCounterTransitive.TransitiveLink(filter, item.fieldDef, item.transitiveDepth, searcher, inner);
				else return new MetricCounter.Link(filter, item.fieldDef, searcher, inner);
			}else if(item.fieldDef.isXLinkDirect()) {
				return new DirectXLinkMetricCounter(searcher, item.fieldDef, (XMetrics)item.xlinkContext);
			}else if(item.fieldDef.isXLinkInverse()) {
				return new InverseXLinkMetricCounter(searcher, item.fieldDef, (XMetrics)item.xlinkContext);
			} else throw new IllegalArgumentException("Invalid field type in metrics: " + item.fieldDef.getType().toString());
		}
	}
	
}
