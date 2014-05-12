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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.dell.doradus.common.FieldDefinition;
import com.dell.doradus.common.Utils;
import com.dell.doradus.olap.store.CubeSearcher;
import com.dell.doradus.search.aggregate.AggregationMetric;
import com.dell.doradus.search.aggregate.BinaryExpression;
import com.dell.doradus.search.aggregate.LongIntegerExpression;
import com.dell.doradus.search.aggregate.MetricExpression;
import com.dell.doradus.search.aggregate.NumberExpression;

public class MetricCollectorFactory {
	
	private static Map<String, Class<? extends IMetricCollector>> m_map = new HashMap<String, Class<? extends IMetricCollector>>();
	
	static {
		// metric: COUNT, SUM, MIN, MAX, AVG
		// f_type: LINK, TEXT, BOOLEAN, INTEGER, LONG, TIMESTAMP
		
		m_map.put("COUNT/LINK", MetricCollectorSum.class);
		m_map.put("COUNT/XLINK", MetricCollectorSum.class);
		m_map.put("COUNT/TEXT", MetricCollectorSum.class);
		m_map.put("COUNT/BOOLEAN", MetricCollectorCount.class);
		m_map.put("COUNT/INTEGER", MetricCollectorCount.class);
		m_map.put("COUNT/LONG", MetricCollectorCount.class);
		m_map.put("COUNT/TIMESTAMP", MetricCollectorCount.class);
		m_map.put("COUNT/FLOAT", MetricCollectorCount.class);
		m_map.put("COUNT/DOUBLE", MetricCollectorCount.class);

		m_map.put("SUM/LINK", MetricCollectorSum.class);
		m_map.put("SUM/XLINK", MetricCollectorSum.class);
		m_map.put("SUM/TEXT", MetricCollectorSum.class);
		m_map.put("SUM/BOOLEAN", MetricCollectorSum.class);
		m_map.put("SUM/INTEGER", MetricCollectorSum.class);
		m_map.put("SUM/LONG", MetricCollectorSum.class);
		m_map.put("SUM/TIMESTAMP", MetricCollectorSum.class);
		m_map.put("SUM/FLOAT", MetricCollectorFloat.Sum.class);
		m_map.put("SUM/DOUBLE", MetricCollectorDouble.Sum.class);

		m_map.put("MIN/LINK", MetricCollectorText.MinLink.class);
		m_map.put("MIN/TEXT", MetricCollectorText.Min.class);
		m_map.put("MIN/BOOLEAN", MetricCollectorMin.MinBoolean.class);
		m_map.put("MIN/INTEGER", MetricCollectorMin.MinNum.class);
		m_map.put("MIN/LONG", MetricCollectorMin.MinNum.class);
		m_map.put("MIN/TIMESTAMP", MetricCollectorMin.MinDate.class);
		m_map.put("MIN/FLOAT", MetricCollectorFloat.Min.class);
		m_map.put("MIN/DOUBLE", MetricCollectorDouble.Min.class);
		
		m_map.put("MAX/LINK", MetricCollectorText.MaxLink.class);
		m_map.put("MAX/TEXT", MetricCollectorText.Max.class);
		m_map.put("MAX/BOOLEAN", MetricCollectorMax.MaxBoolean.class);
		m_map.put("MAX/INTEGER", MetricCollectorMax.MaxNum.class);
		m_map.put("MAX/LONG", MetricCollectorMax.MaxNum.class);
		m_map.put("MAX/TIMESTAMP", MetricCollectorMax.MaxDate.class);
		m_map.put("MAX/FLOAT", MetricCollectorFloat.Max.class);
		m_map.put("MAX/DOUBLE", MetricCollectorDouble.Max.class);

		m_map.put("MINCOUNT/LINK", MetricCollectorMin.MinNum.class);
		m_map.put("MINCOUNT/XLINK", MetricCollectorMin.MinNum.class);
		m_map.put("MINCOUNT/TEXT", MetricCollectorMin.MinNum.class);
		
		m_map.put("MAXCOUNT/LINK", MetricCollectorMax.MaxNum.class);
		m_map.put("MAXCOUNT/XLINK", MetricCollectorMax.MaxNum.class);
		m_map.put("MAXCOUNT/TEXT", MetricCollectorMax.MaxNum.class);
		
		m_map.put("AVERAGE/LINK", MetricCollectorAvg.AvgNum.class);
		m_map.put("AVERAGE/XLINK", MetricCollectorAvg.AvgNum.class);
		m_map.put("AVERAGE/TEXT", MetricCollectorAvg.AvgNum.class);
		m_map.put("AVERAGE/BOOLEAN", MetricCollectorAvg.AvgNum.class);
		m_map.put("AVERAGE/INTEGER", MetricCollectorAvg.AvgNum.class);
		m_map.put("AVERAGE/LONG", MetricCollectorAvg.AvgNum.class);
		m_map.put("AVERAGE/TIMESTAMP", MetricCollectorAvg.AvgDate.class);
		m_map.put("AVERAGE/FLOAT", MetricCollectorFloat.Avg.class);
		m_map.put("AVERAGE/DOUBLE", MetricCollectorDouble.Avg.class);
		
	}

	public static MetricCollectorSet create(CubeSearcher searcher, List<MetricExpression> metrics) {
		MetricCollectorSet collectorSet = new MetricCollectorSet();
		collectorSet.collectors = new IMetricCollector[metrics.size()];
		for(int i = 0; i < metrics.size(); i++) {
			collectorSet.collectors[i] = create(searcher, metrics.get(i));
		}
		return collectorSet;
	}
	
	public static IMetricCollector create(CubeSearcher searcher, MetricExpression metric) {
		if(metric instanceof AggregationMetric) {
			return create(searcher, (AggregationMetric)metric);
		}
		else if(metric instanceof NumberExpression) {
			MetricCollectorExpr.Constant b = new MetricCollectorExpr.Constant();
			b.value = ((NumberExpression)metric).value;
			return b;
		}
		else if(metric instanceof LongIntegerExpression) {
			MetricCollectorExpr.Constant b = new MetricCollectorExpr.Constant();
			b.value = (double)((LongIntegerExpression)metric).value;
			return b;
		}
		else if(metric instanceof BinaryExpression) {
			BinaryExpression be = (BinaryExpression)metric;
			MetricCollectorExpr.Binary b = new MetricCollectorExpr.Binary();
			b.operation = be.operation;
			b.first = create(searcher, be.first);
			b.second = create(searcher, be.second);
			return b;
		}
		else throw new IllegalArgumentException("Invalid expression type: " + metric.getClass().getName());
	}
	
	/*
	public static MetricCollectorSet create(CubeSearcher searcher, List<AggregationMetric> metrics) {
		MetricCollectorSet collectorSet = new MetricCollectorSet();
		collectorSet.collectors = new IMetricCollector[metrics.size()];
		for(int i = 0; i < metrics.size(); i++) {
			collectorSet.collectors[i] = create(searcher, metrics.get(i));
		}
		return collectorSet;
	}
	*/
	
	public static IMetricCollector create(CubeSearcher searcher, AggregationMetric metric) {
		FieldDefinition fieldDef = null;
		if(metric.items != null && metric.items.size() > 0) {
			fieldDef = metric.items.get(metric.items.size() - 1).fieldDef;
		}
		return create(searcher, metric.function, fieldDef);
	}
	
	public static IMetricCollector create(CubeSearcher searcher, String metricFunction, FieldDefinition fieldDef) {
		Utils.require(metricFunction != null, "Undefined metrics function");
		metricFunction = metricFunction.toUpperCase();
		if(fieldDef == null) {
			if(metricFunction == "COUNT") return new MetricCollectorSum();
			else throw new IllegalArgumentException(metricFunction + "(*) is not allowed");
		}
		
		Class<? extends IMetricCollector> clazz = null;
		String key = metricFunction + "/" + fieldDef.getType().toString(); 
		synchronized(m_map) {
			clazz = m_map.get(key);
		}
		if(clazz == null) throw new IllegalArgumentException("Unsupported combination " + key);
		try {
			IMetricCollector collector = clazz.newInstance();
			if(collector instanceof MetricCollectorText) {
				((MetricCollectorText)collector).searcher = searcher;
				((MetricCollectorText)collector).fieldDef = fieldDef;
			}
			return collector;
		} catch (Exception e) { throw new RuntimeException(e.getMessage(), e); }
	}
	
}




