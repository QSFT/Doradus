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
	
	private static Map<String, Class<? extends IMetricValue>> m_map = new HashMap<>();
	
	static {
		// metric: COUNT, SUM, MIN, MAX, AVG, DISTINCT
		// f_type: LINK, TEXT, BOOLEAN, INTEGER, LONG, TIMESTAMP, FLOAT, DOUBLE
		
		m_map.put("COUNT/LINK", MetricValueSum.class);
		m_map.put("COUNT/XLINK", MetricValueSum.class);
		m_map.put("COUNT/TEXT", MetricValueSum.class);
		m_map.put("COUNT/BOOLEAN", MetricValueCount.class);
		m_map.put("COUNT/INTEGER", MetricValueCount.class);
		m_map.put("COUNT/LONG", MetricValueCount.class);
		m_map.put("COUNT/TIMESTAMP", MetricValueCount.class);
		m_map.put("COUNT/FLOAT", MetricValueCount.class);
		m_map.put("COUNT/DOUBLE", MetricValueCount.class);

		m_map.put("SUM/LINK", MetricValueSum.class);
		m_map.put("SUM/XLINK", MetricValueSum.class);
		m_map.put("SUM/TEXT", MetricValueSum.class);
		m_map.put("SUM/BOOLEAN", MetricValueSum.class);
		m_map.put("SUM/INTEGER", MetricValueSum.class);
		m_map.put("SUM/LONG", MetricValueSum.class);
		m_map.put("SUM/TIMESTAMP", MetricValueSum.class);
		m_map.put("SUM/FLOAT", MetricValueFloat.Sum.class);
		m_map.put("SUM/DOUBLE", MetricValueDouble.Sum.class);

		m_map.put("MIN/LINK", MetricValueText.MinLink.class);
		m_map.put("MIN/TEXT", MetricValueText.MinText.class);
		m_map.put("MIN/BOOLEAN", MetricValueMin.MinBoolean.class);
		m_map.put("MIN/INTEGER", MetricValueMin.MinNum.class);
		m_map.put("MIN/LONG", MetricValueMin.MinNum.class);
		m_map.put("MIN/TIMESTAMP", MetricValueMin.MinDate.class);
		m_map.put("MIN/FLOAT", MetricValueFloat.Min.class);
		m_map.put("MIN/DOUBLE", MetricValueDouble.Min.class);
		
		m_map.put("MAX/LINK", MetricValueText.MaxLink.class);
		m_map.put("MAX/TEXT", MetricValueText.MaxText.class);
		m_map.put("MAX/BOOLEAN", MetricValueMax.MaxBoolean.class);
		m_map.put("MAX/INTEGER", MetricValueMax.MaxNum.class);
		m_map.put("MAX/LONG", MetricValueMax.MaxNum.class);
		m_map.put("MAX/TIMESTAMP", MetricValueMax.MaxDate.class);
		m_map.put("MAX/FLOAT", MetricValueFloat.Max.class);
		m_map.put("MAX/DOUBLE", MetricValueDouble.Max.class);

		m_map.put("MINCOUNT/LINK", MetricValueMin.MinNum.class);
		m_map.put("MINCOUNT/XLINK", MetricValueMin.MinNum.class);
		m_map.put("MINCOUNT/TEXT", MetricValueMin.MinNum.class);
		m_map.put("MINCOUNT/BOOLEAN", MetricValueMin.MinNum.class);
		m_map.put("MINCOUNT/INTEGER", MetricValueMin.MinNum.class);
		m_map.put("MINCOUNT/LONG", MetricValueMin.MinNum.class);
		m_map.put("MINCOUNT/TIMESTAMP", MetricValueMin.MinNum.class);
		m_map.put("MINCOUNT/FLOAT", MetricValueMin.MinNum.class);
		m_map.put("MINCOUNT/DOUBLE", MetricValueMin.MinNum.class);
		
		m_map.put("MAXCOUNT/LINK", MetricValueMax.MaxNum.class);
		m_map.put("MAXCOUNT/XLINK", MetricValueMax.MaxNum.class);
		m_map.put("MAXCOUNT/TEXT", MetricValueMax.MaxNum.class);
		m_map.put("MAXCOUNT/BOOLEAN", MetricValueMax.MaxNum.class);
		m_map.put("MAXCOUNT/INTEGER", MetricValueMax.MaxNum.class);
		m_map.put("MAXCOUNT/LONG", MetricValueMax.MaxNum.class);
		m_map.put("MAXCOUNT/TIMESTAMP", MetricValueMax.MaxNum.class);
		m_map.put("MAXCOUNT/FLOAT", MetricValueMax.MaxNum.class);
		m_map.put("MAXCOUNT/DOUBLE", MetricValueMax.MaxNum.class);
		
		m_map.put("AVERAGE/LINK", MetricValueAvg.AvgNum.class);
		m_map.put("AVERAGE/XLINK", MetricValueAvg.AvgNum.class);
		m_map.put("AVERAGE/TEXT", MetricValueAvg.AvgNum.class);
		m_map.put("AVERAGE/BOOLEAN", MetricValueAvg.AvgNum.class);
		m_map.put("AVERAGE/INTEGER", MetricValueAvg.AvgNum.class);
		m_map.put("AVERAGE/LONG", MetricValueAvg.AvgNum.class);
		m_map.put("AVERAGE/TIMESTAMP", MetricValueAvg.AvgDate.class);
		m_map.put("AVERAGE/FLOAT", MetricValueFloat.Avg.class);
		m_map.put("AVERAGE/DOUBLE", MetricValueDouble.Avg.class);

		m_map.put("DISTINCT/LINK", MetricValueDistinct.Id.class);
		//m_map.put("DISTINCT/XLINK", MetricValueAvg.AvgNum.class);
		m_map.put("DISTINCT/TEXT", MetricValueDistinct.Text.class);
		m_map.put("DISTINCT/BOOLEAN", MetricValueDistinct.class);
		m_map.put("DISTINCT/INTEGER", MetricValueDistinct.class);
		m_map.put("DISTINCT/LONG", MetricValueDistinct.class);
		m_map.put("DISTINCT/TIMESTAMP", MetricValueDistinct.class);
		m_map.put("DISTINCT/FLOAT", MetricValueDistinct.class);
		m_map.put("DISTINCT/DOUBLE", MetricValueDistinct.class);
		
	}

	public static MetricCollectorSet create(CubeSearcher searcher, List<MetricExpression> metrics) {
		MetricCollectorSet collectorSet = new MetricCollectorSet();
		collectorSet.collectors = new MetricCollector[metrics.size()];
		for(int i = 0; i < metrics.size(); i++) {
			collectorSet.collectors[i] = create(searcher, metrics.get(i));
		}
		return collectorSet;
	}
	
	public static MetricCollector create(CubeSearcher searcher, MetricExpression metric) {
		if(metric instanceof AggregationMetric) {
			return create(searcher, (AggregationMetric)metric);
		}
		else if(metric instanceof NumberExpression) {
			MetricValueExpr.Constant b = new MetricValueExpr.Constant(((NumberExpression)metric).value);
			return new MetricCollector(b, searcher, null);
		}
		else if(metric instanceof LongIntegerExpression) {
			MetricValueExpr.Constant b = new MetricValueExpr.Constant((double)((LongIntegerExpression)metric).value);
			return new MetricCollector(b, searcher, null);
		}
		else if(metric instanceof BinaryExpression) {
			BinaryExpression be = (BinaryExpression)metric;
			MetricValueExpr.Binary b = new MetricValueExpr.Binary();
			b.operator = be.operation;
			b.first = create(searcher, be.first).get();
			b.second = create(searcher, be.second).get();
			return new MetricCollector(b, searcher, null);
		}
		else throw new IllegalArgumentException("Invalid expression type: " + metric.getClass().getName());
	}
	
	public static MetricCollector create(CubeSearcher searcher, AggregationMetric metric) {
		FieldDefinition fieldDef = null;
		if(metric.items != null && metric.items.size() > 0) {
			fieldDef = metric.items.get(metric.items.size() - 1).fieldDef;
		}
		return create(searcher, metric.function, fieldDef);
	}
	
	public static MetricCollector create(CubeSearcher searcher, String metricFunction, FieldDefinition fieldDef) {
		Utils.require(metricFunction != null, "Undefined metrics function");
		metricFunction = metricFunction.toUpperCase();
		if(fieldDef == null) {
			if("COUNT".equals(metricFunction)) return new MetricCollector(new MetricValueSum(), searcher, fieldDef);
			else throw new IllegalArgumentException(metricFunction + "(*) is not allowed");
		}
		
		Class<? extends IMetricValue> clazz = null;
		String key = metricFunction + "/" + fieldDef.getType().toString(); 
		synchronized(m_map) {
			clazz = m_map.get(key);
		}
		if(clazz == null) throw new IllegalArgumentException("Unsupported combination " + key);
		try {
			IMetricValue value = clazz.newInstance();
			return new MetricCollector(value, searcher, fieldDef);
		} catch (Exception e) { throw new RuntimeException(e.getMessage(), e); }
	}
	
}




