package com.dell.doradus.olap.xlink;

import java.util.HashMap;
import java.util.Map;

import com.dell.doradus.olap.aggregate.IMetricValue;
import com.dell.doradus.olap.io.BSTR;

public class XMetrics {
	public Map<BSTR, IMetricValue> metricsMap = new HashMap<BSTR, IMetricValue>();
}

