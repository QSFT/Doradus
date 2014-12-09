package com.dell.doradus.olap.aggregate.mr;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.dell.doradus.olap.aggregate.AggregationRequest;
import com.dell.doradus.olap.aggregate.AggregationResult;
import com.dell.doradus.olap.aggregate.MetricValueSet;
import com.dell.doradus.olap.aggregate.mr.AggregationCollector.Group;
import com.dell.doradus.search.aggregate.AggregationGroup;
import com.dell.doradus.search.util.HeapList;

public class AggregationResultBuilder {
	
	public static AggregationResult build(AggregationRequest request, AggregationCollector collector) {
		AggregationResult result = build(request, collector.getGroup(), 0);
		if(result == null) result = new AggregationResult();
		result.documentsCount = collector.documentsCount();
		if(collector.getGroup() == null) return result;
		result.groupsCount = collector.getGroup().groups().size();
		result.summary = new AggregationResult.AggregationGroup();
		
		AggregationCollector.Group summary = collector.getGroup();
		result.summary.id = summary.getKey();
		result.summary.name = summary.getKey().name;
		result.summary.metricSet = summary.getValue();
		
		if(result.groupsCount == 0 && request.parts[0].groups.size() == 0) {
			result.groupsCount = 1;
			AggregationResult.AggregationGroup grp = new AggregationResult.AggregationGroup();
			grp.id = null;
			grp.name = "*";
			grp.metricSet = result.summary.metricSet;
			result.groups.add(grp);
		}
		return result;
	}
	
	private static AggregationResult build(AggregationRequest request, AggregationCollector.Group group, int level) {
		if(level == request.parts[0].groups.size()) return null;
		AggregationResult result = new AggregationResult();
		AggregationGroup requestGroup = request.parts[0].groups.get(level);
		if(group == null) return result;
		Collection<AggregationCollector.Group> groups = group.groups();
		List<AggregationCollector.Group> grps;
		Comparator<AggregationCollector.Group> comparer = null;
		switch(requestGroup.selection) {
		case None: {
			comparer = new Comparator<AggregationCollector.Group>() {
				@Override public int compare(Group x, Group y) {
					return x.getKey().compareTo(y.getKey());
				}};
			break;
		} case Top: {
			comparer = new Comparator<AggregationCollector.Group>() {
				@Override public int compare(Group x, Group y) {
					MetricValueSet valueX = x.getValue();
					MetricValueSet valueY = y.getValue();
					boolean specialX = valueX.isDegenerate();
					boolean specialY = valueY.isDegenerate();
					int c = valueY.compareTo(valueX);
					if(c == 0) return x.getKey().compareTo(y.getKey());
					if(specialX && specialY) return c;
					if(!specialX && !specialY) return c;
					return specialX ? 1 : -1;
				}};
			break;
		} case Bottom: {
			comparer = new Comparator<AggregationCollector.Group>() {
				@Override public int compare(Group x, Group y) {
					MetricValueSet valueX = x.getValue();
					MetricValueSet valueY = y.getValue();
					boolean specialX = valueX.isDegenerate();
					boolean specialY = valueY.isDegenerate();
					int c = valueX.compareTo(valueY);
					if(c == 0) return x.getKey().compareTo(y.getKey());
					if(specialX && specialY) return c;
					if(!specialX && !specialY) return c;
					return specialX ? 1 : -1;
				}};
			break;
		} case First: {
			comparer = new Comparator<AggregationCollector.Group>() {
				@Override public int compare(Group x, Group y) {
					return x.getKey().compareTo(y.getKey());
				}};
			break;
		} case Last: {
			comparer = new Comparator<AggregationCollector.Group>() {
				@Override public int compare(Group x, Group y) {
					return y.getKey().compareTo(x.getKey());
				}};
			break;
		} default: throw new RuntimeException("Unknown comparer: " + requestGroup.selection);
		}
		
		if(requestGroup.selectionValue == 0 || requestGroup.selectionValue > groups.size()) {
			grps = new ArrayList<AggregationCollector.Group>(groups);
			Collections.sort(grps, comparer);
		} else {
			HeapList<AggregationCollector.Group> heap =
					new HeapList<AggregationCollector.Group>(requestGroup.selectionValue, comparer);
			for(AggregationCollector.Group g: groups) heap.Add(g);
			grps = heap.values();
		}

		for(AggregationCollector.Group g: grps) {
			AggregationResult.AggregationGroup agroup = new AggregationResult.AggregationGroup();
			agroup.id = g.getKey();
			agroup.name = g.getKey().name;
			agroup.metricSet = g.getValue();
			agroup.innerResult = build(request, g, level + 1);
			result.groups.add(agroup);
		}
		
		return result;
	}
}
