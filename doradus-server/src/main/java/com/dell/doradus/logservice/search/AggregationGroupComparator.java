package com.dell.doradus.logservice.search;

import java.util.Comparator;

import com.dell.doradus.olap.aggregate.AggregationResult;
import com.dell.doradus.olap.aggregate.MetricValueSet;
import com.dell.doradus.search.aggregate.AggregationGroup;

public class AggregationGroupComparator {
    
    public static Comparator<AggregationResult.AggregationGroup> getComparator(AggregationGroup group) {
        Comparator<AggregationResult.AggregationGroup> comparer = null;
        switch(group.selection) {
        case Top: {
            comparer = new Comparator<AggregationResult.AggregationGroup>() {
                @SuppressWarnings("unchecked")
                @Override public int compare(AggregationResult.AggregationGroup x, AggregationResult.AggregationGroup y) {
                    MetricValueSet valueX = x.metricSet;
                    MetricValueSet valueY = y.metricSet;
                    boolean specialX = valueX.isDegenerate();
                    boolean specialY = valueY.isDegenerate();
                    int c = valueY.compareTo(valueX);
                    if(c == 0) return ((Comparable<Object>)x.id).compareTo(y.id);
                    if(specialX && specialY) return c;
                    if(!specialX && !specialY) return c;
                    return specialX ? 1 : -1;
                }};
            break;
        } case Bottom: {
            comparer = new Comparator<AggregationResult.AggregationGroup>() {
                @SuppressWarnings("unchecked")
                @Override public int compare(AggregationResult.AggregationGroup x, AggregationResult.AggregationGroup y) {
                    MetricValueSet valueX = x.metricSet;
                    MetricValueSet valueY = y.metricSet;
                    boolean specialX = valueX.isDegenerate();
                    boolean specialY = valueY.isDegenerate();
                    int c = valueX.compareTo(valueY);
                    if(c == 0) return ((Comparable<Object>)x.id).compareTo(y.id);
                    if(specialX && specialY) return c;
                    if(!specialX && !specialY) return c;
                    return specialX ? 1 : -1;
                }};
            break;
        } case First: case None: {
            comparer = new Comparator<AggregationResult.AggregationGroup>() {
                @SuppressWarnings("unchecked")
                @Override public int compare(AggregationResult.AggregationGroup x, AggregationResult.AggregationGroup y) {
                    return ((Comparable<Object>)x.id).compareTo(y.id);
                }};
            break;
        } case Last: {
            comparer = new Comparator<AggregationResult.AggregationGroup>() {
                @SuppressWarnings("unchecked")
                @Override public int compare(AggregationResult.AggregationGroup x, AggregationResult.AggregationGroup y) {
                    return ((Comparable<Object>)y.id).compareTo(x.id);
                }};
            break;
        } default: throw new RuntimeException("Unknown comparer: " + group.selection);
        }
        return comparer;
    }

}
