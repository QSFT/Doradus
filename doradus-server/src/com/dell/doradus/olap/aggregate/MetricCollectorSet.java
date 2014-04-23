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

public class MetricCollectorSet {
	public IMetricCollector[] collectors;
	public MetricValueSet nullGroup;
	public MetricValueSet summaryGroup;
	public int lastDoc;
	public int lastCountedDoc;
	public int documentsCount;
	public int[] documents;
	
	public void setSize(int size) {
		if(documents != null) {
			if(documents.length != size) throw new IllegalArgumentException("Metrics cannot be applied to different fields");
			return;
		}
		lastDoc = -1;
		lastCountedDoc = -1;
		documents = new int[size];
		for(int i = 0; i < size; i++) {
			documents[i] = -1;
		}
		
		for(int i = 0; i < collectors.length; i++) {
			collectors[i].setSize(size);
		}
	}

	// should be called after all possible calls add(doc, index, valueSet) to ensure a doc is counted
	// against docsCount and nullGroup if necessary 
	public void add(int doc, MetricValueSet valueSet) {
		if(lastCountedDoc != doc) {
			if(nullGroup == null) nullGroup = get(-1);
			nullGroup.add(valueSet);
			lastCountedDoc = doc;
		}
		if(lastDoc != doc) {
			if(summaryGroup == null) summaryGroup = get(-1);
			summaryGroup.add(valueSet);
			lastDoc = doc;
			documentsCount++;
		}
	}
	
	
	public void add(int doc, int index, MetricValueSet valueSet) {
		//if(index < 0) return;
		if(documents[index] == doc) return;
		documents[index] = doc;
		lastCountedDoc = doc;
		
		for(int i = 0; i < collectors.length; i++) {
			collectors[i].add(index, valueSet.values[i]);
		}
	}
	
	public MetricValueSet get(int index) {
		MetricValueSet valueSet = new MetricValueSet();
		valueSet.values = new IMetricValue[collectors.length];
		for(int i = 0; i < collectors.length; i++) {
			valueSet.values[i] = collectors[i].get(index);
		}
		return valueSet;
	}
	
	public void convert(MetricValueSet set) {
		for(int i = 0; i < set.values.length; i++) set.values[i] = collectors[i].convert(set.values[i]);
	}
}
