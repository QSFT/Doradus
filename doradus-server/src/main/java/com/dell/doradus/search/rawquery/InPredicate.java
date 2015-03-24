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

package com.dell.doradus.search.rawquery;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * path IN (1, 2, 3, "AAA", 'BBB', ccc, NULL)   =>
 *    BinaryQuery(path, InPredicate(1,2,3,"AAA","BBB","ccc","NULL")
 *
 */
public class InPredicate implements RawPredicate {
	public LinkPath path;
	public List<String> values = new ArrayList<String>();
	
	public InPredicate() {}
	public InPredicate(LinkPath path) {
		this.path = path;
	}
	public InPredicate(LinkPath path, Collection<String> values) {
		this.path = path;
		this.values.addAll(values);
	}
	public InPredicate(LinkPath path, String... values) {
		this.path = path;
		for(String value: values) this.values.add(value);
	}
	
	@Override public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(path.toString());
		sb.append(" IN (");
		for(int i = 0; i < values.size(); i++) {
			if(i > 0) sb.append(", ");
			sb.append(Encoder.encode(values.get(i)));
		}
		sb.append(')');
		return sb.toString();
	}
}

