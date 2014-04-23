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

package com.dell.doradus.olap.aggregate.mr;

import com.dell.doradus.olap.io.BSTR;

public class MGName implements Comparable<MGName> {
	public String name;
	public BSTR id;
	
	public static MGName NullGroup = new MGName(null, new BSTR("")); 
	
	public MGName() { };
	public MGName(String name) {
		this.name = name;
		this.id = new BSTR(name);
	}
	public MGName(String name, BSTR id) {
		this.name = name;
		this.id = id;
	}
	public MGName(long value) {
		name = "" + value;
		id = new BSTR(value);
	}
	
	
	@Override public int compareTo(MGName o) {
		if(id == null && o.id == null) return 0;
		else if(id == null) return -1;
		else if(o.id == null) return 1;
		else return id.compareTo(o.id);
	}
	@Override public boolean equals(Object obj) {
		MGName other = (MGName) obj;
		if(id == null && other.id == null) return true;
		else if(id == null) return false;
		else if(other.id == null) return false;
		else return id.equals(other.id);
	}
	@Override public int hashCode() {
		return id == null ? 1 : id.hashCode();
	}
	
	@Override public String toString() {
		return name + " [" + id + "]";
	}
}
