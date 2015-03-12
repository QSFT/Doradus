/*
 * Copyright (C) 2015 Dell, Inc.
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

package com.dell.doradus.olap.collections;


public class DefaultIntComparer implements IIntComparer {
	public static DefaultIntComparer DEFAULT = new DefaultIntComparer();

	@Override public boolean isEqual(int x, int y) { return x == y; }

	@Override public int getHash(int x) { return x; }

	@Override public int compare(int x, int y) { return x > y ? 1 : x < y ? -1 : 0; }

	@Override public int getHash(Object o) { return getHash(((Integer)o).intValue()); }
	
	@Override public boolean isEqual(Object o, int y) { return o.equals((Integer)y); }
}
