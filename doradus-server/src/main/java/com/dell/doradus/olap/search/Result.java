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

package com.dell.doradus.olap.search;

import com.dell.doradus.olap.store.BitVector;
import com.dell.doradus.olap.store.IntIterator;
import com.dell.doradus.olap.store.IntList;

public class Result {
	private BitVector m_bv;
	
	public Result(int size) {
		m_bv = new BitVector(size);
	}

	public Result(Result other) {
		m_bv = new BitVector(other.m_bv.size());
		System.arraycopy(other.m_bv.getBuffer(), 0, m_bv.getBuffer(), 0, m_bv.getBuffer().length);
	}
	
	public int size() { return m_bv.size(); }
	public BitVector getBitVector() { return m_bv; }
	
	public int countSet() {
		return m_bv.bitsSet();
	}
	public boolean get(int index) { return m_bv.get(index); }
	public void set(int index) { m_bv.set(index); }
	public void clear(int index) { m_bv.clear(index); }
	public void clear() { m_bv.clearAll(); }
	
	public void not() { m_bv.not(); }
	
	public void or(Result r2) { m_bv.or(r2.m_bv); }
	public void and(Result r2) { m_bv.and(r2.m_bv); }
	public void andNot(Result r2) { m_bv.andNot(r2.m_bv); }
	
	public IntIterator iterate() {
		IntList array = m_bv.getList();
		IntIterator iter = new IntIterator();
		array.set(iter);
		return iter;
	}
}
