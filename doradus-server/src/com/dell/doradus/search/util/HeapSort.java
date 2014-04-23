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

package com.dell.doradus.search.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class HeapSort<T> implements Iterable<T> {
	
	@SuppressWarnings("unchecked")
	protected int compare(T x, T y) { return ((Comparable<T>)x).compareTo(y); }
	
	public class Entry implements Comparable<Entry>{
		public T key;
		public int position;
		
		public Entry(int position) {
			this.position = position;
		}
		
		@Override public int compareTo(Entry other) {
			if (key == other.key) return 0;
			else if (key == null) return -1;
			else if (other.key == null) return 1;
	        else return compare(other.key, key);
		}
	}
	
	class Iter implements Iterator<T> {
		private List<Iterator<T>> m_iterators; 
		private HeapList<Entry> m_heap;
		private Entry m_current;
		
		Iter() {
			m_iterators = new ArrayList<Iterator<T>>(m_iterables.size());
			m_heap = new HeapList<Entry>(m_iterables.size() - 1);
			for(int i=0; i<m_iterables.size(); i++) {
				Iterator<T> iterator = m_iterables.get(i).iterator();
				m_iterators.add(iterator);
				m_current = new Entry(i);
				moveNext();
			}
		}
		
		private void moveNext() {
			Iterator<T> iterator = m_iterators.get(m_current.position);
			if(iterator.hasNext()) m_current.key = iterator.next();
			else m_current.key = null;
			m_current = m_heap.AddEx(m_current);
		}
		
		@Override public boolean hasNext() { return m_current.key != null; }

		@Override
		public T next() {
			T key = m_current.key;
			moveNext();
			return key;
		}

		@Override public void remove() { throw new RuntimeException("remove not implemented"); }
		
	}
	
	private List<Iterable<T>> m_iterables = new ArrayList<Iterable<T>>();
	
	public HeapSort() {}
	
	public void add(Iterable<T> iterator) {
		m_iterables.add(iterator);
	}

	@Override public Iterator<T> iterator() {
		if(m_iterables.size() == 0) return new ArrayList<T>(0).iterator();
		else return new Iter();
	}
	
	
}
