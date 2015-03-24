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

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;


/**
 * Heap list implementation
 */
@SuppressWarnings("unchecked")
public class HeapList<T> {
    private T[] m_Array;
    private int m_Count;
    private int m_Capacity;
    private Comparator<T> m_Comparator;
    private boolean m_bInverse;

    public HeapList(int capacity)
    {
    	this(capacity, null);
    }
    
    public void setInverse(boolean inverse) {
    	m_bInverse = inverse;
    }
    
    //Create a heap list with the specified capacity and the object comparator
    //If comparator is null then the objects are considered to implement Comparable<T>
    public HeapList(int capacity, Comparator<T> comparator) {
        m_Capacity = capacity;
        m_Array = (T[])new Object[m_Capacity +  1];
        m_Comparator = comparator;
    }

	protected boolean greaterThan(T v1, T v2)
    {
		boolean c;
    	if(m_Comparator != null) c = m_Comparator.compare(v1, v2) > 0;
    	else c = ((Comparable<T>)v1).compareTo(v2) > 0;
    	return m_bInverse ? !c : c;
    }

    //returns the count of items that are in the list.
	//Can be lower than or equal than the capacity
    public int getCount() {
        return m_Count;
    }
    //returns the capacity of the list
    public int getCapacity() {
        return m_Capacity;
    }
    //adds a value into the list.
    //If the list has reached its capacity then the greatest item will be "thrown away" from the list
    //returns true if the value has been added 
    public boolean Add(T value) {
        if (m_Count < m_Capacity) {
            m_Array[++m_Count] = value;
            UpHeap();
        }
        else if (greaterThan(m_Array[1], value)) {
            m_Array[1] = value;
            DownHeap();
        }
        else return false;
        return true;
    }

    /// same as Add(value), but returns the item that was thrown away from the heap,
    /// thus allowing to reuse it, without creating new classes
    /// If no object was thrown (heap was not yes populated) return null
	public T AddEx(T value) {
        if (m_Count < m_Capacity) {
            m_Array[++m_Count] = value;
            UpHeap();
            return null;
        }
        else if (m_Capacity == 0) return value;
        else if (greaterThan(m_Array[1], value)) {
            T retVal = m_Array[1];
            m_Array[1] = value;
            DownHeap();
            return retVal;
        }
        else return value;
    }

	public List<T> values() {
    	List<T> array = new ArrayList<T>(m_Count);
    	for(int i = 0; i < m_Count; i++) { array.add(m_Array[i + 1]); }
    	if(m_Comparator != null)Collections.sort(array, m_Comparator);
    	else Collections.sort(array, new Comparator<T>(){
			@Override public int compare(T x, T y) { return ((Comparable<T>)x).compareTo(y); }}); 
        return array;
	}
	
    public T[] GetValues(Class<T> c) {
    	T[] array = (T[])Array.newInstance(c, m_Count);
    	for(int i=0; i<m_Count; i++) {
    		array[i] = m_Array[i + 1];
    	}
    	if(m_Comparator != null)Arrays.sort(array, m_Comparator);
    	else Arrays.sort(array);
        return array;
    }

    private void UpHeap() {
        int i = m_Count;
        T node = m_Array[i];
        int j = (i >> 1);
        while (j > 0 && greaterThan(node, m_Array[j]))
        {
            m_Array[i] = m_Array[j]; // shift parents down
            i = j;
            j = (i >> 1);
        }
        m_Array[i] = node; // install saved node
    }

    private void DownHeap() {
        int i = 1;
        T node = m_Array[i]; // save top node
        int j = i << 1; // find smaller child
        int k = j + 1;
        if (k <= m_Count && greaterThan(m_Array[k], m_Array[j])) {
            j = k;
        }
        while (j <= m_Count && greaterThan(m_Array[j], node)) {
            m_Array[i] = m_Array[j]; // shift up child
            i = j;
            j = i << 1;
            k = j + 1;
            if (k <= m_Count && greaterThan(m_Array[k], m_Array[j])) {
                j = k;
            }
        }
        m_Array[i] = node; // install saved node
    }


}
