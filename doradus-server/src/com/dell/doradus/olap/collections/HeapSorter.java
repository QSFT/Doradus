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


public class HeapSorter {
	
	public static void sort(IIntComparer comparator, int[] buffer, int offset, int length) {
		// heapify the array
		for(int i = 1; i < length; i++) {
			upHeap(comparator, buffer, offset, i);
		}
		// sort heap
		for(int i = length - 1; i > 0; i--) {
			int node = buffer[offset + i];
			buffer[offset + i] = buffer[offset];
			buffer[offset] = node;
			downHeap(comparator, buffer, offset, i);
		}
	}
	
	
	private static void upHeap(IIntComparer comparator, int[] buffer, int start, int index) {
		start--; // use 1-based indexing
		index++;
        int node = buffer[start + index];
        int j = (index >> 1);
        while (j > 0 && comparator.compare(node, buffer[start + j]) > 0)
        {
        	buffer[start + index] = buffer[start + j]; // shift parent node down
            index = j;
            j = (index >> 1);
        }
        buffer[start + index] = node; // insert the node
		
	}
	
	private static void downHeap(IIntComparer comparator, int[] buffer, int start, int length) {
		start--; // use 1-based indexing
        int i = 1;
        int node = buffer[start + i];
        int j = i << 1; 
        int k = j + 1;
        // find smaller child
        if (k <= length && comparator.compare(buffer[start + k], buffer[start + j]) > 0) {
            j = k;
        }
        while (j <= length && comparator.compare(buffer[start + j], node) > 0) {
            buffer[start + i] = buffer[start + j]; // shift up child
            i = j;
            j = i << 1;
            k = j + 1;
            // find smaller child
            if (k <= length && comparator.compare(buffer[start + k], buffer[start + j]) > 0) {
                j = k;
            }
        }
        buffer[start + i] = node; // insert the node
	}
	
}
