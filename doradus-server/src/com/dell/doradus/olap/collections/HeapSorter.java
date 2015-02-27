package com.dell.doradus.olap.collections;


public class HeapSorter {
	
	public static void sort(ILongComparer comparator, long[] buffer, int offset, int length) {
		// heapify the array
		for(int i = 1; i < length; i++) {
			upHeap(comparator, buffer, offset - 1, i + 1);
		}
		// sort heap
		for(int i = length - 1; i > 0; i--) {
			long node = buffer[offset + i];
			buffer[offset + i] = buffer[offset];
			buffer[offset] = node;
			downHeap(comparator, buffer, offset - 1, i);
		}
	}
	
	private static void upHeap(ILongComparer comparator, long[] buffer, int start, int index) {
        long node = buffer[start + index];
        int j = (index >> 1);
        while (j > 0 && comparator.compare(node, buffer[start + j]) > 0)
        {
        	// shift parent node down
        	buffer[start + index] = buffer[start + j];
            index = j;
            j = (index >> 1);
        }
        buffer[start + index] = node; // install saved node
		
	}
	
	private static void downHeap(ILongComparer comparator, long[] buffer, int start, int length) {
        int i = 1;
        long node = buffer[start + i]; // save top node
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
        buffer[start + i] = node; // install saved node
	}
	
}
