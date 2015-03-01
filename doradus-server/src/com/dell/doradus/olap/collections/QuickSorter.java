package com.dell.doradus.olap.collections;


public class QuickSorter {
	
	public static void sort(IIntComparer comparator, int[] buffer, int offset, int length) {
		if(length <= 1) return;
		if(length <= 5) {
			insertionSort(comparator, buffer, offset, length);
			return;
		}
		
		int[] probe = new int[5];
		quickSort(probe, comparator, buffer, offset, length, 0);
		
	}
	private static void quickSort(int[] probe, IIntComparer comparator, int[] buffer, int offset, int length, int depth) {
		if(length <= 1) return;
		
		if(length <= 8) {
			insertionSort(comparator, buffer, offset, length);
			return;
		}

		if(depth >= 20) {
			HeapSorter.sort(comparator, buffer, offset, length);
			return;
		}

		
		int start = offset;
		int end = offset + length;
		int start_low = start;
		int start_hi = end;
		
		// select median
		probe[0] = buffer[start];
		probe[1] = buffer[start + length / 4];
		probe[2] = buffer[start + length / 2];
		probe[3] = buffer[start + 3 * length / 4];
		probe[4] = buffer[end - 1];
		insertionSort(comparator, probe, 0, 5);
		int median = probe[2];
		
		while(true) {
			while(start_low < start_hi && comparator.compare(buffer[start_low], median) <= 0) start_low++;
			while(start_low < start_hi && comparator.compare(buffer[start_hi - 1], median) >= 0) start_hi--;
			if(start_low == start_hi) {
				while(start_low > start && comparator.compare(buffer[start_low - 1], median) == 0) start_low--;
				while(start_hi < end && comparator.compare(buffer[start_hi], median) == 0) start_hi++;
				break;
			}
			swap(buffer, start_low, start_hi - 1);
			start_low++;
			start_hi--;
		}

		quickSort(probe, comparator, buffer, start, start_low - start, depth + 1);
		quickSort(probe, comparator, buffer, start_hi, end - start_hi, depth + 1);
	}
	
	
	private static void insertionSort(IIntComparer comparator, int[] buffer, int offset, int length) {
		int end = offset + length;
		for(int i = offset + 1; i < end; i++) {
			int pos = i;
			while(pos > offset) {
				int c = comparator.compare(buffer[pos], buffer[pos - 1]);
				if(c < 0) swap(buffer, pos, pos - 1);
				pos--;
			}
		}
	}
	
	private static void swap(int[] buffer, int i, int j) {
		int x = buffer[i];
		buffer[i] = buffer[j];
		buffer[j] = x;
	}
	
}
