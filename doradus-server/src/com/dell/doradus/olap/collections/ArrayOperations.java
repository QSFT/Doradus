package com.dell.doradus.olap.collections;

public class ArrayOperations {
	
	///////////// LONG ////////////////
	
	public static int getHash(long x) {
		return (int)(x ^ (x >>> 32));
	}
	
	///////////// BYTES ////////////////
	
	public static void copy(byte[] src, int srcOffset, byte[] dst, int dstOffset, int length) {
		System.arraycopy(src, srcOffset, dst, dstOffset, length);
	}
	
	public static byte[] realloc(byte[] buffer, int newlength) {
		byte[] newbuffer = new byte[newlength];
		if(buffer != null && buffer.length > 0) copy(buffer, 0, newbuffer, 0, buffer.length);
		return newbuffer;
	}
	
	public static int compare(byte[] src, int srcOffset, int srcLength, byte[] dst, int dstOffset, int dstLength) {
		int length = srcLength < dstLength ? srcLength : dstLength;
		int c = compare(src, srcOffset, dst, dstOffset, length);
		if(c != 0) return c;
		else return srcLength < dstLength ? -1 : srcLength > dstLength ? 1 : 0;
	}

	public static int compare(byte[] src, int srcOffset, byte[] dst, int dstOffset, int length) {
		for(int i = 0; i < length; i++) {
			int x = (int)(char)src[srcOffset + i];
			int y = (int)(char)dst[dstOffset + i];
			if(x < y) return -1;
			if(x > y) return 1;
		}
		return 0;
	}

	public static boolean isEqual(byte[] src, int srcOffset, int srcLength, byte[] dst, int dstOffset, int dstLength) {
		if(srcLength != dstLength) return false;
		return isEqual(src, srcOffset, dst, dstOffset, srcLength);
	}
	
	public static boolean isEqual(byte[] src, int srcOffset, byte[] dst, int dstOffset, int length) {
		for(int i = 0; i < length; i++) {
			if(src[srcOffset + i] != dst[dstOffset + i]) return false;
		}
		return true;
	}
	
	public static int getHash(byte[] src, int srcOffset, int length) {
		int h = length;
		for(int i = 0; i < length; i++) {
			h *= 31;
			h += src[srcOffset + i];
		}
		return h;
	}
	

	///////////// CHARS ////////////////
	
	public static void copy(String str, char[] buffer, int offset) {
		str.getChars(0, str.length(), buffer, offset);
	}
	
	public static void copy(char[] src, int srcOffset, char[] dst, int dstOffset, int length) {
		System.arraycopy(src, srcOffset, dst, dstOffset, length);
	}
	
	public static char[] realloc(char[] buffer, int newlength) {
		char[] newbuffer = new char[newlength];
		if(buffer != null && buffer.length > 0) copy(buffer, 0, newbuffer, 0, buffer.length);
		return newbuffer;
	}
	
	public static int compare(char[] src, int srcOffset, int srcLength, char[] dst, int dstOffset, int dstLength) {
		int length = srcLength < dstLength ? srcLength : dstLength;
		int c = compare(src, srcOffset, dst, dstOffset, length);
		if(c != 0) return c;
		else return srcLength < dstLength ? -1 : srcLength > dstLength ? 1 : 0;
	}
	
	public static int compare(char[] src, int srcOffset, char[] dst, int dstOffset, int length) {
		for(int i = 0; i < length; i++) {
			char x = src[srcOffset + i];
			char y = dst[dstOffset + i];
			if(x < y) return -1;
			if(x > y) return 1;
		}
		return 0;
	}

	public static boolean isEqual(char[] src, int srcOffset, int srcLength, char[] dst, int dstOffset, int dstLength) {
		if(srcLength != dstLength) return false;
		return isEqual(src, srcOffset, dst, dstOffset, srcLength);
	}
	
	public static boolean isEqual(char[] src, int srcOffset, char[] dst, int dstOffset, int length) {
		for(int i = 0; i < length; i++) {
			if(src[srcOffset + i] != dst[dstOffset + i]) return false;
		}
		return true;
	}
	
	public static int getHash(char[] src, int srcOffset, int length) {
		int h = length;
		for(int i = 0; i < length; i++) {
			h *= 31;
			h += src[srcOffset + i];
		}
		return h;
	}
	

	///////////// INTS ////////////////
	
	public static void copy(int[] src, int srcOffset, int[] dst, int dstOffset, int length) {
		System.arraycopy(src, srcOffset, dst, dstOffset, length);
	}

	public static int[] realloc(int[] buffer, int newlength) {
		int[] newbuffer = new int[newlength];
		if(buffer != null && buffer.length > 0) copy(buffer, 0, newbuffer, 0, buffer.length);
		return newbuffer;
	}
	
	public static int compare(int[] src, int srcOffset, int srcLength, int[] dst, int dstOffset, int dstLength) {
		int length = srcLength < dstLength ? srcLength : dstLength;
		int c = compare(src, srcOffset, dst, dstOffset, length);
		if(c != 0) return c;
		else return srcLength < dstLength ? -1 : srcLength > dstLength ? 1 : 0;
	}
	
	public static int compare(int[] src, int srcOffset, int[] dst, int dstOffset, int length) {
		for(int i = 0; i < length; i++) {
			int x = src[srcOffset + i];
			int y = dst[dstOffset + i];
			if(x < y) return -1;
			if(x > y) return 1;
		}
		return 0;
	}

	public static boolean isEqual(int[] src, int srcOffset, int srcLength, int[] dst, int dstOffset, int dstLength) {
		if(srcLength != dstLength) return false;
		return isEqual(src, srcOffset, dst, dstOffset, srcLength);
	}
	
	public static boolean isEqual(int[] src, int srcOffset, int[] dst, int dstOffset, int length) {
		for(int i = 0; i < length; i++) {
			if(src[srcOffset + i] != dst[dstOffset + i]) return false;
		}
		return true;
	}
	
	public static int getHash(int[] src, int srcOffset, int length) {
		int h = length;
		for(int i = 0; i < length; i++) {
			h *= 31;
			h += src[srcOffset + i];
		}
		return h;
	}
	
	///////////// LONGS ////////////////
	
	public static void copy(long[] src, int srcOffset, long[] dst, int dstOffset, int length) {
		System.arraycopy(src, srcOffset, dst, dstOffset, length);
	}

	public static long[] realloc(long[] buffer, int newlength) {
		long[] newbuffer = new long[newlength];
		if(buffer != null && buffer.length > 0) copy(buffer, 0, newbuffer, 0, buffer.length);
		return newbuffer;
	}
	
	public static int compare(long[] src, int srcOffset, int srcLength, long[] dst, int dstOffset, int dstLength) {
		int length = srcLength < dstLength ? srcLength : dstLength;
		int c = compare(src, srcOffset, dst, dstOffset, length);
		if(c != 0) return c;
		else return srcLength < dstLength ? -1 : srcLength > dstLength ? 1 : 0;
	}
	
	public static int compare(long[] src, int srcOffset, long[] dst, int dstOffset, int length) {
		for(int i = 0; i < length; i++) {
			long x = src[srcOffset + i];
			long y = dst[dstOffset + i];
			if(x < y) return -1;
			if(x > y) return 1;
		}
		return 0;
	}

	public static boolean isEqual(long[] src, int srcOffset, int srcLength, long[] dst, int dstOffset, int dstLength) {
		if(srcLength != dstLength) return false;
		return isEqual(src, srcOffset, dst, dstOffset, srcLength);
	}
	
	public static boolean isEqual(long[] src, int srcOffset, long[] dst, int dstOffset, int length) {
		for(int i = 0; i < length; i++) {
			if(src[srcOffset + i] != dst[dstOffset + i]) return false;
		}
		return true;
	}
	
	public static int getHash(long[] src, int srcOffset, int length) {
		int h = length;
		for(int i = 0; i < length; i++) {
			h *= 31;
			long l = src[srcOffset + i];
			h += (int)(l ^ (l >>> 32));
		}
		return h;
	}

}
