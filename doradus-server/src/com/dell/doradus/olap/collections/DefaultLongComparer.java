package com.dell.doradus.olap.collections;


public class DefaultLongComparer implements ILongComparer {
	public static DefaultLongComparer DEFAULT = new DefaultLongComparer();

	@Override public boolean isEqual(long x, long y) { return x == y; }

	@Override public int getHash(long x) { return (int)(x ^ (x >>> 32)); }

	@Override public int compare(long x, long y) { return x > y ? 1 : x < y ? -1 : 0; }

	@Override public boolean isEqual(Object o, long y) { return o.equals((Long)y); }
}
