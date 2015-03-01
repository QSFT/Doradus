package com.dell.doradus.olap.collections;


public class DefaultIntComparer implements IIntComparer {
	public static DefaultIntComparer DEFAULT = new DefaultIntComparer();

	@Override public boolean isEqual(int x, int y) { return x == y; }

	@Override public int getHash(int x) { return x; }

	@Override public int compare(int x, int y) { return x > y ? 1 : x < y ? -1 : 0; }

	@Override public int getHash(Object o) { return getHash(((Integer)o).intValue()); }
	
	@Override public boolean isEqual(Object o, int y) { return o.equals((Integer)y); }
}
