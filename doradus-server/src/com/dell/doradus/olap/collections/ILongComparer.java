package com.dell.doradus.olap.collections;

public interface ILongComparer {
	public boolean isEqual(long x, long y);
	public int getHash(long x);
	public int compare(long x, long y);
	public boolean isEqual(Object o, long y);
}
