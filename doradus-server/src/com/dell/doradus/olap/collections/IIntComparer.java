package com.dell.doradus.olap.collections;

public interface IIntComparer {
	public boolean isEqual(int x, int y);
	public int getHash(int x);
	public int compare(int x, int y);
	public int getHash(Object o);
	public boolean isEqual(Object o, int y);
}
