package com.dell.doradus.olap.collections.strings;

import com.dell.doradus.olap.collections.ArrayOperations;
import com.dell.doradus.olap.collections.IIntComparer;
import com.dell.doradus.olap.collections.QuickSorter;
import com.dell.doradus.olap.io.BSTR;

public class BstrList implements IIntComparer {
	private byte[] m_buffer;
	private int m_bufferPosition;
	private int[] m_lengths;
	private int m_lengthPosition;
	private BSTR m_temp;
	
	public BstrList() {
		m_buffer = new byte[65536];
		m_lengths = new int[256];
		m_temp = new BSTR(1024);
	}
	
	public int add(BSTR value) {
		int index = m_lengthPosition / 2;
		int length = value.length;
		ensureCapacity(length);
		ArrayOperations.copy(value.buffer, 0, m_buffer, m_bufferPosition, length);
		m_lengths[m_lengthPosition++] = length;
		m_lengths[m_lengthPosition++] = m_bufferPosition;
		m_bufferPosition += length;
		return index;
	}

	public BSTR get(int index) {
		index *= 2;
		int xlen = m_lengths[index];
		int xpos = m_lengths[index + 1];
		m_temp.assertLength(xlen);
		ArrayOperations.copy(m_buffer, xpos, m_temp.buffer, 0, xlen);
		m_temp.length = xlen;
		return m_temp;
	}
	
	public int size() { return m_lengthPosition / 2; }
	
	public int[] sort() {
		int[] array = new int[size()];
		for(int i = 0; i < array.length; i++) array[i] = i;
		sort(array, array.length);
		return array;
	}
	
	public void sort(int[] array, int length) {
		QuickSorter.sort(this, array, 0, length);
	}
	
	private void ensureCapacity(int length) {
		while(m_bufferPosition + length > m_buffer.length) {
			m_buffer = ArrayOperations.realloc(m_buffer, m_buffer.length * 2);
		}
		if(m_lengthPosition == m_lengths.length) {
			m_lengths = ArrayOperations.realloc(m_lengths, m_lengths.length * 2);
		}
	}
	
	@Override public boolean isEqual(int x, int y) {
		x *= 2;
		y *= 2;
		int xlen = m_lengths[x];
		int ylen = m_lengths[y];
		int xpos = m_lengths[x + 1];
		int ypos = m_lengths[y + 1];
		return ArrayOperations.isEqual(m_buffer, xpos, xlen, m_buffer, ypos, ylen);
	}

	@Override public int getHash(int x) {
		x *= 2;
		int xlen = m_lengths[x];
		int xpos = m_lengths[x + 1];
		return ArrayOperations.getHash(m_buffer, xpos, xlen);
	}

	@Override public int compare(int x, int y) {
		x *= 2;
		y *= 2;
		int xlen = m_lengths[x];
		int ylen = m_lengths[y];
		int xpos = m_lengths[x + 1];
		int ypos = m_lengths[y + 1];
		return ArrayOperations.compare(m_buffer, xpos, xlen, m_buffer, ypos, ylen);
	}

	@Override public int getHash(Object o) {
		if(o instanceof BSTR) {
			BSTR str = (BSTR)o;
			return ArrayOperations.getHash(str.buffer, 0, str.length);
		}
		return o.hashCode();
	}
	
	@Override public boolean isEqual(Object o, int y) {
		y *= 2;
		int ylen = m_lengths[y];
		int ypos = m_lengths[y + 1];
		if(o instanceof BSTR) {
			BSTR str = (BSTR)o;
			return ArrayOperations.isEqual(str.buffer, 0, str.length, m_buffer, ypos, ylen);
		}
		return o.equals(get(y));
	}

}
