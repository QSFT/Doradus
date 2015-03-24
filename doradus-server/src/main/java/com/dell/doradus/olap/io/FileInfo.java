package com.dell.doradus.olap.io;

public class FileInfo implements Comparable<FileInfo> {
	private String m_name;
	private boolean m_uncompressed;
	private boolean m_sharesRow;
	private long m_length;

	public FileInfo(String name) {
		m_name = name;
	}
	
	public FileInfo(String name, String info) {
		m_name = name;
		if(info.charAt(0) == 'u') {
			m_uncompressed = true;
			info = info.substring(1);
		}
		if(info.charAt(0) == 's') {
			m_sharesRow = true;
			info = info.substring(1);
		}
		m_length = Long.parseLong(info);
	}

	public String getName() { return m_name; }
	public long getLength() { return m_length; }
	public boolean getUncompressed() { return m_uncompressed; }
	public boolean isCompressed() { return !m_uncompressed; }
	public boolean getSharesRow() { return m_sharesRow; }
	
	public void setName(String name) { m_name = name; }
	public void setLength(long length) { m_length = length; }
	public void setUncompressed(boolean uncompressed) { m_uncompressed = uncompressed; }
	public void setSharesRow(boolean sharesRow) { m_sharesRow = sharesRow; }
	
	public String asString() {
		String result = String.format("%s%s%d", m_uncompressed ? "u" : "", m_sharesRow ? "s" : "", m_length);
		return result;
	}

	@Override public int compareTo(FileInfo other) {
		return m_name.compareTo(other.m_name);
	}
}