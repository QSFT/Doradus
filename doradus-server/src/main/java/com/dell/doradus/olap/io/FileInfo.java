package com.dell.doradus.olap.io;

public class FileInfo implements Comparable<FileInfo> {
	private String m_name;
	private boolean m_uncompressed;
	private boolean m_sharesRow;
	private long m_length;
	private boolean m_singleRow;
	private long m_compressedLength;

	public FileInfo(String name) {
		m_name = name;
	}
	
	public FileInfo(String name, String info) {
		m_name = name;
		if(info.charAt(0) == 'u') {
			info = info.substring(1);
			m_uncompressed = true;
		}
		if(info.charAt(0) == 's') {
			info = info.substring(1);
			m_sharesRow = true;
		}
		if(info.charAt(0) == 'r') {
			info = info.substring(1);
			m_singleRow = true;
		}
		if(info.charAt(0) == 'c') {
			info = info.substring(1);
			int idx = info.indexOf('c');
			String val = info.substring(0, idx);
			m_compressedLength = Long.parseLong(val);
			m_singleRow = true;
			info = info.substring(idx + 1);
		}
		m_length = Long.parseLong(info);
	}

	public String getName() { return m_name; }
	public long getLength() { return m_length; }
	public boolean getUncompressed() { return m_uncompressed; }
	public boolean isCompressed() { return !m_uncompressed; }
	public boolean getSharesRow() { return m_sharesRow; }
	public boolean getSingleRow() { return m_singleRow; }
	public long getCompressedLength() { return m_compressedLength; }
	
	public void setName(String name) { m_name = name; }
	public void setLength(long length) { m_length = length; }
	public void setUncompressed(boolean uncompressed) { m_uncompressed = uncompressed; }
	public void setSharesRow(boolean sharesRow) { m_sharesRow = sharesRow; }
	public void setSingleRow(boolean singleRow) { m_singleRow = singleRow; }
	public void setCompressedLength(long compressedLength) { m_compressedLength = compressedLength; }
	
	public String asString() {
		StringBuilder sb = new StringBuilder(16);
		if(m_uncompressed) sb.append('u');
		if(m_sharesRow) sb.append('s');
		if(m_singleRow) sb.append('r');
		if(m_compressedLength > 0) {
			sb.append('c');
			sb.append(m_compressedLength);
			sb.append('c');
		}
		sb.append(m_length);
		String result = sb.toString();
		return result;
	}

	@Override public int compareTo(FileInfo other) {
		return m_name.compareTo(other.m_name);
	}
}