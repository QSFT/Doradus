package com.dell.doradus.search.aggregate;

import java.util.Collection;

public class ValueExcludeInclude {

    private Matcher m_exclude;
    private Matcher m_include;
	
	public ValueExcludeInclude(Collection<String> excludes, Collection<String> includes) {
		if (excludes != null) {
			m_exclude = new Matcher(excludes);
		}
        if (includes != null) {
        	m_include = new Matcher(includes);
        }
	}
	
	public boolean accept(String value) {
		value = value == null ? null : value.toLowerCase();
		if (m_exclude != null && m_exclude.match(value)) {
			return false;
		}
		if (m_include != null && (!m_include.match(value))) {
			return false;
		}
		return true;
	}
}
