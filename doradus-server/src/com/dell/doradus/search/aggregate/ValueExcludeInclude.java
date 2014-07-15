package com.dell.doradus.search.aggregate;

import java.util.Collection;

public class ValueExcludeInclude {

    private Collection<String> m_excludes;
    private Collection<String> m_includes;
	
	public ValueExcludeInclude(Collection<String> excludes, Collection<String> includes) {
        m_excludes = excludes;
        m_includes = includes;
	}
	
	public boolean accept(String value) {
		if (m_excludes != null && m_excludes.contains(value)) {
			return false;
		}
		if (m_includes != null && (!m_includes.contains(value))) {
			return false;
		}
		return true;
	}
}
