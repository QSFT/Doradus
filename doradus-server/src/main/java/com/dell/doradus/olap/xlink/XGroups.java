package com.dell.doradus.olap.xlink;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.dell.doradus.olap.aggregate.mr.MGName;
import com.dell.doradus.olap.collections.BdLongSet;
import com.dell.doradus.olap.io.BSTR;

public class XGroups {
	public int maxValues;
	public Map<BSTR, BdLongSet> groupsMap = new HashMap<BSTR, BdLongSet>();
	public List<MGName> groupNames = new ArrayList<MGName>();
}

