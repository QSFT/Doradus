package com.dell.doradus.olap.xlink;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.dell.doradus.olap.aggregate.mr.MGName;
import com.dell.doradus.olap.collections.BdLongSet;
import com.dell.doradus.olap.io.BSTR;

public class XGroups {
	public Map<BSTR, BdLongSet> groupsMap = new HashMap<BSTR, BdLongSet>();
	public List<MGName> groupNames = new ArrayList<MGName>();
	
	public static void mergeGroups(List<XGroups> groups) {
	    if(groups.size() <= 1) return;
	    
	    Map<MGName, Integer> namesMap = new HashMap<MGName, Integer>();
        List<MGName> groupNames = new ArrayList<MGName>();
	    
	    for(int i = 0; i < groups.size(); i++) {
	        XGroups group = groups.get(i);
	        int count = group.groupNames.size();
	        int[] remap = new int[count];
	        for(int j = 0; j < count; j++) {
	            MGName name = group.groupNames.get(j);
	            Integer num = namesMap.get(name);
	            if(num == null) {
	                num = new Integer(groupNames.size());
	                groupNames.add(name);
	                namesMap.put(name, num);
	            }
	            remap[j] = num.intValue();
	        }
	        for(Map.Entry<BSTR, BdLongSet> entry: group.groupsMap.entrySet()) {
	            BdLongSet origSet = entry.getValue();
	            BdLongSet remappedSet = new BdLongSet(origSet.size());
	            for(int j = 0; j < origSet.size(); j++) {
	                long origVal = origSet.get(j);
	                remappedSet.add(remap[(int)origVal]);
	            }
	            entry.setValue(remappedSet);
	        }
	        group.groupNames = groupNames;
	    }
	}
}

