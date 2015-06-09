package com.dell.doradus.logservice.search;

import java.util.ArrayList;

import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.common.Utils;
import com.dell.doradus.search.aggregate.AggregationGroup;

public class Aggregate {

       public static String getAggregateField(TableDefinition tableDef, String fields) {
           if(fields == null) return null;
           ArrayList<ArrayList<AggregationGroup>> groupsSet = AggregationGroup.GetAggregationList(fields, tableDef);
           Utils.require(groupsSet.size() == 1, "Multiple group sets are not supported");
           ArrayList<AggregationGroup> groups = groupsSet.get(0);
           Utils.require(groupsSet.size() == 1, "Multiple groups are not supported");
           AggregationGroup group = groups.get(0);
           Utils.require(group.items.size() == 1, "links not supported");
           Utils.require(group.batch == null, "batch not supported");
           Utils.require(group.batchexAliases == null, "batchex not supported");
           Utils.require(group.batchexFilters == null, "batchex not supported");
           Utils.require(group.exclude == null, "exclude not supported");
           Utils.require(group.filter == null, "filter not supported");
           Utils.require(group.include == null, "include not supported");
           return group.items.get(0).name;
       }
}
