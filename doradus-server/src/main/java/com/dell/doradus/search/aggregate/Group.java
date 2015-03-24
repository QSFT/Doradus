/*
 * Copyright (C) 2014 Dell, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dell.doradus.search.aggregate;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.slf4j.Logger;

import com.dell.doradus.common.FieldType;
import com.dell.doradus.common.Utils;

abstract class Group {
	static Logger log = Aggregate.log;

	static final String SPECIAL_GROUP_NAME_PREFIX = "\u0000";   // CommonDefs.MV_SCALAR_SEP_CHAR;
	static final String NULL_GROUP_NAME = SPECIAL_GROUP_NAME_PREFIX + "(null)";
	static final String COMPOSITE_GROUP_NAME = SPECIAL_GROUP_NAME_PREFIX + "*";
	
	String m_key;
	HashMap<String, Group> m_subgroups;
	List<String> m_definedselection;

	Group (String key) {
		m_key = key;
	}

	abstract void update(String value);	
	abstract Object getMetric();
	
	Group subgroup(String key) {
		if (m_subgroups == null) {
			m_subgroups = new HashMap<String, Group>();
		}
		Group subgroup = m_subgroups.get(key);
		if (subgroup == null) {
			subgroup = createSubgroup(key);
			m_subgroups.put(key, subgroup);
		}
		return subgroup;
	}
	
	Group createSubgroup(String key) {
		try {
			return this.getClass().getDeclaredConstructor(String.class).newInstance(key);
		} catch (Exception e) {
			throw new RuntimeException("Can't create group instance",e);
		}
	}
	
    void defineGroupsSelection(List<String> groupkeys) {
        m_definedselection = groupkeys;
    }

	Collection<Group> subgroups(GroupOutputParameters outputParameter) {
		if (m_subgroups == null) {
			return null;
		}
		if (m_definedselection != null) {
		    List<Group> list = new ArrayList<Group>();
		    for (String groupkey : m_definedselection) {
		        Group selectedGroup = m_subgroups.get(groupkey);
		        if (selectedGroup != null)
		            list.add(selectedGroup);
		    }
		    return list;
		}
		return outputParameter.sortGroups(m_subgroups.values());
	}

	static Group getGroup(String function, FieldType ft) {
		if (function.equals("COUNT")) {
			return new CountGroup("");
		}
		else if (function.equals("DISTINCT")) {
			return new DistinctGroup("");
		}
		else if (function.equals("MIN")) {
			if (ft != null && (ft == FieldType.LONG || ft == FieldType.INTEGER)) {
				return new LongMinGroup("");
			}
			else if (ft != null && (ft == FieldType.FLOAT || ft == FieldType.DOUBLE)) {
				return new DoubleMinGroup("");
			}
			else {
				return new StringMinGroup("");
			}
		}
		else if (function.equals("MAX")) {
			if (ft != null && (ft == FieldType.LONG || ft == FieldType.INTEGER)) {
				return new LongMaxGroup("");
			}
			else if (ft != null && (ft == FieldType.FLOAT || ft == FieldType.DOUBLE)) {
				return new DoubleMaxGroup("");
			}
			else {
				return new StringMaxGroup("");
			}
		}
		else if (function.equals("AVERAGE")) {
			if (ft != null && (ft == FieldType.LONG || ft == FieldType.INTEGER)) {
				return new BigIntegerAverageGroup("");
			}
			else if (ft != null && (ft == FieldType.FLOAT || ft == FieldType.DOUBLE)) {
				return new DoubleAverageGroup("");
			}
			else if (ft != null && (ft == FieldType.TIMESTAMP)) {
				return new DateAverageGroup("");
			}
			else {
				throw new IllegalArgumentException("AVERAGE aggretate function is applicable to numeric fields only");				
			}
		}
		else if (function.equals("SUM")) {
			if (ft != null && (ft == FieldType.LONG || ft == FieldType.INTEGER)) {
				return new BigIntegerSumGroup("");
			}
			else if (ft != null && (ft == FieldType.FLOAT || ft == FieldType.DOUBLE)) {
				return new DoubleSumGroup("");
			}
			else {
				throw new IllegalArgumentException("SUM aggretate function is applicable to numeric fields only");				
			}
		}		
		throw new IllegalArgumentException("Unsupported aggretate function: " + function);
	}
	
	static int compare(Group group1, Group group2) {
		Object metric1 = group1.getMetric();
		Object metric2 = group2.getMetric();
		if (metric1 != null && metric2 != null) {
    		if (metric1 instanceof Long && metric2 instanceof Long) {
    			return ((Long)metric1).compareTo((Long)metric2);
    		}
            if (metric1 instanceof BigInteger && metric2 instanceof BigInteger) {
                return ((BigInteger)metric1).compareTo((BigInteger)metric2);
            }
            if (metric1 instanceof Double && metric2 instanceof Double) {
                return ((Double)metric1).compareTo((Double)metric2);
            }
   			return metric1.toString().compareTo(metric2.toString());
		} else {
		    if (metric1 != null) {
		        return 1;
		    }
            if (metric2 != null) {
                return -1;
            }
            return 0;
		}
	}
	
    public String getDisplayName() {
		if (!m_key.contains(SPECIAL_GROUP_NAME_PREFIX)) {
			return m_key;
		}
		return m_key.replace(SPECIAL_GROUP_NAME_PREFIX, "");
	}
    
    public String getMetricValue() {
        Object metric = getMetric();
        return metric == null ? "" : metric.toString();
    }
	
	public int compareName(Group anotherGroup) {
		return m_key.compareTo(anotherGroup.m_key);		
	}

	public boolean isComposite() {
		return m_key == COMPOSITE_GROUP_NAME;
	}
}

class NullGroup extends Group {

	NullGroup(String key) {
		super(key);
	}

	@Override
	void update(String value) {
	}

	@Override
	Object getMetric() {
		return null;
	}
}

class CountGroup extends Group {
	CountGroup(String key) {
		super(key);
	}
	private long m_count;
	@Override
	void update(String value) {
		m_count++;
	}
	@Override
	Object getMetric() {
		return m_count;
	}	
}

class DistinctGroup extends Group {
	DistinctGroup(String key) {
		super(key);
	}
	private HashSet<String> m_valueSet;
	@Override
	void update(String value) {
		if (m_valueSet == null) {
			m_valueSet = new HashSet<String>();
		}
		m_valueSet.add(value);
	}
	@Override
	Object getMetric() {
		if (m_valueSet == null) {
			return 0;
		}
		return m_valueSet.size();
	}	
}

abstract class MathGroup extends Group {
	MathGroup(String key) {
		super(key);
	}
	long m_count;
	abstract void doUpdate(String value);
	abstract Object getResult();
	@Override
	void update(String value) {
		try {
		    // TODO: RLG kludge?
		    if (value != null) {
		        doUpdate(value);
		        m_count++;
		    }
		}catch (Exception ex) {
			log.debug("Failed to update '%s' group metric value", getDisplayName(), ex);
		}
	}
	@Override
	Object getMetric() {
		if (m_count == 0) {
			return null;
		}
		return getResult();
	}	
}

class LongMinGroup extends MathGroup {
	LongMinGroup(String key) {
		super(key);
	}
	private long m_value;
	@Override
	void doUpdate(String value) {
		long longValue = Long.parseLong(value);
		if (m_count == 0 || longValue < m_value) {
			m_value = longValue;
		}
	}
	@Override
	Object getResult() {
		return new Long(m_value);
	}	
}

class DoubleMinGroup extends MathGroup{
	DoubleMinGroup(String key) {
		super(key);
	}
	private double m_value;
	@Override
	void doUpdate(String value) {
		double doubleValue = Double.parseDouble(value);
		if (m_count == 0 || doubleValue < m_value) {
			m_value = doubleValue;
		}
	}
	@Override
	Object getResult() {
		return new Double(m_value);
	}	
}

class StringMinGroup extends MathGroup {
	StringMinGroup(String key) {
		super(key);
	}
	private String m_value;
	@Override
	void doUpdate(String value) {
		if (value != null) {
			if (m_value == null || m_value.compareTo(value) > 0) {
				m_value = value;
			}
		}
	}
	@Override
	Object getResult() {
		return m_value;
	}	
}

class LongMaxGroup extends MathGroup {
	LongMaxGroup(String key) {
		super(key);
	}
	private long m_value;
	@Override
	void doUpdate(String value) {
		long longValue = Long.parseLong(value);
		if (m_count == 0 || longValue > m_value) {
			m_value = longValue;
		}
	}
	@Override
	Object getResult() {
		return new Long(m_value);
	}	
}

class DoubleMaxGroup extends MathGroup{
	DoubleMaxGroup(String key) {
		super(key);
	}
	private double m_value;
	@Override
	void doUpdate(String value) {
		double doubleValue = Double.parseDouble(value);
		if (m_count == 0 || doubleValue > m_value) {
			m_value = doubleValue;
		}
	}
	@Override
	Object getResult() {
		return new Double(m_value);
	}	
}

class StringMaxGroup extends MathGroup {
	StringMaxGroup(String key) {
		super(key);
	}
	private String m_value;
	@Override
	void doUpdate(String value) {
		if (value != null) {
			if (m_value == null || m_value.compareTo(value) < 0) {
				m_value = value;
			}
		}
	}
	@Override
	Object getResult() {
		return m_value;
	}	
}

abstract class AverageGroup extends MathGroup
{
	AverageGroup(String key) {
		super(key);
	}
	
	abstract Object getValue();
}

class LongAverageGroup extends AverageGroup {
	LongAverageGroup(String key) {
		super(key);
	}
	private long m_value;
	@Override
	void doUpdate(String value) {
		m_value += Long.parseLong(value);
	}
	@Override
	Object getResult() {
		return new Double(m_value/m_count);
	}
	@Override
	Object getValue() {
		return m_value;
	}	
}

class BigIntegerAverageGroup extends AverageGroup {
	BigIntegerAverageGroup(String key) {
		super(key);
	}
	private BigInteger m_value = BigInteger.ZERO;
	@Override
	void doUpdate(String value) {
		m_value = m_value.add(new BigInteger(value));
	}
	@Override
	Object getResult() {
		BigInteger result = m_value.divide(BigInteger.valueOf(m_count));
		int digits = result.abs().toString().length();  // Digits in integral part
		MathContext mc = new MathContext(digits + 3);	// Plus 3 digits for fractional part
		return new BigDecimal(m_value).divide(new BigDecimal(m_count), mc);
	}
	@Override
	Object getValue() {
		return m_value;
	}	
}

class DoubleAverageGroup extends AverageGroup {
	DoubleAverageGroup(String key) {
		super(key);
	}
	private double m_value;
	@Override
	void doUpdate(String value) {
		m_value += Double.parseDouble(value);
	}
	@Override
	Object getResult() {
		return new Double(m_value/m_count);
	}
	@Override
	Object getValue() {
		return m_value;
	}	
}

class DateAverageGroup extends AverageGroup {
	DateAverageGroup(String key) {
		super(key);
	}
	private long m_value;
	@Override
	void doUpdate(String value) {
		m_value += Utils.dateFromString(value).getTime();
	}
	@Override
	Object getResult() {
		return Utils.formatDateUTC(m_value/m_count);
	}
	@Override
	Object getValue() {
		return m_value;
	}	
}

class LongSumGroup extends MathGroup {
	private long m_value;
	LongSumGroup(String key) {
		super(key);
	}
	@Override
	void doUpdate(String value) {
		m_value += Long.parseLong(value);
	}
	@Override
	Object getResult() {
		return new Long(m_value);
	}	
}

class BigIntegerSumGroup extends MathGroup {
	private BigInteger m_value = BigInteger.ZERO;
	BigIntegerSumGroup(String key) {
		super(key);
	}
	@Override
	void doUpdate(String value) {
		m_value = m_value.add( new BigInteger(value));
	}
	@Override
	Object getResult() {
		return m_value;
	}	
}

class DoubleSumGroup extends MathGroup {
	private double m_value;
	DoubleSumGroup(String key) {
		super(key);
	}
	@Override
	void doUpdate(String value) {
		m_value += Double.parseDouble(value);
	}
	@Override
	Object getResult() {
		return new Double(m_value);
	}	
}
