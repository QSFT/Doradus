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

package com.dell.doradus.olap;

import java.util.Date;

import com.dell.doradus.common.UNode;

/**
 * Encapsulates the parameters that can be provided for an OLAP merge request.
 */
public class MergeOptions {
	// &expire-date parameter: set the date when this shard should be deleted. null means don't set
    private Date m_expireDate = null;
    // &timeout parameter: time to wait, in seconds, after merge and before deleting old segment, 
    // to allow current searches do their job
    private int m_timeout = 0;
    // &force-merge parameter: if true, then perform merge even if there is only one segment
    private boolean m_forceMerge = false;

    public Date getExpireDate() { return m_expireDate; }
    public int getTimeout() { return m_timeout; }
    public boolean getForceMerge() { return m_forceMerge; }
    
    public MergeOptions() {}
    
    public MergeOptions(Date expireDate, int timeout, boolean forceMerge) {
    	m_expireDate = expireDate;
    	m_timeout = timeout;
    	m_forceMerge = forceMerge;
    }
    
    public MergeOptions(UNode node) {
        assert node != null;
        ParsedQuery parsedQuery = new ParsedQuery("merge", node);
        m_expireDate = parsedQuery.getDate("expire-date");
        m_timeout = parsedQuery.getInt("timeout", 0);
        m_forceMerge = parsedQuery.getBoolean("force-merge", false);
        parsedQuery.checkInvalidParameters();
    }
    
    public MergeOptions(String queryParam) throws IllegalArgumentException {
        assert queryParam != null;
        ParsedQuery parsedQuery = new ParsedQuery(queryParam);
        m_expireDate = parsedQuery.getDate("expire-date");
        m_timeout = parsedQuery.getInt("timeout", 0);
        m_forceMerge = parsedQuery.getBoolean("force-merge", false);
        parsedQuery.checkInvalidParameters();
    }
    
}
