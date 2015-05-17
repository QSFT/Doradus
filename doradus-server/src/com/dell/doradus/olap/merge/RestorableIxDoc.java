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

package com.dell.doradus.olap.merge;

import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.olap.Olap;
import com.dell.doradus.olap.io.BSTR;
import com.dell.doradus.olap.io.FileDeletedException;
import com.dell.doradus.olap.store.IdReader;
import com.dell.doradus.service.olap.OLAPService;

public class RestorableIxDoc implements Comparable<RestorableIxDoc> {
    private Olap m_olap;
    private String m_shard;
    private TableDefinition m_tableDef;
    public int segment;
	public IdReader reader;
    public BSTR id;

    public RestorableIxDoc(int segment, TableDefinition tableDef, String shard)
    {
        m_olap = OLAPService.instance().getOlap();
        m_shard = shard;
        m_tableDef = tableDef;
        this.segment = segment;
        this.reader = new IdReader(m_olap.getDirectory(m_tableDef.getAppDef(), m_shard), m_tableDef.getTableName());
    }

    public void next() {
        try {
            nextInternal();
        } catch(FileDeletedException e) {
            this.reader = new IdReader(m_olap.getDirectory(m_tableDef.getAppDef(), m_shard), m_tableDef.getTableName());
            scanToNext();
        }
    }
    
    private void nextInternal() {
        if(reader.next()) id = reader.cur_id;
        else id = null;
    }
    
    private void scanToNext() {
        if(id == null) {
            if(reader.next()) id = reader.cur_id;
            else id = null;
            return;
        }
        while(reader.next()) {
            if(reader.cur_id.compareTo(id) > 0) {
                id = reader.cur_id;
                return;
            }
        }
        id = null;
        return;
    }
    
	@Override
	public int compareTo(RestorableIxDoc other) {
		if (id == other.id) return 0;
		else if (id == null) return -1;
		else if (other.id == null) return 1;
        int c = other.id.compareTo(id);
        if(c != 0) return c;
        else return other.segment - segment;
	}

}
