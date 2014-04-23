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

package com.dell.doradus.search;

import java.util.ArrayList;
import java.util.TreeMap;

import com.dell.doradus.common.CommonDefs;
import com.dell.doradus.common.FieldDefinition;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.search.query.Query;

public class FieldSet {
	/**
	 * limits the number of 
	 */
	public int limit = -1;
	public Query filter = null;
	public TableDefinition tableDef;
	public ArrayList<String> ScalarFields = new ArrayList<String>();
	public TreeMap<String, FieldSet> LinkFields = new TreeMap<String, FieldSet>();
	
	public FieldSet(TableDefinition tableDef) {
		this.tableDef = tableDef;
	}
	
	public FieldSet(TableDefinition tableDef, String text) {
		text = tableDef.replaceAliaces(text);
		this.tableDef = tableDef;
		Set(text);
		Fixup();
	}
	
	public boolean IsOnlyID() {
		return LinkFields.size() == 0 && ScalarFields.size() == 1 && ScalarFields.get(0).equals(CommonDefs.ID_FIELD);
	}
	
	public void Set(String fields)
	{
		if(fields == null || fields.equals("") || fields.equals("*")) {
			ScalarFields.add("*");
			return;
		}
        FieldSetTokenizer tok = new FieldSetTokenizer(this, fields);
		tok.ProcessField();
	}
	
	public void Fixup() {
		if(ScalarFields.contains("*") && ScalarFields.size() > 1) {
			ScalarFields.clear();
			ScalarFields.add("*");
		}
		if(ScalarFields.contains("_all")) {
			ScalarFields.clear();
			ScalarFields.add("*");
			LinkFields.clear();
		    for(FieldDefinition fieldDef: tableDef.getFieldDefinitions()) {
		        if(!fieldDef.isLinkField()) continue;
		        LinkFields.put(fieldDef.getName(), new FieldSet(tableDef.getLinkExtentTableDef(fieldDef), "*"));
		    }
		}

        //NT 2012-08-06: return _ID values for all links
        if(ScalarFields.contains("_local")) {
            ScalarFields.clear();
            ScalarFields.add("*");
            LinkFields.clear();
            for(FieldDefinition fieldDef: tableDef.getFieldDefinitions()) {
                if(!fieldDef.isLinkField()) continue;
                LinkFields.put(fieldDef.getName(), new FieldSet(tableDef.getLinkExtentTableDef(fieldDef), CommonDefs.ID_FIELD));
            }
        }
        
		for(FieldSet linkSet: LinkFields.values()) {
			linkSet.Fixup();
		}
		
		ScalarFields.remove("_ID");
	}
	
	public void expand() {
		if(ScalarFields.contains("*")) {
			ScalarFields.clear();
			for(FieldDefinition fd : tableDef.getFieldDefinitions()) {
				if(!fd.isScalarField()) continue;
				ScalarFields.add(fd.getName());
			}
		}
		for(FieldSet fs : LinkFields.values()) {
			fs.expand();
		}
	}
}
