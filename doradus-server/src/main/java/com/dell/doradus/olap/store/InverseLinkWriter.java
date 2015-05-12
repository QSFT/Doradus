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

package com.dell.doradus.olap.store;

import com.dell.doradus.common.FieldDefinition;
import com.dell.doradus.olap.io.VDirectory;
import com.dell.doradus.olap.io.VOutputStream;

public class InverseLinkWriter {

    public static boolean shouldWriteInverse(FieldDefinition link) {
        FieldDefinition inverse = link.getInverseLinkDef();
        String key1 = link.getTableName() + "/" + link.getName();
        String key2 = inverse.getTableName() + "/" + inverse.getName();
        return key1.compareTo(key2) > 0;
    }
    
    public static void writeInverse(VDirectory dir, FieldDefinition link, SegmentStats stats) {
        String table = link.getTableName();
        String field = link.getName();
        FieldDefinition inverse = link.getInverseLinkDef();
        String invTable = inverse.getTableName();
        String invField = inverse.getName();
        int docs = stats.getTable(table).documents;
        int fields = stats.getTable(invTable).documents;
        VOutputStream out_doc = dir.create(table + "." + field + ".inverse");
        out_doc.writeString(invTable);
        out_doc.writeString(invField);
        out_doc.writeVInt(docs);
        out_doc.writeVInt(fields);
        out_doc.close();
        stats.addInverseLinkField(link, fields);
    }
    

}
