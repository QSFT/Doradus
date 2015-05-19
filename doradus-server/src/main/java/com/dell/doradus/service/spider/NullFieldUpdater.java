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

package com.dell.doradus.service.spider;

import com.dell.doradus.common.DBObject;

/**
 * Provides a {@link FieldUpdater} that performs no updates for a given field. This allows
 * batch updates to contain but skip system fields (e.g., "_table") that do not contribute
 * any data to the database. All update methods silently return. 
 *
 */
public class NullFieldUpdater extends FieldUpdater {

    public NullFieldUpdater(ObjectUpdater objUpdater, DBObject dbObj, String fieldName) {
        super(objUpdater, dbObj, fieldName);
    }

    @Override
    public void addValuesForField() {
    }

    @Override
    public boolean updateValuesForField(String currentValue) {
        return false;
    }

    @Override
    public void deleteValuesForField() {
    }

}   // class NullFieldUpdater
