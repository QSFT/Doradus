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

package com.dell.doradus.search.parser.grammar;

public class Literal implements GrammarItem {
    String value;
    String type;
    int ptr;

    public Literal(String value, String type, int ptr) {
        this.value = value;
        this.type = type;
        this.ptr =ptr;
    }

    @Override
    public String getType() {
        return type;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getValue() {
        return value;
    }

    public int getPtr() {
        return ptr;
    }

    public String toString() {
      if (value != null)
          return value;
        return "";
    }
}



