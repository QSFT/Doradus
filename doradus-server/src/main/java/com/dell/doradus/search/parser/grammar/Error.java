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

public class Error implements GrammarRule {

    protected String name;
    protected String message;

    public Error(String name) {
        this.name = name;
    }

    public Error(String name, String message) {
        this.name = name;
        this.message = message;
    }

    public Context Match(Context context) {
       if (message != null)
           throw new GrammarException(name, message, context);

        throw new GrammarException(name, context);
    }

    public String Name() {
        return name;
    }
}
