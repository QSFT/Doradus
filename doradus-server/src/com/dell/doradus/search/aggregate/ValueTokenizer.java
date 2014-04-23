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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import com.dell.doradus.fieldanalyzer.SimpleTextAnalyzer;

public interface ValueTokenizer {

    public Collection<String> tokenize(String value);
}

class TextTokenizer implements ValueTokenizer {

    private Collection<String> m_excludes;

    TextTokenizer() {
        this(null);
    }

    TextTokenizer(Collection<String> excludes) {
        m_excludes = excludes;
    }

    @Override
    public Collection<String> tokenize(String value) {
        if (value == null) {
            return null;
        }
        List<String> tokens = Arrays.asList(SimpleTextAnalyzer.instance().tokenize(value));
        if (m_excludes != null && m_excludes.size() != 0) {
            tokens = new ArrayList<String>(tokens);
            tokens.removeAll(m_excludes);
        }
        return tokens;
    }
}
