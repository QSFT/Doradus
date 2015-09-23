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

package com.dell.doradus.service.db;

import java.util.Iterator;

public class SequenceIterator<T> implements Iterator<T> {
    private Sequence<T> m_sequence;
    private boolean m_bStarted = false;
    private T m_next;
    
    public SequenceIterator(Sequence<T> sequence) {
        m_sequence = sequence;
    }

    @Override public boolean hasNext() {
        if(!m_bStarted) {
            m_bStarted = true;
            m_next = m_sequence.next();
        }
        return m_next != null;
    }

    @Override public T next() {
        if(!hasNext()) throw new RuntimeException("Reading past the end of the iterator");
        T next = m_next;
        m_next = m_sequence.next();
        return next;
    }

}
