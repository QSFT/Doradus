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

package com.dell.doradus.testprocessor.xmlreflector;

import java.util.*;
import com.dell.doradus.testprocessor.common.*;

public class XDefinitions
{
    static private class Definition
    {
        public Definition(String name, String value) {
            Name = name;
            Value = value;
            Link = null;
        }

        public String     Name;
        public String     Value;
        public Definition Link;
    }

    private Map<String, Definition> m_dictionary = new HashMap<String, Definition>();

    public void setInteger(String name, int value)
    throws Exception
    {
        set(name, Integer.toString(value));
    }
    public int getInteger(String name)
    throws Exception
    {
        String raw = get(name);
        return (Integer) Utils.convert(raw, int.class);
    }
    public void setBoolean(String name, boolean value)
    throws Exception
    {
        set(name, Boolean.toString(value));
    }
    public boolean getBoolean(String name)
    throws Exception
    {
        String raw = get(name);
        return (Boolean) Utils.convert(raw, boolean.class);
    }
    public void setString(String name, String value)
    throws Exception
    {
        set(name, value);
    }
    public String getString(String name)
    throws Exception
    {
        return get(name);
    }
    public void remove(String name)
    {
        m_dictionary.remove(name);
    }

    public boolean isDefined(String name)
    {
        return m_dictionary.get(name) != null;
    }

    public String expand(String src)
    throws Exception
    {
        if (src == null) return null;
        return expand(src, null);
    }

    public List<String> expand(List<String> src)
    throws Exception
    {
        if (src == null) return src;

        List<String> result = new ArrayList<String>(src.size());
        for (int i = 0; i < src.size(); i++) {
            result.add(i, expand(src.get(i)));
        }
        return result;
    }

    public String[] expand(String[] src)
    throws Exception
    {
        if (src == null) return src;

        String[] result = new String[src.length];
        for (int i = 0; i < src.length; i++) {
            result[i] = expand(src[i]);
        }
        return result;
    }

    private void set(String name, String value)
    throws Exception
    {
        try {
            Definition definition = m_dictionary.get(name);
            if (definition != null) {
                definition.Value = value;
            } else {
                definition = new Definition(name, value);
            }
            m_dictionary.put(name, definition);
        }
        catch (Exception ex) {
            String msg = "Failed to define \"" + name + "\"";
            throw new Exception(msg, ex);
        }
    }

    private String get(String name)
    throws Exception
    {
        Definition definition = m_dictionary.get(name);
        if (definition == null) {
            String msg = "Definition of \"" + name + "\" is not found";
            throw new Exception(msg);
        }
        return definition.Value;
    }

    private String expand(String src, Definition link) throws Exception
    {
        if (src == null || src.trim().isEmpty()) return src;

        StringBuilder result = new StringBuilder();

        for (int i1 = 0; true;)
        {
            int i2 = src.indexOf("${", i1);
            if (i2 < 0) {
                result.append(src.substring(i1));
                break;
            }

            result.append(src.substring(i1, i2));
            i1 = i2 + 2;

            i2 = src.indexOf('}', i1);
            if (i2 < 0) {
                result.append("${");
                result.append(src.substring(i1));
                break;
            }

            String name = src.substring(i1, i2);
            if (name.contains("${")) {
                result.append("${");
                continue;
            }

            Definition def = m_dictionary.get(name);
            if (def == null) {
                result.append("${");
                result.append(name);
                result.append("}");
            } else if (def.Link == null) {
                def.Link = link;
                result.append(expand(def.Value, def));
                def.Link = null;
            } else {
                CyclicDefinitionFound(def, link);
            }

            i1 = i2 + 1;
        }

        return result.toString();
    }

    private void CyclicDefinitionFound(Definition def, Definition link) throws Exception
    {
        StringBuilder msg = new StringBuilder();
        def.Link = null;

        msg.insert(0, def.Name);
        while (link != null) {
            msg.insert(0, link.Name + " --> ");
            link = link.Link;
        }

        msg.insert(0, "Cyclic definition found: ");

        Iterator<Definition> defs = m_dictionary.values().iterator();
        while (defs.hasNext()) {
            defs.next().Link = null;
        }

        throw new Exception(msg.toString());
    }
}
