package com.dell.doradus.spider2.fastjson;

import com.dell.doradus.spider2.Binary;


public class MapEntry {
    private Binary m_key;
    private JsonNode m_value;
    
    protected MapEntry(Binary key, JsonNode value) {
        m_key = key;
        m_value = value;
    }

    public Binary getKey() { return m_key; }
    public JsonNode getValue() { return m_value; }
    
    
}
