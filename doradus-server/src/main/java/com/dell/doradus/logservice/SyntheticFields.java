package com.dell.doradus.logservice;

import java.util.ArrayList;
import java.util.List;

import com.dell.doradus.logservice.pattern.Substr;
import com.dell.doradus.olap.io.BSTR;

public class SyntheticFields {
    private List<Substr> m_substrings = new ArrayList<>();
    private BSTR m_baseFieldName;
    private List<BSTR> m_fields = new ArrayList<>();
    private ChunkField m_baseField;
    private List<ChunkField> m_chunkFields;
    
    public SyntheticFields(String pattern) {
        int idx = pattern.indexOf('=');
        if(idx <= 0) throw new IllegalArgumentException("Wrong pattern format: expected Field=aaa{F1}bbb{F2}ccc");
        String baseField = pattern.substring(0, idx);
        pattern = pattern.substring(idx + 1);
        setup(baseField, pattern);
    }
    
    public SyntheticFields(String baseField, String pattern) {
        setup(baseField, pattern);
    }
    
    private void setup(String baseField, String pattern) {
        m_baseFieldName = new BSTR(baseField);
        if(pattern.contains("}{")) throw new IllegalArgumentException("Two fields cannot follow each other without some text between");
        while(true) {
            int fieldStart = pattern.indexOf('{');
            if(fieldStart < 0) {
                m_substrings.add(new Substr(pattern));
                break;
            }
            int fieldEnd = pattern.indexOf('}');
            if(fieldEnd < 0) throw new IllegalArgumentException("Missing closing bracket");
            m_substrings.add(new Substr(pattern, 0, fieldStart));
            String fieldName = pattern.substring(fieldStart + 1, fieldEnd);
            m_fields.add(new BSTR(fieldName));
            pattern = pattern.substring(fieldEnd + 1);
        }
        if(m_fields.size() != m_substrings.size() - 1) throw new IllegalArgumentException("Invalid pattern");
    }

    public BSTR getBaseFieldName() { return m_baseFieldName; }
    public List<BSTR> getFieldNames() { return m_fields; }
    
    public void setFields(ChunkField baseField, List<ChunkField> fields) {
        m_baseField = baseField;
        m_chunkFields = fields;
    }
    
    public void createFields() {
        int valuesCount = m_baseField.getValuesCount();
        int[] lengths = m_baseField.getLengths();
        int[] offsets = m_baseField.getOffsets();
        byte[] data = m_baseField.getBuffer();

        List<int[]> fieldOffsets = new ArrayList<>(valuesCount);
        List<int[]> fieldLengths = new ArrayList<>(valuesCount);
        
        for(int i = 0; i < m_fields.size(); i++) {
            fieldOffsets.add(new int[valuesCount]);
            fieldLengths.add(new int[valuesCount]);
        }
        
        for(int valueIndex = 0; valueIndex < valuesCount; valueIndex++) {
            int valueStart = offsets[valueIndex];
            int valueEnd = valueStart + lengths[valueIndex];

            Substr s = m_substrings.get(0);
            if(s.length > 0) {
                if(!s.startsWith(data, valueStart, valueEnd)) continue;
                //valueStart = s.contains(data, valueStart, valueEnd);
                //if(valueStart < 0) continue;
                valueStart += s.length;
            }

            for(int field = 0; field < m_fields.size(); field++) {
                s = m_substrings.get(field + 1);
                int next = -1;
                if(s.length > 0) { 
                    next = s.contains(data, valueStart, valueEnd);
                }
                if(next < 0) {
                    fieldOffsets.get(field)[valueIndex] = valueStart;
                    fieldLengths.get(field)[valueIndex] = valueEnd - valueStart;
                    break;
                } else {
                    fieldOffsets.get(field)[valueIndex] = valueStart;
                    fieldLengths.get(field)[valueIndex] = next - valueStart;
                    valueStart = next + s.length;
                }
            }
            
        }
        
        for(int i = 0; i < m_fields.size(); i++) {
            m_chunkFields.get(i).set(m_baseField.getIndexes(), fieldOffsets.get(i), fieldLengths.get(i));
        }
        
    }
    
}

