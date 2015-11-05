package com.dell.doradus.logservice;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import com.dell.doradus.olap.collections.MemoryStream;
import com.dell.doradus.olap.io.BSTR;

public class ChunkReader {
    private int m_size;
    private int m_fieldsCount;
    private MemoryStream m_data = new MemoryStream();
    private MemoryStream m_input = new MemoryStream();
    private ChunkTimestampField m_timestamps;
    private ArrayList<ChunkField> m_fields = new ArrayList<>(); 
    private HashMap<BSTR, Integer> m_fieldsMap = new HashMap<>();
    private SyntheticFields m_synth;
    
    public ChunkReader() {}

    public void setSyntheticFields(String pattern) {
        m_synth = new SyntheticFields(pattern); 
    }
    
    public void read(byte[] data) {
        m_size = 0;
        m_fieldsCount = 0;
        m_data.clear();
        m_input = new MemoryStream(data);
        m_timestamps = null;
        m_fields.clear();
        m_fieldsMap.clear();
        byte version = (byte)m_input.readByte();
        if(version != 1) throw new RuntimeException("Unknown format");
        m_size = m_input.readVInt();
        m_fieldsCount = m_input.readVInt();
        m_timestamps = new ChunkTimestampField(m_size, m_input);
        for(int f = 0; f < m_fieldsCount; f++) {
            ChunkField field = new ChunkField(m_size, f, m_input, m_data);
            m_fields.add(field);
            m_fieldsMap.put(field.getFieldName(), new Integer(f));
        }
        if(!m_input.end()) throw new RuntimeException("Unexpected data");
        
        if(m_synth != null) {
            addSyntheticField();
        }
    }

    public void readAll() {
        m_timestamps.readField();
        for(ChunkField field: m_fields) {
            field.readValues();
            field.readIndexes();
        }
    }
    
    public int size() { return m_size; }
    
    public int fieldsCount() { return m_fieldsCount; }
    
    public ChunkTimestampField getTimestampField() { return m_timestamps; }
    
    public ChunkField getField(int field) { return m_fields.get(field); }
    
    public long getTimestamp(int doc) { return m_timestamps.getTimestamp(doc); }
    
    public Date getDate(int doc) { return new Date(getTimestamp(doc)); }
    
    public ArrayList<BSTR> getFieldNames() {
        ArrayList<BSTR> list = new ArrayList<>(m_fields.size());
        for(ChunkField field: m_fields) list.add(field.getFieldName());
        return list;
    }
    
    public int getFieldIndex(BSTR field) {
        Integer i = m_fieldsMap.get(field);
        if(i == null) return -1;
        else return i.intValue();
    }
    
    public void getFieldValue(int doc, int field, BSTR value) {
        m_fields.get(field).getFieldValue(doc, value);
    }

    public String getFieldValue(int doc, int field) {
        return m_fields.get(field).getFieldValue(doc);
    }

    private void addSyntheticField() {
        int baseFieldIndex = getFieldIndex(m_synth.getBaseFieldName());
        if(baseFieldIndex < 0) return;
        ChunkField baseField = getField(baseFieldIndex); 
        List<BSTR> fieldNames = m_synth.getFieldNames();
        List<ChunkField> chunkFields = new ArrayList<>();
        for(int i = 0; i < fieldNames.size(); i++) {
            ChunkField field = null;
            BSTR fieldName = fieldNames.get(i);
            int fieldIndex = getFieldIndex(fieldName);
            if(fieldIndex > 0) field = getField(fieldIndex);
            else {
                int f = m_fieldsCount;
                field = new ChunkField(m_synth, fieldName, f, m_data);
                m_fields.add(field);
                m_fieldsMap.put(field.getFieldName(), new Integer(f));
                m_fieldsCount++;
            }
            chunkFields.add(field);
        }
        m_synth.setFields(baseField, chunkFields);
    }
    
    public void printSize(StringBuilder sb) {
    	m_timestamps.printSize(sb);
    	for(ChunkField fld: m_fields) fld.printSize(sb);
    }
    
}
