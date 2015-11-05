package com.dell.doradus.logservice;

import com.dell.doradus.common.Utils;
import com.dell.doradus.logservice.search.StrRef;
import com.dell.doradus.logservice.store.Temp;
import com.dell.doradus.olap.collections.MemoryStream;
import com.dell.doradus.olap.io.BSTR;

public class ChunkField {
    private int m_size;
    private int m_fieldIndex;
    private MemoryStream m_input;
    private MemoryStream m_data;
    private BSTR m_fieldName;
    private int m_valuesOffset;
    private int m_valuesCount;
    private int m_indexesOffset;
    private int[] m_offsets;
    private int[] m_lengths;
    private int[] m_prefixes;
    private int[] m_suffixes;
    private int[] m_indexes;
    private SyntheticFields m_synth;
    
    private int m_CmpValuesSize;
    private int m_CmpIndexesSize;
    private int m_CmpTotalSize;
    
    public ChunkField(int size, int fieldIndex, MemoryStream input, MemoryStream data) {
        m_size = size;
        m_fieldIndex = fieldIndex;
        m_input = input;
        m_data = data;
        
        m_fieldName = m_input.readString();
        m_valuesCount = m_input.readVInt();
        
        m_valuesOffset = m_input.position();
        Temp.skipCompressed(m_input);
        Temp.skipCompressed(m_input);
        Temp.skipCompressed(m_input);
        Temp.skipCompressed(m_input);
        
        m_indexesOffset = m_input.position();
        if(m_valuesCount >= 256) {
            Temp.skipCompressed(m_input);
            Temp.skipCompressed(m_input);
        } else {
            Temp.skipCompressed(m_input);
        }
        
        m_CmpValuesSize = m_indexesOffset - m_valuesOffset;
        m_CmpIndexesSize = m_input.position() - m_indexesOffset;
        m_CmpTotalSize = m_CmpValuesSize + m_CmpIndexesSize;
    }

    public ChunkField(SyntheticFields synth, BSTR fieldName, int fieldIndex, MemoryStream data) {
        m_synth = synth;
        m_fieldIndex = fieldIndex;
        m_data = data;
        m_fieldName = fieldName;
    }
    
    
    public void readValues() {
        if(m_offsets != null) return;
        
        if(m_synth != null) {
            m_synth.createFields();
            return;
        }
        
        m_data.seek(m_data.length());
        m_input.seek(m_valuesOffset);
        m_offsets = new int[m_valuesCount];
        m_lengths = new int[m_valuesCount];
        m_prefixes = new int[m_valuesCount];
        m_suffixes = new int[m_valuesCount];
        
        MemoryStream s_pfx = Temp.readCompressed(m_input);
        MemoryStream s_sfx = Temp.readCompressed(m_input);
        MemoryStream s_len = Temp.readCompressed(m_input);
        MemoryStream s_dat = Temp.readCompressed(m_input);
        
        int lastPos = m_data.position();
        int lastLen = 0;
        for(int i = 0; i < m_valuesCount; i++) {
            int pfx = s_pfx.readVInt();
            int sfx = s_sfx.readVInt();
            int len = s_len.readVInt();
            m_offsets[i] = m_data.position();
            m_lengths[i] = len;
            m_prefixes[i] = pfx;
            m_suffixes[i] = sfx;
            int ifx = len - sfx - pfx;
            if(pfx > 0) m_data.write(m_data.getBuffer(), lastPos, pfx);
            if(ifx > 0) {
                m_data.write(s_dat.getBuffer(), s_dat.position(), ifx);
                s_dat.skip(ifx);
            }
            if(sfx > 0) m_data.write(m_data.getBuffer(), lastPos + lastLen - sfx, sfx);
            lastPos = m_offsets[i];
            lastLen = len;
        }
    }
    
    public void readIndexes() {
        if(m_indexes != null) return;
        
        if(m_synth != null) {
            m_synth.createFields();
            return;
        }
        
        m_indexes = new int[m_size];
        m_input.seek(m_indexesOffset);
        if(m_valuesCount >= 256) {
            MemoryStream s_lst = Temp.readCompressed(m_input);
            MemoryStream s_fst = Temp.readCompressed(m_input);
            for(int i = 0; i < m_size; i++) {
                int n = (s_fst.readVInt() << 8) + s_lst.readByte();
                m_indexes[i] = n;
            }
        } else {
            MemoryStream s_lst = Temp.readCompressed(m_input);
            for(int i = 0; i < m_size; i++) {
                int n = s_lst.readByte();
                m_indexes[i] = n;
            }
        }
    }
    

    //for synthetic fields
    public void set(int[] indexes, int[] offsets, int[] lengths) {
        m_size = indexes.length;
        m_valuesCount = offsets.length;
        m_offsets = offsets;
        m_lengths = lengths;
        m_prefixes = new int[offsets.length];
        m_suffixes = new int[offsets.length];
        m_indexes = indexes;
    }
    
    
    public int size() { return m_size; }
    
    public byte[] getBuffer() { return m_data.getBuffer(); }
    
    public BSTR getFieldName() { return m_fieldName; }
    
    public int getFieldIndex() { return m_fieldIndex; }
    
    public int getValuesCount() {
        if(m_synth != null) readValues();
        return m_valuesCount;
    }
    
    public int[] getOffsets() { 
        readValues();
        return m_offsets;
    }
    
    public int[] getLengths() {
        readValues();
        return m_lengths;
    }
    
    public int[] getPrefixes() {
        readValues();
        return m_prefixes;
    }
    
    public int[] getSuffixes() {
        readValues();
        return m_suffixes;
    }
    
    public int[] getIndexes() {
        readIndexes();
        return m_indexes;
    }
    
    
    public void getFieldValue(int doc, BSTR value) {
        readValues();
        readIndexes();
        int index = m_indexes[doc];
        value.length = 0;
        m_data.seek(m_offsets[index]);
        int len = m_lengths[index];
        value.assertLength(len);
        m_data.read(value.buffer, 0, len);
        value.length = len;
    }

    public String getFieldValue(int doc) {
        readValues();
        readIndexes();
        int index = m_indexes[doc];
        return Utils.toString(m_data.getBuffer(), m_offsets[index], m_lengths[index]);
    }

    public void getFieldValue(int doc, StrRef value) {
        readValues();
        readIndexes();
        int index = m_indexes[doc];
        value.set(m_data.getBuffer(), m_offsets[index], m_lengths[index]);
    }

    public void getValue(int valueNumber, StrRef value) {
        readValues();
        value.set(m_data.getBuffer(), m_offsets[valueNumber], m_lengths[valueNumber]);
    }
    
    public void printSize(StringBuilder sb) {
    	sb.append("Field: " + m_fieldName.toString() + "; Total: " + m_CmpTotalSize + "; Values: " + m_CmpValuesSize + "; Indexes: " + m_CmpIndexesSize + "\n");
    }
}

