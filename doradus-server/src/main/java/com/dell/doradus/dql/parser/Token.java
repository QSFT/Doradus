package com.dell.doradus.dql.parser;


//Basic chunk of Query Language.
//Type can be a character representing syntax (dot, comma, colon, etc, or 0 as identifier/term, or -1 as EOF
public class Token {
    public static Token EOF = new Token(0, 0, null, -1);
    private int m_pointer;
    private int m_length;
    private String m_value;
    private int m_type;
    
    Token(int pointer, int length, String value, int type) {
        m_pointer = pointer;
        m_length = length;
        m_value = value;
        m_type = type;
    }
    
    public int getPointer() { return m_pointer; }
    public int getLength() { return m_length; }
    public int getEnd() { return m_pointer + m_length; }
    public String getValue() { return m_value; }
    public int getType() { return m_type; }
    
    public boolean isEOF() { return m_type == -1; }
    public boolean isTerm() { return m_type == 0; }
    public boolean isSyntax() { return m_type > 0; }

    @Override public boolean equals(Object obj) {
        if(obj == null) {
            return isEOF();
        }
        else if(obj instanceof Token) {
            Token x = (Token)obj;
            if(isTerm()) return getValue().equals(x.getValue());
            else return getType() == x.getType();
        } else if(obj instanceof String) {
            String x = (String)obj;
            if(isEOF()) return x == null;
            else return getValue().equals(x);
        } else if(obj instanceof Character) {
            Character x = (Character)obj;
            if(!isSyntax()) return false;
            else return getType() == x.charValue();
        } else {
            return false;
        }
    }
    
    @Override public String toString() { return m_value; }
}
