package com.dell.doradus.dql.parser;

public class Token {
    public static Token EOF = new Token(0, 0, null, TokenType.EOF);
    private int m_pointer;
    private int m_length;
    private String m_value;
    private TokenType m_type;

    public Token(int pointer, int length, String value, TokenType type) {
        m_pointer = pointer;
        m_length = length;
        m_value = value;
        m_type = type;
    }
    
    public int getPointer() { return m_pointer; }
    public int getLength() { return m_length; }
    public int getEnd() { return m_pointer + m_length; }
    public String getValue() { return m_value; }
    public TokenType getType() { return m_type; }
    
    public boolean isEOF() { return m_type == TokenType.EOF; }
    public boolean isTerm() { return m_type == TokenType.TERM; }
    public boolean isSyntax() { return m_type == TokenType.SYNTAX; }

    @Override public boolean equals(Object obj) {
        Token x = (Token)obj;
        if(m_type != x.m_type) return false;
        else if(m_value == null) return x.m_value == null;
        else return m_value.equals(x.m_value);
    }
    
    @Override public String toString() { return m_value; }
}
