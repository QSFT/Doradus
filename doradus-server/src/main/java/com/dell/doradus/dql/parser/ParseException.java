package com.dell.doradus.dql.parser;

public class ParseException extends IllegalArgumentException {
    private static final long serialVersionUID = 1L;

    public ParseException(String message) {
        super(message);
    }
    
    public ParseException(int position, String message) {
        super(message + " [" + position + "]");
    }

    public ParseException(Token token, String message) {
        this(token.getPointer(), message);
    }

    public ParseException(Token token) {
        this(token.getPointer(), "" + token.getValue() + " unexpected");
    }

}
