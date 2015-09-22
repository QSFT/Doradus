package com.dell.doradus.dql.parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


//Basic chunk of Query Language.
//Type can be a character representing syntax (dot, comma, colon, etc, or 0 as identifier/term, or -1 as EOF
public class TokenReader {
    private static Set<Character> SYNTAX = new HashSet<>(
            Arrays.asList('.', ',', ':', '(', ')', '[', ']', '+', '-', '*', '/', '^', '>', '<', '='));
    
    public static List<Token> tokenize(String str) {
        List<Token> list = new ArrayList<>();
        int pointer = 0;
        while(true) {
            Token token = read(str, pointer);
            pointer = token.getEnd();
            list.add(token);
            if(token.isEOF()) break;
        }
        return list;
    }
    
    public static Token read(String str, int pointer) {
        while(pointer < str.length() && Character.isWhitespace(str.charAt(pointer))) pointer++;
        if(pointer == str.length()) return new Token(pointer, 0, null, -1);
        
        char ch = str.charAt(pointer);
        if(Character.isJavaIdentifierStart(ch) || Character.isDigit(ch)) {
            return readIdentifier(str, pointer);
        } else if(ch == '"' || ch == '\'') {
            return readTerm(str, pointer);
        } else if(SYNTAX.contains(new Character(ch))) {
            return new Token(pointer, 1, str.substring(pointer, pointer + 1), ch);
        } else {
            throw new IllegalArgumentException("Unexpected character: " + ch);
        }
        
    }

    private static Token readIdentifier(String str, int pointer) {
        StringBuilder sb = new StringBuilder();
        int start = pointer;
        while(pointer < str.length()) {
            char ch = str.charAt(pointer);
            if(!Character.isJavaIdentifierPart(ch)) break;
            sb.append(ch);
            pointer++;
        }
        if(start == pointer) throw new IllegalArgumentException("Identifier expected");
        return new Token(start, pointer - start, sb.toString(), 0);
    }

    private static Token readTerm(String str, int pointer) {
        StringBuilder sb = new StringBuilder();
        int start = pointer;
        char ch = str.charAt(pointer++);
        if(ch != '\'' && ch != '"') throw new IllegalArgumentException("Quote expected");
        boolean isSingleQuote = (ch == '\'');
        while(true) {
            if(pointer == str.length()) throw new IllegalArgumentException("Missing closing quote");
            ch = str.charAt(pointer++);
            if((isSingleQuote && ch == '\'') || (!isSingleQuote && ch == '"')) {
                break;
            } else if(ch == '\\') {
                if(pointer == str.length()) throw new IllegalArgumentException("Unexpected end of string");
                ch = str.charAt(pointer++);
                if(ch == 'n') sb.append('\n');
                else if(ch == 't') sb.append('\t');
                else if(ch == '0') sb.append('\0');
                else if(ch == '\\') sb.append('\\');
                else if(ch == '"') sb.append('\"');
                else if(ch == '\'') sb.append('\'');
                else if(ch == 'u') {
                    int code = ((digit(str, pointer++) * 16 + digit(str, pointer++)) * 16 +  digit(str, pointer++)) * 16 + digit(str, pointer++);
                    ch = (char)code;
                    sb.append(ch);
                }
            } else {
                sb.append(ch);
            }
        }

        return new Token(start, pointer - start, sb.toString(), 0);
    }
    
    
    private static int digit(String str, int pointer) {
        if(pointer == str.length()) throw new IllegalArgumentException("Unexpected end of string");
        char ch = str.charAt(pointer);
        if(ch < '0' || ch > '9') throw new IllegalArgumentException("Digit expected");
        return ch - '0';
    }
    
}
