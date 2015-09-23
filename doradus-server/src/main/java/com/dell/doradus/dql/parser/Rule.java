package com.dell.doradus.dql.parser;

import java.util.List;


public abstract class Rule {
    private String m_name;
    
    public Rule(String name) {
        m_name = name; 
    }
    
    public String getName() {
        return m_name;
    }
    
    public abstract ASTNode parse(List<Token> list, int position);
    
}
