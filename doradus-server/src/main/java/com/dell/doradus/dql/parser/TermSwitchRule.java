package com.dell.doradus.dql.parser;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


public class TermSwitchRule extends Rule {
    private Set<String> m_set;
    
    public TermSwitchRule(String name, String...rules) {
        super(name);
        m_set = new HashSet<>(Arrays.asList(rules));
    }
    
    public ASTNode parse(List<Token> list, int position) {
        Token x = list.get(position);
        if(m_set.contains(x.getValue())) {
            return new ASTNode(getName(), x.getValue(), position, 1);
        }
        throw new ParseException(x);
    }
    
}
