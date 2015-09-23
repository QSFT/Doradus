package com.dell.doradus.dql.parser;

import java.util.Arrays;
import java.util.List;


public class SwitchRule extends Rule {
    private List<Rule> m_list;
    
    public SwitchRule(String name, Rule...rules) {
        super(name);
        m_list = Arrays.asList(rules);
    }
    
    public ASTNode parse(List<Token> list, int position) {
        for(Rule rule: m_list) {
            try {
                ASTNode node = rule.parse(list, position);
                return node;
            }catch(IllegalArgumentException e) {
                continue;
            }
        }
        throw new IllegalArgumentException("Unexpected: " + list.get(position));
    }
    
}
