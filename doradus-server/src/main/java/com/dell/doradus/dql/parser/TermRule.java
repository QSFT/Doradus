package com.dell.doradus.dql.parser;

import java.util.List;


public class TermRule extends Rule {

    public TermRule() {
        super("TERM");
    }
    
    public ASTNode parse(List<Token> list, int position) {
        Token x = list.get(position);
        if(x.isTerm()) return new ASTNode(getName(), x.getValue(), position, 1);
        else throw new ParseException(x, "Term expected");
    }
    
}
