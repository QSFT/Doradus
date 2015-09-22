package com.dell.doradus.dql.parser;

import java.util.List;


public interface Rule {
    ASTNode parse(List<Token> list, int position);
}
