package com.dell.doradus.dql.parser;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;


public class ASTNode {
    private String m_name;
    private Token m_value;
    private int m_tokensCount;
    private List<ASTNode> m_subnodes;
    
    public ASTNode(String name) {
        m_name = name;
    }

    public String getName() { return m_name; }
    public List<ASTNode> getNodes() { return m_subnodes; }

    public Token getValue() { return m_value; }
    public void setValue(Token value) { m_value = value; }

    public int getTokensCount() { return m_tokensCount; }
    public void setTokensCount(int tokensCount) { m_tokensCount = tokensCount; }
    
    public void addNode(ASTNode node) {
        if(m_subnodes == null) m_subnodes = new ArrayList<>();
        m_subnodes.add(node);
    }
    
    public void addNodes(Collection<ASTNode> nodes) {
        if(m_subnodes == null) m_subnodes = new ArrayList<>();
        m_subnodes.addAll(nodes);
    }

    @Override
    public String toString() {
        if(m_value == null) return m_name;
        else return m_name + ": " + m_value;
    }
    
}
