package com.dell.doradus.dql.parser;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;


public class ASTNode {
    private String m_name;
    private String m_value;
    private int m_position;
    private int m_tokensCount;
    private List<ASTNode> m_subnodes;
    
    public ASTNode(String name, String value, int position, int tokensCount) {
        m_name = name;
        m_value = value;
        m_position = position;
        m_tokensCount = tokensCount;
    }

    public String getName() { return m_name; }
    public String getValue() { return m_value; }
    public int getTokensCount() { return m_tokensCount; }
    public int getPosition() { return m_position; }
    public List<ASTNode> getNodes() { return m_subnodes; }
    
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
        return m_name + ": " + m_value;
    }
    
}
