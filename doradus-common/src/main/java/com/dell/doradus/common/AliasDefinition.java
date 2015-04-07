/*
 * Copyright (C) 2014 Dell, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dell.doradus.common;

/**
 * Represents an "alias" definition, which is a derived field. An alias consists of a name
 * and an expression. The name is an identifier that must begin with the character
 * {@link CommonDefs#ALIAS_FIRST_CHAR}. The expression is a string that is not evaluated
 * during schema processing but expanded when referenced in queries. 
 */
final public class AliasDefinition {
    // Member variables:
    private final String    m_tableName;        // soft link to table that owns alias
    private       String    m_aliasName;
    private       String    m_expression;

    /**
     * Return true if the given string is a valid alias name. Alias names must begin with
     * {@link CommonDefs#ALIAS_FIRST_CHAR}. The rest of the identifier must be digits,
     * upper-case or lower-case letters, or underscores.
     * 
     * @param name  Candidate alias name.
     * @return      True if the string is a valid alias identifier.
     */
    public static boolean isValidName(String name) {
        return name != null &&
               name.length() > 1 &&
               name.charAt(0) == CommonDefs.ALIAS_FIRST_CHAR &&
               Utils.allAlphaNumUnderscore(name.substring(1));
    }   // isValidName
    
    /**
     * Create an AliasDefinition object that belongs to the table with the given name.
     * 
     * @param tableName Name of table that owns the new AliasDefinition object.
     */
    public AliasDefinition(String tableName) {
        assert tableName != null && tableName.length() > 0;
        m_tableName = tableName;
    }   // constructor

    /**
     * Parse the alias definition rooted at the given UNode, copying its properties into
     * this object.
     * 
     * @param aliasNode Root of an alias definition.
     */
    public void parse(UNode aliasNode) {
        assert aliasNode != null;
        
        // Ensure the alias name is valid and save it.
        setName(aliasNode.getName());
        
        // The only child element we expect is "expression".
        for (String childName : aliasNode.getMemberNames()) {
            // All child nodes must be values.
            UNode childNode = aliasNode.getMember(childName);
            Utils.require(childNode.isValue(),
                          "Value of alias attribute must be a string: " + childNode);
            Utils.require(childName.equals("expression"),
                          "'expression' expected: " + childName);
            Utils.require(m_expression == null,
                          "'expression' can only be specified once");
            setExpression(childNode.getValue());
        }

        // Ensure expression was specified.
        Utils.require(m_expression != null, "Alias definition missing 'expression': " + aliasNode);
    }   // parse

    ////// Getters
    
    public String getName() {
        return m_aliasName;
    }   // getName
    
    public String getExpression() {
        return m_expression;
    }   // getExpression
    
    public String getTableName() {
        return m_tableName;
    }   // getTableName
    
    /**
     * Serialize this alias definition into a {@link UNode} tree and return the root node.
     * 
     * @return  The root node of a {@link UNode} tree representing this alias.
     */
    public UNode toDoc() {
        // The root node is a MAP whose name is the alias name. We set its tag name to
        // "alias" for XML.
        UNode statNode = UNode.createMapNode(m_aliasName, "alias");
        
        // Add the expression parameter always, marked as an attribute.
        statNode.addValueNode("expression", getExpression(), true);
        return statNode;
    }   // toDoc

    ////// Setters
    
    public void setName(String name) {
        Utils.require(isValidName(name),
                      "Invalid alias name: " + name);
        m_aliasName = name;
    }   // setName

    public void setExpression(String expression) {
        Utils.require(expression != null && expression.length() > 0,
                      "'expression' cannot be empty");
        m_expression = expression;
    }   // setExpression
    
    /**
     * Update this alias definition to match the given updated one. The updated alias must
     * have the same name and table name.
     * 
     * @param newAliasDef   Updated alias definition.
     */
    public void update(AliasDefinition newAliasDef) {
        assert m_aliasName.equals(newAliasDef.m_aliasName);
        assert m_tableName.equals(newAliasDef.m_tableName);
        
        // Currently, all we have to do is copy over the "expression" property.
        m_expression = newAliasDef.m_expression;
    }   // update
    
    ///// Validation methods
    
    /**
     * Verify that the given new alias definition is a legal change to this one. We
     * currently allow any change, so we just return true if there are any difference.
     * 
     * @param  newAliasDef  Candidate modified version of this alias.
     * @return              True if any differences are found.
     */
    public boolean validateDifferences(AliasDefinition newAliasDef) {
        return !newAliasDef.m_expression.equals(m_expression);
    }   // validateDifferences

}   // class AliasDefinition
