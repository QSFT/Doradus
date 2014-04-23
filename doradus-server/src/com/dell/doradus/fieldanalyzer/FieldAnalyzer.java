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

package com.dell.doradus.fieldanalyzer;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Map;
import java.util.Set;

import com.dell.doradus.common.CommonDefs;
import com.dell.doradus.common.FieldDefinition;
import com.dell.doradus.common.FieldType;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.common.Utils;

/**
 * Abstract class for analyzers. This is tentative and likely to change.
 */
public abstract class FieldAnalyzer {
	
    // Default package in which we expect analyzer classes:
    private static final String DEFAULT_ANALYZER_PACKAGE = "com.dell.doradus.fieldanalyzer.";
    
    // Name of the "instance" method we expect each analyzer to have:
    private static final String INSTANCE_METHOD_NAME = "instance";
    
    // Cache of analyzer objects since findAnalyzer() is expensive. We use a Hashtable
    // since it is synchronized:
    private static final Map<String, FieldAnalyzer> g_analyzerCache =
        new Hashtable<String, FieldAnalyzer>();
    
    // Collection of compatible FieldTypes for this analyzer:
    private final Collection<FieldType> m_compatibleTypes;
    
    // Constructor
    protected FieldAnalyzer() {
        m_compatibleTypes = getCompatibleFieldTypes();
    }
    
    /**
     * This method is called once by the constructor to cache the set of field types
     * for which this analyzer can be used.
     *  
     * @return Collection of the {@link FieldType}s with which this analyzer can be used.
     */
    abstract protected Collection<FieldType> getCompatibleFieldTypes();
    
    /**
     * Return the singleton instance of the {@link FieldAnalyzer} class for the given
     * scalar field. If the field's analyzer name does not contain a package path, the
     * default {@value #DEFAULT_ANALYZER_PACKAGE} is prepended to it. If no analyzer with
     * the given name can be found, an IllegalArgumentException is thrown.
     * 
     * @param fieldDef  {@link FieldDefinition} of a scalar field.
     * @return          Singleton instance object defined for the scalar field.
     */
    public static FieldAnalyzer findAnalyzer(FieldDefinition fieldDef) {
        Utils.require(fieldDef.isScalarField(), "Must be a scalar field: %s", fieldDef);
        
        String analyzerName = fieldDef.getAnalyzerName();
        Utils.require(analyzerName != null, "Scalar field has no analyzer: %s", fieldDef);
        return findAnalyzer(analyzerName);
    }   // findAnalyzer
        
    /**
     * Return the singleton instance of the {@link FieldAnalyzer} for the given scalar
     * field belonging to the given table. If the scalar is not defined in the table, the
     * field is assumed to be text and the {@link TextAnalyzer} instance is returned.
     * Otherwise, the field definition's analyzer name is passed to {@link #findAnalyzer(String)}
     * whose result is returned. This method works with scalar fields that are not defined
     * in the table.
     * 
     * @param tableDef  Table that owns the scalar field.
     * @param fieldName Name of the scalar field.
     * @return          Singleton instance object of the analyzer that should be used to
     *                  index the given scalar field.
     */
    public static FieldAnalyzer findAnalyzer(TableDefinition tableDef, String fieldName) {
        assert tableDef != null;
        assert fieldName != null;
        
        FieldAnalyzer analyzer = TextAnalyzer.instance();
        FieldDefinition fieldDef = tableDef.getFieldDef(fieldName);
        if (fieldDef != null) {
            Utils.require(fieldDef.isScalarField(), "Must be a scalar field: " + fieldDef);
            analyzer = findAnalyzer(fieldDef.getAnalyzerName());
        }
        return analyzer;
    }   // findAnalyzer
    
    /**
     * Return the singleton instance of the {@link FieldAnalyzer} class for the given
     * analyzer name. If the given analyzer name does not contain a package path, the
     * default {@value #DEFAULT_ANALYZER_PACKAGE} is prepended to it. If no analyzer with
     * the given name can be found, an IllegalArgumentException is thrown.
     * 
     * @param analyzerName  Name of a field analyzer (e.g. "NullAnalyzer").
     * @return              Singleton analyzer object.
     */
    public static FieldAnalyzer findAnalyzer(String analyzerName) {
        Utils.require(!Utils.isEmpty(analyzerName), "analyzerName");
        
        // See if full package name was given.
        if (analyzerName.indexOf('.') < 0) {
            // Prepend default package name and, if needed, append "Analyzer".
            analyzerName = DEFAULT_ANALYZER_PACKAGE + analyzerName;
            if (!analyzerName.endsWith("Analyzer")) {
                analyzerName += "Analyzer";
            }
        }
        
        // Try the analyzer cache first.
        FieldAnalyzer fieldAnalyzer = g_analyzerCache.get(analyzerName);
        if (fieldAnalyzer != null) {
            return fieldAnalyzer;
        }
        
        // Analyzer not in the cache.
        try {
            // Attempt to load the corresponding class. This throws if the class cannot be found.
            @SuppressWarnings("unchecked")
            Class<FieldAnalyzer> analyzerClass = (Class<FieldAnalyzer>)Class.forName(analyzerName);
            
            // Attempt to find the "instance" static method. This also throws if not found.
            Method instanceMethod = analyzerClass.getMethod(INSTANCE_METHOD_NAME);
            
            // Attempt to invoke the instance method, which should return an instance of the
            // FieldAnalyzer we want. Since we expect it to be static, we pass null for the
            // "object" parameter. We also expect it to have no parameters, so we pass none.
            fieldAnalyzer = (FieldAnalyzer)instanceMethod.invoke(null);
            
            // Here, found it. Add to cache and return it.
            g_analyzerCache.put(analyzerName, fieldAnalyzer);   // OK if duplicate
            return fieldAnalyzer;
        } catch (Exception e) {
            // Didn't find the class or method we were looking for.
            throw new IllegalArgumentException("Analyzer not found: " + analyzerName);
        }
    }   // findAnalyzer
    
    /**
     * Verify that the analyzer defined for the given field is a known analyzer and that it
     * is valid for its declared type.  This method can only be called for scalar fields,
     * and the field's type and analyzer must be set. This method calls
     * {@link #findAnalyzer(FieldDefinition)} and then looks for the field's type in the
     * set of {@link FieldType}s returned by {@link #compatibleFieldTypes()}.
     * 
     * @param fieldDef  {@link FieldDefinition} of a scalar field.
     */
    public static void verifyAnalyzer(FieldDefinition fieldDef) {
        assert fieldDef != null;
        
        FieldAnalyzer analyzer = findAnalyzer(fieldDef);
        Utils.require(analyzer.compatibleFieldTypes().contains(fieldDef.getType()),
                      "Invalid analyzer for field type '%s': %s", fieldDef.getType(), fieldDef.getAnalyzerName());
    }   // verifyAnalyzer
    
    /**
     * Tokenize the given String value and return the array of tokens that should be indexed.
     * 
     * @param value Field value to be indexed as a String.
     * @return      List of terms that should be indexed.
     */
    abstract public String[] tokenize(String value);
    
    /**
     * Return the set of scalar {@link FieldType}s with which this analyzer can be used.
     * Each analyzer can be used for at least one field type.
     * 
     * @return  Collection of field types for which this analyzer can be used.
     */
    public Collection<FieldType> compatibleFieldTypes() {
        return m_compatibleTypes;
    }
    
    /**
     * Analyze the given String value and return the set of terms that should be indexed.
     * 
     * @param value Field value to be indexed as a binary value.
     * @return      Set of terms that should be indexed.
     */
    public Set<String> extractTerms(String value) {
        try {
	        Set<String> result = new HashSet<String>();
    		Set<String> split = Utils.split(value.toLowerCase(), CommonDefs.MV_SCALAR_SEP_CHAR);
    		for(String s : split) {
    			String[] tokens = tokenize(s);
		        for (String token : tokens) {
		            if (token.length() == 0) continue;
		            result.add(token);
		        }
    		}
	        return result;
        } catch (Exception e) {
            // Turn into an IllegalArgumentException
            throw new IllegalArgumentException("Error parsing field value: " + e.getLocalizedMessage());
        }
    }
    
    /**
     * Form the row key that should be used to index the given term, belonging to the
     * field with the given name. The analyzer determines if and how to use the field name
     * and term to create the term key. This method may throw if the analyzer thinks that
     * its terms should not be indexed.
     * 
     * @param fieldName Name of field in which term was found.
     * @param term      Value of term found.
     * @return          Row key that should be used in the Term record that represents the
     *                  term. The row key will be case-adjusted if applicable.
     */
    public String formTermKey(String fieldName, String term) {
    	return fieldName + "/" + term;
    }
    
    public static String makeTermKey(String field, String term) {
    	return field + "/" + term.toLowerCase();
    }

    public static String makeAllKey() {
    	return "_";
    }

}   // abstract class FieldAnalyzer
