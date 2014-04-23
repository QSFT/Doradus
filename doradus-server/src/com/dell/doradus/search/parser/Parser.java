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

package com.dell.doradus.search.parser;

import com.dell.doradus.search.parser.grammar.Context;
import com.dell.doradus.search.parser.grammar.GrammarException;
import com.dell.doradus.search.parser.grammar.GrammarRule;

public class Parser {

    private static Parser doradusParser;
    private static Parser aggregationParser;
    private static Parser aggregationMetricParser;

    private static Parser statisticMetricParser;
    private static Parser statisticParametersParser;
    private static Parser fieldSetParser;
    private static Parser sortOrderParser;

    public static final String UNEXPECTED_CHARACTER = "Unexpected character";

    GrammarRule root;

    public static Parser GetAggregationMetricParser() {
        if (aggregationMetricParser == null)
            aggregationMetricParser = new Parser(DoradusSearchQueryGrammar.GetGrammar(DoradusSearchQueryGrammar.AggregationMetricGrammar));
        return aggregationMetricParser;
    }

    public static Parser GetDoradusQueryParser() {
        if (doradusParser == null)
            doradusParser = new Parser(DoradusSearchQueryGrammar.GetGrammar());
        return doradusParser;
    }

    public static Parser GetAggregationQueryParser() {
        if (aggregationParser == null)
            aggregationParser = new Parser(DoradusSearchQueryGrammar.GetGrammar(DoradusSearchQueryGrammar.AggregationQueryGrammar));
        return aggregationParser;
    }

    public static Parser GetStatisticMetricParser() {
        if (statisticMetricParser == null)
            statisticMetricParser = new Parser(DoradusSearchQueryGrammar.GetGrammar(DoradusSearchQueryGrammar.StatisticMetricGrammar));
        return statisticMetricParser;
    }

    public static Parser GetStatisticParametersParser() {
        if (statisticParametersParser == null)
            statisticParametersParser = new Parser(DoradusSearchQueryGrammar.GetGrammar(DoradusSearchQueryGrammar.StatisticParameterGrammar));
        return statisticParametersParser;
    }

    public static Parser GetFieldSetParser() {
        if (fieldSetParser == null)
            fieldSetParser = new Parser(DoradusSearchQueryGrammar.GetGrammar(DoradusSearchQueryGrammar.FieldSetGrammar));
        return fieldSetParser;
    }

    public static Parser GetSortOrderParser() {
        if (sortOrderParser == null)
            sortOrderParser = new Parser(DoradusSearchQueryGrammar.GetGrammar(DoradusSearchQueryGrammar.SortOrderGrammar));
        return sortOrderParser;
    }


    public Parser(GrammarRule rule) {
        root = rule;
    }

    public ParseResult Parse(String inputString) {
        return Parser.Parse(root, new Context(inputString));
    }

    private static String  GetCurrentChar( Context context) {
         if (context.ptr >= context.inputString.length())
             return "EOL";
        return "" + context.inputString.charAt(context.ptr);
    }
    private static ParseResult Error(String message,   Context context ) {
        if (context != null)
         return new ParseResult(context, message + " '" + GetCurrentChar(context) +"' position "
                + (context.ptr + 1) + ",  input='" + context.inputString.substring( context.ptr) +"'");

        return  new ParseResult(context, message);
    }
    
    public static ParseResult Parse(GrammarRule rule, Context context) {

        if (context.inputString == null || context.inputString.trim().isEmpty()) {
            return new ParseResult(context, null);
        }

        try {
            Context result = rule.Match(context);
            if (result != null) {
                if (result.ptr == context.inputString.length()) {
                    return new ParseResult(result, null);
                } else {
                    return Error(UNEXPECTED_CHARACTER + ":", result);
                }
            }
            return Error(UNEXPECTED_CHARACTER + ":", context);

        } catch (GrammarException ge) {
            if (ge.getContext() == null)
                return Error(ge.getMessage(),null);

            if (ge.getDescription() != null)
                return Error(ge.getDescription(),ge.getContext());

            return Error(UNEXPECTED_CHARACTER + ":", ge.getContext());
        }
    }

}
