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

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

import com.dell.doradus.search.parser.grammar.CharacterRule;
import com.dell.doradus.search.parser.grammar.Context;
import com.dell.doradus.search.parser.grammar.Grammar;
import com.dell.doradus.search.parser.grammar.GrammarItem;
import com.dell.doradus.search.parser.grammar.GrammarRule;
import com.dell.doradus.search.parser.grammar.GrammarToken;
import com.dell.doradus.search.parser.grammar.Keyword;
import com.dell.doradus.search.parser.grammar.ListRule;
import com.dell.doradus.search.parser.grammar.Semantic;
import com.dell.doradus.search.parser.grammar.SequenceRule;
import com.dell.doradus.search.parser.grammar.StringToken;
import com.dell.doradus.search.parser.grammar.SwitchRule;
import com.dell.doradus.search.parser.grammar.Token;

public class DoradusSearchQueryGrammar {

    public static String SearchQueryGrammar = "SearchQuery";
    public static String AggregationQueryGrammar = "AggregationGroup";
    public static String AggregationMetricGrammar = "AggregationMetric";
    public static String StatisticMetricGrammar = "StatisticMetric";
    public static String StatisticQueryGrammar = "StatisticQuery";
    public static String StatisticParameterGrammar = "StatisticParameter";
    public static String FieldSetGrammar = "FieldSetGrammar";
    public static String SortOrderGrammar = "SortOrderGrammar";

    public static String SkipWhitespace = "SkipWhitespaces";
    public static String SkipWhitespaceOptionRule = "SkipWhitespaceRule";
    public static String NotSkipWhitespaceOptionRule = "NotSkipWhitespaceRule";


    private static Hashtable<String, GrammarRule> grammars;

    public static GrammarRule GetGrammar(String name) {
        if (grammars == null)
            grammars = getGrammar();
        return grammars.get(name);
    }

    public static GrammarRule GetGrammar() {
        return GetGrammar(SearchQueryGrammar);
    }

    private static Hashtable<String, GrammarRule> getGrammar() {

        // Lucene reserved characters + - && || ! ( ) { } [ ] ^ " ~ * ? : \
        // Do we need the same set?
        // Our allowed set for term value is  LetterOrDigit ? *
        // For other characters use quoted strings
        //Lexem WORD = new Lexem(Grammar.CharacterType.LetterOrDigit, "?*");

        Semantic processEscape = new Semantic("processEscape") {
            public Context Match(Context context) {
                GrammarItem item = context.items.get(context.items.size() - 1);
                char c = (char) Integer.parseInt(item.getValue().substring(2), 16);
                item.setValue("" + c);
                return context;
            }
        };

        Semantic processEscapeChar = new Semantic("processEscapeChar") {
            public Context Match(Context context) {
                GrammarItem item = context.items.get(context.items.size() - 1);
                char c = item.getValue().charAt(1);
                switch (c) {
                    case 't':
                        c = '\t';
                        break;
                    case 'b':
                        c = '\b';
                        break;
                    case 'n':
                        c = '\n';
                        break;
                    case 'r':
                        c = '\r';
                        break;
                    case '\f':
                        c = '\f';
                        break;
                    case '\'':
                        c = '\'';
                        break;
                    case '"':
                        c = '\"';
                        break;

                }

                item.setValue("" + c);
                return context;
            }
        };

        Semantic RemoveQuotes = new Semantic("RemoveQuotes") {
            public Context Match(Context context) {
                GrammarItem item = context.items.get(context.items.size() - 1);

                String newValue = item.getValue().substring(1, item.getValue().length() - 1);
                item.setValue(newValue);
                return context;
            }
        };

        GrammarToken PLUS = new GrammarToken("+");
        GrammarToken MINUS = new GrammarToken("-");
        GrammarToken COLON = new GrammarToken(":");
        GrammarToken STAR = new GrammarToken("*");
        GrammarToken DIVIDE = new GrammarToken("/");
        GrammarToken LEFTPAREN = new GrammarToken("(");
        GrammarToken RIGHTPAREN = new GrammarToken(")");


        GrammarToken LEFTBRACKET = new GrammarToken("[");
        GrammarToken RIGHTBRACKET = new GrammarToken("]");

        GrammarToken LEFTBRACE = new GrammarToken("{");
        GrammarToken RIGHTBRACE = new GrammarToken("}");
        GrammarToken TO = new GrammarToken("TO");

        GrammarToken LESS = new GrammarToken("<");
        GrammarToken LESSEQUAL = new GrammarToken("<=");
        GrammarToken EQUAL = new GrammarToken("=");
        GrammarToken GREATER = new GrammarToken(">");
        GrammarToken GREATEREQUAL = new GrammarToken(">=");
        GrammarToken COMMA = new GrammarToken(",");
        GrammarToken TRANSITIVE = new GrammarToken("^");
        GrammarToken DOT = new GrammarToken(".");

        GrammarToken ESCAPE = new GrammarToken("\\");
        GrammarToken ESCAPEU = new GrammarToken("u");
        GrammarToken UNDERSCORE = new GrammarToken("_");

        GrammarToken QUESTION = new GrammarToken("?");
        GrammarToken FETCH = new GrammarToken("@");
        GrammarToken DIES = new GrammarToken("#");
        GrammarToken IN = new GrammarToken("IN");
        GrammarToken LOWER = new GrammarToken("LOWER");
        GrammarToken UPPER = new GrammarToken("UPPER");

        GrammarToken SLASH = new GrammarToken("/");

        SwitchRule ReservedEscapeCharacter = new SwitchRule("ReservedEscapeCharacter",
                new GrammarToken("t"), new GrammarToken("b"), new GrammarToken("n"),
                new GrammarToken("r"), new GrammarToken("f"), new GrammarToken("'"),
                new GrammarToken("\""), new GrammarToken("\\")
        );

        Token UnicodeCharacter = new Token(Grammar.Rule("UnicodeCharacter", ESCAPE, ESCAPEU, Grammar.HexDigit, Grammar.HexDigit, Grammar.HexDigit, Grammar.HexDigit));

        GrammarRule EscapeSequence = new SwitchRule(SwitchRule.First, "EscapeSequence",
                Grammar.Rule(UnicodeCharacter, processEscape),
                Grammar.Rule(new Token(Grammar.Rule("esc", ESCAPE, ReservedEscapeCharacter)), processEscapeChar)
        );

        GrammarToken SINGLEQUOTE = new GrammarToken("'");
        GrammarToken DOUBLEQUOTE = new GrammarToken("\"");

        SwitchRule SingleQuoteStringCharacters = new SwitchRule("SingleQuoteStringCharacters");

        SingleQuoteStringCharacters.setMode(SwitchRule.First);
        SingleQuoteStringCharacters.body = Grammar.asRule(
                Grammar.Rule(EscapeSequence, Grammar.MustMatchAction, SingleQuoteStringCharacters),
                Grammar.Rule(Grammar.ExceptChar('\''), SingleQuoteStringCharacters),
                Grammar.emptyRule
        );

        SwitchRule DoubleQuoteStringCharacters = new SwitchRule("DoubleQuoteStringCharacters");

        DoubleQuoteStringCharacters.body = Grammar.asRule(
                Grammar.Rule(EscapeSequence, DoubleQuoteStringCharacters),
                Grammar.Rule(Grammar.ExceptChar('"'), DoubleQuoteStringCharacters),
                //Grammar.Rule(DOUBLEQUOTE, DOUBLEQUOTE, DOUBLEQUOTE, DoubleQuoteStringCharacters),
                Grammar.emptyRule
        );

        GrammarRule SingleQuotedString = Grammar.Rule("SingleQuotedString",
                new Token(Grammar.Rule(SINGLEQUOTE, SingleQuoteStringCharacters, SINGLEQUOTE)), RemoveQuotes);

        GrammarRule DoubleQuotedString = Grammar.Rule("DoubleQuotedString",
                new Token(Grammar.Rule(DOUBLEQUOTE, DoubleQuoteStringCharacters, DOUBLEQUOTE)), RemoveQuotes);

        boolean optimizeStringLiterals = true;

        if (optimizeStringLiterals) {
            DoubleQuotedString =  new StringToken("DStringToken", '"');
            SingleQuotedString =  new StringToken("SStringToken", '\'');
        }

        GrammarRule FieldNameLiteralStart = new SwitchRule(SwitchRule.First, "FieldNameLiteralStart",
                Grammar.Letter,
                Grammar.Digit,
                MINUS,
                STAR,
                UNDERSCORE,
                QUESTION,
                FETCH,
                DIES
        );

        //TODO Optimize -
        GrammarRule FieldNameLiteralMiddle = new SwitchRule("FieldNameLiteralMiddle",
                FieldNameLiteralStart,
                MINUS
        );

        GrammarRule LiteralStart = new SwitchRule(SwitchRule.First, "LiteralStart",
                FieldNameLiteralStart,
                EscapeSequence
        );

        GrammarRule LiteralMiddle = new SwitchRule("LiteralMiddle",
                LiteralStart,
                MINUS
        );

        SwitchRule LiteralContinue = new SwitchRule("LiteralContinue");
        LiteralContinue.optionalMode=true;

        LiteralContinue.body = Grammar.asRule(
                Grammar.Rule(LiteralMiddle, LiteralContinue),
                Grammar.emptyRule
        );

        GrammarRule TimeZoneCharacter = new SwitchRule(SwitchRule.First, "TimeZoneCharacter",
                LiteralMiddle,
                PLUS,
                SLASH
        );


        SequenceRule  TimeZoneLiteral = new SequenceRule("TimeZoneLiteral");

        GrammarRule TimeZoneLiteralContinue = new SwitchRule("TimeZoneLiteralContinue",
                Grammar.Rule(TimeZoneCharacter, TimeZoneLiteral),
                Grammar.emptyRule
        );

        TimeZoneLiteral.body = Grammar.asRule(
                TimeZoneCharacter, TimeZoneLiteralContinue
        );

        //TimeZoneLiteral.body = Grammar.asRule(
        //        Grammar.Rule(TimeZoneCharacter, TimeZoneLiteralContinue),
        //        Grammar.emptyRule
        //);

        Token TimeZoneToken  = new Token(TimeZoneLiteral);

        SwitchRule FieldNameLiteralContinue = new SwitchRule("FieldNameLiteralContinue");

        FieldNameLiteralContinue.body = Grammar.asRule(
                Grammar.Rule(FieldNameLiteralMiddle, FieldNameLiteralContinue),
                Grammar.emptyRule
        );

        GrammarRule Literal = Grammar.Rule("Literal", LiteralStart, LiteralContinue);

        GrammarRule FieldNameLiteral = Grammar.Rule("FieldNameLiteral", FieldNameLiteralStart, FieldNameLiteralContinue);

        GrammarRule WORD = new Token(Literal);

        boolean optimizeLiterals = true;

        if (optimizeLiterals)
            WORD = Grammar.WordLiteral;

        Token FIELDNAMEWORD = new Token(FieldNameLiteral);

        SequenceRule NumberLiteral = new SequenceRule("NumberLiteral");

        GrammarRule NumberLiteralContinue = new SwitchRule("NumberLiteralContinue",
                NumberLiteral,
                Grammar.emptyRule
        );

        NumberLiteral.body = Grammar.asRule( Grammar.Digit, NumberLiteralContinue);

        Token NUMBER = new Token(NumberLiteral);

        Keyword OR = new Keyword("OR", WORD);
        Keyword AND = new Keyword("AND", WORD);
        Keyword NOT = new Keyword("NOT", WORD);

        Keyword ANY = new Keyword("ANY", WORD);
        Keyword ALL = new Keyword("ALL", WORD);
        Keyword NONE = new Keyword("NONE", WORD);

        Keyword IS = new Keyword("IS", WORD);
        Keyword NULL = new Keyword("NULL", WORD);

        Keyword YEAR = new Keyword("YEAR", WORD);

        Keyword WEEK = new Keyword("WEEK", WORD);
        Keyword QUARTER = new Keyword("QUARTER", WORD);
        Keyword MOHTH = new Keyword("MONTH", WORD);
        Keyword DAY = new Keyword("DAY", WORD);
        Keyword HOUR = new Keyword("HOUR", WORD);
        Keyword MINUTE = new Keyword("MINUTE", WORD);
        Keyword SECOND = new Keyword("SECOND", WORD);

        Keyword WHERE = new Keyword("WHERE", WORD);
        Keyword TRUNCATE = new Keyword("TRUNCATE", WORD);
        Keyword BATCH = new Keyword("BATCH", WORD);
        Keyword SETS = new Keyword(SemanticNames.SETS, WORD);

        Keyword TOP = new Keyword("TOP", WORD);
        Keyword BOTTOM = new Keyword("BOTTOM", WORD);
        Keyword FIRST = new Keyword("FIRST", WORD);
        Keyword LAST = new Keyword("LAST", WORD);
        GrammarToken GMT = new GrammarToken("GMT");

        Keyword MIN = new Keyword("MIN", WORD, false);
        Keyword MAX = new Keyword("MAX", WORD, false);
        Keyword COUNT = new Keyword("COUNT", WORD, false);
        Keyword MINCOUNT = new Keyword("MINCOUNT", WORD, false);
        Keyword MAXCOUNT = new Keyword("MAXCOUNT", WORD, false);
        Keyword SUM = new Keyword("SUM", WORD, false);
        Keyword AVERAGE = new Keyword("AVERAGE", WORD, false);
        Keyword DISTINCT = new Keyword("DISTINCT", WORD, false);
        Keyword DATEDIFF = new Keyword("DATEDIFF", WORD, false);
        Keyword TERMS = new Keyword("TERMS", WORD, false);
        Keyword GROUP = new Keyword("GROUP", WORD, false);
        Keyword EXCLUDE = new Keyword("EXCLUDE", WORD, false);
        Keyword INCLUDE = new Keyword("INCLUDE", WORD, false);
        Keyword AS = new Keyword("AS", WORD, false);

        Keyword Period = new Keyword("PERIOD", WORD, false);
        Keyword Today = new Keyword("TODAY", WORD, false);
        Keyword LastDay = new Keyword("LASTDAY", WORD, false);

        Keyword ThisHour = new Keyword("THISHOUR", WORD, false);
        Keyword LastHour = new Keyword("LASTHOUR", WORD, false);
        Keyword ThisMinute = new Keyword("THISMINUTE", WORD, false);
        Keyword LastMinute = new Keyword("LASTMINUTE", WORD, false);
        Keyword ThisWeek = new Keyword("THISWEEK", WORD, false);
        Keyword LastWeek = new Keyword("LASTWEEK", WORD, false);
        Keyword ThisMonth = new Keyword("THISMONTH", WORD, false);
        Keyword LastMonth = new Keyword("LASTMONTH", WORD, false);
        Keyword ThisYear = new Keyword("THISYEAR", WORD, false);
        Keyword LastYear = new Keyword("LASTYEAR", WORD, false);

        Keyword Now = new Keyword("NOW", WORD, false);

        Keyword Minute = new Keyword("MINUTE", WORD, false);
        Keyword Hour = new Keyword("HOUR", WORD, false);
        Keyword Day = new Keyword("DAY", WORD, false);
        Keyword Week = new Keyword("WEEK", WORD, false);
        Keyword Month = new Keyword("MONTH", WORD, false);
        Keyword Year = new Keyword("YEAR", WORD, false);

        Keyword Minutes1 = new Keyword("MINUTES", WORD, false);
        Keyword Hours1 = new Keyword("HOURS", WORD, false);
        Keyword Days1 = new Keyword("DAYS", WORD, false);
        Keyword Weeks1 = new Keyword("WEEKS", WORD, false);
        Keyword Months1 = new Keyword("MONTHS", WORD, false);
        Keyword Years1 = new Keyword("YEARS", WORD, false);

        CharacterRule CharE = new CharacterRule('E');
        CharacterRule Chare = new CharacterRule('e');

        GrammarRule Exponent = new SwitchRule("Exponent",
                CharE,
                Chare
        );

        //for option skip or not skip whitespaces
        GrammarRule SkipWhiteSpacesRule = new SwitchRule("AllowedWhitespaces",
                Grammar.WhiteSpaces,
                Grammar.emptyRule
        );

        GrammarRule OptWhiteSpaces = new SwitchRule("OptWhiteSpaces",
                Grammar.WhiteSpaces,
                Grammar.emptyRule
        );

        SequenceRule SpaceSeparatedTermList = Grammar.Rule("TermList");

        GrammarRule TermListContinue = new SwitchRule("TermListContinue",
                Grammar.Rule(Grammar.WhiteSpaces, Grammar.Semantic("ImpliedOr"), SpaceSeparatedTermList),
                Grammar.emptyRule
        );

        GrammarRule StringLiteral = new SwitchRule("StringLiteral",
                SingleQuotedString,
                DoubleQuotedString
        );

        GrammarRule Term = new SwitchRule("Term",
                WORD,
                StringLiteral
        );


        GrammarRule PlusMinus = new SwitchRule("PlusMinus",
                PLUS,
                MINUS
        );

        GrammarRule ExponentField = new SwitchRule("ExponentField",
                Grammar.Rule(Exponent,  PlusMinus, NUMBER),
                Grammar.Rule(Exponent, NUMBER),
                Grammar.emptyRule
        );

        GrammarRule FloatPointNumberContinue = new SwitchRule("FloatPointNumberContinue",
                Grammar.Rule(DOT , NUMBER, ExponentField),
                ExponentField
        );


        GrammarRule FloatPointLiteral =  new SwitchRule("FloatPointNumber",
                Grammar.Rule(PlusMinus, NUMBER,  FloatPointNumberContinue),
                Grammar.Rule(NUMBER,  FloatPointNumberContinue)
        );

        GrammarRule FloatPointNumber = new Token(FloatPointLiteral);


        SequenceRule FieldName = Grammar.Rule("FieldName", FIELDNAMEWORD);

        SpaceSeparatedTermList.body = Grammar.asRule(Term, TermListContinue);

        GrammarRule RangeStart = new SwitchRule("RangeStart",
                LEFTBRACE,
                LEFTBRACKET
        );

        GrammarRule RangeEnd = new SwitchRule("RangeEnd",
                RIGHTBRACKET,
                RIGHTBRACE
        );

        SequenceRule PeriodDefinition = new SequenceRule("PeriodDefinition");

        SequenceRule NowFunction = new SequenceRule("NowFunction");


        SwitchRule TermOrFunction = new SwitchRule(SwitchRule.First, "TermOrFunction",
                NowFunction,
                FloatPointNumber,
                Term
        );

        GrammarRule RangeExpressionValue = Grammar.Rule("RangeExpressionValue", TermOrFunction);

        GrammarRule RangeExpression = new SwitchRule(SwitchRule.First, "RangeExpression",
                Grammar.Rule(OptWhiteSpaces, RangeStart, OptWhiteSpaces, Grammar.MustMatchAction, RangeExpressionValue, Grammar.WhiteSpaces, TO, Grammar.WhiteSpaces, RangeExpressionValue, OptWhiteSpaces, RangeEnd),
                PeriodDefinition
        );

        ListRule CommaSeparatedTermList = new ListRule("CommaSeparatedTermList",
                TermOrFunction,
                Grammar.Rule(OptWhiteSpaces, COMMA, OptWhiteSpaces)
                );

        GrammarRule EqualsExpressionValue = new SwitchRule(SwitchRule.First, "EqualsExpressionValue",
                RangeExpression,
                TermOrFunction,
                Grammar.Rule(LEFTPAREN, Grammar.MustMatchAction, OptWhiteSpaces, CommaSeparatedTermList, OptWhiteSpaces, RIGHTPAREN)
        );

        SequenceRule Query = Grammar.Rule("Query");

        GrammarRule LogicOperation = new SwitchRule("LogicOperation",
                OR,
                Grammar.Rule(AND),
                Grammar.Rule(Grammar.Semantic("ImpliedAnd"), NOT)
        );

        GrammarRule OptionalLogicOperation = new SwitchRule(SwitchRule.First, "OptionalLogicOperation",
                Grammar.Rule(LogicOperation, Grammar.MustMatchAction, OptWhiteSpaces),
                Grammar.Rule(Grammar.Semantic("ImpliedAnd"), Grammar.emptyRule)
        );

        SequenceRule ContainsExpression = new SequenceRule("ContainsExpression");


        GrammarRule Value = new SwitchRule("Value",
                Grammar.Rule(OptWhiteSpaces, LEFTPAREN, OptWhiteSpaces, ContainsExpression, OptWhiteSpaces, RIGHTPAREN),
                TermOrFunction,
                RangeExpression

        );

        GrammarRule ContainsExpressionContinue = new SwitchRule(SwitchRule.First, "ContainsExpressionContinue",
                Grammar.Rule(Grammar.WhiteSpaces, OptionalLogicOperation, ContainsExpression),
                Grammar.emptyRule
        );

        ContainsExpression.body = Grammar.asRule(Value, ContainsExpressionContinue);

        GrammarRule CompareOperation = new SwitchRule("CompareOperation",
                LESS,
                LESSEQUAL,
                GREATER,
                GREATEREQUAL
        );

        GrammarRule TimestampSubfield = new SwitchRule("TimestampSubfield",
                YEAR,
                MOHTH,
                DAY,
                HOUR,
                MINUTE,
                SECOND

        );

        GrammarRule DotSemantic = Grammar.Semantic("dotSemantic");
        GrammarRule LinkFunctionEndSemantic = Grammar.Semantic("linkFunctionEndSemantic");

        ////// General Transitive clause, used outside  ANY, ALL NONE

        //GrammarRule OptionalTransitiveField = new SwitchRule("OptionalTransitiveField",
        //        Grammar.Rule(DOT, FieldName, DotSemantic),
        //        Grammar.emptyRule
        //
        //);

        GrammarRule TransitiveLimit = Grammar.Rule("TransitiveLimit",
                LEFTPAREN, OptWhiteSpaces, Grammar.MustMatchAction, NUMBER, OptWhiteSpaces, RIGHTPAREN, Grammar.Semantic("transitiveValue")
        );


        GrammarRule OptionalTransitiveLimit = new SwitchRule("OptionalTransitiveLimit",
                TransitiveLimit,
                Grammar.emptyRule

        );

        GrammarRule OptionalTransitiveFunction = new SwitchRule(SwitchRule.First, "OptionalTransitiveFunction",
                Grammar.Rule(TRANSITIVE, Grammar.MustMatchAction, OptionalTransitiveLimit),
                Grammar.Rule(Grammar.emptyRule)

        );

        SequenceRule FieldPath = new SequenceRule("FieldPath");

        GrammarRule ExplicitFunctionType = new SwitchRule(SwitchRule.First, "ExplicitFunctionType",
                ANY,
                ALL,
                NONE
        );

        GrammarRule ExplicitQuantifierFunction = Grammar.Rule("ExplicitQuantifierFunction",
                ExplicitFunctionType, OptWhiteSpaces, LEFTPAREN, Grammar.MustMatchAction, OptWhiteSpaces,
                FieldPath, OptWhiteSpaces, RIGHTPAREN, LinkFunctionEndSemantic, OptWhiteSpaces);

        GrammarRule SearchCriteria = new SwitchRule(SwitchRule.First, "SearchCriteria",
                Grammar.Rule(OptWhiteSpaces, COLON, OptWhiteSpaces, Grammar.MustMatchAction, Value),
                Grammar.Rule(SkipWhiteSpacesRule, EQUAL, SkipWhiteSpacesRule, Grammar.MustMatchAction, EqualsExpressionValue),
                Grammar.Rule(SkipWhiteSpacesRule, IN, Grammar.SetTextValue("="), SkipWhiteSpacesRule, Grammar.MustMatchAction, EqualsExpressionValue),
                Grammar.Rule(OptWhiteSpaces, CompareOperation, Grammar.MustMatchAction, OptWhiteSpaces, TermOrFunction),
                Grammar.Rule(SkipWhiteSpacesRule, IS, SkipWhiteSpacesRule, Grammar.MustMatchAction, NULL)
        );


        GrammarRule FieldOrLinkOperation = new SwitchRule(SwitchRule.First, "FieldOrLinkOperation",
                SearchCriteria,
                Grammar.emptyRule
        );

        SequenceRule WhereQuery = Grammar.Rule("WhereQuery");

        GrammarRule WhereParenthesizedExpression = Grammar.Rule("WhereParenthesizedExpression",
                OptWhiteSpaces, LEFTPAREN, Grammar.MustMatchAction, Query, OptWhiteSpaces, RIGHTPAREN
        );

        GrammarRule WhereExpression = new SwitchRule(SwitchRule.First, "WhereExpression",
                NowFunction,
                Grammar.Rule(Term, FieldOrLinkOperation)
        );

        GrammarRule WhereClauseType = new SwitchRule(SwitchRule.First, "WhereClauseType",
                WhereParenthesizedExpression,
                WhereExpression
        );

        SwitchRule WhereClause = new SwitchRule("WhereClause");

        WhereClause.body = Grammar.asRule(
                Grammar.Rule(NOT, OptWhiteSpaces, Query),
                WhereClauseType
        );

        WhereQuery.body = Grammar.asRule(OptWhiteSpaces, Query);

        GrammarRule NumberRangeExpression = Grammar.Rule("NumberRangeExpression", OptWhiteSpaces,
                RangeStart, OptWhiteSpaces, Grammar.MustMatchAction, NUMBER, Grammar.WhiteSpaces, TO, Grammar.WhiteSpaces, NUMBER, OptWhiteSpaces, RangeEnd
        );

        GrammarRule CountLinkValue = new SwitchRule("CountLinkValue",
                NUMBER,
                NumberRangeExpression
        );

        GrammarRule CountExpressionContinue = new SwitchRule("CountExpressionContinue",
                Grammar.Rule(OptWhiteSpaces, EQUAL, Grammar.MustMatchAction, OptWhiteSpaces, CountLinkValue),
                Grammar.Rule(OptWhiteSpaces, CompareOperation, Grammar.MustMatchAction, OptWhiteSpaces, NUMBER),
                Grammar.Rule(OptWhiteSpaces, COLON, Grammar.MustMatchAction, OptWhiteSpaces, NumberRangeExpression)
        );

        SwitchRule WhereClauseEnd = new SwitchRule("WhereContinue");
        WhereClauseEnd.setMode(SwitchRule.First);

        GrammarRule StartingWhereClause = Grammar.Rule("StartingWhereClause",
                WHERE , Grammar.DropLexem, OptWhiteSpaces, LEFTPAREN, OptWhiteSpaces,
                Query, OptWhiteSpaces, RIGHTPAREN,  WhereClauseEnd
        );


        GrammarRule NextClause = new SwitchRule("NextClause",
                Grammar.Rule(Grammar.WhiteSpaces, OptionalLogicOperation, Query),
                Grammar.emptyRule
        );

        SwitchRule ExpressionContinue = new SwitchRule("ExpressionContinue");
        ExpressionContinue.setMode(SwitchRule.First);

        GrammarRule QueryWhere = Grammar.Rule("QueryWhere",
                WHERE, Grammar.MustMatchAction, OptWhiteSpaces,
                LEFTPAREN, Grammar.Semantic("("), OptWhiteSpaces, Grammar.MustMatchAction, Query, OptWhiteSpaces, RIGHTPAREN
        );

        SwitchRule FieldPathContinue = new SwitchRule("FieldPathContinue");
        GrammarRule FieldPathNext = Grammar.Rule("FieldPathNext", FieldName, OptionalTransitiveFunction, DotSemantic, FieldPathContinue);


        FieldPathContinue.body = Grammar.asRule(
                Grammar.Rule(DOT, Grammar.Semantic("WHERE_FILTER_START"),
                        QueryWhere, Grammar.Semantic("WHERE_FILTER_END"), Grammar.MustMatchAction, FieldPathContinue),
                Grammar.Rule(DOT, FieldPathNext),
                Grammar.emptyRule
        );

        FieldPath.body = Grammar.asRule(FieldName, OptionalTransitiveFunction, FieldPathContinue);

        SwitchRule WhereContinue = new SwitchRule("WhereContinue");
        WhereContinue.setMode(SwitchRule.First);

        List<GrammarRule> bodyw = new ArrayList<>();

            bodyw.add(Grammar.Rule(DOT, Grammar.Semantic("WHERE_SIBLING_START"), QueryWhere,
                    Grammar.Semantic("WHERE_SIBLING_END"), Grammar.MustMatchAction, WhereContinue));
            bodyw.add(Grammar.Rule( Grammar.Semantic(")"), Grammar.Semantic("SearchCriteriaStart"),  SearchCriteria ));
            bodyw.add(Grammar.Rule(Grammar.Semantic("WHERE_CONTINUE_START"), ExpressionContinue, Grammar.Semantic("WHERE_CONTINUE_END")));
            bodyw.add(Grammar.Rule(Grammar.Semantic(")"), Grammar.emptyRule));

        WhereContinue.body = bodyw;


        GrammarRule ExpressionContinueWithoutTransitive = new SwitchRule(SwitchRule.First, "ExpressionContinueWithoutTransitive",
                Grammar.Rule(DOT, TimestampSubfield, Grammar.MustMatchAction, Grammar.SetType("lexem"), DotSemantic, SearchCriteria),
                Grammar.Rule(DOT, QueryWhere , WhereContinue ),
                Grammar.Rule(DOT, ExplicitQuantifierFunction, DotSemantic, ExpressionContinue),
                Grammar.Rule(DOT, Grammar.MustMatchAction, FieldName, DotSemantic, ExpressionContinue),
                Grammar.Rule(SearchCriteria)

        );

        GrammarRule CountExpressionFieldPath = new SwitchRule(SwitchRule.First, "CountExpressionFieldPath",
                Grammar.Rule(WHERE, Grammar.DropLexem, OptWhiteSpaces, LEFTPAREN, Grammar.DropLexem,
                        OptWhiteSpaces, FieldPath, OptionalTransitiveFunction, OptWhiteSpaces, RIGHTPAREN, Grammar.DropLexem ),
                Grammar.Rule(FieldPath, OptionalTransitiveFunction)
        );

        GrammarRule CountExpression = Grammar.Rule("CountExpression",
                COUNT, Grammar.MustMatchAction, OptWhiteSpaces, LEFTPAREN,
                OptWhiteSpaces,  CountExpressionFieldPath, OptWhiteSpaces, RIGHTPAREN, LinkFunctionEndSemantic,
                OptWhiteSpaces, CountExpressionContinue
        );


        GrammarRule QuantifierToken = new SwitchRule("QuantifierToken", 
                new Keyword("EQUALS", WORD),
                new Keyword("DIFFERS", WORD),
                new Keyword("INTERSECTS", WORD),
                new Keyword("CONTAINS", WORD),
                new Keyword("DISJOINT", WORD)
        );
        
        GrammarRule EqualsExpression = Grammar.Rule("EqualsExpression",
                QuantifierToken, Grammar.MustMatchAction, OptWhiteSpaces, LEFTPAREN, Grammar.DropLexem, OptWhiteSpaces,
                CountExpressionFieldPath, OptWhiteSpaces, COMMA, Grammar.DropLexem, OptWhiteSpaces, CountExpressionFieldPath, RIGHTPAREN, Grammar.DropLexem
        );

        GrammarRule DCountExpression = Grammar.Rule("DCountExpression",
                new Keyword("DCOUNT", WORD), Grammar.MustMatchAction, OptWhiteSpaces, LEFTPAREN, Grammar.DropLexem, OptWhiteSpaces,
                CountExpressionFieldPath, OptWhiteSpaces, RIGHTPAREN, Grammar.DropLexem, CountExpressionContinue
        );
        
        
        
        /*
        List<GrammarRule> body = new ArrayList<>();
            body.add(Grammar.Rule(DOT, TimestampSubfield, Grammar.MustMatchAction, Grammar.SetType("lexem"), DotSemantic, SearchCriteria));
            body.add(Grammar.Rule(DOT, QueryWhere , WhereContinue ));
            body.add(Grammar.Rule(DOT, ExplicitQuantifierFunction, DotSemantic, ExpressionContinue));
            body.add(Grammar.Rule(DOT, Grammar.MustMatchAction, FieldName, DotSemantic, ExpressionContinue));
            //body.add( Grammar.Rule(TRANSITIVE, Grammar.MustMatchAction, OptionalTransitiveLimit, OptionalTransitiveField, SearchCriteria));
            body.add( Grammar.Rule(TRANSITIVE, Grammar.MustMatchAction, OptionalTransitiveLimit, ExpressionContinueWithoutTransitive));

            body.add(SearchCriteria);
         */

        List<GrammarRule> body = new ArrayList<>();
            body.add(ExpressionContinueWithoutTransitive);
            body.add(Grammar.Rule(TRANSITIVE, Grammar.MustMatchAction, OptionalTransitiveLimit, ExpressionContinueWithoutTransitive));
            body.add(SearchCriteria);

        ExpressionContinue.body = body;

        GrammarRule Expression = new SwitchRule(SwitchRule.First, "Expression",
                CountExpression,
                EqualsExpression,
                DCountExpression,
                StartingWhereClause,
                NowFunction,
                FloatPointNumber,
                Grammar.Rule( ExplicitQuantifierFunction, ExpressionContinue),
                Grammar.Rule( FieldName, ExpressionContinue),
                Term

        );

        WhereClauseEnd.body = Grammar.asRule(
                Grammar.Rule( DOT, Grammar.DropLexem, Grammar.Semantic("ImpliedAnd"), Expression  ),
                Grammar.emptyRule
        ) ;


        SwitchRule Clause = new SwitchRule(SwitchRule.First, "Clause",
                Grammar.Rule(NOT, OptWhiteSpaces, Query),
                Grammar.Rule(OptWhiteSpaces, LEFTPAREN, Grammar.MustMatchAction, OptWhiteSpaces, Query, OptWhiteSpaces, RIGHTPAREN),
                Expression
        );

        Query.body = Grammar.asRule(OptWhiteSpaces, Clause, Grammar.MustMatchAction, NextClause);

        GrammarRule DoradusQuery = Grammar.Rule("DoradusQuery",
                Query, SkipWhiteSpacesRule, Grammar.Semantic("EOF")
        );

        GrammarRule FloatPointNumberOrTerm = new SwitchRule(SwitchRule.First, "FloatPointNumberOrTerm",
                FloatPointNumber,
                Term
        );

        ListRule NumbersList = new ListRule("NumbersList",
                    Grammar.Rule(FloatPointNumberOrTerm, Grammar.SetType("BatchValue")),
                    Grammar.Rule(OptWhiteSpaces, COMMA, Grammar.SetType("ignore"), OptWhiteSpaces)
                );

        GrammarRule ExcludeItemValue = new SwitchRule(SwitchRule.First, "ExcludeItemValue",
                FloatPointNumber,
                Term,
                NULL
        );

        ListRule ExcludeNameList = new ListRule("ExcludeNameList",
                Grammar.Rule(ExcludeItemValue, Grammar.SetType("excludeValue")),
                Grammar.Rule(SkipWhiteSpacesRule, COMMA, Grammar.SetType("ignore"), SkipWhiteSpacesRule, Grammar.MustMatchAction)
        );

        SequenceRule AggregationFieldPath = new SequenceRule("AggregationFieldPath");

        GrammarRule ExcludeOrInclude = new SwitchRule("ExcludeOrInclude",
                Grammar.Rule(EXCLUDE, Grammar.SetType("ExcludeList")),
                Grammar.Rule(INCLUDE, Grammar.SetType("IncludeList"))
        );

        GrammarRule AggregationFieldPathEnd = Grammar.Rule("AggregationFieldPathEnd",
                DOT, ExcludeOrInclude,  Grammar.MustMatchAction, SkipWhiteSpacesRule, LEFTPAREN, Grammar.SetType("ignore"),
                SkipWhiteSpacesRule, ExcludeNameList, SkipWhiteSpacesRule,  RIGHTPAREN, Grammar.SetType("ignore")
        );

        GrammarRule AggregationFieldPathContinue = new SwitchRule(SwitchRule.First, "AggregationFieldPathContinue",
                Grammar.Rule(AggregationFieldPathEnd, AggregationFieldPathEnd), // Include + Exclude
                AggregationFieldPathEnd,
                Grammar.Rule(DOT, TimestampSubfield, Grammar.SetType(SemanticNames.TRUNCATE_SUBFIELD_VALUE)) ,
                Grammar.Rule(DOT, AggregationFieldPath),
                Grammar.emptyRule
        );

        SequenceRule Subfield = new SequenceRule("Subfield");

        GrammarRule SubfieldContinue = new SwitchRule(SwitchRule.First, "SubfieldContinue",
                Grammar.Rule(DOT, TimestampSubfield, Grammar.SetType(SemanticNames.TRUNCATE_SUBFIELD_VALUE)),
                Grammar.Rule(DOT, Subfield),
                Grammar.Rule(TRANSITIVE, Grammar.SetType("transitive"), LEFTPAREN, Grammar.DropLexem, NUMBER, Grammar.SetType("transitiveValue"), RIGHTPAREN, Grammar.DropLexem, DOT, Subfield),
                Grammar.Rule(TRANSITIVE, Grammar.SetType("transitive"), DOT, Subfield),
                Grammar.Rule(TRANSITIVE, Grammar.SetType("transitive") ),
                Grammar.emptyRule
        );

        Subfield.body = Grammar.asRule(WORD, SubfieldContinue);

        SwitchRule OptionalWhereClause = new SwitchRule("OptionalWhereClause");
        OptionalWhereClause.setMode(SwitchRule.First);

        List<GrammarRule> obody = new ArrayList<>();

        obody.add(Grammar.Rule(DOT,  WHERE, Grammar.MustMatchAction, OptWhiteSpaces, LEFTPAREN,
                OptWhiteSpaces, Query, OptWhiteSpaces, RIGHTPAREN, Grammar.Semantic("EOF"), Grammar.Semantic("ENDWHERE"), OptionalWhereClause ));
        obody.add(Grammar.emptyRule);

        OptionalWhereClause.body = obody;

        GrammarRule OptionalTransitiveClause = new SwitchRule(SwitchRule.First, "OptionalTransitiveClause",
                Grammar.Rule(TRANSITIVE, Grammar.SetType("transitive"), LEFTPAREN, Grammar.DropLexem, NUMBER, Grammar.SetType("transitiveValue"), RIGHTPAREN, Grammar.DropLexem),
                Grammar.Rule(TRANSITIVE, Grammar.SetType("transitive") ),
                Grammar.emptyRule
        );

        GrammarRule AggregationFieldName = Grammar.Rule("AggregationFieldName",
                WORD, OptionalTransitiveClause, Grammar.NotLexem("AS"), OptionalWhereClause
        );

        GrammarRule AggregationFieldCaseFunctionName = new SwitchRule(SwitchRule.First, "AggregationFieldCaseFunctionName",
                Grammar.Rule(LOWER, Grammar.SetType("LOWER")),
                Grammar.Rule(UPPER, Grammar.SetType("UPPER"))
        );

        SwitchRule AggregationFieldSubfieldPathWhere = new SwitchRule("AggregationFieldSubfieldPathWhere");

        SwitchRule AggregationFieldCaseFunctionClause = new SwitchRule("AggregationFieldCaseFunctionClause",
                Grammar.Rule(AggregationFieldCaseFunctionName, OptWhiteSpaces, LEFTPAREN,
                OptWhiteSpaces, AggregationFieldSubfieldPathWhere, OptWhiteSpaces, RIGHTPAREN),
                AggregationFieldSubfieldPathWhere
        );

        SequenceRule StopWordList = new SequenceRule("StopWordList");

        GrammarRule StopWordListContinue = new SwitchRule("StopWordListContinue",
                Grammar.Rule(Grammar.WhiteSpaces, Grammar.MustMatchAction, StopWordList),
                Grammar.emptyRule
        );

        StopWordList.body = Grammar.asRule(WORD, Grammar.SetType("stopValue"), StopWordListContinue);

        GrammarRule OptionalStopWordList = new SwitchRule(SwitchRule.First, "OptionalStopWordList",
                Grammar.Rule(OptWhiteSpaces,
                        COMMA, Grammar.SetType("ignore"), OptWhiteSpaces,
                        LEFTPAREN, Grammar.SetType("ignore"), OptWhiteSpaces, StopWordList, OptWhiteSpaces,
                        RIGHTPAREN, Grammar.SetType("ignore")),
                Grammar.Rule(Grammar.emptyRule, Grammar.Semantic("stopWordAny"))
        );

        SwitchRule AggregationQueryTermsClause = new SwitchRule(SwitchRule.First, "AggregationQueryTermsClause",
                Grammar.Rule(OptWhiteSpaces, TERMS, Grammar.SetType("TERMS"), Grammar.MustMatchAction, OptWhiteSpaces,
                        LEFTPAREN, Grammar.SetType("ignore"),
                        OptWhiteSpaces, AggregationFieldCaseFunctionClause,
                        OptionalStopWordList, OptWhiteSpaces,
                        RIGHTPAREN, Grammar.SetType("ignore")),
                AggregationFieldCaseFunctionClause
        );

        GrammarRule AggregationFieldDateFunctionName = new SwitchRule("AggregationFieldDateFunctionName",
                TimestampSubfield,
                WEEK,
                QUARTER
        );

        /*
        TimeZone (Java 2 Platform SE v1.4.2)

        CustomID:
            GMT Sign Hours : Minutes
            GMT Sign Hours Minutes
            GMT Sign Hours

        Sign: one of
            + -

        Hours:
            Digit
            Digit Digit

        Minutes:
            Digit Digit

        Digit: one of
        0 1 2 3 4 5 6 7 8 9
        */

        GrammarRule Hours = new SwitchRule("Hours",
                Grammar.Digit,
                Grammar.Rule(Grammar.Digit, Grammar.Digit)
        );

        GrammarRule Minutes = Grammar.Rule("Minutes",
                Grammar.Rule(Grammar.Digit, Grammar.Digit)
        );

        Token GmtValue4 = new Token(
            Grammar.Rule(Grammar.Digit, Grammar.Digit, Grammar.Digit, Grammar.Digit)
        );
        Token GmtValue3 = new Token(
                Grammar.Rule(Grammar.Digit, Grammar.Digit, Grammar.Digit)
        );
        Token GmtValue2 = new Token(
                Grammar.Rule(Grammar.Digit, Grammar.Digit)
        );
        Token GmtValue1 = new Token(
                Grammar.Rule(Grammar.Digit)
        );

        Token HoursColonMinutes = new Token(
                Grammar.Rule(Hours, COLON, Minutes)
        );

        Token GmtValue = new Token(
                new SwitchRule(SwitchRule.First, "GmtValue",
                    Grammar.Rule(HoursColonMinutes),
                    Grammar.Rule(GmtValue4),
                    Grammar.Rule(GmtValue3),
                    Grammar.Rule(GmtValue2),
                    Grammar.Rule(GmtValue1) )
        );

        Token GmtSignValue = new Token(Grammar.Rule("GmtSignValue",
                PlusMinus, GmtValue)
        );

        GrammarRule TimeZoneValue = Grammar.Rule("TimeZoneValue",
                    GMT, Grammar.DropLexem, GmtSignValue
        );

        SwitchRule GmtTime =
                new SwitchRule("GmtOption",
                        Grammar.Rule(GMT, GmtSignValue),
                        TimeZoneValue
        );

        SwitchRule TimeZoneDefinition =
                new SwitchRule(SwitchRule.First, "TimeZoneDefinition",
                        Grammar.Rule(TimeZoneValue, Grammar.SetType("TimeZoneValue")),
                        Grammar.Rule(SINGLEQUOTE, Grammar.DropLexem, TimeZoneValue, Grammar.SetType("TimeZoneValue"),
                                SINGLEQUOTE, Grammar.DropLexem ),
                        Grammar.Rule(DOUBLEQUOTE, Grammar.DropLexem, TimeZoneValue, Grammar.SetType("TimeZoneValue"),
                                DOUBLEQUOTE, Grammar.DropLexem ),
                        Grammar.Rule(StringLiteral, Grammar.SetType(SemanticNames.TIMEZONEDISPLAYNAME)),
                        Grammar.Rule(TimeZoneToken, Grammar.SetType(SemanticNames.TIMEZONEDISPLAYNAME))

        );

        SwitchRule TimeZone =
                new SwitchRule(SwitchRule.First, "TimeZone",
                        Grammar.Rule(GmtTime, Grammar.SetType("TimeZoneValue")),
                        Grammar.Rule(SINGLEQUOTE, Grammar.DropLexem, GmtTime, Grammar.SetType("TimeZoneValue"),
                                SINGLEQUOTE, Grammar.DropLexem ),
                        Grammar.Rule(DOUBLEQUOTE, Grammar.DropLexem, GmtTime, Grammar.SetType("TimeZoneValue"),
                                DOUBLEQUOTE, Grammar.DropLexem ),
                        Grammar.Rule(StringLiteral, Grammar.SetType(SemanticNames.TIMEZONEDISPLAYNAME)),
                        Grammar.Rule(TimeZoneToken, Grammar.SetType(SemanticNames.TIMEZONEDISPLAYNAME))

        );

        GrammarRule OptionalTimeZone = new SwitchRule("OptionalTimeZone",
                Grammar.Rule(COMMA, Grammar.SetType("ignore"), OptWhiteSpaces, TimeZone),
                Grammar.emptyRule

        );

        GrammarRule OptionalPeriodTimeZone = new SwitchRule(SwitchRule.First, "OptionalPeriodTimeZone",
                Grammar.Rule(
                        OptWhiteSpaces, TimeZoneDefinition ),
                Grammar.SetType("PeriodGMT") //Grammar.emptyRule

        );

        GrammarRule ThisPeriodUnits = new SwitchRule(SwitchRule.First, "ThisPeriodUnits",
                Grammar.Rule(ThisMinute),
                Grammar.Rule(ThisHour),
                Grammar.Rule(Today),
                Grammar.Rule(ThisWeek),
                Grammar.Rule(ThisMonth),
                Grammar.Rule(ThisYear)
        );

        GrammarRule LastPeriodUnits = new SwitchRule(SwitchRule.First, "LastPeriodUnits",
                Grammar.Rule(LastMinute),
                Grammar.Rule(LastHour),
                Grammar.Rule(LastDay),
                Grammar.Rule(LastWeek),
                Grammar.Rule(LastMonth),
                Grammar.Rule(LastYear)
        );

        GrammarRule OptionalLastPeriodValue = new SwitchRule(SwitchRule.First, "OptionalLastPeriodValue",
                Grammar.Rule(OptWhiteSpaces, LEFTPAREN,  Grammar.SetType("ignore"),
                        OptWhiteSpaces, NUMBER , Grammar.SetType(SemanticNames.LastPeriodValue) , OptWhiteSpaces, RIGHTPAREN , Grammar.SetType("ignore")),
                Grammar.emptyRule

        );

        GrammarRule PeriodUnits = new SwitchRule(SwitchRule.First, "PeriodUnits",
                Grammar.Rule(LastPeriodUnits, Grammar.SetType(SemanticNames.LastPeriodUnits), OptionalLastPeriodValue),
                Grammar.Rule(ThisPeriodUnits, Grammar.SetType(SemanticNames.ThisPeriodUnits)) ,
                Grammar.Error("Wrong syntax: Expected Last* or This*")
        );

        GrammarRule PeriodDefinitionContinue = new SwitchRule(SwitchRule.First, "PeriodDefinitionContinue",
                Grammar.Rule( DOT , Grammar.SetType("ignore"),
                         PeriodUnits, Grammar.Semantic("CalculatePeriod")),
                Grammar.Error("error", "Wrong syntax for Period definition")
        );

        PeriodDefinition.body = Grammar.asRule(
                OptWhiteSpaces, Period, Grammar.SetType("ignore"),
                OptWhiteSpaces, LEFTPAREN,  Grammar.SetType("ignore"),
                Grammar.MustMatchAction, OptionalPeriodTimeZone, OptWhiteSpaces, RIGHTPAREN , Grammar.SetType("ignore"),
                PeriodDefinitionContinue

        );

        GrammarRule NowUnits = new SwitchRule("NowUnits",
                Minute,
                Hour,
                Day,
                Week,
                Month,
                Year,
                Minutes1,
                Hours1,
                Days1,
                Weeks1,
                Months1,
                Years1

        );

        GrammarRule NowUnitsDefinition = Grammar.Rule("NowUnitsDefinition",
                NowUnits , Grammar.SetType(SemanticNames.NowUnits)
        );

        GrammarRule SignNumber = new SwitchRule("SignNumber",
                Grammar.Rule( PLUS, Grammar.SetType("ignore"), NUMBER, Grammar.SetType(SemanticNames.PositiveNumber)),
                Grammar.Rule( MINUS, Grammar.SetType("ignore"), NUMBER, Grammar.SetType(SemanticNames.NegativeNumber))
        );

        GrammarRule NowDefinitionContinue = new SwitchRule( "NowDefinitionContinue",
                Grammar.Rule( TimeZoneDefinition, Grammar.WhiteSpaces, SignNumber , Grammar.WhiteSpaces, NowUnitsDefinition),
                Grammar.Rule( SignNumber , Grammar.WhiteSpaces, NowUnitsDefinition ),
                TimeZoneDefinition,
                Grammar.emptyRule

        );

        NowFunction.body = Grammar.asRule(
                OptWhiteSpaces, Now , Grammar.SetType(SemanticNames.Now),  OptWhiteSpaces , LEFTPAREN, Grammar.SetType("ignore"),
                OptWhiteSpaces, NowDefinitionContinue,  OptWhiteSpaces, RIGHTPAREN,  Grammar.SetType("ignore") , Grammar.Semantic(SemanticNames.CalculateNow)
                );

        GrammarRule AggregationQueryTruncateClause = new SwitchRule("AggregationQueryTruncateClause",
                Grammar.Rule(TRUNCATE, Grammar.SetType("TRUNCATE"), Grammar.MustMatchAction,
                        OptWhiteSpaces, LEFTPAREN, OptWhiteSpaces, AggregationQueryTermsClause, OptWhiteSpaces, COMMA,
                        Grammar.SetType("ignore"), OptWhiteSpaces, AggregationFieldDateFunctionName, Grammar.SetType("TruncateValue"), OptWhiteSpaces, OptionalTimeZone, OptWhiteSpaces, RIGHTPAREN),
                Grammar.Rule(AggregationQueryTermsClause, OptWhiteSpaces, Grammar.Semantic("EOAE"))
        );


        GrammarRule BatchexCase = new SwitchRule("BatchexCase",
        		Query, OptWhiteSpaces, AS, OptWhiteSpaces, Term, OptWhiteSpaces
        );
        ListRule BatchexList = new ListRule("BatchexList",
                BatchexCase,
                Grammar.Rule(OptWhiteSpaces, COMMA, Grammar.SetType(SemanticNames.NEXTSETS), OptWhiteSpaces)
                );
        
        GrammarRule AggregationQueryBatchClause = new SwitchRule("AggregationQueryBatchClause",
                Grammar.Rule(SETS, Grammar.SetType(SemanticNames.SETS), Grammar.MustMatchAction, OptWhiteSpaces,
                        LEFTPAREN, Grammar.SetType("ignore"), OptWhiteSpaces, BatchexList, OptWhiteSpaces, RIGHTPAREN, Grammar.SetType(SemanticNames.ENDSETS)),
                Grammar.Rule(BATCH, Grammar.SetType("BATCH"), Grammar.MustMatchAction, OptWhiteSpaces,
                        LEFTPAREN, OptWhiteSpaces, AggregationQueryTruncateClause,
                        OptWhiteSpaces, COMMA, Grammar.SetType("ignore"), OptWhiteSpaces,
                        NumbersList, OptWhiteSpaces, RIGHTPAREN),
                AggregationQueryTruncateClause
                );

        GrammarRule TopBottom = new SwitchRule("TopBottom",
                TOP,
                BOTTOM,
                FIRST,
                LAST
        );

        SequenceRule AggregationQueryList = new SequenceRule("AggregationQueryList");

        GrammarRule AggregationQueryListContinue = new SwitchRule("AggregationQueryListContinue",
                Grammar.Rule(OptWhiteSpaces, COMMA, OptWhiteSpaces, AggregationQueryList),
                Grammar.emptyRule
        );

        GrammarRule AggregationQuery = new SwitchRule("AggregationQuery",
                AggregationQueryBatchClause,
                Grammar.Rule(TopBottom, Grammar.MustMatchAction, Grammar.SetType("topbottom"),
                        OptWhiteSpaces, LEFTPAREN, OptWhiteSpaces, NUMBER, Grammar.SetType("topbottomvalue"),
                        OptWhiteSpaces, COMMA, Grammar.SetType("ignore"),
                        OptWhiteSpaces, AggregationQueryBatchClause, OptWhiteSpaces, RIGHTPAREN)
        );

        GrammarRule AggregationAliasName = new SwitchRule(SwitchRule.First, "AggregationAliasName",
                Grammar.Rule(SkipWhiteSpacesRule, AS, Grammar.SetType("ignore"), SkipWhiteSpacesRule, Term, Grammar.SetType("alias") ),
                Grammar.Rule(DOT, Grammar.DropLexem,  AS, Grammar.SetType("ignore"),
                        LEFTPAREN, Grammar.DropLexem, OptWhiteSpaces, Term, Grammar.SetType("alias"), OptWhiteSpaces, RIGHTPAREN),
                Grammar.emptyRule
        );

        AggregationQueryList.body = Grammar.asRule(OptWhiteSpaces,
                AggregationQuery, AggregationAliasName, OptWhiteSpaces, AggregationQueryListContinue
        );

        SwitchRule AggregationGroupValues = new SwitchRule(SwitchRule.First, "AggregationGroupValues",
                STAR,
                AggregationQueryList
        );
        GrammarRule AggregationGroup = Grammar.Rule("AggregationGroup",
                OptWhiteSpaces, GROUP, OptWhiteSpaces, LEFTPAREN,
                OptWhiteSpaces, AggregationGroupValues, OptWhiteSpaces, RIGHTPAREN, Grammar.SetType("endGroup")
        );

        SequenceRule AggregationGroupList = new SequenceRule("AggregationGroupList");

        SwitchRule AggregationGroupListContinue = new SwitchRule("AggregationGroupListContinue",
                Grammar.Rule(OptWhiteSpaces, COMMA, Grammar.SetType("newGroup"), OptWhiteSpaces, AggregationGroupList),
                Grammar.emptyRule
        );

        AggregationGroupList.body = Grammar.asRule(
                AggregationGroup, OptWhiteSpaces, AggregationGroupListContinue
        );

        AggregationFieldPath.body = Grammar.asRule(
                AggregationFieldName, AggregationFieldPathContinue
        );

        GrammarRule AggregationClause = new SwitchRule(SwitchRule.First, "AggregationClause",
                AggregationGroupList,
                AggregationQueryList
        );

        //Aggregation metric field name (exclude * and ? )
        GrammarRule AggregationWordStart = new SwitchRule(SwitchRule.First, "AggregationWordStart",
                Grammar.Letter,
                Grammar.Digit,
                UNDERSCORE,
                FETCH,
                DIES
                //EscapedChar  don not allow escape chars in field names
        );

        SwitchRule AggregationWordNext = new SwitchRule("AggregationWordNext");

        AggregationWordNext.body = Grammar.asRule(
                Grammar.Rule(AggregationWordStart, AggregationWordNext),
                Grammar.emptyRule
        );

        SwitchRule StatisticMetricFunctionName = new SwitchRule("StatisticMetricFunctionName",
                SUM,
                AVERAGE,
                COUNT,
                MINCOUNT,
                MAXCOUNT
        );

        SwitchRule AggregationMetricFunctionName = new SwitchRule("AggregationMetricFunctionName",
                MIN,
                MAX,
                DISTINCT,
                StatisticMetricFunctionName
        );

        SwitchRule MetricFunctionBinary = new SwitchRule("MetricFunctionBinary",
                new Keyword("ROUNDUP", WORD)
        );
        
        
        AggregationFieldSubfieldPathWhere.body = Grammar.asRule(
                Grammar.Rule(WHERE , OptWhiteSpaces, LEFTPAREN, OptWhiteSpaces,
                        Query, OptWhiteSpaces, RIGHTPAREN, Grammar.Semantic("EOF"), Grammar.Semantic("ENDWHERE"),
                        DOT, Grammar.SetType("DOT") , AggregationFieldSubfieldPathWhere  ),
                Grammar.Rule(MetricFunctionBinary, Grammar.SetType("MetricFunctionBinary"), OptWhiteSpaces, LEFTPAREN, OptWhiteSpaces,
                        AggregationFieldSubfieldPathWhere, OptWhiteSpaces,
                        COMMA, Grammar.SetType("MetricFunctionComma"), OptWhiteSpaces, Term, Grammar.SetType("MetricFunctionParameter"), RIGHTPAREN),
                AggregationFieldPath
        );

        GrammarRule AggregationMetricFunctionQuery = Grammar.Rule("AggregationMetricFunctionQuery",
                OptWhiteSpaces, AggregationMetricFunctionName, Grammar.SetType("AggregationMetricFunctionName"), Grammar.MustMatchAction,
                OptWhiteSpaces, LEFTPAREN,
                OptWhiteSpaces,
                AggregationFieldSubfieldPathWhere, OptWhiteSpaces, RIGHTPAREN
        );

        SwitchRule DateDiffParameterValue = new SwitchRule(SwitchRule.First, "DateDiffParameterValue",
                Grammar.Rule(NowFunction),
                Grammar.Rule(Term, Grammar.SetType("datediff"))
        );

        GrammarRule DateDiffParameters = Grammar.Rule("DateDiffParameters",
                OptWhiteSpaces, AggregationFieldDateFunctionName, Grammar.SetType("datediff"),
                OptWhiteSpaces, COMMA, Grammar.DropLexem, OptWhiteSpaces, DateDiffParameterValue,  OptWhiteSpaces,
                COMMA, Grammar.DropLexem, OptWhiteSpaces, DateDiffParameterValue , Grammar.Semantic("datediff_calc")
        );

        GrammarRule DateDiffFunction = Grammar.Rule("DateDiffFunction",
                OptWhiteSpaces, DATEDIFF, Grammar.DropLexem, OptWhiteSpaces, LEFTPAREN, Grammar.DropLexem,
                OptWhiteSpaces,
                DateDiffParameters, OptWhiteSpaces, RIGHTPAREN, Grammar.DropLexem
        );



        SwitchRule WhereStar = new SwitchRule("WhereStar");
        WhereStar.body = Grammar.asRule(
                Grammar.Rule(WHERE , OptWhiteSpaces, LEFTPAREN, OptWhiteSpaces,
                        Query, OptWhiteSpaces, RIGHTPAREN, Grammar.Semantic("EOF"), Grammar.Semantic("ENDWHERE"),  DOT, Grammar.SetType("DOT") , WhereStar  ),
                STAR
        );

        GrammarRule AggregationMetricCountStarQuery = Grammar.Rule("AggregationMetricCountStarQuery",
                OptWhiteSpaces, COUNT, Grammar.SetType("AggregationMetricFunctionName"), OptWhiteSpaces, LEFTPAREN,
                OptWhiteSpaces, WhereStar, OptWhiteSpaces, RIGHTPAREN
        );

        SwitchRule SignNumberContinue = new SwitchRule("SignNumberContinue",
                Grammar.Rule(DOT, NUMBER),
                Grammar.emptyRule
        );


        GrammarRule DoubleNumberLiteral = new SwitchRule(SwitchRule.First, "DoubleNumberLiteral",
                Grammar.Rule( PLUS, Grammar.MustMatchAction, NUMBER, SignNumberContinue),
                Grammar.Rule( MINUS, Grammar.MustMatchAction, NUMBER,SignNumberContinue),
                Grammar.Rule( NUMBER, SignNumberContinue)
        );


        Token DOUBLENUMBER = new Token(DoubleNumberLiteral);

        GrammarRule AggregationMetricQuery = new SwitchRule(SwitchRule.First, "AggregationMetricQuery",
                AggregationMetricCountStarQuery,
                DateDiffFunction,
                AggregationMetricFunctionQuery,
                Grammar.Rule(OptWhiteSpaces, FloatPointNumber, Grammar.SetType("number")),
                Grammar.Rule(OptWhiteSpaces, DOUBLENUMBER, Grammar.SetType("number")),
                Grammar.Rule(OptWhiteSpaces, Subfield) //here goes only fieldPath
        );

        GrammarRule ArithmeticOperation = new SwitchRule(SwitchRule.First, "ArithmeticOperation",
                PLUS,
                MINUS,
                STAR,
                DIVIDE
        );

        SwitchRule AggregationMetricExpression = new SwitchRule("AggregationMetricExpression");

        GrammarRule AggregationMetricExpressionContinue = new SwitchRule(SwitchRule.First, "AggregationMetricExpressionContinue",
                Grammar.Rule(OptWhiteSpaces, ArithmeticOperation,  Grammar.SetType("op"),OptWhiteSpaces, AggregationMetricExpression),
                Grammar.emptyRule
        );


        GrammarRule OptionalMetricAlias = new SwitchRule(SwitchRule.First, "OptionalMetricAlias",
                Grammar.Rule(Grammar.WhiteSpaces,  AS, Grammar.DropLexem,
                        Grammar.WhiteSpaces, Term, Grammar.SetType("ALIAS_NAME") ) ,
                Grammar.emptyRule
        );
        
        GrammarRule AggregationMetricQueryTerm = Grammar.Rule("AggregationMetricQueryTerm",
                AggregationMetricQuery, OptionalMetricAlias, Grammar.InputPointer, Grammar.Semantic("endMetric"), OptWhiteSpaces, AggregationMetricExpressionContinue);

        AggregationMetricExpression.body = Grammar.asRule(
                Grammar.Rule(OptWhiteSpaces, LEFTPAREN, Grammar.SetType("op"), OptWhiteSpaces, AggregationMetricExpression, OptWhiteSpaces, RIGHTPAREN, Grammar.SetType("op"), AggregationMetricExpressionContinue, OptionalMetricAlias),
                AggregationMetricQueryTerm
        );

        SequenceRule AggregationMetricQueryList = new SequenceRule("AggregationMetricQueryList");

        GrammarRule AggregationMetricQueryListContinue = new SwitchRule("AggregationMetricQueryListContinue",
                Grammar.Rule(OptWhiteSpaces, COMMA, OptWhiteSpaces, AggregationMetricQueryList),
                Grammar.Rule(OptWhiteSpaces, Grammar.emptyRule)
        );

        AggregationMetricQueryList.body = Grammar.asRule(
                AggregationMetricExpression, AggregationMetricQueryListContinue
        );
        //Statistic parameters definition

        SwitchRule StatisticParameterValue = new SwitchRule(SwitchRule.First, "StatisticParameterValue",
                FloatPointNumber,
                Term,
                StringLiteral
        );


        GrammarRule StatisticRangeExpression = Grammar.Rule("StatisticRangeExpression", OptWhiteSpaces,
                LEFTBRACKET, OptWhiteSpaces, Grammar.MustMatchAction, StatisticParameterValue, Grammar.WhiteSpaces, TO, Grammar.WhiteSpaces, StatisticParameterValue, OptWhiteSpaces,
                RIGHTBRACKET
        );



        GrammarRule StatisticParameterContinue = new SwitchRule(SwitchRule.First, "StatisticParameterContinue",
                Grammar.Rule(OptWhiteSpaces, EQUAL, Grammar.MustMatchAction, OptWhiteSpaces, StatisticParameterValue, Grammar.Semantic("value")),
                Grammar.Rule(OptWhiteSpaces, COLON, Grammar.MustMatchAction, OptWhiteSpaces, StatisticRangeExpression, Grammar.Semantic("rangeValue"))
        );

        GrammarRule StatisticParameter = new SwitchRule(SwitchRule.First, "StatisticParameter",
                Grammar.Rule(OptWhiteSpaces, GROUP, Grammar.MustMatchAction,
                        OptWhiteSpaces, LEFTPAREN, OptWhiteSpaces, NUMBER, Grammar.Semantic("level"),
                        OptWhiteSpaces, RIGHTPAREN, StatisticParameterContinue),
                OptWhiteSpaces

        );

        SequenceRule FieldOrLinkName = new SequenceRule("FieldOrLinkName");
       // SequenceRule FieldOrLinkList = new SequenceRule("FieldOrLinkList");

        GrammarRule FieldOrLinkNameNext = new SwitchRule(SwitchRule.Longest, "FieldOrLinkNameNext",
                Grammar.Rule(OptWhiteSpaces, DOT, Grammar.SetType("DOT"), FieldOrLinkName),
                Grammar.emptyRule
        );

        SequenceRule FieldSet = new SequenceRule("FieldSet");

        GrammarRule FieldOrLinkNameContinue = new SwitchRule(SwitchRule.Longest, "FieldOrLinkNameContinue",
                Grammar.Rule(OptWhiteSpaces, LEFTPAREN, Grammar.SetType("LEFTPAREN"), Grammar.MustMatchAction, FieldSet, OptWhiteSpaces, RIGHTPAREN,
                        Grammar.SetType("RIGHTPAREN"), FieldOrLinkNameNext),
                FieldOrLinkNameNext
        );

        GrammarRule OptionalAlias = new SwitchRule(SwitchRule.First, "OptionalLimit",
                Grammar.Rule(DOT, Grammar.DropLexem, AS, Grammar.DropLexem, LEFTPAREN, Grammar.DropLexem,
                        OptWhiteSpaces, Term, Grammar.SetType("ALIAS_NAME"), OptWhiteSpaces,  RIGHTPAREN, Grammar.DropLexem) ,
                Grammar.Rule(Grammar.WhiteSpaces,  AS, Grammar.DropLexem,
                        Grammar.WhiteSpaces, Term, Grammar.SetType("ALIAS_NAME") ) ,
                Grammar.emptyRule
        );
        GrammarRule OptionalLimit = new SwitchRule(SwitchRule.First, "OptionalLimit",
                Grammar.Rule(OptWhiteSpaces, LEFTBRACKET, Grammar.SetType("BRACKET"), OptWhiteSpaces, NUMBER,
                        Grammar.SetType("LIMIT"), OptWhiteSpaces,
                        RIGHTBRACKET, Grammar.SetType("BRACKET"), OptionalAlias),
                OptionalAlias
        );

        SwitchRule OptionalWhereClause1 = new SwitchRule("OptionalWhereClause1");

        List<GrammarRule> optionalWhereBodyList = new ArrayList<>();
            optionalWhereBodyList.add(Grammar.Rule(DOT, Grammar.SetType("DOT"), WHERE, Grammar.MustMatchAction, OptWhiteSpaces, LEFTPAREN,
                    OptWhiteSpaces, Query, OptWhiteSpaces, RIGHTPAREN, Grammar.Semantic("EOF"), Grammar.Semantic("ENDWHERE"), OptionalWhereClause1));
            optionalWhereBodyList.add(Grammar.emptyRule);

        OptionalWhereClause1.body = optionalWhereBodyList;

        SwitchRule OptionalWhereAndLimit = new SwitchRule("OptionalWhereAndLimit",
                Grammar.Rule(OptionalWhereClause1, OptionalLimit),
                OptionalLimit
        );
        OptionalWhereAndLimit.optionalMode=true;

        FieldOrLinkName.body = Grammar.asRule(
                OptWhiteSpaces, FieldName, OptionalWhereAndLimit, FieldOrLinkNameContinue
        );

        GrammarRule FieldSetContinue = new SwitchRule(SwitchRule.First, "FieldSetContinue",
                Grammar.Rule(OptWhiteSpaces, COMMA, Grammar.SetType("COMMA"), Grammar.MustMatchAction, FieldSet),
                Grammar.Rule(Grammar.emptyRule)
        );

        //FieldOrLinkList.body = Grammar.asRule(
        //        FieldOrLinkValue, FieldSetContinue
        //);

        FieldSet.body = Grammar.asRule(
                FieldOrLinkName, FieldSetContinue, OptWhiteSpaces
        );


        Keyword DESC = new Keyword("DESC", WORD);
        Keyword ASC = new Keyword("ASC", WORD);



        GrammarRule SortOrderContinue = new SwitchRule(SwitchRule.First, "SortOrderContinue",
                Grammar.Rule(SkipWhiteSpacesRule, DESC, Grammar.SetType("DESC") ),
                Grammar.Rule(SkipWhiteSpacesRule, ASC, Grammar.SetType("ASC") ),
                Grammar.Rule(OptWhiteSpaces, Grammar.emptyRule)
        );


        GrammarRule SortOrder = Grammar.Rule("SortOrder",
                AggregationFieldPath, SortOrderContinue
        );

        Hashtable<String, GrammarRule> result = new Hashtable<>();
        result.put(SearchQueryGrammar, DoradusQuery);
        result.put(AggregationQueryGrammar, AggregationClause);
        result.put(AggregationMetricGrammar, AggregationMetricQueryList);

        result.put(StatisticMetricGrammar, AggregationMetricQuery);
        result.put(StatisticQueryGrammar, AggregationQueryList);
        result.put(StatisticParameterGrammar, StatisticParameter);

        result.put(FieldSetGrammar, FieldSet);
        result.put(SortOrderGrammar, SortOrder);

        result.put(SkipWhitespace, OptWhiteSpaces);
        result.put(SkipWhitespaceOptionRule, SkipWhiteSpacesRule);
        result.put(NotSkipWhitespaceOptionRule, Grammar.emptyRule);
        return result;
    }
}
