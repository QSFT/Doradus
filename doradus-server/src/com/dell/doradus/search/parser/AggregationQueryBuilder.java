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

import com.dell.doradus.common.FieldDefinition;
import com.dell.doradus.common.FieldType;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.common.Utils;
import com.dell.doradus.search.aggregate.*;
import com.dell.doradus.search.parser.grammar.Context;
import com.dell.doradus.search.parser.grammar.GrammarItem;
import com.dell.doradus.search.parser.grammar.Literal;
import com.dell.doradus.search.query.AndQuery;
import com.dell.doradus.search.query.LinkQuery;
import com.dell.doradus.search.query.OrQuery;
import com.dell.doradus.search.query.Query;

import java.text.SimpleDateFormat;
import java.util.*;

public class AggregationQueryBuilder {

    static HashSet<String> availableTimeZones;

    public static AggregationMetric BuildStatisticMetric(String string, TableDefinition definition) {
        string = definition.replaceAliaces(string);
        Parser parser = Parser.GetStatisticMetricParser();
        ParseResult res1 = parser.Parse(string);
        if (res1.error == null) {
            ArrayList<AggregationMetric> metric = BuildMetrics(res1.context, definition);
            return metric.get(0);
        } else {
            throw new IllegalArgumentException(res1.error);
        }
    }

    public static Statistic.StatisticParameter BuildStatisticParameters(String string) {
        Parser parser = Parser.GetStatisticParametersParser();
        ParseResult res1 = parser.Parse(string);
        if (res1.error == null) {
            Statistic.StatisticParameter parameter = BuildStatisticParameters(res1.context);
            return parameter;
        } else {
            throw new IllegalArgumentException(res1.error);
        }
    }

    public static List<AggregationGroup> BuildStatistic(String string, TableDefinition definition) {
        List<AggregationGroup> group = Build(string, definition);
        return group;
    }

    //For compatibility : now all must use BuildAggregationMetrics
    public static AggregationMetric BuildAggregationMetric(String string, TableDefinition definition) {
        ArrayList<AggregationMetric> metrics = BuildAggregationMetrics(string, definition);
        return metrics.get(0);
    }

    public static ArrayList<AggregationMetric> BuildAggregationMetrics(String string, TableDefinition definition) {
        string = definition.replaceAliaces(string);
        Parser parser = Parser.GetAggregationMetricParser();
        ParseResult res1 = parser.Parse(string);
        if (res1.error == null) {
            ArrayList<AggregationMetric> metric = BuildMetrics(res1.context, definition);
            return metric;
        } else {
            throw new IllegalArgumentException(res1.error);
        }
    }

    public static ArrayList<MetricExpression> BuildAggregationMetricsExpression(String string, TableDefinition definition) {
        string = definition.replaceAliaces(string);
        Parser parser = Parser.GetAggregationMetricParser();
        ParseResult res1 = parser.Parse(string);
        if (res1.error == null) {
            return BuildMetricsExpression(res1.context, definition);
        } else {
            throw new IllegalArgumentException(res1.error);
        }
    }

    public static SortOrder BuildSortOrder(String string, TableDefinition definition) {
        string = definition.replaceAliaces(string);
        Parser parser = Parser.GetSortOrderParser();
        ParseResult res1 = parser.Parse(string);
        if (res1.error == null) {
            SortOrder order = BuildSort(res1.context, definition);
            return order;
        } else {
            throw new IllegalArgumentException(res1.error);
        }
    }

    public static List<AggregationGroup> Build(String string, TableDefinition definition) {
        string = definition.replaceAliaces(string);
        Parser parser = Parser.GetAggregationQueryParser();
        ParseResult res1 = parser.Parse(string);
        if (res1.error == null) {

            List<AggregationGroup> group = Build(res1.context, definition);
            return group;
        } else {
            throw new IllegalArgumentException(res1.error);
        }
    }

    public static ArrayList<ArrayList<AggregationGroup>> BuildAggregation(String string, TableDefinition definition) {
        string = definition.replaceAliaces(string);
        Parser parser = Parser.GetAggregationQueryParser();
        ParseResult res1 = parser.Parse(string);
        if (res1.error == null) {
            ArrayList<ArrayList<AggregationGroup>> group = BuildAg(res1.context, definition);
            return group;
        } else {
            throw new IllegalArgumentException(res1.error);
        }
    }

    private static Statistic.StatisticParameter BuildStatisticParameters(Context context) {

        Statistic.StatisticParameter result = new Statistic.StatisticParameter();
        Stack<String> stack = new Stack<String>();
        for (int i = 0; i < context.items.size(); i++) {
            GrammarItem grammarItem = context.items.get(i);
            if (grammarItem.getType().equals("lexem") || grammarItem.getType().equals("string"))
                stack.push(grammarItem.getValue());

            if (grammarItem.getType().equals("semantic")) {
                if (grammarItem.getValue().equals("level")) {
                    String level = stack.pop();
                    try {
                        result.level = Integer.parseInt(level);
                        continue;
                    } catch (Exception e) {
                        throw new IllegalArgumentException("Wrong value for level: " + level);
                    }
                }

                if (grammarItem.getValue().equals("value")) {
                    String value = stack.pop();
                    result.maxValue = value;
                    result.minValue = value;
                    continue;
                }
                if (grammarItem.getValue().equals("rangeValue")) {
                    result.maxValue = stack.pop();
                    result.minValue = stack.pop();
                    continue;
                }
            }
        }
        if (result.maxValue == null || result.maxValue == null)
            result = null;
        return result;
    }

    private static ArrayList<AggregationMetric> BuildMetrics(Context context, TableDefinition definition) {
        if (context == null)
            throw new IllegalArgumentException("Cannot create query:Context is null");

        if (context.items.isEmpty())
            return null;

        ArrayList<AggregationMetric> result = new ArrayList<AggregationMetric>();

        AggregationMetric metric = new AggregationMetric(definition);
        ArrayList<Item> items = extractTokens(context);

        TableDefinition tableDef = definition;
        boolean fieldDetected = false;
        int ptr = 0;
        int inputPtr = 0;

        for (int i = 0; i < items.size(); i++) {
            Item item = items.get(i);

            if (item.item.getType().equals(SemanticNames.AGGREGATION_METRIC_FUNCTION_NAME)) {
                ptr = item.item.getPtr();
                metric.function = item.item.getValue();
                continue;
            }

            if (item.item.getType().equals("InputPointer")) {
                inputPtr = item.item.getPtr();
                continue;
            }

            if (item.item.getType().equals("token") && item.item.getValue().equals(",")) {
                result.add(metric);
                tableDef = definition;
                fieldDetected = false;
                metric.sourceText = context.inputString.substring(ptr, inputPtr);
                metric = new AggregationMetric(definition);
                continue;
            }

            if (fieldDetected)
                throw new IllegalArgumentException("Error: Not a link " + QueryUtils.FullLinkName(metric.items));

            AggregationGroupItem ai = new AggregationGroupItem();
            if (metric.items == null)
                metric.items = new ArrayList<AggregationGroupItem>();

            metric.items.add(ai);
            ai.name = item.item.getValue();
            if (tableDef != null) {
                FieldDefinition fd = tableDef.getFieldDef(ai.name);
                ai.fieldDef = fd;
                if (fd == null) {
                    if (i != (items.size() - 1)) {
                        throw new IllegalArgumentException(" Undefined Link " + QueryUtils.FullLinkName(metric.items));
                    }
                    if (!QueryUtils.isSystemField(ai.name)) {
                        throw new IllegalArgumentException("Unknown system field " + ai.name);
                    }
                }
                if (tableDef.isLinkField(ai.name) || (fd != null && fd.isXLinkField())) {
                    ai.isLink = true;
                    if (fieldDetected)
                        throw new IllegalArgumentException("Error: Not a link " + QueryUtils.FullLinkName(metric.items));
                    tableDef = tableDef.getLinkExtentTableDef(fd);
                    if (tableDef == null) {
                        throw new IllegalArgumentException(" Cannot get table definition for link " + QueryUtils.FullLinkName(metric.items));
                    }
                } else {

                    if (fd != null && fd.getType() == FieldType.GROUP)
                        throw new IllegalArgumentException("Group fields are not allowed in metrics");

                    fieldDetected = true;
                }
                ai.tableDef = tableDef;
            }
            if (item.queryItems != null) {
                for (int j = 0; j < item.queryItems.size(); j++) {
                    ArrayList<GrammarItem> filterItems = item.queryItems.get(j);
                    ai.query = CompileQuery(tableDef, ai.query, filterItems);
                }
            }
        }

        metric.sourceText = context.inputString.substring(ptr);
        result.add(metric);
        return result;
    }

    private static void doOperation(String op1, Stack<MetricExpression> expressions, Stack<String> operations) {
        if (op1.equals("+")) {
            BinaryExpression me = new BinaryExpression();
            me.second = expressions.pop();
            me.first = expressions.pop();
            me.operation = BinaryExpression.MetricOperation.PLUS;
            expressions.push(me);//DoOperation(op1, builderContext);
        }

        if (op1.equals("-")) {
            BinaryExpression me = new BinaryExpression();
            me.second = expressions.pop();
            me.first = expressions.pop();
            me.operation = BinaryExpression.MetricOperation.MINUS;
            expressions.push(me);//DoOperation(op1, builderContext);
        }
        if (op1.equals("*")) {
            BinaryExpression me = new BinaryExpression();
            me.second = expressions.pop();
            me.first = expressions.pop();
            me.operation = BinaryExpression.MetricOperation.MULTIPLAY;
            expressions.push(me);//DoOperation(op1, builderContext);
        }
        if (op1.equals("/")) {
            BinaryExpression me = new BinaryExpression();
            me.second = expressions.pop();
            me.first = expressions.pop();
            me.operation = BinaryExpression.MetricOperation.DIVIDE;
            expressions.push(me);//DoOperation(op1, builderContext);
        }
    }

    //////////////////////////////////////// old metrics
    private static void pushOp(String operation, Stack<MetricExpression> expressions, Stack<String> operations) {
        if (operation.equals("(")) {
            operations.push(operation);
            return;
        }

        if (operation.equals(")")) {
            while (!operations.isEmpty()) {
                String op = operations.pop();
                if (op.equals("("))
                    return;
                doOperation(op, expressions, operations);
            }
        }


        if (operation.equals("*") || operation.equals("/")) {
            if (operations.isEmpty())
                operations.push(operation);
            else {
                String op1 = operations.peek();
                if (!op1.equals("(")) {
                    if (op1.equals("+") || op1.equals("-")) {
                        operations.push(operation);
                    } else {
                        doOperation(operations.pop(), expressions, operations);
                        operations.push(operation);
                    }
                } else {
                    operations.push(operation);
                }

            }
        }
        if (operation.equals("+") || operation.equals("-")) {
            if (operations.isEmpty())
                operations.push(operation);
            else {
                String op1 = operations.peek();
                if (!op1.equals("(")) {
                    doOperation(operations.pop(), expressions, operations);
                }
                operations.push(operation);
            }
        }
    }

    private static Item DropItem(ArrayList<Item> grammarItems) {
        return grammarItems.remove(grammarItems.size() - 1);
    }

    private static ArrayList<MetricExpression> BuildMetricsExpression(Context context, TableDefinition definition) {
        if (context == null)
            throw new IllegalArgumentException("Cannot create query:Context is null");

        if (context.items.isEmpty())
            return null;

        AggregationMetric metric = new AggregationMetric(definition);
        ArrayList<Item> items = extractMetricTokens(context);
        TableDefinition tableDef = definition;
        boolean fieldDetected = false;
        int ptr = 0;
        int endMetricPtr = 0;

        ArrayList<MetricExpression> resultList = new ArrayList<MetricExpression>();
        Stack<MetricExpression> expressions = new Stack<MetricExpression>();
        Stack<String> operations = new Stack<String>();
        for (int i = 0; i < items.size(); i++) {

            Item item = items.get(i);

            if (item.item.getType().equals("op")) {
                pushOp(item.item.getValue(), expressions, operations);
                continue;
            }

            if (item.item.getType().equals("datediff")) {
                operations.push(item.item.getValue());
                continue;
            }

            if (item.item.getType().equals("semantic") && item.item.getValue().equals("datediff_calc")) {
                String unit2 = operations.pop(); //item.item.getValue();
                String unit1 = operations.pop();
                String unit = operations.pop();
                GregorianCalendar c1, c2;
                try {
                    c1 = Utils.parseDate(unit1);
                } catch (Exception e) {
                    throw new IllegalArgumentException("Bad date time format:" + unit1);
                }
                try {
                    c2 = Utils.parseDate(unit2);
                } catch (Exception e) {
                    throw new IllegalArgumentException("Bad date time format:" + unit2);
                }

                LongIntegerExpression le = new LongIntegerExpression();
                le.value = TimeUtils.getTimeDifference(unit, c1, c2);
                expressions.push(le);
                metric = null;
                continue;
            }


            if (item.item.getType().equals("InputPointer")) {
                endMetricPtr = item.item.getPtr();
                continue;
            }

            if (item.item.getType().equals("number")) {
                NumberExpression ne = new NumberExpression();
                try {
                    ne.value = Double.parseDouble(item.item.getValue());
                } catch (Exception e) {
                    throw new IllegalArgumentException("Cannot convert '" + item.item.getValue() + "' to double");
                }
                expressions.push(ne);
                metric = null;
                continue;
            }

            if (item.item.getType().equals(SemanticNames.TRANSITIVE_VALUE) || item.item.getType().equals(SemanticNames.TRANSITIVE)) {
                AggregationGroupItem agItem = metric.items.get(metric.items.size() - 1);

                if (item.item.getType().equals(SemanticNames.TRANSITIVE_VALUE)) {
                    agItem.transitiveDepth = Integer.parseInt(item.item.getValue());
                } else {
                    agItem.isTransitive = true;
                }

                if (item.queryItems != null) {
                    for (int j = 0; j < item.queryItems.size(); j++) {
                        ArrayList<GrammarItem> filterItems = item.queryItems.get(j);
                        agItem.query = CompileQuery(tableDef, agItem.query, filterItems);
                    }
                }


                continue;
            }

            if (item.item.getType().equals(SemanticNames.AGGREGATION_METRIC_FUNCTION_NAME)) {
                ptr = item.item.getPtr();
                metric.function = item.item.getValue();

                if (item.queryItems != null) {
                    for (int j = 0; j < item.queryItems.size(); j++) {
                        ArrayList<GrammarItem> filterItems = item.queryItems.get(j);
                        metric.filter = CompileQuery(tableDef, metric.filter, filterItems);
                    }
                }
                continue;
            }

            if (item.item.getType().equals("semantic") && item.item.getValue().equals("endMetric")) {
                if (metric != null) {
                    expressions.push(metric);
                    metric.sourceText = context.inputString.substring(ptr, endMetricPtr);
                }
                tableDef = definition;
                fieldDetected = false;
                metric = new AggregationMetric(definition);

                continue;

            }

            if (item.item.getType().equals(SemanticNames.TRUNCATE_SUBFIELD_VALUE)) {
                metric.subField = AggregationGroup.SubField.valueOf(item.item.getValue());
                continue;
            }

            if (item.item.getType().equals("token") && item.item.getValue().equals(",")) {
                while (!operations.isEmpty()) {
                    doOperation(operations.pop(), expressions, operations);
                }
                MetricExpression me = expressions.pop();
                if (!expressions.isEmpty())
                    throw new IllegalArgumentException("Bad expression");

                resultList.add(me);

                tableDef = definition;
                fieldDetected = false;
                //metric.sourceText = context.inputString.substring(ptr, item.item.getPtr());
                metric = new AggregationMetric(definition);

                continue;
            }

            if (fieldDetected)
                throw new IllegalArgumentException("Error: Not a link " + QueryUtils.FullLinkName(metric.items));

            AggregationGroupItem ai = new AggregationGroupItem();
            if (metric.items == null)
                metric.items = new ArrayList<AggregationGroupItem>();

            metric.items.add(ai);
            ai.name = item.item.getValue();
            if (tableDef != null) {
                FieldDefinition fd = tableDef.getFieldDef(ai.name);
                ai.fieldDef = fd;
                if (fd == null) {
                    if (i != (items.size() - 1)) {
                        throw new IllegalArgumentException(" Undefined Link " + QueryUtils.FullLinkName(metric.items));
                    }
                    if (!QueryUtils.isSystemField(ai.name)) {
                        throw new IllegalArgumentException("Unknown system field " + ai.name);
                    }
                }
                if (tableDef.isLinkField(ai.name) || (fd != null && fd.isXLinkField())) {
                    ai.isLink = true;
                    if (fieldDetected)
                        throw new IllegalArgumentException("Error: Not a link " + QueryUtils.FullLinkName(metric.items));
                    tableDef = tableDef.getLinkExtentTableDef(fd);
                    if (tableDef == null) {
                        throw new IllegalArgumentException(" Cannot get table definition for link " + QueryUtils.FullLinkName(metric.items));
                    }
                } else {

                    if (fd != null && fd.getType() == FieldType.GROUP)
                        throw new IllegalArgumentException("Group fields are not allowed in metrics");

                    fieldDetected = true;
                }
                ai.tableDef = tableDef;
            }
            if (item.queryItems != null) {
                if (ai.isLink) {
                    for (int j = 0; j < item.queryItems.size(); j++) {
                        ArrayList<GrammarItem> filterItems = item.queryItems.get(j);
                        ai.query = CompileQuery(tableDef, ai.query, filterItems);
                    }
                } else {
                    throw new IllegalArgumentException(ai.name + " is not a link name. Filters are supported for links");
                }
            }

        }

        while (!operations.isEmpty()) {

            doOperation(operations.pop(), expressions, operations);
        }
        MetricExpression me = expressions.pop();
        if (!expressions.isEmpty())
            throw new IllegalArgumentException("Bad expression");

        resultList.add(me);
        return resultList;
    }

    public static Query CompileQuery(TableDefinition tableDef, Query query, ArrayList<GrammarItem> filterItems) {
        Query filter = DoradusQueryBuilder.Build(filterItems, tableDef);
        if (query == null)
            return filter;
        else {
            if (!(query instanceof AndQuery)) {
                AndQuery andq = new AndQuery();
                andq.subqueries.add(query);
                andq.subqueries.add(filter);
                return andq;
            } else {
                AndQuery andq = (AndQuery) query;
                andq.subqueries.add(filter);
                return andq;
            }
        }
    }

    private static SortOrder BuildSort(Context context, TableDefinition definition) {
        if (context == null)
            throw new IllegalArgumentException("Cannot create query:Context is null");

        if (context.items.isEmpty())
            return null;

        SortOrder result = new SortOrder();

        ArrayList<Item> items = extractTokens(context);
        TableDefinition tableDef = definition;
        boolean fieldDetected = false;

        for (int i = 0; i < items.size(); i++) {
            Item item = items.get(i);

            if (item.item.getType().equals(SemanticNames.DESC)) {
                result.ascending = false;
                continue;
            }

            if (item.item.getType().equals(SemanticNames.ASC)) {
                result.ascending = true;
                continue;
            }

            if (fieldDetected)
                throw new IllegalArgumentException("Error: Not a link " + QueryUtils.FullLinkName(result.items));

            AggregationGroupItem ai = new AggregationGroupItem();
            if (result.items == null)
                result.items = new ArrayList<AggregationGroupItem>();

            result.items.add(ai);
            ai.name = item.item.getValue();
            if (tableDef != null) {
                FieldDefinition fd = tableDef.getFieldDef(ai.name);
                ai.fieldDef = fd;
                if (fd == null) {
                    if (i != (items.size() - 1)) {
                        throw new IllegalArgumentException(" Undefined Link " + QueryUtils.FullLinkName(result.items));
                    }
                    if (!QueryUtils.isSystemField(ai.name)) {
                        throw new IllegalArgumentException("Unknown field " + ai.name);
                    }
                }
                if (tableDef.isLinkField(ai.name) || (fd != null && fd.isXLinkField())) {
                    ai.isLink = true;
                    if (fieldDetected)
                        throw new IllegalArgumentException("Error: Not a link " + QueryUtils.FullLinkName(result.items));
                    tableDef = tableDef.getLinkExtentTableDef(fd);
                    if (tableDef == null) {
                        throw new IllegalArgumentException(" Cannot get table definition for link " + QueryUtils.FullLinkName(result.items));
                    }
                } else {

                    if (fd != null && fd.getType() == FieldType.GROUP)
                        throw new IllegalArgumentException("Group fields are not allowed in sort order");

                    fieldDetected = true;
                }
                ai.tableDef = tableDef;
            }

        }
        if (!fieldDetected)
            throw new IllegalArgumentException("Sort field must be scalar field");

        return result;
    }

    private static ArrayList<ArrayList<AggregationGroup>> BuildAg(Context context, TableDefinition definition) {
        if (context == null)
            throw new IllegalArgumentException("Cannot create query:Context is null");

        if (context.items.isEmpty())
            return null;

        ArrayList<ArrayList<AggregationGroup>> resultList = new ArrayList<ArrayList<AggregationGroup>>();

        ArrayList<ArrayList<Item>> groups = extractAgTokens(context);

        for (int j = 0; j < groups.size(); j++) {
            ArrayList<Item> items = groups.get(j);
            ArrayList<AggregationGroup> result = processItems(context, definition, items);
            resultList.add(result);
        }

        return resultList;
    }

    private static List<AggregationGroup> Build(Context context, TableDefinition definition) {
        if (context == null)
            throw new IllegalArgumentException("Cannot create query:Context is null");

        if (context.items.isEmpty())
            return null;

        return processItems(context, definition, extractTokens(context)); //new ArrayList<AggregationGroup>();
    }

    private static ArrayList<AggregationGroup> processItems(Context context, TableDefinition definition, ArrayList<Item> items) {
        AggregationGroup aggregationGroup = new AggregationGroup(definition);
        TableDefinition tableDef = definition;
        boolean fieldDetected = false;

        ArrayList<AggregationGroup> result = new ArrayList<AggregationGroup>();
        int startPos = -1;
        int lastPos = -1;

        boolean includeList = false;

        for (int i = 0; i < items.size(); i++) {

            Item item = items.get(i);
            if (item.item.getPtr() != -1) {
                lastPos = item.item.getPtr() + item.item.getValue().length();
            }
            if (startPos == -1)
                startPos = item.item.getPtr();

            if (item.item.getType().equals("ignore")) {
                SetFilter(aggregationGroup, tableDef, item);
                continue;
            }

            if (item.item.getType().equals("global")) {
                SetFilter(aggregationGroup, tableDef, item);
                continue;
            }
            if (item.item.getType().equals("endGroup")) {
                lastPos = item.item.getPtr();
                if (aggregationGroup.items != null) {
                    result.add(aggregationGroup);
                    aggregationGroup.text = context.inputString.substring(startPos, item.item.getPtr());
                    startPos = item.item.getPtr() + item.item.getValue().length();
                    aggregationGroup = new AggregationGroup(definition);
                    tableDef = definition;
                    fieldDetected = false;
                }
            } else if (item.item.getValue().equals(",")) {
                result.add(aggregationGroup);
                aggregationGroup.text = context.inputString.substring(startPos, item.item.getPtr());
                startPos = item.item.getPtr() + item.item.getValue().length();
                aggregationGroup = new AggregationGroup(definition);
                SetFilter(aggregationGroup, tableDef, item);
                tableDef = definition;
                fieldDetected = false;
            } else {
                String type = item.item.getType();

                if (type.equals(SemanticNames.TRUNCATE_VALUE)) {
                    aggregationGroup.truncate = item.item.getValue();
                    continue;
                }

                if (type.equals(SemanticNames.TRUNCATE_SUBFIELD_VALUE)) {
                    aggregationGroup.subField = AggregationGroup.SubField.valueOf(item.item.getValue());
                    continue;
                }


                if (type.equals(SemanticNames.TIMEZONEVALUE)) {
                    aggregationGroup.timeZone = item.item.getValue().trim();

                    char ch = aggregationGroup.timeZone.charAt(0);
                    if (ch == '+' || ch == '-') {
                        aggregationGroup.timeZone = "GMT" + aggregationGroup.timeZone;
                    } else if (Character.getType(ch) == Character.DECIMAL_DIGIT_NUMBER) {
                        aggregationGroup.timeZone = "GMT+" + aggregationGroup.timeZone;
                    }
                    String prefix = aggregationGroup.timeZone.substring(0, 4);
                    String val = aggregationGroup.timeZone.substring(4);
                    switch (val.length()) {
                        case 1:
                            aggregationGroup.timeZone = prefix + "0" + val + ":00";
                            break;
                        case 2:
                            aggregationGroup.timeZone = prefix + val + ":00";
                            break;
                        case 3:
                            aggregationGroup.timeZone = prefix + "0" + val.substring(0, 1) + ":" + val.substring(1);
                            break;
                        case 4:
                            if (val.charAt(1) == ':')
                                aggregationGroup.timeZone = prefix + "0" + val;
                            else {
                                aggregationGroup.timeZone = prefix + val.substring(0, 2) + ":" + val.substring(2);
                            }
                            break;
                        case 5:
                            break;
                        default:
                            throw new IllegalArgumentException("Internal error: bad timezone(1)");

                    }

                    TimeZone zone = TimeZone.getTimeZone(aggregationGroup.timeZone);
                    String id = zone.getID();
                    if (id.compareTo(aggregationGroup.timeZone) != 0) {
                        throw new IllegalArgumentException("Bad timezone value: '" + aggregationGroup.timeZone + "'");
                    }
                    continue;
                }

                if (type.equals(SemanticNames.TIMEZONEDISPLAYNAME)) {
                    aggregationGroup.timeZone = item.item.getValue();
                    if (!isCorrectTimeZone(aggregationGroup.timeZone))
                        throw new IllegalArgumentException("Unknown timezone: '" + aggregationGroup.timeZone + "'");
                    continue;
                }


                if (type.equals(SemanticNames.GROUP)) {
                    //aggregationGroup.groupKey = SemanticNames.GROUP;
                    continue;
                }

                if (type.equals(SemanticNames.BATCH_VALUE)) {
                    if (aggregationGroup.batch == null)
                        aggregationGroup.batch = new ArrayList<Object>();

                    aggregationGroup.batch.add(item.item.getValue());

                    continue;
                }

                if (type.equals(SemanticNames.TOPBOTTOMVALUE)) {
                    aggregationGroup.selectionValue = Integer.parseInt(item.item.getValue());
                    continue;
                }

                if (type.equals(SemanticNames.UPPER)) {
                    aggregationGroup.tocase = SemanticNames.UPPER;

                    continue;
                }

                if (type.equals(SemanticNames.LOWER)) {
                    aggregationGroup.tocase = SemanticNames.LOWER;
                    continue;
                }

                if (type.equals(SemanticNames.TRUNCATE) || type.equals(SemanticNames.BATCH)) {
                    SetFilter(aggregationGroup, tableDef, item);
                    continue;
                }
                if (type.equals(SemanticNames.TERMS))
                    continue;

                if (type.equals(SemanticNames.EXCLUDELIST)) {
                    includeList = false;
                    continue;
                }

                if (type.equals(SemanticNames.INCLUDELIST)) {
                    includeList = true;
                    continue;
                }
                if (type.equals(SemanticNames.EXCLUDE)) {
                    continue;
                }

                if (type.equals(SemanticNames.STOPVALUEANY)) {
                    if (aggregationGroup.stopWords == null) {
                        aggregationGroup.stopWords = new ArrayList<String>();
                    } else {
                        throw new IllegalArgumentException("Internal error, stopwords list is already defined");
                    }
                    continue;
                }
                if (type.equals(SemanticNames.STOPVALUE)) {
                    if (aggregationGroup.stopWords == null) {
                        aggregationGroup.stopWords = new ArrayList<String>();
                    }
                    aggregationGroup.stopWords.add(item.item.getValue());
                    continue;
                }

                if (type.equals(SemanticNames.EXCLUDEVALUE)) {
                    String value = item.item.getValue();
                    if ("NULL".equals(value))
                        value = null;

                    if (includeList) {
                        if (aggregationGroup.include == null) {
                            aggregationGroup.include = new ArrayList<String>();
                        }
                        aggregationGroup.include.add(value);
                        continue;
                    } else {
                        if (aggregationGroup.exclude == null) {
                            aggregationGroup.exclude = new ArrayList<String>();
                        }
                        aggregationGroup.exclude.add(value);
                        continue;
                    }
                }

                if (type.equals(SemanticNames.ALIAS)) {
                    aggregationGroup.name = item.item.getValue();
                    continue;
                }

                if (item.item.getValue().equals(")")) {
                    continue;
                }

                if (type.equals(SemanticNames.TOPBOTTOM)) {
                    if (item.item.getValue().equals("TOP"))
                        aggregationGroup.selection = AggregationGroup.Selection.Top;
                    else if (item.item.getValue().equals("BOTTOM"))
                        aggregationGroup.selection = AggregationGroup.Selection.Bottom;
                    else if (item.item.getValue().equals("FIRST"))
                        aggregationGroup.selection = AggregationGroup.Selection.First;
                    else if (item.item.getValue().equals("LAST"))
                        aggregationGroup.selection = AggregationGroup.Selection.Last;
                    else throw new RuntimeException("TOP/BOTTOM/FIRST/LAST allowed");

                    SetFilter(aggregationGroup, tableDef, item);
                    continue;
                }

                if (type.equals(SemanticNames.TRANSITIVE_VALUE) || type.equals(SemanticNames.TRANSITIVE)) {
                    AggregationGroupItem agItem = aggregationGroup.items.get(aggregationGroup.items.size() - 1);
                    if (type.equals(SemanticNames.TRANSITIVE_VALUE)) {
                        agItem.transitiveDepth = Integer.parseInt(item.item.getValue());
                    } else {
                        agItem.isTransitive = true;
                    }
                    if (item.queryItems != null) {
                        for (int j = 0; j < item.queryItems.size(); j++) {
                            ArrayList<GrammarItem> filterItems = item.queryItems.get(j);
                            agItem.query = CompileQuery(tableDef, agItem.query, filterItems);
                            GrammarItem last = filterItems.get(filterItems.size() - 2);
                            lastPos = last.getPtr() + last.getValue().length();
                        }
                    }
                    continue;
                }

                AggregationGroupItem ai = new AggregationGroupItem();
                if (aggregationGroup.items == null)
                    aggregationGroup.items = new ArrayList<AggregationGroupItem>();
                aggregationGroup.items.add(ai);

                ai.name = item.item.getValue();
                if (tableDef != null) {
                    FieldDefinition fd = tableDef.getFieldDef(ai.name);
                    ai.fieldDef = fd;
                    if (fd == null)
                        throw new IllegalArgumentException(" Undefined field: " + QueryUtils.FullLinkName(aggregationGroup.items));

                    if (fd.isGroupField()) {
                        //Take the first
                        ai.nestedLinks = GetNestedFieldsInfo(fd);
                        ai.isLink = true;
                        for (FieldDefinition nestedFieldDef : fd.getNestedFields()) {

                            if (nestedFieldDef.isGroupField()) {
                                List<LinkInfo> info = GetNestedFieldsInfo(nestedFieldDef);
                                if (info.size() == 0)
                                    throw new IllegalArgumentException(" There are no fields in group " + nestedFieldDef.getName());
                                //Take the first field (we assume all links point to the same table)
                                nestedFieldDef = info.get(0).fieldDef;
                            }

                            //TODO Check all nestedfields - they mustbe all links or not
                            if (tableDef.isLinkField(nestedFieldDef.getName()) || nestedFieldDef.isXLinkField()) {
                                TableDefinition td = tableDef.getLinkExtentTableDef(nestedFieldDef);
                                if (td == null) {
                                    throw new IllegalArgumentException(" Cannot get table definition for " + QueryUtils.FullLinkName(aggregationGroup.items));
                                }
                                tableDef = td;
                            }
                            ai.tableDef = tableDef;
                            break;
                        }
                    } else if (fd.isLinkField() || fd.isXLinkField()) {
                        ai.isLink = true;
                        if (fieldDetected)
                            throw new IllegalArgumentException("Error: Not a link " + QueryUtils.FullLinkName(aggregationGroup.items));
                        tableDef = tableDef.getLinkExtentTableDef(fd);
                        if (tableDef == null) {
                            throw new IllegalArgumentException(" Cannot get table definition for link " + QueryUtils.FullLinkName(aggregationGroup.items));
                        }
                    } else {
                        if (fieldDetected)
                            throw new IllegalArgumentException("Error: Not a link " + QueryUtils.FullLinkName(aggregationGroup.items));
                        fieldDetected = true;
                    }

                    ai.tableDef = tableDef;
                }
                if (item.queryItems != null) {

                    for (int j = 0; j < item.queryItems.size(); j++) {
                        ArrayList<GrammarItem> filterItems = item.queryItems.get(j);
                        //Query filter = DoradusQueryBuilder.Build(filterItems, tableDef);
                        ai.query = CompileQuery(tableDef, ai.query, filterItems);
                        //ai.query = DoradusQueryBuilder.Build(item.queryItems, tableDef);
                        GrammarItem last = filterItems.get(filterItems.size() - 2);
                        lastPos = last.getPtr() + last.getValue().length();
                    }
                }
            }
        }

        if (aggregationGroup != null && aggregationGroup.items != null) {
            aggregationGroup.text = context.inputString.substring(startPos, lastPos);
            result.add(aggregationGroup);
        }

        for (int i = 0; i < result.size(); i++) {
            AggregationGroup group = result.get(i);
            if (group.batch != null && group.items != null) {
                AggregationGroupItem item = group.items.get(group.items.size() - 1);
                if (item.fieldDef == null)
                    throw new IllegalArgumentException("Unknown field/link name " + item.name);

                FieldType itemType = item.fieldDef.getType();
                if (itemType == FieldType.GROUP || itemType == FieldType.LINK || itemType == FieldType.XLINK || itemType == FieldType.BINARY || itemType == FieldType.BOOLEAN)
                    throw new IllegalArgumentException("Error: BATCH is not supported for " + itemType.toString() + " field type");

                //Convert values to field type
                for (int j = 0; j < group.batch.size(); j++) {
                    try {
                        group.batch.set(j, convert(itemType, (String) group.batch.get(j)));
                    } catch (Exception e) {
                        throw new IllegalArgumentException("Wrong batch value '" + group.batch.get(j) + "' for field " + item.fieldDef.getName());
                    }
                }

                //Check values order
                if (group.batch.size() > 1) {
                    Object first = group.batch.get(0);
                    for (int j = 1; j < group.batch.size(); j++) {
                        switch (compareBatchValues(itemType, first, group.batch.get(j))) {
                            case 0:
                                throw new IllegalArgumentException("Duplicated batch values are not allowed");
                            case 1:
                                throw new IllegalArgumentException("Batch values must be in an ascending order");

                        }
                        first = group.batch.get(j);
                    }
                }

            }
            if (group.truncate != null && group.items != null) {
                AggregationGroupItem item = group.items.get(group.items.size() - 1);
                if (item.fieldDef == null)
                    throw new IllegalArgumentException("Unknown field/link name " + item.name);

                if (!(item.fieldDef.getType() == FieldType.TIMESTAMP))
                    throw new IllegalArgumentException("Error: TRUNCATE may be applied only for TIMESTAMP fields");
            }
        }

        for (int i = 0; i < result.size(); i++) {
            AggregationGroup group = result.get(i);
            group.whereFilter = getWhereQuery(group);
        }
        return result;
    }

    private static void SetFilter(AggregationGroup aggregationGroup, TableDefinition tableDef, Item item) {
        if (item.queryItems != null) {
            for (int j = 0; j < item.queryItems.size(); j++) {
                aggregationGroup.filter = CompileQuery(tableDef, aggregationGroup.filter, item.queryItems.get(j));
            }
        }
    }

    private static LinkQuery GetLast(LinkQuery q) {
        while (q.innerQuery != null)
            q = (LinkQuery) q.innerQuery;
        return q;
    }

    private static Query getWhereQuery(AggregationGroup group) {
        ArrayList<Query> queryList = new ArrayList<Query>();

        for (int i = 0; i < group.items.size(); i++) {
            AggregationGroupItem item = group.items.get(i);
            if (item.query != null) {
                if (item.isLink) {
                    ArrayList<LinkQuery> groups = new ArrayList<LinkQuery>();
                    for (int j = 0; j <= i; j++) {
                        AggregationGroupItem next = group.items.get(j);

                        if (next.fieldDef != null && next.fieldDef.isGroupField()) {
                            //group
                            if (groups.isEmpty()) {
                                for (int k = 0; k < next.nestedLinks.size(); k++) {
                                    LinkQuery lq = new LinkQuery(LinkQuery.ANY, next.nestedLinks.get(k).name, null);
                                    groups.add(lq);
                                }
                            } else {
                                ArrayList<LinkQuery> newSet = new ArrayList<LinkQuery>();
                                for (int k = 0; k < next.nestedLinks.size(); k++) {
                                    LinkInfo info = next.nestedLinks.get(k);

                                    for (int l = 0; l < groups.size(); l++) {
                                        LinkQuery linkQuery = (LinkQuery) QueryUtils.CloneQuery(groups.get(l));
                                        LinkQuery last = GetLast(linkQuery);
                                        last.innerQuery = new LinkQuery(LinkQuery.ANY, info.name, null);
                                        newSet.add(linkQuery);
                                    }
                                }
                                groups = newSet;
                            }
                        } else {
                            if (groups.isEmpty()) {
                                groups.add(new LinkQuery(LinkQuery.ANY, next.name, null));
                            } else {
                                for (int k = 0; k < groups.size(); k++) {
                                    LinkQuery linkQuery = groups.get(k);
                                    LinkQuery last = GetLast(linkQuery);
                                    LinkQuery lq = new LinkQuery(LinkQuery.ANY, next.name, null);
                                    last.innerQuery = lq;
                                }
                            }
                        }
                    }
                    switch (groups.size()) {
                        case 0:
                            throw new IllegalArgumentException("Cannot create query for " + item.name);
                        case 1:
                            GetLast(groups.get(0)).innerQuery = QueryUtils.CloneQuery(item.query);
                            queryList.add(groups.get(0));
                            break;
                        default:
                            for (int j = 0; j < groups.size(); j++) {
                                GetLast(groups.get(j)).innerQuery = QueryUtils.CloneQuery(item.query);
                            }
                            OrQuery or = new OrQuery();
                            or.subqueries.addAll(groups);
                            queryList.add(or);
                            break;
                    }
                } else
                    throw new IllegalArgumentException("Error: " + item.name + " is not a link or group name");
            }
        }
        if (!queryList.isEmpty()) {
            if (queryList.size() > 1) {
                AndQuery andQuery = new AndQuery();
                andQuery.subqueries.addAll(queryList);
                return andQuery;
            } else {
                return queryList.get(0);
            }
        } else {
            return null;
        }
    }

    private static ArrayList<ArrayList<Item>> extractAgTokens(Context context) {

        ArrayList<ArrayList<Item>> result = new ArrayList<ArrayList<Item>>();

        ArrayList<Item> items = new ArrayList<Item>();
        for (int i = 0; i < context.items.size(); i++) {
            GrammarItem grammarItem = context.items.get(i);

            if (grammarItem.getType().equals("newGroup")) {

                Item item = new Item();
                item.item = grammarItem;
                item.item.setType("endGroup");
                items.add(item);

                result.add(items);
                items = new ArrayList<Item>();
                continue;
            }
            if (grammarItem.getType().equals("semantic") && grammarItem.getValue().equals("stopWordAny")) {
                grammarItem.setType(SemanticNames.STOPVALUEANY);
            }

            if (grammarItem.getType().equals("ignore")) {
                //we need this item for original query text extraction
                Item item = new Item();
                item.item = grammarItem;
                items.add(item);
            }

            if (grammarItem.getValue().equals(SemanticNames.WHERE)) {
                ArrayList<GrammarItem> sublist = new ArrayList<GrammarItem>();
                Item prev = null;
                if (items.size() == 0) {
                    prev = new Item();
                    prev.item = grammarItem;
                    prev.item.setType("global");
                    items.add(prev);
                } else
                    prev = items.get(items.size() - 1);

                if (prev.queryItems == null)
                    prev.queryItems = new ArrayList<ArrayList<GrammarItem>>();
                prev.queryItems.add(sublist);
                while (true) {
                    i++;
                    grammarItem = context.items.get(i);
                    if (grammarItem.getValue().equals(SemanticNames.ENDWHERE)) {
                        break;
                    }

                    sublist.add(grammarItem);
                }
            } else {
                String type = grammarItem.getType();

                if (type.equals(SemanticNames.LEXEM) ||
                        type.equals(SemanticNames.TRUNCATE_VALUE) ||
                        type.equals(SemanticNames.TRUNCATE_SUBFIELD_VALUE) ||
                        type.equals(SemanticNames.BATCH_VALUE) ||
                        type.equals(SemanticNames.TOPBOTTOMVALUE) ||
                        type.equals(SemanticNames.AGGREGATION_METRIC_FUNCTION_NAME) ||
                        type.equals(SemanticNames.TIMEZONEVALUE) ||
                        type.equals(SemanticNames.TIMEZONEDISPLAYNAME) ||
                        type.equals(SemanticNames.TOPBOTTOM) ||
                        type.equals(SemanticNames.STOPVALUEANY) ||
                        type.equals(SemanticNames.LOWER) ||
                        type.equals(SemanticNames.UPPER) ||
                        type.equals(SemanticNames.GROUP) ||
                        type.equals(SemanticNames.TERMS) ||
                        type.equals(SemanticNames.TRUNCATE) ||
                        type.equals("endGroup") ||
                        type.equals(SemanticNames.BATCH) ||
                        type.equals(SemanticNames.STOPVALUE) ||
                        type.equals(SemanticNames.ALIAS) ||
                        type.equals(SemanticNames.EXCLUDEVALUE) ||
                        type.equals(SemanticNames.EXCLUDELIST) ||
                        type.equals(SemanticNames.INCLUDELIST) ||
                        type.equals(SemanticNames.TRANSITIVE) || type.equals(SemanticNames.TRANSITIVE_VALUE)
                        ) {
                    Item item = new Item();
                    item.item = grammarItem;
                    items.add(item);
                }

                if (grammarItem.getType().equals("token") && (grammarItem.getValue().equals(",") || grammarItem.getValue().equals(")"))) {
                    Item item = new Item();
                    item.item = grammarItem;
                    items.add(item);
                }
            }
        }
        if (items.size() > 0) {
            result.add(items);
        }
        return result;
    }

    private static ArrayList<Item> extractTokens(Context context) {

        ArrayList<Item> items = new ArrayList<Item>();
        for (int i = 0; i < context.items.size(); i++) {
            GrammarItem grammarItem = context.items.get(i);

            if (grammarItem.getType().equals("InputPointer")) {
                Item item = new Item();
                item.item = grammarItem;
                items.add(item);
                continue;
            }

            if (grammarItem.getType().equals("op") || grammarItem.getType().equals("number"))
                continue;

            if (grammarItem.getType().equals("semantic") && grammarItem.getValue().equals("endMetric"))
                continue;

            if (grammarItem.getType().equals("semantic") && grammarItem.getValue().equals("stopWordAny")) {
                grammarItem.setType(SemanticNames.STOPVALUEANY);
            }

            if (grammarItem.getValue().equals(SemanticNames.WHERE)) {
                ArrayList<GrammarItem> sublist = new ArrayList<GrammarItem>();
                Item prev = items.get(items.size() - 1);
                if (prev.queryItems == null) {
                    prev.queryItems = new ArrayList<ArrayList<GrammarItem>>();
                }
                prev.queryItems.add(sublist);
                while (true) {
                    i++;
                    grammarItem = context.items.get(i);
                    if (grammarItem.getValue().equals(SemanticNames.ENDWHERE)) {
                        break;
                    }

                    sublist.add(grammarItem);
                }
            } else {
                String type = grammarItem.getType();

                if (type.equals(SemanticNames.LEXEM) ||
                        type.equals(SemanticNames.TRUNCATE_VALUE) ||
                        type.equals(SemanticNames.TRUNCATE_SUBFIELD_VALUE) ||
                        type.equals(SemanticNames.BATCH_VALUE) ||
                        type.equals(SemanticNames.TOPBOTTOMVALUE) ||
                        type.equals(SemanticNames.AGGREGATION_METRIC_FUNCTION_NAME) ||
                        type.equals(SemanticNames.TIMEZONEVALUE) ||
                        type.equals(SemanticNames.TIMEZONEDISPLAYNAME) ||
                        type.equals(SemanticNames.TOPBOTTOM) ||
                        type.equals(SemanticNames.STOPVALUEANY) ||
                        type.equals(SemanticNames.STOPVALUE) ||
                        type.equals(SemanticNames.EXCLUDEVALUE) ||
                        type.equals(SemanticNames.ALIAS) ||
                        type.equals(SemanticNames.ASC) ||
                        type.equals(SemanticNames.DESC) ||
                        type.equals("IncludeList") ||
                        type.equals("ExcludeList")
                        ) {
                    Item item = new Item();
                    item.item = grammarItem;
                    items.add(item);
                }

                if (grammarItem.getType().equals("token") && grammarItem.getValue().equals(",")) {
                    Item item = new Item();
                    item.item = grammarItem;
                    items.add(item);
                }
            }
        }

        return items;
    }

    private static void AddLinkItem(ArrayList<Item> grammarItems, GrammarItem gitem) {
        Item item = new Item();
        item.item = gitem;
        grammarItems.add(item);
    }

    private static ArrayList<Item> extractMetricTokens(Context context) {

        Calendar calendar = Calendar.getInstance();
        long currentDate = calendar.getTimeInMillis();

        ArrayList<Item> items = new ArrayList<Item>();
        for (int i = 0; i < context.items.size(); i++) {
            GrammarItem grammarItem = context.items.get(i);
            String itemType = grammarItem.getType();

            if (grammarItem.getType().equals("op") || grammarItem.getType().equals("number") || grammarItem.getType().equals("InputPointer")) {
                Item item = new Item();
                item.item = grammarItem;
                items.add(item);
                continue;
            }
            if (grammarItem.getType().equals("datediff") || (grammarItem.getType().equals("semantic") && grammarItem.getValue().equals("datediff_calc"))) {
                Item item = new Item();
                item.item = grammarItem;
                items.add(item);
                continue;
            }

            if (grammarItem.getValue().equals(SemanticNames.CalculateNow)) {
                Item last = DropItem(items);
                String type = last.item.getType();
                String value = last.item.getValue();
                String timezone = "UTC";
                String timezoneValue = null;
                String units = null;
                Integer intvalue = null;

                while (!type.equals(SemanticNames.Now)) {
                    if (type.equals(SemanticNames.NowUnits)) {
                        units = value;
                    }
                    if (type.equals(SemanticNames.PositiveNumber)) {
                        try {
                            intvalue = Integer.parseInt(value);
                        } catch (Exception e) {
                            throw new IllegalArgumentException("Bad now offset units  value: " + value);
                        }
                    }
                    if (type.equals(SemanticNames.NegativeNumber)) {
                        try {
                            intvalue = Integer.parseInt("-" + value);
                        } catch (Exception e) {
                            throw new IllegalArgumentException("Bad now offset units  value: " + "-" + value);
                        }
                    }
                    if (type.equals(SemanticNames.TIMEZONEVALUE)) {
                        timezoneValue = value;
                    }
                    if (type.equals(SemanticNames.TIMEZONEDISPLAYNAME)) {
                        timezone = value;
                    }
                    last = DropItem(items);
                    type = last.item.getType();
                    value = last.item.getValue();
                }

                if (timezoneValue != null) {
                    calendar = TimeUtils.getCalendarByValue(timezoneValue);
                } else {
                    calendar = TimeUtils.getCalendarByName(timezone);
                }
                calendar.setTimeInMillis(currentDate);
                Calendar start = TimeUtils.getNowValue(calendar, units, intvalue);
                //AddLinkItem(items, new Literal(TimeUtils.toUtcTime(start), "string", -1));
                Item item = new Item();
                item.item = new Literal(TimeUtils.toUtcTime(start), "datediff", -1);
                items.add(item);
                continue;
            }

            if (itemType.equals("PeriodGMT")) {
                calendar = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
                calendar.setTimeInMillis(currentDate);
                continue;
            }

            if (itemType.equals("TimeZoneDisplayName")) {
                calendar = TimeUtils.getCalendarByName(grammarItem.getValue());
                calendar.setTimeInMillis(currentDate);
                continue;
            }

            if (itemType.equals("TimeZoneValue")) {
                calendar = TimeUtils.getCalendarByValue(grammarItem.getValue());
                calendar.setTimeInMillis(currentDate);
                continue;
            }

            if (itemType.equals("transitiveValue")) {
                AddLinkItem(items, grammarItem);
                continue;
            }

            if (itemType.equals("transitive")) {
                AddLinkItem(items, grammarItem);
                continue;
            }


            if (itemType.equals(SemanticNames.Now)) {
                AddLinkItem(items, grammarItem);
                continue;
            }
            if (itemType.equals(SemanticNames.PositiveNumber)) {
                AddLinkItem(items, grammarItem);
                continue;
            }
            if (itemType.equals(SemanticNames.NegativeNumber)) {
                AddLinkItem(items, grammarItem);
                continue;
            }
            if (itemType.equals(SemanticNames.NowUnits)) {
                AddLinkItem(items, grammarItem);
                continue;
            }

            if (grammarItem.getType().equals("semantic") && grammarItem.getValue().equals("endMetric")) {
                Item item = new Item();
                item.item = grammarItem;
                items.add(item);
                continue;
            }

            if (grammarItem.getType().equals("semantic") && grammarItem.getValue().equals("stopWordAny")) {
                grammarItem.setType(SemanticNames.STOPVALUEANY);
            }

            if (grammarItem.getValue().equals(SemanticNames.WHERE)) {
                ArrayList<GrammarItem> sublist = new ArrayList<GrammarItem>();
                Item prev = items.get(items.size() - 1);
                if (prev.queryItems == null) {
                    prev.queryItems = new ArrayList<ArrayList<GrammarItem>>();
                }
                prev.queryItems.add(sublist);
                while (true) {
                    i++;
                    grammarItem = context.items.get(i);
                    if (grammarItem.getValue().equals(SemanticNames.ENDWHERE)) {
                        break;
                    }

                    sublist.add(grammarItem);
                }
            } else {
                String type = grammarItem.getType();

                if (type.equals(SemanticNames.LEXEM) ||
                        type.equals(SemanticNames.TRUNCATE_VALUE) ||
                        type.equals(SemanticNames.TRUNCATE_SUBFIELD_VALUE) ||
                        type.equals(SemanticNames.BATCH_VALUE) ||
                        type.equals(SemanticNames.TOPBOTTOMVALUE) ||
                        type.equals(SemanticNames.AGGREGATION_METRIC_FUNCTION_NAME) ||
                        type.equals(SemanticNames.TIMEZONEVALUE) ||
                        type.equals(SemanticNames.TIMEZONEDISPLAYNAME) ||
                        type.equals(SemanticNames.TOPBOTTOM) ||
                        type.equals(SemanticNames.STOPVALUEANY) ||
                        type.equals(SemanticNames.STOPVALUE) ||
                        type.equals(SemanticNames.EXCLUDEVALUE) ||
                        type.equals(SemanticNames.ALIAS) ||
                        type.equals(SemanticNames.ASC) ||
                        type.equals(SemanticNames.DESC) ||
                        type.equals("IncludeList") ||
                        type.equals("ExcludeList")
                        ) {
                    Item item = new Item();
                    item.item = grammarItem;
                    items.add(item);
                }

                if (grammarItem.getType().equals("token") && grammarItem.getValue().equals(",")) {
                    Item item = new Item();
                    item.item = grammarItem;
                    items.add(item);
                }
            }
        }
        return items;
    }

    public static List<LinkInfo> GetNestedFieldsInfo(FieldDefinition groupFieldDef) {
        ArrayList<LinkInfo> result = new ArrayList<LinkInfo>();
        for (FieldDefinition nestedFieldDef : groupFieldDef.getNestedFields()) {

            if (nestedFieldDef.isGroupField())
                result.addAll(GetNestedFieldsInfo(nestedFieldDef));
            else
                result.add(new LinkInfo(nestedFieldDef.getName(), nestedFieldDef));
        }
        return result;
    }

    public static List<String> GetMetricFields(AggregationMetric metric) {
        if (metric == null)
            return null;

        ArrayList<String> result = new ArrayList<String>();

        if (metric.items == null)
            return result;

        for (int i = 0; i < metric.items.size(); i++) {
            AggregationGroupItem aggregationGroupItem = metric.items.get(i);
            result.add(aggregationGroupItem.name);
        }
        return result;
    }

    public static int compareBatchValues(FieldType type, Object arg1, Object arg2) {
        switch (type) {
            case INTEGER: {
                return ((Integer) arg1).compareTo(((Integer) arg2));
            }
            case LONG: {
                return ((Long) arg1).compareTo(((Long) arg2));
            }
            case FLOAT: {
                return ((Float) arg1).compareTo(((Float) arg2));
            }
            case DOUBLE: {
                return ((Double) arg1).compareTo(((Double) arg2));
            }

            case TEXT:
                return ((String) arg1).compareTo(((String) arg2));
            case TIMESTAMP:
                return ((Date) arg1).compareTo(((Date) arg2));

            default:
                throw new IllegalArgumentException("Not supported type for batch: " + type);
        }
    }

    private static Object convert(FieldType type, String value) {

        switch (type) {
            case BOOLEAN:
                return value;
            case INTEGER: {
                return Integer.parseInt(value);
            }
            case LONG: {
                return Long.parseLong(value);
            }
            case FLOAT: {
                return Float.parseFloat(value);
            }
            case DOUBLE: {
                return Double.parseDouble(value);
            }

            case TEXT:
                return value;
            case TIMESTAMP:

                for (SimpleDateFormat sdf : QueryUtils.DATE_FORMATS) {
                    try {
                        sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
                        return sdf.parse(value);
                    } catch (Exception e) {
                        // Skip this format.
                    }
                }
                throw new IllegalArgumentException("Unknown timestamp format: " + value);

            case BINARY:
            default:
                throw new IllegalArgumentException("Unknown type: " + type);
        }
    }

    static boolean isCorrectTimeZone(String name) {
        String[] timeZoneIds = TimeZone.getAvailableIDs();
        if (availableTimeZones == null) {
            HashSet<String> zones = new HashSet<String>();
            for (final String id : timeZoneIds) {
                zones.add(id);
            }
            availableTimeZones = zones;
        }
        return availableTimeZones.contains(name);

    }
}

class Item {
    public GrammarItem item;
    public ArrayList<ArrayList<GrammarItem>> queryItems;
}
