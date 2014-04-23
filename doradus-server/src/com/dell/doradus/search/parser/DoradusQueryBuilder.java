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
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.search.parser.grammar.Context;
import com.dell.doradus.search.parser.grammar.GrammarItem;
import com.dell.doradus.search.parser.grammar.Literal;
import com.dell.doradus.search.query.*;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Stack;
import java.util.TimeZone;

enum QueryFieldType {
    Field,
    Link,
    Group,
    MultiValueScalar,
    Unknown
}

interface QueryVisitor {
    public void visit(Query query);
}

interface QueryTreeVisitor {
    public Query visit(Stack<String> links, Query query);
}

class BuilderContext {

    public Stack<Query> queries = new Stack<Query>();
    public Stack<TableDefinition> tables = new Stack<TableDefinition>();
    public TableDefinition definition;
    private Stack<String> operations = new Stack<String>();

    BuilderContext() {
        this(null);
    }

    BuilderContext(TableDefinition def) {
        definition = def;
    }

    public String operationPop() {
        return operations.pop();
    }

    public void operationPush(String operation) {
        operations.push(operation);
    }

    public boolean operationEmpty() {
        return operations.empty();
    }

    public String operationPeek() {
        return operations.peek();
    }
}

class FieldVisitor implements QueryVisitor {

    String field;
    String operation;

    public FieldVisitor(String field) {
        this(field, null);
    }

    public FieldVisitor(String field, String operation) {
        this.field = field;
        this.operation = operation;
    }

    public void visit(Query query) {
        if (query instanceof BinaryQuery) {
            BinaryQuery bq = (BinaryQuery) query;
            bq.field = field;
            if (operation != null)
                bq.operation = operation;
        }
        if (query instanceof RangeQuery) {
            RangeQuery bq = (RangeQuery) query;
            bq.field = field;
            //if (operation != null)
            //    bq.operation = operation;
        }
    }
}

class LinkIdVisitor implements QueryTreeVisitor {

    String field;
    String quantifier;

    public LinkIdVisitor(String quantifier, String field) {
        this.field = field;
        this.quantifier = quantifier;
    }

    public Query visit(Stack<String> links, Query query) {
        if (query instanceof BinaryQuery) {
            BinaryQuery bq = (BinaryQuery) query;
            LinkIdQuery li = new LinkIdQuery(quantifier, field, bq.value);
            return li;
        }
        return query;
    }
}

class MultiValueVisitor implements QueryTreeVisitor {

    String field;
    String quantifier;
    String operation;

    public MultiValueVisitor(String quantifier, String field, String operation) {
        this.field = field;
        this.quantifier = quantifier;
        this.operation = operation;
    }

    public Query visit(Stack<String> links, Query query) {
        if (query instanceof BinaryQuery) {
            BinaryQuery bq = (BinaryQuery) query;
            bq.operation = operation;
            bq.field = field;
            MVSBinaryQuery mq = new MVSBinaryQuery(quantifier, bq);
            return mq;
        }
        return query;
    }
}

class DatePartQueryVisitor implements QueryTreeVisitor {

    String field;
    int value;
    String operation;

    public DatePartQueryVisitor(String field, String operation, int value) {
        this.field = field;
        this.operation = operation;
        this.value = value;
    }

    public Query visit(Stack<String> links, Query query) {
        if (query instanceof BinaryQuery) {
            BinaryQuery bq = (BinaryQuery) query;
            bq.operation = operation;
            bq.field = field;
            DatePartBinaryQuery mq = new DatePartBinaryQuery(value, bq);
            return mq;
        }
        return query;
    }
}

class LinkIdReplaceVisitor extends LinkIdVisitor {

    public LinkIdReplaceVisitor(String quantifier, String field) {
        super(quantifier, field);
    }

    public Query visit(Stack<String> links, Query query) {
        if (query instanceof BinaryQuery) {
            BinaryQuery bq = (BinaryQuery) query;
            if (bq.field == null && BinaryQuery.CONTAINS.equals(bq.operation)) {
                //otarakanov: transitive link query has IdQuery as its inner query
                //LinkIdQuery li = new LinkIdQuery(quantifier, field, bq.value);
                IdQuery li = new IdQuery(bq.value);
                return li;
            }
        }
        return query;
    }
}

class FieldNameVisitor implements QueryTreeVisitor {

    public static final String SystemFieldPrefix = "_";

    public Query visit(Stack<String> links, Query query) {
        if (query == null)
            return query;

        String name = QueryUtils.GetFieldName(query);

        if (name == null)
            return query;

        if (name.startsWith(SystemFieldPrefix)) {
            if (!QueryUtils.isSystemField(name))
                throw new IllegalArgumentException("Unknown system field " + name);
        }
        return query;
    }
}

class LinkCheckVisitor implements QueryTreeVisitor {

    TableDefinition tableDefinition;

    public LinkCheckVisitor(TableDefinition tableDef) {
        tableDefinition = tableDef;
    }

    public Query visit(Stack<String> links, Query query) {

        if (query instanceof BinaryQuery) {
            BinaryQuery bq = (BinaryQuery) query;
            if (bq.field != null) {
                if (bq.field.equals("_ID")) {
                    if (bq.operation.equals(BinaryQuery.CONTAINS)) {
                        throw new IllegalArgumentException("Operation ':' is not supported for _ID field");
                    }
                    IdQuery id = new IdQuery(bq.value);
                    return id;
                }

                if (BinaryQuery.EQUALS.equals(bq.operation)) {
                    if (tableDefinition != null) {
                        if (!links.empty()) {
                            ArrayList<String> path = QueryUtils.GetLinkPath(links);
                            path.add(bq.field);
                            if (QueryUtils.IsLink(path, tableDefinition)) {
                                LinkIdQuery li = new LinkIdQuery(LinkQuery.ANY, bq.field, bq.value);
                                return li;
                            }
                        } else {
                            if (tableDefinition.isLinkField(bq.field)) {
                                LinkIdQuery li = new LinkIdQuery(LinkQuery.ANY, bq.field, bq.value);
                                return li;
                            }
                        }
                    }
                }
            }
        }

        if (query instanceof LinkQuery || query instanceof TransitiveLinkQuery) {
            if (tableDefinition != null) {
                ArrayList<String> path = new ArrayList<String>();
                if (!links.empty()) {
                    path = QueryUtils.GetLinkPath(links);
                }
                String linkName = "";
                if (query instanceof LinkQuery) {
                    linkName = ((LinkQuery) query).link;
                } else {
                    linkName = ((TransitiveLinkQuery) query).link;
                }
                path.add(linkName);

                switch (QueryUtils.GetFieldType(path, tableDefinition)) {
                    case Link:
                        break;
                    case Group:
                        FieldDefinition groupfield = QueryUtils.GetField(path, tableDefinition);
                        ArrayList<String> nestedLinks = QueryUtils.GetNestedFields(groupfield);
                        if (nestedLinks.size() == 0) {
                            throw new IllegalArgumentException("Group field error: " + groupfield.getName() + " (" + QueryUtils.LinkName(path) + ") Does not contain any links ");
                        }
                        //TO Do : add group field processing
                        break;

                    default:
                        //todo show table name
                        throw new IllegalArgumentException("Error: " + linkName + " (" + QueryUtils.LinkName(path) + ") is not a link");
                }
            }
        }

        if (query instanceof LinkIdQuery) {
            if (tableDefinition != null) {
                LinkIdQuery q = (LinkIdQuery) query;
                ArrayList<String> path = new ArrayList<String>();
                if (!links.empty()) {
                    path = QueryUtils.GetLinkPath(links);
                }
                path.add(q.link);
                if (!QueryUtils.IsLink(path, tableDefinition)) {
                    BinaryQuery bq = new BinaryQuery(BinaryQuery.EQUALS, q.link, q.id);
                    return bq;
                }
            }
        }
        return query;
    }

}

class LinkItem {
    public String operation;
    public GrammarItem item;
    public GrammarItem value;
    public ArrayList<LinkItem> items = new ArrayList<LinkItem>();
    public ArrayList<ArrayList<LinkItem>> filters;

    LinkItem(GrammarItem item) {
        this.item = item;
    }

    public String toString() {
        String result = "null";
        if (item != null) {
            result = item.toString();
        }
        return result;
    }
}

public class DoradusQueryBuilder {

    public static void traverse(Query query, QueryVisitor visitor) {
        visitor.visit(query);
        if (query instanceof AndQuery) {
            AndQuery aq = (AndQuery) query;
            for (int i = 0; i < aq.subqueries.size(); i++) {
                traverse(aq.subqueries.get(i), visitor);
            }
            return;
        }

        if (query instanceof BinaryQuery) {
            visitor.visit(query);
            return;
        }
        if (query instanceof LinkCountQuery) {
            visitor.visit(query);
            return;
        }
        if (query instanceof LinkIdQuery) {
            return;
        }
        if (query instanceof LinkQuery) {
            LinkQuery lq = (LinkQuery) query;
            visitor.visit(lq);
            traverse(lq.innerQuery, visitor);
            return;
        }
        if (query instanceof NoneQuery) {
            return;
        }
        if (query instanceof NotQuery) {
            NotQuery nq = (NotQuery) query;
            traverse(nq.innerQuery, visitor);
            return;
        }
        if (query instanceof OrQuery) {
            OrQuery aq = (OrQuery) query;
            for (int i = 0; i < aq.subqueries.size(); i++) {
                visitor.visit(aq.subqueries.get(i));
                traverse(aq.subqueries.get(i), visitor);
            }
            return;
        }
        if (query instanceof RangeQuery) {
            visitor.visit(query);
            return;
        }

        if (query instanceof TransitiveLinkQuery) {
            TransitiveLinkQuery lq = (TransitiveLinkQuery) query;
            visitor.visit(lq);
            traverse(lq.innerQuery, visitor);
            return;
        }
        throw new IllegalArgumentException("Unknown type of query:" + query.getClass());
    }

    public static Query traverseTree(Stack<String> links, Query query, QueryTreeVisitor visitor) {

        query = visitor.visit(links, query);

        if (query instanceof AndQuery) {
            AndQuery aq = (AndQuery) query;
            for (int i = 0; i < aq.subqueries.size(); i++) {
                Query nextChild = aq.subqueries.get(i);
                nextChild = visitor.visit(links, nextChild);
                aq.subqueries.set(i, nextChild);
                traverseTree(links, nextChild, visitor);
            }
            return query;
        }

        if (query instanceof LinkQuery) {
            LinkQuery lq = (LinkQuery) query;
            links.push(lq.link);
            lq.innerQuery = traverseTree(links, lq.innerQuery, visitor);
            links.pop();
            return query;
        }

        if (query instanceof TransitiveLinkQuery) {
            TransitiveLinkQuery lq = (TransitiveLinkQuery) query;
            links.push(lq.link);
            lq.innerQuery = traverseTree(links, lq.innerQuery, visitor);
            links.pop();
            return query;
        }


        if (query instanceof NotQuery) {
            NotQuery nq = (NotQuery) query;
            nq.innerQuery = visitor.visit(links, nq.innerQuery);
            nq.innerQuery = traverseTree(links, nq.innerQuery, visitor);
            return query;
        }
        if (query instanceof OrQuery) {
            OrQuery aq = (OrQuery) query;
            for (int i = 0; i < aq.subqueries.size(); i++) {
                Query nextChild = aq.subqueries.get(i);
                nextChild = visitor.visit(links, nextChild);
                aq.subqueries.set(i, nextChild);
                traverseTree(links, nextChild, visitor);
            }
            return query;
        }
        return query;
    }

    public static void pushGrammarItem(BuilderContext builderContext, GrammarItem item) {
        BinaryQuery bq = new BinaryQuery(BinaryQuery.CONTAINS, null, item.getValue());
        builderContext.queries.push(bq);
    }

    public static Query Build(ArrayList<GrammarItem> originalItems, TableDefinition definition) {
        ArrayList<LinkItem> items = extractTokens(originalItems);
        BuilderContext context = new BuilderContext();
        context.definition = definition;
        return SearchQueryBuilder.build(items, context);
    }

    public static Query Build(Context context) {
        return Build(context, null);
    }

    public static Query Build(Context context, TableDefinition definition) {

        if (context == null)
            throw new IllegalArgumentException("Cannot create query:Context is null");

        if (context.items.isEmpty())
            return new NoneQuery();

        return Build(context.items, definition);
    }

    public static Query Build(String string, TableDefinition definition) {
        string = definition.replaceAliaces(string);
        Parser parser = Parser.GetDoradusQueryParser();
        ParseResult result = parser.Parse(string);
        if (result.error == null) {
            return DoradusQueryBuilder.Build(result.context, definition);
        } else {
            throw new IllegalArgumentException(result.error);
        }
    }

    public static ArrayList<GrammarItem> ParseFieldSet(String string) {
        Parser parser = Parser.GetFieldSetParser();
        ParseResult result = parser.Parse(string);
        if (result.error == null) {
            return result.context.items;
        } else {
            throw new IllegalArgumentException(result.error);
        }
    }

    private static void AddLinkItem(ArrayList<LinkItem> grammarItems, GrammarItem item) {
        grammarItems.add(new LinkItem(item));
    }

    private static void MergeLastTwo(ArrayList<LinkItem> grammarItems) {
        LinkItem bi1 = grammarItems.remove(grammarItems.size() - 1);
        LinkItem bi2 = grammarItems.remove(grammarItems.size() - 1);

        if (bi2.item == null) {
            bi2.items.add(bi1);
            grammarItems.add(bi2);
            return;
        }

        if (bi2.item.getType().equals("token")) {
            grammarItems.add(bi2);
            grammarItems.add(bi1);

        } else {
            LinkItem bi3 = new LinkItem(null);
            bi3.items.add(bi2);
            bi3.items.add(bi1);
            grammarItems.add(bi3);
        }
    }

    private static void SetOperation(ArrayList<LinkItem> grammarItems, GrammarItem gi) {
        LinkItem bi1 = grammarItems.get(grammarItems.size() - 1);
        if (bi1.items.size() > 0)
            bi1.items.get(bi1.items.size() - 1).operation = gi.getValue();
        else
            bi1.operation = gi.getValue();
    }

    private static void SetValue(ArrayList<LinkItem> grammarItems, GrammarItem gi) {
        LinkItem bi1 = grammarItems.get(grammarItems.size() - 1);
        if (bi1.items.size() > 0)
            bi1.items.get(bi1.items.size() - 1).value = gi;
        else
            bi1.value = gi;
    }

    private static LinkItem DropItem(ArrayList<LinkItem> grammarItems, String value) {
        LinkItem bi = grammarItems.remove(grammarItems.size() - 1);
        if (value != null) {
            if (!(bi.item != null && bi.item.getValue().equals(value)))
                throw new IllegalArgumentException("Internal error: value does not match: " + value);
        }
        return bi;
    }

    private static LinkItem DropItem(ArrayList<LinkItem> grammarItems) {
        return DropItem(grammarItems, null);
    }

    public static ArrayList<LinkItem> extractTokens(ArrayList<GrammarItem> grammarItems) {
        ArrayList<LinkItem> items = new ArrayList<LinkItem>();
        Stack<ArrayList<LinkItem>> stack = new Stack<ArrayList<LinkItem>>();

        Calendar calendar = Calendar.getInstance();
        long currentDate = calendar.getTimeInMillis();
        boolean WHERE_SIBLING_START = false;

        for (int i = 0; i < grammarItems.size(); i++) {
            GrammarItem grammarItem = grammarItems.get(i);
            String itemType = grammarItem.getType();

            if (itemType.equals("lexem") || itemType.equals("string")) {
                AddLinkItem(items, grammarItem);
                continue;
            }

            if (itemType.equals("ignore"))
                continue;

            if (itemType.equals("semantic")) {
                if (grammarItem.getValue().equals("ImpliedAnd")) {
                    AddLinkItem(items, new Literal("AND", "token", -1));
                    continue;
                }

                if (grammarItem.getValue().equals("SearchCriteriaStart")) {
                    AddLinkItem(items, grammarItem);
                    continue;
                }

                if (grammarItem.getValue().equals("WHERE_FILTER_START")) {
                    stack.push(items);
                    items = new ArrayList<LinkItem>();
                    AddLinkItem(items, new Literal("ADDF", "token", -1));
                    WHERE_SIBLING_START = true;
                    continue;
                }

                if (grammarItem.getValue().equals("WHERE_FILTER_END")) {
                    AddLinkItem(items, new Literal(")", "token", -1));
                    ArrayList<LinkItem> filter = items;
                    items = stack.pop();
                    LinkItem last = DropItem(items);
                    items.add(last);

                    if (last.item == null) {
                        if (last.items.size() == 0)
                            throw new IllegalArgumentException("Internal error:empty filter");
                        last = last.items.get(last.items.size() - 1);
                    }

                    if (last.filters == null)
                        last.filters = new ArrayList<ArrayList<LinkItem>>();
                    last.filters.add(filter);

                    continue;
                }


                if (grammarItem.getValue().equals("WHERE_CONTINUE_START")) {
                    AddLinkItem(items, new Literal("AND", "token", -1));
                    AddLinkItem(items, new Literal("(", "token", -1));
                    continue;
                }

                if (grammarItem.getValue().equals("WHERE_CONTINUE_END")) {
                    AddLinkItem(items, new Literal(")", "token", -1));
                    AddLinkItem(items, new Literal(")", "token", -1));
                    continue;
                }

                if (grammarItem.getValue().equals("WHERE_SIBLING_START")) {
                    AddLinkItem(items, new Literal("AND", "token", -1));
                    WHERE_SIBLING_START = true;
                    continue;
                }
                if (grammarItem.getValue().equals("WHERE_SIBLING_END")) {

                    AddLinkItem(items, new Literal(")", "token", -1));

                    continue;
                }
                if (grammarItem.getValue().equals("ImpliedOr")) {
                    AddLinkItem(items, new Literal("OR", "token", -1));
                    continue;
                }

                if (grammarItem.getValue().equals("transitiveValue")) {
                    DropItem(items, ")");
                    LinkItem bi = DropItem(items);
                    DropItem(items, "(");
                    SetValue(items, bi.item);
                    continue;
                }

                if (grammarItem.getValue().equals("dotSemantic")) {
                    MergeLastTwo(items);
                    continue;
                }

                if (grammarItem.getValue().equals("linkFunctionEndSemantic")) {
                    DropItem(items, ")");
                    LinkItem bi = DropItem(items);
                    DropItem(items, "(");
                    LinkItem function = DropItem(items);
                    function.operation = function.item.getValue();
                    function.item = null;
                    function.items.add(bi);

                    items.add(function);
                    continue;
                }
                if (grammarItem.getValue().equals(SemanticNames.CalculateNow)) {
                    LinkItem last = DropItem(items);
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
                    AddLinkItem(items, new Literal(TimeUtils.toUtcTime(start), "string", -1));
                    continue;
                }

                if (grammarItem.getValue().equals("CalculatePeriod")) {
                    LinkItem last = DropItem(items);
                    String value = last.item.getType();
                    String units = last.item.getValue();
                    if (value.equals(SemanticNames.LastPeriodValue)) {
                        value = last.item.getValue();
                        units = DropItem(items).item.getValue();
                    } else
                        value = null;

                    Calendar start = TimeUtils.getPeriodStart(calendar, units, value);
                    Calendar end = TimeUtils.getPeriodEnd(calendar, units);
                    AddLinkItem(items, new Literal("[", "token", -1));
                    AddLinkItem(items, new Literal(TimeUtils.toUtcTime(start), "string", -1));
                    AddLinkItem(items, new Literal("TO", "token", -1));
                    AddLinkItem(items, new Literal(TimeUtils.toUtcTime(end), "string", -1));
                    if (TimeUtils.isThisUnit(units))
                        AddLinkItem(items, new Literal("}", "token", -1));
                    else
                        AddLinkItem(items, new Literal("]", "token", -1));
                    continue;
                }

                AddLinkItem(items, grammarItem);
            }

            if (itemType.equals("token")) {
                boolean notProcess = true;
                if (grammarItem.getValue().equals(",")) {
                    AddLinkItem(items, new Literal("OR", "token", -1));
                    notProcess = false;
                }
                if (grammarItem.getValue().equals("-")) {
                    AddLinkItem(items, new Literal("NOT", "token", -1));
                    notProcess = false;
                }
                if (grammarItem.getValue().equals("^")) {
                    SetOperation(items, grammarItem);
                    continue;
                }
                if (grammarItem.getValue().equals(".")) {
                    continue;
                }

                if (grammarItem.getValue().equals("IS")) {
                    AddLinkItem(items, new Literal("=", "token", -1));
                    continue;
                }

                if (grammarItem.getValue().equals("NULL")) {
                    AddLinkItem(items, new Literal(null, "lexem", -1));
                    continue;
                }

                if (grammarItem.getValue().equals("WHERE")) {
                    if (WHERE_SIBLING_START) {
                        WHERE_SIBLING_START = false;
                        continue;
                    }
                }
                if (notProcess)
                    AddLinkItem(items, grammarItem);
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

            if (itemType.equals(SemanticNames.ThisPeriodUnits)) {
                AddLinkItem(items, grammarItem);
                continue;
            }
            if (itemType.equals(SemanticNames.LastPeriodUnits)) {
                AddLinkItem(items, grammarItem);
                continue;
            }
            if (itemType.equals(SemanticNames.LastPeriodValue)) {
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
        }
        return items;
    }


}

