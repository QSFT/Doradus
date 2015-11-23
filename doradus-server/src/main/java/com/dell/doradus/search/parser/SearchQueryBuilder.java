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
import java.util.Stack;

import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.common.Utils;
import com.dell.doradus.search.aggregate.AggregationGroup;
import com.dell.doradus.search.aggregate.AggregationGroupItem;
import com.dell.doradus.search.parser.grammar.GrammarItem;
import com.dell.doradus.search.query.AndQuery;
import com.dell.doradus.search.query.BinaryQuery;
import com.dell.doradus.search.query.PathComparisonQuery;
import com.dell.doradus.search.query.FieldCountQuery;
import com.dell.doradus.search.query.FieldCountRangeQuery;
import com.dell.doradus.search.query.LinkCountQuery;
import com.dell.doradus.search.query.LinkCountRangeQuery;
import com.dell.doradus.search.query.LinkIdQuery;
import com.dell.doradus.search.query.LinkQuery;
import com.dell.doradus.search.query.MVSBinaryQuery;
import com.dell.doradus.search.query.NotQuery;
import com.dell.doradus.search.query.OrQuery;
import com.dell.doradus.search.query.PathCountRangeQuery;
import com.dell.doradus.search.query.Query;
import com.dell.doradus.search.query.RangeQuery;
import com.dell.doradus.search.query.TransitiveLinkQuery;

public class SearchQueryBuilder {

    private static boolean isImmediate(String operation) {
        if (operation.equals("}") || operation.equals("]") ||
                operation.equals(")") ||
                operation.equals("AND") || operation.equals("OR") ||
                operation.equals("EOF") || operation.equals("Criteria"))

            return true;
        return false;
    }

    public static void pushQuery(BuilderContext builderContext, Query query) {
        builderContext.queries.push(query);

        if (!builderContext.operationEmpty()) {
            String operation = builderContext.operationPop();
            if (operation.equals("NOT") || operation.equals(":")
                    || operation.equals(">") || operation.equals(">=") || operation.equals("<") || operation.equals("<=")) {
                DoOperation(operation, builderContext);
            } else {
                builderContext.operationPush(operation);
            }
        }
    }

    static void pushOperation(BuilderContext builderContext, String operation) {
        if (!isImmediate(operation)) {
            if (operation.equals("WHERE")) {
                Query q = builderContext.queries.peek();
                TableDefinition newDef = QueryUtils.GetTableContext(q, builderContext.definition);
                builderContext.tables.push(builderContext.definition);
                builderContext.definition = newDef;
            }
            builderContext.operationPush(operation);
            return;
        }

        if (operation.equals("}") || operation.equals("]")) {
            boolean maxI = false;
            if (operation.equals("]"))
                maxI = true;
            String op = builderContext.operationPop();
            if (op.equals("TO")) {
                DoOperation(op, builderContext);
                op = builderContext.operationPop();
                if (op.equals("{") || op.equals("[")) {
                    boolean minI = false;
                    if (op.equals("["))
                        minI = true;

                    RangeQuery rq = (RangeQuery) builderContext.queries.pop();
                    rq.minInclusive = minI;
                    rq.maxInclusive = maxI;

                    builderContext.queries.push(rq);
                    String op1 = builderContext.operationPeek();

                    if (op1.equals("=")) {
                        op1 = builderContext.operationPop();
                        DoOperation(op1, builderContext);
                    }
                    return;
                }
            }
        }

        if (operation.equals(")")) {
            while (!builderContext.operationEmpty()) {
                String op = builderContext.operationPop();
                if (op.equals("(")) {
                    if (!builderContext.operationEmpty()) {
                        String op1 = builderContext.operationPeek();
                        if (op1.equals(":") || op1.equals("=") || op1.equals("~=") ||
                            op1.equals("ANY") || op1.equals("ALL") || op1.equals("NONE")) {
                                DoOperation(builderContext.operationPop(), builderContext);
                                break;
                        }
                    }
                    break;
                } else {
                    DoOperation(op, builderContext);
                }
            }
        } else {
            int tokenPrecedence = 1;
            if (operation.equals("AND"))
                tokenPrecedence = 2;
            while (!builderContext.operationEmpty()) {
                String opTop = builderContext.operationPop();
                if (opTop.equals("(")) {
                    builderContext.operationPush(opTop);
                    break;
                } else {
                    int previousTokenPrecedence = 2;
                    if (opTop.equals("OR"))
                        previousTokenPrecedence = 1;
                    else
                        previousTokenPrecedence = 2;

                    if (previousTokenPrecedence < tokenPrecedence) // if prec of new op less than prec of old
                    {
                        builderContext.operationPush(opTop); // save newly-popped op
                        break;
                    } else {
                        if (operation.equals("Criteria")) {
                            Query f = builderContext.queries.pop();
                            Query s = builderContext.queries.pop();
                            Query t = QueryUtils.CloneQuery(s);
                            builderContext.queries.push(s);
                            builderContext.queries.push(f);
                            DoOperation(opTop, builderContext);
                            builderContext.queries.push(t);
                            builderContext.operationPush("ANDC") ;
                            return;
                        } else
                            DoOperation(opTop, builderContext);
                    }
                }
            }
            if (operation.equals("Criteria")) {
                operation = "ANDC";
            }

            builderContext.operationPush(operation);
        }
    }

    static void DoOperation(String op, BuilderContext builderContext) {

        if (op.equals("ADDF") || op.equals("Criteria")) {
            return;
        }

        if (op.equals("ANDC")) {
            Query second = builderContext.queries.pop();
            Query first = builderContext.queries.pop();
            Query result = QueryUtils.MergeAND(first, second);
            builderContext.queries.push(result);
            return;
        }


        if (op.equals("AND")) {
            Query second = builderContext.queries.pop();
            Query first = builderContext.queries.pop();

            if (first instanceof AndQuery) {
                ((AndQuery) first).subqueries.add(second);
                pushQuery(builderContext, first);
            } else {
                pushQuery(builderContext, CreateAndQuery(first, second));
            }
            return;
        }

        if (op.equals("OR")) {
            Query second = builderContext.queries.pop();
            Query first = builderContext.queries.pop();

            if (first instanceof OrQuery) {
                ((OrQuery) first).subqueries.add(second);
                pushQuery(builderContext, first);
            } else {
                pushQuery(builderContext, CreateOrQuery(first, second));
            }
            return;
        }

        if (op.equals("WHERE")) {
            Query first = builderContext.queries.pop();

            builderContext.definition = builderContext.tables.pop();
            Query second = builderContext.queries.pop();
            PerformWhereOperation(builderContext, first, second);
            return;
        }

        if (op.equals("=")) {
            PerformAssign(builderContext, BinaryQuery.EQUALS);
            return;
        }

        if (op.equals("NULL")) {
            PerformAssign(builderContext, "NULL");
            Query current = builderContext.queries.pop();
            boolean none = isNonePresent(current);
            if (current instanceof LinkQuery) {
                LinkQuery lq = (LinkQuery)current;
                if (LinkQuery.ALL.equals(lq.quantifier) || LinkQuery.NONE.equals(lq.quantifier)) {
                    while (lq.innerQuery != null) {
                        Query q = lq.innerQuery;
                        if (q instanceof LinkQuery) {
                            LinkQuery candidad  = (LinkQuery)q;
                            if (candidad.quantifier.equals(LinkQuery.ANY)) {
                                lq.innerQuery=new NotQuery(candidad);
                                if (none)   {
                                    removeNone(current);
                                    builderContext.queries.push(new NotQuery(current));
                                }
                                else
                                builderContext.queries.push(current);
                                return;
                            }else {
                             lq =candidad;
                            }
                        } else {
                            lq.innerQuery=new NotQuery(q);
                            if (none) {
                                builderContext.queries.push(new NotQuery(current));
                                removeNone(current);
                            }
                            else
                                builderContext.queries.push(current);return;
                        }
                    }
                    throw new RuntimeException("Wrong usage of ALL or ANY");
                } else {
                    builderContext.queries.push(new NotQuery(current));
                }
            } else {
                builderContext.queries.push(new NotQuery(current));
            }
            return;
        }

        if (op.equals(":")) {
            PerformAssign(builderContext, BinaryQuery.CONTAINS);
            return;
        }

        if (op.equals("TO")) {
            Query second = builderContext.queries.pop();
            Query first = builderContext.queries.pop();
            RangeQuery rq = new RangeQuery(null,
                    ((BinaryQuery) first).value, false, ((BinaryQuery) second).value, false);
            builderContext.queries.push(rq);
            return;

        }

        if (op.equals(">") || op.equals(">=") || op.equals("<") || op.equals("<=")) {
            PerformCompare(builderContext, op);
            return;
        }

        if (op.equals("NOT")) {
            Query query = builderContext.queries.pop();
            NotQuery notQuery = new NotQuery();
            notQuery.innerQuery = query;
            builderContext.queries.push(notQuery);
            return;
        }

        if (op.equals("EOF"))
            return;

        throw new IllegalArgumentException("Error, unsupported operation:" + op);
    }

    private static void PerformCompare(BuilderContext builderContext, String op) {
        Query first = builderContext.queries.pop();
        Query second = builderContext.queries.pop();
        PerformCompare1(builderContext, first, second, op);
    }

    private static void PerformCompare1(BuilderContext builderContext, Query q, Query first, String op) {

        if(first instanceof PathCountRangeQuery) {
        	PathCountRangeQuery pcrq = (PathCountRangeQuery)first;
            pcrq.range = CreateRangeQuery(op, (BinaryQuery) q, "");
            builderContext.queries.push(first);
            return;
        }
    	
    	
        String field = "";
        if (QueryUtils.HasInnerQuery(first)) {
            Query iq = QueryUtils.GetLastChild(first);
            field = QueryUtils.GetLinkQueryLink(iq);
            QueryFieldType fieldtype = QueryUtils.GetFieldType(QueryUtils.GetPath(first, builderContext.definition), builderContext.definition);

            Query pat = QueryUtils.GetParent(first, iq);

            if (QueryUtils.GetLinkQuantifier(iq).equals("COUNT")) {
                if (fieldtype == QueryFieldType.Link || fieldtype == QueryFieldType.Field ||
                        fieldtype == QueryFieldType.MultiValueScalar || fieldtype == QueryFieldType.Unknown) {

                    boolean isLink = fieldtype == QueryFieldType.Link;
                    if (iq instanceof LinkQuery) {
                        LinkQuery llq = (LinkQuery) iq;
                        if (llq.quantifier.equals("COUNT")) {
                            SetLinkQueryQuantifier(first, LinkQuery.ANY);
                            RangeQuery rq = CreateRangeQuery(op, (BinaryQuery) q, field);
                            if (isLink) {
                                LinkCountRangeQuery lcrq = new LinkCountRangeQuery(field, rq);
                                lcrq.filter = llq.filter;
                                QueryUtils.SetInnerQuery(pat, lcrq);
                            } else {
                                if (llq.filter != null)
                                    throw new IllegalArgumentException("Filters are not supported for non link fields: " + field);
                                QueryUtils.SetInnerQuery(pat, new FieldCountRangeQuery(field, rq));

                            }
                            builderContext.queries.push(first);
                            return;
                        }
                    }
                }
                throw new IllegalArgumentException("Error COUNT: " + field + " is not a link or field");
            }

            switch (fieldtype) {
                case Link:
                    throw new IllegalArgumentException("Operation '" + op + "' is not supported for links (" + field + ")");
                case MultiValueScalar:
                    throw new IllegalArgumentException("Operation '" + op + "' is not supported for scalar collections (" + field + ")");
                default:
                    break;
            }

            RangeQuery rq = CreateRangeQuery(op, (BinaryQuery) q, field);
            Query parent = QueryUtils.GetParent(first, iq);
            if (parent == null) {
                builderContext.queries.push(rq);
            } else {
                QueryUtils.SetInnerQuery(parent, rq);
                builderContext.queries.push(first);
            }

        } else {
            if (first instanceof LinkQuery && QueryUtils.GetLinkQuantifier(first).equals("COUNT")) {
                LinkQuery lqOrigin = (LinkQuery) first;
                String fname = lqOrigin.link;
                QueryFieldType fieldtype = QueryUtils.GetFieldType(QueryUtils.GetPath(first, builderContext.definition), builderContext.definition);

                RangeQuery rq = CreateRangeQuery(op, (BinaryQuery) q, fname);

                switch (fieldtype) {
                    case Link:
                        LinkCountRangeQuery lcrq = new LinkCountRangeQuery(fname, rq);
                        lcrq.filter = lqOrigin.filter;
                        builderContext.queries.push(lcrq);
                        return;
                    case Group:
                        throw new IllegalArgumentException("Error. COUNT is not supported for group fields: " + fname);
                    default:
                        if (lqOrigin.filter != null)
                            throw new IllegalArgumentException("Filters are not supported for non link fields: " + fname);

                        builderContext.queries.push(new FieldCountRangeQuery(fname, rq));
                        return;

                }
            }
            if (!(first instanceof BinaryQuery)) {
                if (first instanceof LinkQuery) {
                    field = ((LinkQuery) first).link;
                } else {
                    throw new IllegalArgumentException("Error: unsupported operation " + op);
                }
            } else {
                field = ((BinaryQuery) first).value;
            }
            if (builderContext.definition != null) {
                if (builderContext.definition.isLinkField(field))
                    throw new IllegalArgumentException("Error: unsupported operation " + op + " for link " + field);
            }
            builderContext.queries.push(CreateRangeQuery(op, (BinaryQuery) q, field));
        }
        return;
    }

    private static void PerformWhereOperation(BuilderContext builderContext, Query first, Query secondQuery) {

        Query second = secondQuery;
        boolean notQuery = second instanceof NotQuery;
        if (notQuery)
            second = ((NotQuery) second).innerQuery;

        if (second instanceof LinkQuery || second instanceof TransitiveLinkQuery)   {
            if (QueryUtils.HasInnerQuery(second)) {
                Query last = QueryUtils.GetLastChild(second);
                //TODO check innerQueryFields   (datepartBinary, mvsbinary , not)
                if (last instanceof LinkQuery || last instanceof TransitiveLinkQuery)
                    QueryUtils.SetInnerQuery(last, first);
                else {
                    builderContext.queries.push(second);
                    builderContext.queries.push(first);
                    return;
                }
                if (notQuery)
                    builderContext.queries.push(secondQuery);
                else
                    builderContext.queries.push(second);
                return;
            } else {
                QueryUtils.SetInnerQuery(second, first);
                builderContext.queries.push(second);
                return;
            }
        } else {
            if (second instanceof BinaryQuery) {
                String field = ((BinaryQuery) second).value;
                LinkQuery lq = new LinkQuery(LinkQuery.ANY, field, first);
                if (notQuery) {
                    ((NotQuery) secondQuery).innerQuery = lq;
                    builderContext.queries.push(secondQuery);
                } else {
                    builderContext.queries.push(lq);
                }
                return;
            }
        }
        throw new IllegalArgumentException("Internal error: unexpected usage of and");
    }

    private static void removeNone(Query q) {
        while (isNonePresent(q)) {
            if (q instanceof  LinkQuery) {
                ((LinkQuery)q).quantifier=LinkQuery.ANY;
                q= ((LinkQuery)q).innerQuery;
            }
            if (q instanceof  TransitiveLinkQuery) {
                ((TransitiveLinkQuery)q).quantifier=LinkQuery.ANY;
                q= ((TransitiveLinkQuery)q).innerQuery;
            }

            if (q instanceof  MVSBinaryQuery) {
                ((MVSBinaryQuery)q).quantifier=LinkQuery.ANY;
                q= ((MVSBinaryQuery)q).innerQuery;
            }

            if (q instanceof LinkIdQuery) {
                ((LinkIdQuery)q).quantifier=LinkQuery.ANY;
                return;
            }

            if (q == null)
                return;
        }
    }

        private static boolean isNonePresent(Query q) {
        if (q instanceof  LinkQuery) {
            return LinkQuery.NONE.equals(((LinkQuery)q).quantifier);
        }
        if (q instanceof  TransitiveLinkQuery) {
            return LinkQuery.NONE.equals(((TransitiveLinkQuery)q).quantifier);
        }

        if (q instanceof  MVSBinaryQuery) {
            return LinkQuery.NONE.equals(((MVSBinaryQuery)q).quantifier);
        }

        if (q instanceof LinkIdQuery) {
            return LinkQuery.NONE.equals(((LinkIdQuery)q).quantifier);
        }
        return false;
    }

    private static void PerformAssign(BuilderContext builderContext, String operationType) {
        Query first = builderContext.queries.pop();
        Query second = builderContext.queries.pop();
        if (operationType.equals(BinaryQuery.EQUALS) && first instanceof OrQuery && isNonePresent(second) ) {
            OrQuery orQuery = new OrQuery() ;
            OrQuery source = (OrQuery)first;
            removeNone(second);
            for (int i = 0; i < source.subqueries.size(); i++) {
                Query query = source.subqueries.get(i);
                LinkQueryAssign(builderContext, query, QueryUtils.CloneQuery(second), operationType);
                orQuery.subqueries.add(builderContext.queries.pop());
            }
            builderContext.queries.push(new NotQuery(orQuery));
        } else {
            if (operationType.equals(BinaryQuery.EQUALS) && isNonePresent(second) )
            {
                removeNone(second);
                LinkQueryAssign(builderContext, first, second, operationType);
                builderContext.queries.push(new NotQuery(builderContext.queries.pop()));
            } else {
                if (operationType.equals("NULL"))   {
                    LinkQueryAssign(builderContext, first, second, BinaryQuery.EQUALS);
                } else {
                    LinkQueryAssign(builderContext, first, second, operationType);
                }
            }
        }
    }

    private static RangeQuery CreateRangeQuery(String op, BinaryQuery bq, String field) {
        String min = null;
        String max = null;
        boolean minI = false;
        boolean maxI = false;

        if (op.equals(">")) {
            return new RangeQuery(field, bq.value, minI, max, maxI);
        }
        if (op.equals(">=")) {
            return new RangeQuery(field, bq.value, true, max, maxI);
        }
        if (op.equals("<")) {
            return new RangeQuery(field, min, minI, bq.value, maxI);
        }
        if (op.equals("<=")) {
            return new RangeQuery(field, min, minI, bq.value, true);
        }
        return new RangeQuery(field, min, minI, max, maxI);
    }

    private static boolean LinkQueryAssign(BuilderContext builderContext, Query first, Query second, String op) {

        Query lq = second;
        
        if(lq instanceof PathCountRangeQuery) {
        	PathCountRangeQuery pcrq = (PathCountRangeQuery)lq;
            if (first instanceof BinaryQuery) {
                int countValue = Integer.parseInt(((BinaryQuery)first).value);
                pcrq.range = new RangeQuery(null, "" + countValue, true, "" + countValue, true);
            }
            else if (first instanceof RangeQuery) {
                RangeQuery rq = (RangeQuery) first;
                pcrq.range = rq;
            }
            else throw new IllegalArgumentException("Invalid DCOUNT expression");
            builderContext.queries.push(second);
            return false;
        }
        
        ArrayList<String> path = QueryUtils.GetPath(lq, builderContext.definition);

        QueryFieldType fieldType = QueryUtils.GetFieldType(path, builderContext.definition);

        if (QueryUtils.HasInnerQuery(lq)) {
            Query last = QueryUtils.GetLastChild(lq);
            Query pat = QueryUtils.GetParent(lq, last);
            String lastname = QueryUtils.GetLinkQueryLink(last);

            if (path.size() > 1) {
                if (QueryUtils.TruncateUnits.containsKey(lastname)) {
                    QueryUtils.CheckPath(path, path.size() - 2, builderContext.definition, false);
                }
            }

            if (QueryUtils.GetLinkQuantifier(last).equals("COUNT")) {
                switch (fieldType)  {
                    case Link:
                    case Field:
                    case MultiValueScalar:

                        boolean isLink = fieldType == QueryFieldType.Link;
                        if (last instanceof LinkQuery) {
                            LinkQuery llq = (LinkQuery) last;
                            if (llq.quantifier.equals("COUNT")) {
                                if (isLink)
                                    SetLinkQueryQuantifier(second, LinkQuery.ANY);
                                if (first instanceof BinaryQuery) {
                                    int countValue = Integer.parseInt(((BinaryQuery)first).value);
                                    if (isLink) {
                                        QueryUtils.SetInnerQuery(pat, new LinkCountQuery(QueryUtils.GetLinkQueryLink(last), countValue));
                                    } else {
                                        QueryUtils.SetInnerQuery(pat, new FieldCountQuery(QueryUtils.GetLinkQueryLink(last), countValue));
                                    }
                                    String quantifier = QueryUtils.GetLinkQuantifier(second);
                                    if (quantifier != null && quantifier.equals("COUNT"))
                                        SetLinkQueryQuantifier(second, LinkQuery.ANY);

                                    builderContext.queries.push(second);
                                    return false;
                                }
                                if (first instanceof RangeQuery) {
                                    RangeQuery rq = (RangeQuery) first;
                                    rq.field = lastname;
                                    if (isLink) {
                                        QueryUtils.SetInnerQuery(pat, new LinkCountRangeQuery(lastname, rq));
                                    } else {
                                        QueryUtils.SetInnerQuery(pat, new FieldCountRangeQuery(lastname, rq));
                                    }
                                    String quantifier = QueryUtils.GetLinkQuantifier(second);
                                    if (quantifier != null && quantifier.equals("COUNT"))
                                        SetLinkQueryQuantifier(second, LinkQuery.ANY);

                                    builderContext.queries.push(second);
                                    return false;
                                }
                            }
                        }
                    default:
                        throw new IllegalArgumentException("Error COUNT: " + QueryUtils.GetLinkQueryLink(last) + " is not a link or field" );

                }

                /*
                if (fieldType == QueryFieldType.Link) {
                    if (last instanceof LinkQuery) {
                        LinkQuery llq = (LinkQuery) last;
                        if (llq.quantifier.equals("COUNT")) {
                            SetLinkQueryQuantifier(second, LinkQuery.ANY);
                            if (first instanceof BinaryQuery) {
                                int countValue = Integer.parseInt((String) ((BinaryQuery) first).value);
                                Query newFirst = new LinkCountQuery(QueryUtils.GetLinkQueryLink(last), countValue);
                                QueryUtils.SetInnerQuery(pat, newFirst);
                                builderContext.queries.push(second);
                                return false;
                            }
                            if (first instanceof RangeQuery) {
                                RangeQuery rq = (RangeQuery) first;
                                rq.field = lastname;
                                LinkCountRangeQuery lcrq = new LinkCountRangeQuery(lastname, rq);
                                QueryUtils.SetInnerQuery(pat, lcrq);
                                builderContext.queries.push(second);
                                return false;
                            }
                        }
                    }

                }
                */
            }
            QueryFieldType fType = QueryUtils.GetBasicFieldType(path, builderContext.definition);
            switch (fieldType) {

                case MultiValueScalar:
                    if (first instanceof RangeQuery) {
                        DoradusQueryBuilder.traverse(first, new FieldVisitor(QueryUtils.GetLinkQueryLink(last), op));
                        QueryUtils.SetInnerQuery(pat, first);
                    } else {
                        if (last instanceof TransitiveLinkQuery) {
                            throw new IllegalArgumentException(lastname + "  is not a link");
                        }
                        Query mq = DoradusQueryBuilder.traverseTree(new Stack<String>(), first,
                                new MultiValueVisitor(QueryUtils.GetLinkQuantifier(last), QueryUtils.GetLinkQueryLink(last), op));
                        QueryUtils.SetInnerQuery(pat, mq);
                    }
                    break;

                case Field:
                    if (last instanceof TransitiveLinkQuery) {
                        throw new IllegalArgumentException(lastname + "  is not a link");
                    }
                    DoradusQueryBuilder.traverse(first, new FieldVisitor(QueryUtils.GetLinkQueryLink(last), op));
                    QueryUtils.SetInnerQuery(pat, first);
                    break;

                case Group:
                case Link:
                    if (fType == QueryFieldType.Link) {
                        if (last instanceof TransitiveLinkQuery) {
                            Query newFirst = DoradusQueryBuilder.traverseTree(new Stack<String>(), first,
                                    new LinkIdReplaceVisitor(QueryUtils.GetLinkQuantifier(last), lastname));
                            QueryUtils.SetInnerQuery(last, newFirst);
                            break;
                        }
                        if (BinaryQuery.CONTAINS.equals(op))
                            throw new IllegalArgumentException("Operation ':' is nor supported for links");

                        Query newFirst = DoradusQueryBuilder.traverseTree(new Stack<String>(), first,
                                new LinkIdVisitor(QueryUtils.GetLinkQuantifier(last), QueryUtils.GetLinkQueryLink(last)));
                        QueryUtils.SetInnerQuery(pat, newFirst);
                        break;
                    }
                default:
                    if (last instanceof TransitiveLinkQuery) {
                        throw new IllegalArgumentException("Error :" + lastname + " is not a link");
                    }

                    if (QueryUtils.TruncateUnits.containsKey(lastname)) {
                        String truncateField = QueryUtils.GetLinkQueryLink(pat);
                        Query tq = DoradusQueryBuilder.traverseTree(new Stack<String>(), first,
                                new DatePartQueryVisitor(truncateField, op, QueryUtils.TruncateUnits.get(lastname).intValue()));
                        if (pat instanceof TransitiveLinkQuery)
                            throw new IllegalArgumentException(" Error: Cannot apply datetime function for transitive query (" + truncateField + ")");
                        if (tq instanceof RangeQuery)
                            throw new IllegalArgumentException(" Error: Date parts for range query are not supported (" + truncateField + ")");
                        Query insertPoint = QueryUtils.GetParent(lq, pat);
                        if (insertPoint != null)
                            QueryUtils.SetInnerQuery(insertPoint, tq);
                        else
                            second = tq;
                    } else {
                        DoradusQueryBuilder.traverse(first, new FieldVisitor(QueryUtils.GetLinkQueryLink(last), op));
                        QueryUtils.SetInnerQuery(pat, first);
                    }
                    break;
            }
        } else {
            String quantifier = "";
            String fname = "";
            boolean quantifierDefined = false;

            Query parent1 = QueryUtils.GetParent(second, lq);
            if (lq instanceof BinaryQuery) {

                BinaryQuery bq = (BinaryQuery) lq;
                ArrayList<String> t = new ArrayList<String>();
                t.add(bq.value);
                fieldType = QueryUtils.GetFieldType(t, builderContext.definition);
                quantifier = LinkQuery.ANY;
                fname = bq.value;
            } else {
                quantifier = QueryUtils.GetLinkQuantifier(lq);
                quantifierDefined = true;
                fname = QueryUtils.GetLinkQueryLink(lq);
                //fieldType = QueryUtils.GetFieldType(t, builderContext.definition);

            }

            if (quantifier.equals("COUNT")) {
                if (fieldType == QueryFieldType.Link || fieldType == QueryFieldType.Unknown ||  fieldType == QueryFieldType.Group ||
                        fieldType == QueryFieldType.Field || fieldType == QueryFieldType.MultiValueScalar ) {
                    boolean isLink = (fieldType == QueryFieldType.Link) || (fieldType == QueryFieldType.Group) ;
                    if (lq instanceof LinkQuery) {
                        LinkQuery llq = (LinkQuery) lq;
                        if (isLink)
                            SetLinkQueryQuantifier(second, LinkQuery.ANY);
                        if (first instanceof BinaryQuery) {
                            int countValue = Integer.parseInt((String) ((BinaryQuery) first).value);
                            if (isLink) {
                                LinkCountQuery newFirst = new LinkCountQuery(fname, countValue);
                                newFirst.filter = llq.filter;
                                second = newFirst;
                            } else {
                                if (llq.filter != null)
                                    throw new IllegalArgumentException("Filters are not supported for non link fields: " + fname);
                                second = new FieldCountQuery(fname, countValue);
                            }
                        } else if (first instanceof RangeQuery) {
                            RangeQuery rq = (RangeQuery) first;
                            rq.field = fname;
                            if (isLink) {
                                LinkCountRangeQuery lcrq = new LinkCountRangeQuery(fname, rq);
                                lcrq.filter = llq.filter;
                                second = lcrq;
                            } else {
                                if (llq.filter != null)
                                    throw new IllegalArgumentException("Filters are not supported for non link fields: " + fname);
                                second = new FieldCountRangeQuery(fname, rq);
                            }
                        } else {
                            throw new IllegalArgumentException("Error : unsupported type for COUNT value");
                        }
                    }
                }  else {
                    throw new IllegalArgumentException("Error COUNT: " + fname + " is  not a link or field");
                }
            } else
            {
                QueryFieldType originalType = fieldType;
                fieldType = QueryUtils.GetBasicFieldType(path, builderContext.definition);
                switch (originalType) {
                    case MultiValueScalar:
                        if (first instanceof RangeQuery) {
                            DoradusQueryBuilder.traverse(first, new FieldVisitor(fname, op));
                            if (parent1 != null) {
                                QueryUtils.SetInnerQuery(parent1, first);
                            } else {
                                second = first;
                            }
                        } else {
                            Query mq = DoradusQueryBuilder.traverseTree(new Stack<String>(), first,
                                    new MultiValueVisitor(quantifier, fname, op));
                            if (parent1 != null) {
                                QueryUtils.SetInnerQuery(parent1, mq);
                            } else {
                                if (second instanceof TransitiveLinkQuery)
                                    throw new IllegalArgumentException("Error:" + fname + " is not a link");
                                second = mq;
                            }
                        }
                        break;
                    case Group:
                    case Link:

                        if (fieldType == QueryFieldType.Link) {
                            if (lq instanceof TransitiveLinkQuery) {
                                Query newFirst = DoradusQueryBuilder.traverseTree(new Stack<String>(), first,
                                        new LinkIdReplaceVisitor(QueryUtils.GetLinkQuantifier(lq), fname));
                                QueryUtils.SetInnerQuery(lq, newFirst);
                                break;
                            }

                            if (BinaryQuery.CONTAINS.equals(op))
                                throw new IllegalArgumentException("Operation ':' is nor supported for links");

                            Query newF = DoradusQueryBuilder.traverseTree(new Stack<String>(), first, new LinkIdVisitor(quantifier, fname));
                            if (parent1 != null) {
                                QueryUtils.SetInnerQuery(parent1, newF);
                            } else {
                                second = newF;
                            }
                            break;
                        }

                    default:
                        if (second instanceof TransitiveLinkQuery)
                            throw new IllegalArgumentException("Error:" + fname + " is not a link");

                        if (quantifierDefined) {
                            Query mvs = DoradusQueryBuilder.traverseTree(new Stack<String>(), first,
                                    new MultiValueVisitor(quantifier, fname, op));
                            if (parent1 != null) {
                                QueryUtils.SetInnerQuery(parent1, mvs);
                            } else
                            {
                                if (second instanceof TransitiveLinkQuery)
                                    throw new IllegalArgumentException("Error:" + fname + " is not a link");
                                second = mvs;
                            }
                        }  else {
                            DoradusQueryBuilder.traverse(first, new FieldVisitor(fname, op));
                            if (parent1 != null) {
                                QueryUtils.SetInnerQuery(parent1, first);
                            } else {
                                second = first;
                            }
                        }
                        break;
                }
            }
        }
        builderContext.queries.push(second);
        return false;
    }

    private static void SetLinkQueryQuantifier(Query query, String value) {
        Query current = query;
        while (current != null && QueryUtils.HasInnerQuery(current)) {
            QueryUtils.SetLinkQuantifier(current, value);
            current = QueryUtils.GetInnerQuery(current);
        }
    }

    private static void assignFilter(Query query, Query filter) {
        if (query instanceof LinkCountQuery)
            ((LinkCountQuery) query).filter = filter;
        else if (query instanceof LinkQuery)
            ((LinkQuery) query).filter = filter;
        else if (query instanceof TransitiveLinkQuery)
            ((TransitiveLinkQuery) query).filter = filter;
        else if (query instanceof LinkCountRangeQuery)
            ((LinkCountRangeQuery) query).filter = filter;
        else
            throw new IllegalArgumentException("Internal error: attempt to assign filter for query");

    }

    private static Query getQuery(LinkItem item, String operation, BuilderContext context) {

        String op = operation;
        if (item.operation != null) {
            if (item.operation.equals("ANY"))
                op = LinkQuery.ANY;
            if (item.operation.equals("NONE")) {
                op = LinkQuery.NONE;
            }
            if (item.operation.equals("ALL"))
                op = LinkQuery.ALL;

            if (item.operation.equals("COUNT")) {
                op = item.operation;
            }
        }

        AndQuery andFilter = null;
        Query myFilter = null;

        if (item.filters != null) {
            for (int f = 0; f < item.filters.size(); f++) {
                ArrayList<LinkItem> filter = item.filters.get(f);
                TableDefinition newDef = QueryUtils.GetTableDefinition(getPath(item), context.definition);
                BuilderContext newContext = new BuilderContext(newDef);
                Query nextfilter = build(filter, newContext);
                if (myFilter == null)
                    myFilter = nextfilter;
                else {
                    if (andFilter == null) {
                        andFilter = new AndQuery();
                        andFilter.subqueries.add(myFilter);
                    }
                    andFilter.subqueries.add(nextfilter);
                }
            }
        }
        Query result = null;

        if (item.item != null) {
            if (item.transitive != null && item.transitive.equals("^")) {
                TransitiveLinkQuery tq = new TransitiveLinkQuery(op, 0, item.item.getValue(), null);
                if (item.operation == null)
                    tq.quantifier=LinkQuery.ANY;

                if (item.value != null) {
                    int tValue = Integer.parseInt(item.value.getValue());
                    tq.depth = tValue;
                }
                result = tq;
            } else {
                if (op.equals("COUNT")) {
                    result = new LinkQuery(op, item.item.getValue(), null);
                } else {
                    if (item.operation == null)
                        result = new LinkQuery(LinkQuery.ANY, item.item.getValue(), null);
                    else
                        result = new LinkQuery(op, item.item.getValue(), null);
                    }
                op = operation;
            }
        }
        if (result != null) {
            if (andFilter != null) {
                assignFilter(result, andFilter);
            } else {
                if (myFilter != null) {
                    assignFilter(result, myFilter);
                }
            }
        }

        for (int k = 0; k < item.items.size(); k++) {
            LinkItem lkitem = item.items.get(k);

            Query query = getQuery(lkitem, op, context);
            if (result == null)
                result = query;
            else {
                Query last = QueryUtils.GetLastChild(result);
                QueryUtils.SetInnerQuery(last, query);
            }
        }
        return result;
    }

    private static ArrayList<String> getPath(LinkItem item) {
        ArrayList<String> result = new ArrayList<String>();
        if (item.item != null)
            result.add(item.item.getValue());
        else {
            for (int k = 0; k < item.items.size(); k++) {
                ArrayList<String> nextPath = getPath(item.items.get(k));
                result.addAll(nextPath);
            }
        }
        return result;
    }

    private static GrammarItem GetGrammarItem(LinkItem item) {

        if (item.transitive != null)
            return null;

        if (item.item != null) {
            if (item.operation == null)
                return item.item;
        }
        return null;
    }

    public static Query build(ArrayList<LinkItem> items, BuilderContext builderContext) {


        for (int i = 0; i < items.size(); i++) {
            GrammarItem grammarItem = GetGrammarItem(items.get(i));
            if (grammarItem == null) {
                //this is link
                Query q = getQuery(items.get(i), LinkQuery.ANY, builderContext);
                builderContext.queries.push(q);
                continue;
            }

            if (grammarItem.getType().equals("lexem") || grammarItem.getType().equals("string")) {
                DoradusQueryBuilder.pushGrammarItem(builderContext, grammarItem);
                continue;
            }

            if (grammarItem.getType().equals("semantic")) {
                if (grammarItem.getValue().equals("(") || grammarItem.getValue().equals(")")) {
                    pushOperation(builderContext, grammarItem.getValue());
                    continue;
                }

                if (grammarItem.getValue().equals("SearchCriteriaStart")) {
                    pushOperation(builderContext, "Criteria");
                    continue;
                }

            }

            if (grammarItem.getValue().equals("EOF")) {
                pushOperation(builderContext, grammarItem.getValue());
                continue;
            }

            if (grammarItem.getType().equals("token")) {
                String value = grammarItem.getValue();
                if("EQUALS".equals(value) || "INTERSECTS".equals(value) || "CONTAINS".equals(value) || "DISJOINT".equals(value) || "DIFFERS".equals(value)) {
                    AggregationGroup group1 = getLinkPath(items.get(i + 1), builderContext);
                    AggregationGroup group2 = getLinkPath(items.get(i + 2), builderContext);
                    pushQuery(builderContext, new PathComparisonQuery(value, group1, group2));
                    i += 2;
                    continue;
                }
                if("DCOUNT".equals(value)) {
                    AggregationGroup path = getLinkPath(items.get(i + 1), builderContext);
                    pushQuery(builderContext, new PathCountRangeQuery(path, null));
                    i += 1;
                    continue;
                }
                
                pushOperation(builderContext, grammarItem.getValue());
            }
        }

        while (!builderContext.operationEmpty()) {
            String op = builderContext.operationPop();
            DoOperation(op, builderContext);
        }

        Query query = builderContext.queries.pop();

        if (!builderContext.queries.empty()) {
            throw new IllegalArgumentException("Internal error: queries stack is not empty:" + builderContext.queries.pop());
        }

        //Process group links
        query = DoradusQueryBuilder.traverseTree(new Stack<String>(), query, new LinkCheckVisitor(builderContext.definition));

        //Check for system fields
        query = DoradusQueryBuilder.traverseTree(new Stack<String>(), query, new FieldNameVisitor());
        return query;
    }

    
    private static AggregationGroup getLinkPath(LinkItem item, BuilderContext context) {
        AggregationGroup group = new AggregationGroup(context.definition);
        group.items = new ArrayList<>();
        TableDefinition currentTable = context.definition;

        // is one-link path special case
        if(item.item != null) {
            AggregationGroupItem it = createItem(item, currentTable);
            group.items.add(it);
            return group;
        }
        
        if(item.filters != null && item.filters.size() > 0) {
            AndQuery filter = new AndQuery();
            for(ArrayList<LinkItem> f: item.filters) {
                Query q = build(f, new BuilderContext(currentTable));
                filter.subqueries.add(q);
            }
            group.filter = filter;
        }
        
        for(int i = 0; i < item.items.size(); i++) {
            LinkItem child = item.items.get(i);
            AggregationGroupItem it = createItem(child, currentTable);
            if(i != item.items.size() - 1) {
                Utils.require(it.fieldDef.isLinkField() || it.fieldDef.isXLinkField(), "Not a link: " + it.name);
                if(it.fieldDef.isXLinkField() && it.isTransitive) throw new IllegalArgumentException("XLink cannot be transitive");
                currentTable = it.fieldDef.getInverseTableDef();
            }
            group.items.add(it);
        }
        return group;
    }
    
    private static AggregationGroupItem createItem(LinkItem child, TableDefinition currentTable) {
        String name = child.item.getValue();
        AggregationGroupItem it = new AggregationGroupItem();
        it.fieldDef = currentTable.getFieldDef(name);
        Utils.require(it.fieldDef != null, "Invalid field: " + name);
        it.name = name;
        it.isLink = it.fieldDef.isLinkField();
        if(it.isLink) {
            it.tableDef = it.fieldDef.getInverseTableDef();
        }
        if(child.filters != null && child.filters.size() > 0) {
            AndQuery filter = new AndQuery();
            for(ArrayList<LinkItem> f: child.filters) {
                Query q = build(f, new BuilderContext(it.tableDef));
                filter.subqueries.add(q);
            }
            it.query = filter;
        }
        
        if (child.transitive != null && child.transitive.equals("^")) {
        	it.isTransitive = true;
            if (child.value != null) {
                int tValue = Integer.parseInt(child.value.getValue());
                it.transitiveDepth = tValue;
            }
        }
        
        return it;
    }
    
    //////////////////////TODO  Create factory methods
    public static AndQuery CreateAndQuery(Query first, Query second) {
        AndQuery and = new AndQuery();
        and.subqueries.add(first);
        and.subqueries.add(second);
        return and;
    }

    public static OrQuery CreateOrQuery(Query first, Query second) {
        OrQuery and = new OrQuery();
        and.subqueries.add(first);
        and.subqueries.add(second);
        return and;
    }

}

