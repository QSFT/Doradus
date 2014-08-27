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

import com.dell.doradus.common.CommonDefs;
import com.dell.doradus.common.FieldDefinition;
import com.dell.doradus.common.FieldType;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.search.aggregate.AggregationGroupItem;
import com.dell.doradus.search.query.*;

import java.text.SimpleDateFormat;
import java.util.*;

public class QueryUtils {


    public static final SimpleDateFormat[] DATE_FORMATS = new SimpleDateFormat[]{
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"),
            new SimpleDateFormat("yyyy-MM-dd HH:mm"),
            new SimpleDateFormat("yyyy-MM-dd HH"),
            new SimpleDateFormat("yyyy-MM-dd"),
            new SimpleDateFormat("yyyy-MM"),
            new SimpleDateFormat("yyyy")
    };

    @SuppressWarnings("serial")
    public static final Map<String, Integer> TruncateUnits = new HashMap<String, Integer>() {
        {
            put("SECOND", new Integer(Calendar.SECOND));
            put("MINUTE", new Integer(Calendar.MINUTE));
            put("HOUR", new Integer(Calendar.HOUR_OF_DAY));
            put("DAY", new Integer(Calendar.DAY_OF_MONTH));
            put("MONTH", new Integer(Calendar.MONTH));
            put("YEAR", new Integer(Calendar.YEAR));
        }
    };

    @SuppressWarnings("serial")
    public static final Map<Integer, String> TruncateUnitsNames = new HashMap<Integer, String>() {
        {
            put(Calendar.SECOND, "SECOND");
            put(Calendar.MINUTE, "MINUTE");
            put(Calendar.HOUR_OF_DAY, "HOUR");
            put(Calendar.DAY_OF_MONTH, "DAY");
            put(Calendar.MONTH, "MONTH");
            put(Calendar.YEAR, "YEAR");
        }
    };


    protected static String GetLinkQuantifier(Query q) {
        if (q instanceof LinkQuery) {
            LinkQuery lq = (LinkQuery) q;
            return lq.quantifier;
        }
        if (q instanceof TransitiveLinkQuery) {
            TransitiveLinkQuery lq = (TransitiveLinkQuery) q;
            return lq.quantifier;
        }

        throw new IllegalArgumentException("Internal error: not a link type:" + q.getClass().getSimpleName());
    }

    protected static void SetLinkQuantifier(Query q, String val) {
        if (q instanceof LinkQuery || q instanceof TransitiveLinkQuery) {
            if (q instanceof LinkQuery) {
                LinkQuery lq = (LinkQuery) q;
                lq.quantifier = val;
            }
            if (q instanceof TransitiveLinkQuery) {
                TransitiveLinkQuery lq = (TransitiveLinkQuery) q;
                lq.quantifier = val;
            }
            return;
        }

        throw new IllegalArgumentException("Internal error: not a link type:" + q.getClass().getSimpleName());
    }

    protected static void SetLinkName(Query q, String val) {
        if (q instanceof LinkQuery || q instanceof TransitiveLinkQuery) {
            if (q instanceof LinkQuery) {
                LinkQuery lq = (LinkQuery) q;
                lq.link = val;
            }
            if (q instanceof TransitiveLinkQuery) {
                TransitiveLinkQuery lq = (TransitiveLinkQuery) q;
                lq.link = val;
            }
            return;
        }

        throw new IllegalArgumentException("Internal error: cannot set link name, query is not a link type:" + q.getClass().getSimpleName());
    }


    protected static Query GetLast(Query q) {
        while (q instanceof LinkQuery || q instanceof TransitiveLinkQuery) {
            if (q instanceof LinkQuery) {
                LinkQuery lq = (LinkQuery) q;
                if (lq.innerQuery != null) {
                    q = lq.innerQuery;
                } else
                    return q;
            }
            if (q instanceof TransitiveLinkQuery) {
                TransitiveLinkQuery lq = (TransitiveLinkQuery) q;
                if (lq.innerQuery != null)
                    q = lq.innerQuery;
                else
                    return lq;
            }
        }
        return null;
    }

    protected static Query GetLastChild(Query q) {
        while (q instanceof LinkQuery || q instanceof TransitiveLinkQuery || q instanceof NotQuery) {
            if (q instanceof LinkQuery) {
                LinkQuery lq = (LinkQuery) q;
                if (lq.innerQuery == null) {
                    return lq;
                } else
                    q = lq.innerQuery;
            }
            if (q instanceof TransitiveLinkQuery) {
                TransitiveLinkQuery lq = (TransitiveLinkQuery) q;
                if (lq.innerQuery == null)
                    return lq;
                else
                    q = lq.innerQuery;
            }
            if (q instanceof NotQuery) {
                NotQuery lq = (NotQuery) q;
                q = lq.innerQuery;
            }

        }
        return q;
    }

    protected static Query GetInnerQuery(Query q, int index, TableDefinition tableDefinition) {

        ArrayList<String> path = new ArrayList<String>();
        int current = 0;
        if (q instanceof LinkQuery || q instanceof TransitiveLinkQuery || q instanceof NotQuery)
            while (q instanceof LinkQuery || q instanceof TransitiveLinkQuery || q instanceof NotQuery) {
                if (q instanceof LinkQuery) {
                    LinkQuery lq = (LinkQuery) q;
                    if (current == index)
                        return lq;
                    current++;

                    if (lq.innerQuery == null) {
                        throw new IllegalArgumentException("Internal Error: GetInnerQuery bad index " + index);
                    } else
                        q = lq.innerQuery;
                }
                if (q instanceof TransitiveLinkQuery) {
                    TransitiveLinkQuery lq = (TransitiveLinkQuery) q;
                    if (current == index)
                        return lq;
                    current++;

                    if (!IsLink(path, tableDefinition))
                        throw new IllegalArgumentException("Internal Error: GetInnerQuery: " + lq.link + " is not a link");
                    if (lq.innerQuery == null)
                        throw new IllegalArgumentException("Internal Error: GetInnerQuery bad index " + index);
                    else
                        q = lq.innerQuery;
                }
                if (q instanceof NotQuery) {
                    NotQuery lq = (NotQuery) q;
                    q = lq.innerQuery;
                }
            }

        throw new IllegalArgumentException("Internal Error: GetInnerQuery index out of range" + index);
    }

    public static TableDefinition GetTableContext(Query q, TableDefinition definition) {
        TableDefinition tableDef = definition;
        ArrayList<String> path = QueryUtils.GetPath(q, tableDef);

        for (int i = 0; i < path.size(); i++) {

            FieldDefinition fd = tableDef.getFieldDef(path.get(i));
            if (fd == null) {
                if (i != (path.size() - 1)) {
                    throw new IllegalArgumentException(" Undefined Link " + path.get(i));
                }
            }
            if (tableDef.isLinkField(path.get(i)) || (fd != null && fd.isXLinkField())) {
                tableDef = tableDef.getLinkExtentTableDef(fd);
                if (tableDef == null) {
                    throw new IllegalArgumentException(" Cannot get table definition for link " + path.get(i));
                }
            } else {

                if (fd != null && fd.getType() == FieldType.GROUP) {
                    ArrayList<String> nestedLinks = QueryUtils.GetNestedFields(fd);
                    if (nestedLinks.size() == 0) {
                        throw new IllegalArgumentException("Group field error: " + fd.getName() + " (" + QueryUtils.LinkName(path) + ") Does not contain any links ");
                    } else {
                        fd =  tableDef.getFieldDef(nestedLinks.get(0));
                        if (fd != null)
                            tableDef = tableDef.getLinkExtentTableDef(fd);
                    }
                }
            }
        }
        return tableDef;
    }

    public static ArrayList<FieldDefinition> GetNestedFieldDefinitions(FieldDefinition groupFieldDef) {
        ArrayList<FieldDefinition> result = new ArrayList<>();
        if (groupFieldDef.isGroupField()) {
                for (FieldDefinition nestedFieldDef : groupFieldDef.getNestedFields()) {
                    if (nestedFieldDef.isGroupField()) {
                        ArrayList<FieldDefinition> nested = GetNestedFieldDefinitions(nestedFieldDef);
                        if (nested != null)
                            result.addAll(nested);
                    } else {
                        result.add(nestedFieldDef);
                    }
                }
        }
        if (result.size() > 0)
            return result;
        return null;
    }

    protected static ArrayList<String> GetPath(Query q, TableDefinition tableDefinition) {

        ArrayList<String> path = new ArrayList<String>();
        if (q instanceof LinkQuery || q instanceof TransitiveLinkQuery || q instanceof NotQuery) {
            while (q instanceof LinkQuery || q instanceof TransitiveLinkQuery || q instanceof NotQuery) {
                if (q instanceof LinkQuery) {
                    LinkQuery lq = (LinkQuery) q;
                    path.add(lq.link);
                    if (lq.innerQuery == null) {
                        return path;
                    } else
                        q = lq.innerQuery;
                }
                if (q instanceof TransitiveLinkQuery) {
                    TransitiveLinkQuery lq = (TransitiveLinkQuery) q;
                    path.add(lq.link);
                    if (lq.innerQuery == null)
                        return path;
                    else
                        q = lq.innerQuery;
                }
                if (q instanceof NotQuery) {
                    NotQuery lq = (NotQuery) q;
                    q = lq.innerQuery;
                }
            }
        } else {
            if (q instanceof BinaryQuery) {
                BinaryQuery bq = (BinaryQuery) q;
                if (bq.field != null)
                    path.add(bq.field);
                else
                    path.add((String) bq.value);
            }
        }
        return path;
    }


    protected static Query MergeAND(Query first, Query second) {
        OrQuery or = new OrQuery();

        if (first instanceof OrQuery) {
            OrQuery f1=(OrQuery)first;
            OrQuery s1=(OrQuery)second;
            for (int i = 0; i < f1.subqueries.size(); i++) {
                Query query = f1.subqueries.get(i);
                Query query2 =  s1.subqueries.get(i);
                AndQuery and1 = new AndQuery();
                and1.subqueries.add(query);
                and1.subqueries.add(query2);
                or.subqueries.add(and1);
            }
            return or;
        } else {
            AndQuery and = new AndQuery();
            and.subqueries.add(first);
            and.subqueries.add(second);
            return and;
        }
    }

    protected static Query GetParent(Query q, Query child) {
        while (q instanceof LinkQuery || q instanceof TransitiveLinkQuery || q instanceof NotQuery) {
            if (q instanceof NotQuery) {
                NotQuery notQ = (NotQuery) q;
                if (notQ.innerQuery != null) {
                    if (notQ.innerQuery == child)
                        return notQ;
                }
                q = ((NotQuery) q).innerQuery;
                continue;
            }

            if (q instanceof LinkQuery) {
                LinkQuery lq = (LinkQuery) q;
                if (lq.innerQuery != null) {
                    if (lq.innerQuery == child)
                        return lq;
                    q = lq.innerQuery;
                } else
                    return null;
            }
            if (q instanceof TransitiveLinkQuery) {
                TransitiveLinkQuery lq = (TransitiveLinkQuery) q;
                if (lq.innerQuery != null) {
                    if (lq.innerQuery == child)
                        return lq;
                    q = lq.innerQuery;
                } else
                    return null;
            }
        }
        return null;
    }

    protected static boolean IsLink(ArrayList<String> path, TableDefinition tableDefinition) {

        TableDefinition tableDef = tableDefinition;
        for (int i = 0; i < path.size(); i++) {
            if (tableDef == null)
                return false;

            FieldDefinition fd = tableDef.getFieldDef(path.get(i));
            if (fd == null)
                return false;

            //TODO check nested GROUP fields
            if (fd.isGroupField()) {
                ArrayList<FieldDefinition> result = GetNestedFieldDefinitions(fd);
                if (result != null)
                    fd = result.get(0);
            }

            if (fd.isLinkField() || fd.isXLinkField()) {
                tableDef = tableDef.getLinkExtentTableDef(fd);
            } else {
                return false;
            }
        }
        return tableDef != null;
    }

    protected static void CheckPath(ArrayList<String> path, int depth, TableDefinition tableDefinition, boolean lastLink) {
        TableDefinition tableDef = tableDefinition;
        if (tableDef == null)
            return;

        for (int i = 0; i < depth - 1; i++) {
            String name = path.get(i);
            FieldDefinition fd = tableDef.getFieldDef(name);
            if (fd == null)
                throw new IllegalArgumentException("Error: " + name + " is not a link");
            if (tableDef.isLinkField(name) || ( fd.isXLinkField())) {

                tableDef = tableDef.getLinkExtentTableDef(fd);
                if (tableDef == null)
                    throw new IllegalArgumentException("Error: table definition is not found for link " + name);

            } else {
                throw new IllegalArgumentException("Error: " + name + " is not a link");
            }
        }
        //
        String lname = path.get(depth);
        FieldDefinition fd =tableDef.getFieldDef(lname);
        boolean lasttIsLink = tableDef.isLinkField(lname) || (fd != null && fd.isXLinkField());
        if (lastLink) {
            if (!lasttIsLink)
                throw new IllegalArgumentException("Error: " + lname + " is not a link");
        } else {
            if (lasttIsLink)
                throw new IllegalArgumentException("Error: " + lname + " is a link");
        }
    }

    public static ArrayList<String> GetNestedFields(FieldDefinition groupFieldDef) {
        ArrayList<String> result = new ArrayList<String>();

        for (FieldDefinition nestedFieldDef : groupFieldDef.getNestedFields()) {
            if (nestedFieldDef.isGroupField()) {
                result.addAll(GetNestedFields(nestedFieldDef));
            } else {
                result.add(nestedFieldDef.getName());
            }
        }
        return result;
    }

    protected static FieldDefinition GetField(ArrayList<String> path, TableDefinition tableDefinition) {
        return GetField(path, path.size() - 1, tableDefinition);
    }

    protected static FieldDefinition GetField(ArrayList<String> path, int index, TableDefinition tableDefinition) {
        if (tableDefinition == null)
            return null;

        FieldDefinition fd = null;
        TableDefinition tableDef = tableDefinition;
        int current = 0;
        while (true) {
            if (tableDef == null)
                return null;

            fd = tableDef.getFieldDef(path.get(current));
            if (tableDef.isLinkField(path.get(current)) || (fd != null && fd.isXLinkField())) {
                if (fd == null)
                    return null;
                tableDef = tableDef.getLinkExtentTableDef(fd);
            }

            if (current == index)
                return fd;

            if (fd != null && fd.isGroupField()) {
                ArrayList<String> nested = GetNestedFields(fd);
                fd = tableDef.getFieldDef(nested.get(0));
                if (tableDef.isLinkField(nested.get(0)) || (fd != null && fd.isXLinkField()))
                    tableDef = tableDef.getLinkExtentTableDef(fd);
            }

            current++;
        }
    }

    protected static TableDefinition GetTableDefinition(ArrayList<String> path, TableDefinition tableDefinition) {
        return GetTableDefinition(path, path.size(), tableDefinition);
    }

    protected static TableDefinition GetTableDefinition(ArrayList<String> path, int index, TableDefinition tableDefinition) {
        if (tableDefinition == null)
            return null;

        FieldDefinition fd = null;
        TableDefinition tableDef = tableDefinition;
        for (int i = 0; i < index; i++) {
            if (tableDef == null)
                return null;

            fd = tableDef.getFieldDef(path.get(i));
            if (tableDef.isLinkField(path.get(i)) || (fd != null && fd.isXLinkField())) {
                if (fd == null)
                    return null;
                tableDef = tableDef.getLinkExtentTableDef(fd);
            } else {
                if (fd !=null) {
                    if (fd.isGroupField()) {
                        ArrayList<String> nested = GetNestedFields(fd);
                        fd = tableDef.getFieldDef(nested.get(0));
                        if (tableDef.isLinkField(nested.get(0)) || (fd != null && fd.isXLinkField()))
                            tableDef = tableDef.getLinkExtentTableDef(fd);
                        else
                            return tableDef;
                    }
                }
            }

        }
        return tableDef;
    }

    protected static QueryFieldType GetFieldType(ArrayList<String> path, TableDefinition tableDefinition) {
        return GetFieldType(path, path.size() - 1, tableDefinition);
    }

    protected static QueryFieldType GetFieldType(ArrayList<String> path, int index, TableDefinition tableDefinition) {
        FieldDefinition fd = GetField(path, index, tableDefinition);
        return GetQueryFieldType(fd);
    }


    protected static QueryFieldType GetBasicFieldType(ArrayList<String> path, TableDefinition tableDefinition) {
        return GetBasicFieldType(path, path.size() - 1, tableDefinition);
    }

    protected static QueryFieldType GetBasicFieldType(ArrayList<String> path, int index, TableDefinition tableDefinition) {
        FieldDefinition fd = GetField(path, index, tableDefinition);
        if (fd != null)
            return GetBasicQueryFieldType(fd);
        else
            return  QueryFieldType.Unknown;
    }

    public static QueryFieldType GetBasicQueryFieldType(FieldDefinition groupFieldDef) {
        ArrayList<FieldDefinition> result = GetNestedFieldDefinitions(groupFieldDef);
        if (result != null)
            return GetQueryFieldType(result.get(0));
        else
            return GetQueryFieldType(groupFieldDef);
    }

    private static QueryFieldType GetQueryFieldType(FieldDefinition fd) {
        if (fd == null)
            return QueryFieldType.Unknown;

        if (fd.isGroupField())
            return QueryFieldType.Group;

        if (fd.isLinkField())
            return QueryFieldType.Link;

        if (fd.isXLinkField())
            return QueryFieldType.Link;

        if (fd.isCollection())
            return QueryFieldType.MultiValueScalar;

        if (fd.isScalarField())
            return QueryFieldType.Field;

        return QueryFieldType.Unknown;
    }

    protected static String GetLinkQueryLink(Query q) {

        if (q instanceof LinkQuery) {
            LinkQuery lq = (LinkQuery) q;
            return lq.link;
        }
        if (q instanceof TransitiveLinkQuery) {
            TransitiveLinkQuery lq = (TransitiveLinkQuery) q;
            return lq.link;
        }
        return null;
    }

    protected static boolean HasInnerQuery(Query q) {
        if (q instanceof LinkQuery || q instanceof TransitiveLinkQuery || q instanceof NotQuery) {
            if (q instanceof LinkQuery) {
                LinkQuery lq = (LinkQuery) q;
                return lq.innerQuery != null;
            }
            if (q instanceof TransitiveLinkQuery) {
                TransitiveLinkQuery lq = (TransitiveLinkQuery) q;
                return lq.innerQuery != null;
            }
            if (q instanceof NotQuery) {
                return true;
            }
        }
        return false;
    }

    protected static Query GetInnerQuery(Query q) {
        if (q instanceof LinkQuery) {
            LinkQuery lq = (LinkQuery) q;
            return lq.innerQuery;
        }
        if (q instanceof TransitiveLinkQuery) {
            TransitiveLinkQuery lq = (TransitiveLinkQuery) q;
            return lq.innerQuery;
        }
        return null;
    }

    static void SetInnerQuery(Query q, Query value) {
        if (q instanceof LinkQuery) {
            LinkQuery lq = (LinkQuery) q;
            lq.innerQuery = value;
            return;
        }
        if (q instanceof TransitiveLinkQuery) {
            TransitiveLinkQuery lq = (TransitiveLinkQuery) q;
            lq.innerQuery = value;
            return;
        }
        if (q instanceof NotQuery) {
            NotQuery lq = (NotQuery) q;
            lq.innerQuery = value;
            return;

        }
        throw new IllegalArgumentException(" Internal error 2");
    }

    public static Query CloneQuery(Query q) {
        if (q == null)
            return null;

        CloneAction z = CloneAction.valueOf(q.getClass().getSimpleName());
        return z.Clone(q);
    }

    static String FullLinkName(List<AggregationGroupItem> items) {
        return FullLinkName(items, items.size());
    }

    static String FullLinkName(List<AggregationGroupItem> items, int len) {
        StringBuilder builder = new StringBuilder();
        String delimiter = "";
        for (int i = 0; i < len; i++) {
            builder.append(delimiter);
            builder.append(items.get(i).name);
            delimiter = ".";
        }
        return builder.toString();
    }

    static String LinkName(List<String> items) {
        StringBuilder builder = new StringBuilder();
        String delimiter = "";
        for (int i = 0; i < items.size(); i++) {
            builder.append(delimiter);
            builder.append(items.get(i));
            delimiter = ".";
        }
        return builder.toString();
    }

    public static ArrayList<String> GetLinkPath(Stack<String> links) {
        ArrayList<String> path = new ArrayList<String>();
        for (int i = 0; i < links.size(); i++) {
            path.add(links.get(i));
        }
        return path;
    }

    public enum CloneAction {

        AllQuery {
            Query Clone(Query q) {
                if (q == null)
                    return null;
                return new AllQuery();
            }
        },

        AndQuery {
            Query Clone(Query q) {
                if (q == null)
                    return null;
                AndQuery andQuery = (AndQuery) q;

                AndQuery aq = new AndQuery();
                for (int i = 0; i < andQuery.subqueries.size(); i++) {
                    aq.subqueries.add(CloneQuery(andQuery.subqueries.get(i)));
                }

                return aq;
            }
        },

        BinaryQuery {
            Query Clone(Query q) {
                if (q == null)
                    return null;
                BinaryQuery binaryQuery = (BinaryQuery) q;
                return new BinaryQuery(binaryQuery.operation, binaryQuery.field, binaryQuery.value);
            }
        },

        DatePartBinaryQuery {
            Query Clone(Query q) {
                if (q == null)
                    return null;

                DatePartBinaryQuery linkQuery = (DatePartBinaryQuery) q;
                return new DatePartBinaryQuery(linkQuery.part, (BinaryQuery) CloneQuery(linkQuery.innerQuery));
            }
        },

        IdQuery {
            Query Clone(Query q) {
                if (q == null)
                    return null;
                return new IdQuery(((IdQuery) q).id);
            }
        },

        LinkCountRangeQuery {
            Query Clone(Query q) {
                if (q == null)
                    return null;

                LinkCountRangeQuery linkQuery = (LinkCountRangeQuery) q;
                LinkCountRangeQuery lc = new LinkCountRangeQuery(linkQuery.link, (RangeQuery) CloneQuery(linkQuery.range));
                if (linkQuery.filter != null);
                    lc.filter = CloneQuery(linkQuery.filter);
                return lc;

            }
        },

        LinkCountQuery {
            Query Clone(Query q) {
                if (q == null)
                    return null;

                LinkCountQuery linkQuery = (LinkCountQuery) q;
                LinkCountQuery lc = new LinkCountQuery(linkQuery.link, linkQuery.count);

                if (linkQuery.filter != null);
                    lc.filter = CloneQuery(linkQuery.filter);
                return lc;


            }
        },

        FieldCountRangeQuery {
            Query Clone(Query q) {
                if (q == null)
                    return null;

                FieldCountRangeQuery fcrq = (FieldCountRangeQuery) q;
                FieldCountRangeQuery lc = new FieldCountRangeQuery(fcrq.field, (RangeQuery) CloneQuery(fcrq.range));
                return lc;

            }
        },

        FieldCountQuery {
            Query Clone(Query q) {
                if (q == null)
                    return null;

                FieldCountQuery fcq = (FieldCountQuery) q;
                FieldCountQuery lc = new FieldCountQuery(fcq.field, fcq.count);
                return lc;

            }
        },



        LinkIdQuery {
            Query Clone(Query q) {
                if (q == null)
                    return null;

                LinkIdQuery linkQuery = (LinkIdQuery) q;
                return new LinkIdQuery(linkQuery.quantifier, linkQuery.link, linkQuery.id);

            }
        },

        LinkQuery {
            Query Clone(Query q) {

                if (q == null)
                    return null;

                LinkQuery linkQuery = (LinkQuery) q;
                LinkQuery lq = new LinkQuery(linkQuery.quantifier, linkQuery.link, null);

                lq.innerQuery = CloneQuery(linkQuery.innerQuery);

                if (linkQuery.filter != null);
                    lq.filter = CloneQuery(linkQuery.filter);
                return lq;
            }
        },

        MVSBinaryQuery {
            Query Clone(Query q) {
                if (q == null)
                    return null;
                MVSBinaryQuery mvsQuery = (MVSBinaryQuery) q;
                return new MVSBinaryQuery(mvsQuery.quantifier, (BinaryQuery) CloneQuery(mvsQuery.innerQuery));
            }
        },

        NoneQuery {
            Query Clone(Query q) {
                if (q == null)
                    return null;
                return new NoneQuery();
            }
        },

        NotQuery {
            Query Clone(Query q) {
                if (q == null)
                    return null;
                NotQuery notQuery = (NotQuery) q;
                NotQuery nq = new NotQuery();
                nq.innerQuery = CloneQuery(notQuery.innerQuery);
                return nq;
            }
        },

        OrQuery {
            Query Clone(Query q) {
                if (q == null)
                    return null;
                OrQuery orQuery = (OrQuery) q;

                OrQuery oq = new OrQuery();
                for (int i = 0; i < orQuery.subqueries.size(); i++) {
                    oq.subqueries.add(CloneQuery(orQuery.subqueries.get(i)));
                }

                return oq;
            }
        },

        RangeQuery {
            Query Clone(Query q) {
                if (q == null)
                    return null;
                RangeQuery orQuery = (RangeQuery) q;

                return new RangeQuery(orQuery.field, orQuery.min, orQuery.minInclusive, orQuery.max, orQuery.maxInclusive);
            }
        },

        TransitiveLinkQuery {
            Query Clone(Query q) {
                if (q == null)
                    return null;
                TransitiveLinkQuery orQuery = (TransitiveLinkQuery) q;

                TransitiveLinkQuery lc = new TransitiveLinkQuery(orQuery.quantifier, orQuery.depth, orQuery.link, CloneQuery(orQuery.innerQuery));

                if (orQuery.filter != null);
                    lc.filter = CloneQuery(orQuery.filter);
                return lc;


            }
        };

        abstract Query Clone(Query q);
    }

    protected static String GetFieldName(Query q) {

        if (q instanceof BinaryQuery)
            return ((BinaryQuery) q).field;

        if (q instanceof IdQuery)
            return CommonDefs.SystemFields._ID.toString();

        if (q instanceof LinkCountRangeQuery)
            return ((LinkCountRangeQuery) q).link;

        if (q instanceof LinkCountQuery)
            return ((LinkCountQuery) q).link;

        if (q instanceof LinkIdQuery)
            return ((LinkIdQuery) q).link;

        if (q instanceof LinkQuery)
            return ((LinkQuery) q).link;

        if (q instanceof MVSBinaryQuery)
            return ((MVSBinaryQuery) q).quantifier;

        if (q instanceof RangeQuery)
            return ((RangeQuery) q).field;

        if (q instanceof TransitiveLinkQuery)
            return ((TransitiveLinkQuery) q).link;
        return null;
    }

    public static boolean isSystemField(String name) {
        if (name != null) {
            return CommonDefs.SystemFields._ID.toString().equals(name);
        }
        return false;

    }

}
