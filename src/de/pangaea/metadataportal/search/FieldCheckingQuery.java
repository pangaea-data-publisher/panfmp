/*
 *   Copyright 2007 panFMP Developers Team c/o Uwe Schindler
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package de.pangaea.metadataportal.search;

import org.apache.lucene.search.Query;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.IndexReader;
import java.io.IOException;
import java.util.*;

public final class FieldCheckingQuery extends Query {

    public FieldCheckingQuery(String field, Query query) {
        this.field=field;
        this.query=query;
    }

    public Query getQuery() {
        return query;
    }

    @Override
    public String toString(String field) {
        return query.toString(field);
    }

    @Override
    public final boolean equals(Object o) {
        if (o instanceof FieldCheckingQuery) {
            FieldCheckingQuery q=(FieldCheckingQuery)o;
            return this.query.equals(q.query) && this.field.equals(q.field);
        } else return false;
    }

    @Override
    public final int hashCode() {
        return query.hashCode()^0x743fb1ae+field.hashCode()^0xd2dd34aa;
    }

    @Override
    public Query rewrite(IndexReader reader) throws java.io.IOException {
        Query q = query.rewrite(reader);
        q.extractTerms(new TermCheckerSet(field));
        return q;
    }

    // members
    private String field;
    private Query query;

    // this class is a helper for checking a query for invalid (wrong field) Terms. It implements a set but only tests
    // in the add() method for invalid Terms. Nothing more is done, the Set is always empty!
    private final class TermCheckerSet extends AbstractSet<Term> {

        protected TermCheckerSet(String field) {
            this.field=field.intern();
        }

        @Override
        public int size() {
            return 0;
        }

        @Override
        public Iterator<Term> iterator() {
            return Collections.<Term>emptySet().iterator();
        }

        @Override
        public boolean add(Term t) {
            if (t.field()!=field) throw new IllegalArgumentException("A query on a specific field may not reference other fields by prefixing with 'fieldname:'!");
            return false;
        }

        private String field;
    }
}