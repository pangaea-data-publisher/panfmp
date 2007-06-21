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

import org.apache.lucene.search.*;
import org.apache.lucene.index.*;
import java.util.*;

public class Suggest {
    private static org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory.getLog(Suggest.class);

    private static Term findLastTerm(Query q) {
        Term last=null;
        if (q instanceof BooleanQuery) {
            BooleanClause[] clauses=((BooleanQuery)q).getClauses();
            if (clauses.length>0) last=findLastTerm(clauses[clauses.length-1].getQuery());
        } else if (q instanceof TermQuery) {
            last=((TermQuery)q).getTerm();
        } else if (q instanceof PhraseQuery) {
            Term[] terms=((PhraseQuery)q).getTerms();
            if (terms.length>0) last=terms[terms.length-1];
            else last=null;
        } else last=null; // disable autocomplete if last term is not supported type
        return last;
    }

    public static String[] suggest(IndexReader reader, Query q, String qStr, int count) throws java.io.IOException {
        Term base=findLastTerm(q);
        if (base!=null) {
            // get strings before and after this term
            int pos=qStr.toLowerCase().lastIndexOf(base.text().toLowerCase()); // case insensitive
            if (pos<0) {
                log.error("Term '"+base+"' not found in query '"+qStr+"' (should never happen)!");
                return null; // should never happen
            }
            String before=qStr.substring(0,pos),after=qStr.substring(pos+base.text().length());

            // scan
            TermEnum terms=reader.terms(base);
            List<Term> termList=new ArrayList<Term>();
            try {
                int c=0;
                do {
                    Term t=terms.term();
                    if (t!=null && base.field()==t.field()) {
                        if (t.text().startsWith(base.text())) {
                            termList.add(t);
                            c++;
                        } else break;
                    } else break;
                } while (c<count && terms.next());
                String[] suggests=new String[c];
                for (int i=0; i<c; i++)
                    suggests[i]=before+termList.get(i).text()+after;
                return suggests;
            } finally {
                terms.close();
            }
        } else return null;
    }

}