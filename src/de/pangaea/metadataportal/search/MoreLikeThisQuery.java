/*
 *   Copyright 2004-2008
 *       The Apache Software Foundation
 *   and
 *       panFMP Developers Team c/o Uwe Schindler
 *
 *   INFO: Large parts of code were borrowed from MoreLikeThis from
 *   Lucene Contrib, but simplified and optimized for panFMP.
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

import de.pangaea.metadataportal.utils.IndexConstants;

import org.apache.lucene.util.PriorityQueue;
import org.apache.lucene.util.ToStringUtils;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;

import java.io.IOException;

/**
 * This implementation of a "more like this" is based on an algorithm from Lucene Contrib and
 * optimized for use with <b>panFMP</b>. The constructor needs a <b>panFMP</b> document ID
 * and optional a field name (which must have term vectors enabled).
 * If you use the default field for similarity calculation, the default field must have term vectors.
 *
 * <p>Lucene does let you access the document frequency of terms, with IndexReader.docFreq().
 * Term frequencies can be computed by re-tokenizing the text, which, for a single document,
 * is usually fast enough.  But looking up the docFreq() of every term in the document is
 * probably too slow.
 * 
 * <p>You can use some heuristics to prune the set of terms, to avoid calling docFreq() too much,
 * or at all.  Since you're trying to maximize a tf*idf score, you're probably most interested
 * in terms with a high tf. Choosing a tf threshold even as low as two or three will radically
 * reduce the number of terms under consideration.  Another heuristic is that terms with a
 * high idf (i.e., a low df) tend to be longer.  So you could threshold the terms by the
 * number of characters, not selecting anything less than, e.g., six or seven characters.
 * With these sorts of heuristics you can usually find small set of, e.g., ten or fewer terms
 * that do a pretty good job of characterizing a document.
 * 
 * <p>It all depends on what you're trying to do.  If you're trying to eek out that last percent
 * of precision and recall regardless of computational difficulty so that you can win a TREC
 * competition, then the techniques I mention above are useless.  But if you're trying to
 * provide a "more like this" button on a search results page that does a decent job and has
 * good performance, such techniques might be useful.
 *
 * <p>Depending on the size of your index and the size and makeup of your documents you
 * may want to call the other set methods to control how the similarity queries are
 * generated:
 * <ul>
 * <li> {@link #setMinTermFreq setMinTermFreq(...)}
 * <li> {@link #setMinDocFreq setMinDocFreq(...)}
 * <li> {@link #setMinWordLen setMinWordLen(...)}
 * <li> {@link #setMaxWordLen setMaxWordLen(...)}
 * <li> {@link #setMaxQueryTerms setMaxQueryTerms(...)}
 * <li> {@link #setFractionTermsToMatch setFractionTermsToMatch(...)}
 * </ul> 
 *
 * <p>At {@link #rewrite} time the reader is used to construct the
 * actual {@link BooleanQuery} containing the relevant terms.
 *
 * @author Uwe Schindler
 * @author David Spencer
 * @author Bruce Ritchie
 * @author Mark Harwood
 */
public final class MoreLikeThisQuery extends Query {

	private static org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory.getLog(MoreLikeThisQuery.class);

	/**
	 * Ignore terms with less than this frequency in the source doc.
	 * @see #getMinTermFreq
	 * @see #setMinTermFreq	 
	 */
	public static final int DEFAULT_MIN_TERM_FREQ = 0;

	/**
	 * Ignore words which do not occur in at least this many docs.
	 * @see #getMinDocFreq
	 * @see #setMinDocFreq	 
	 */
	public static final int DEFAULT_MIN_DOC_FREQ = 5;

	/**
	 * Ignore words less than this length or if 0 then this has no effect.
	 * @see #getMinWordLen
	 * @see #setMinWordLen	 
	 */
	public static final int DEFAULT_MIN_WORD_LENGTH = 0;

	/**
	 * Ignore words greater than this length or if 0 then this has no effect.
	 * @see #getMaxWordLen
	 * @see #setMaxWordLen	 
	 */
	public static final int DEFAULT_MAX_WORD_LENGTH = 0;

	/**
	 * Return a Query with no more than this many terms.
	 *
	 * @see BooleanQuery#getMaxClauseCount
	 * @see #getMaxQueryTerms
	 * @see #setMaxQueryTerms	 
	 */
	public static final int DEFAULT_MAX_QUERY_TERMS = 384;
	
	/**
	 * Boost terms in query based on score.
	 * @see #isBoostByScore
	 * @see #setBoostByScore
	 */
	public static final boolean DEFAULT_BOOST_BY_SCORE = true;

	/**
	 * How many terms must match a similar document (fraction, 1.0 means all).
	 *
	 * @see #getFractionTermsToMatch
	 * @see #setFractionTermsToMatch	 
	 */
	public static final float DEFAULT_FRACTION_TERMS_TO_MATCH = 0.5f;
	
	private int minTermFreq = DEFAULT_MIN_TERM_FREQ;
	private int minDocFreq = DEFAULT_MIN_DOC_FREQ;
	private boolean boostByScore = DEFAULT_BOOST_BY_SCORE;
	private int minWordLen = DEFAULT_MIN_WORD_LENGTH;
	private int maxWordLen = DEFAULT_MAX_WORD_LENGTH;
	private int maxQueryTerms = DEFAULT_MAX_QUERY_TERMS;
	private float fractionTermsToMatch = DEFAULT_FRACTION_TERMS_TO_MATCH;
	private String matchingField=IndexConstants.FIELDNAME_CONTENT;
	private String docIdentifier;
	
	/**
	 * Creates a MoreLikeThisQuery instance finding similar documents of a <b>panFMP</b> document ID.
	 * The default field must have term vectors enabled in <b>panFMP</b>'s configuration.
	 */
	public MoreLikeThisQuery(String docIdentifier) {
		this.docIdentifier=docIdentifier;
	}

	/**
	 * Creates a MoreLikeThisQuery instance finding similar documents of a <b>panFMP</b> document ID.
	 * This variant exspects a field name, which term vector contents are used to build the query.
	 * This is optimal, if you want to find similarities only by comparing e.g. titles of documents.
	 */
	public MoreLikeThisQuery(String docIdentifier, String matchingField) {
		this.docIdentifier=docIdentifier;
		this.matchingField=matchingField.intern();
	}

	/**
	 * Returns the frequency below which terms will be ignored in the source doc. The default
	 * frequency is the {@link #DEFAULT_MIN_TERM_FREQ}.
	 *
	 * @return the frequency below which terms will be ignored in the source doc.
	 */
	public int getMinTermFreq() {
		return minTermFreq;
	}

	/**
	 * Sets the frequency below which terms will be ignored in the source doc.
	 *
	 * @param minTermFreq the frequency below which terms will be ignored in the source doc.
	 */
	public void setMinTermFreq(int minTermFreq) {
		if (minTermFreq<0)
			throw new IllegalArgumentException("minTermFreq must be 0 for no limit, >0 for a minimum term frequency");
		this.minTermFreq = minTermFreq;
	}

	/**
	 * Returns the frequency at which words will be ignored which do not occur in at least this
	 * many docs. The default frequency is {@link #DEFAULT_MIN_DOC_FREQ}.
	 *
	 * @return the frequency at which words will be ignored which do not occur in at least this
	 * many docs.
	 */
	public int getMinDocFreq() {
		return minDocFreq;
	}

	/**
	 * Sets the frequency at which words will be ignored which do not occur in at least this
	 * many docs.
	 *
	 * @param minDocFreq the frequency at which words will be ignored which do not occur in at
	 * least this many docs.
	 */
	public void setMinDocFreq(int minDocFreq) {
		if (minDocFreq<0)
			throw new IllegalArgumentException("minDocFreq must be 0 for no limit, >0 for a minimum document frequency");
		this.minDocFreq = minDocFreq;
	}

	/**
	 * Returns whether to boost terms in query based on "score" or not.
	 *
	 * @return whether to boost terms in query based on "score" or not.
	 * @see #setBoostByScore
	 */
	public boolean isBoostByScore() {
		return boostByScore;
	}

	/**
	 * Sets whether to boost terms in query based on "score" or not.
	 *
	 * @param boostByScore true to boost terms in query based on "score", false otherwise.
	 * The default is {@link #DEFAULT_BOOST_BY_SCORE}.
	 * @see #isBoostByScore
	 */
	public void setBoostByScore(boolean boostByScore) {
		this.boostByScore = boostByScore;
	}

	/**
	 * Returns the minimum word length below which words will be ignored. Set this to 0 for no
	 * minimum word length. The default is {@link #DEFAULT_MIN_WORD_LENGTH}.
	 *
	 * @return the minimum word length below which words will be ignored.
	 */
	public int getMinWordLen() {
		return minWordLen;
	}

	/**
	 * Sets the minimum word length below which words will be ignored.
	 *
	 * @param minWordLen the minimum word length below which words will be ignored.
	 */
	public void setMinWordLen(int minWordLen) {
		if (minWordLen<0)
			throw new IllegalArgumentException("minWordLen must be 0 for no limit, >0 for a minimum word length when finding relevant terms");
		this.minWordLen = minWordLen;
	}

	/**
	 * Returns the maximum word length above which words will be ignored. Set this to 0 for no
	 * maximum word length. The default is {@link #DEFAULT_MAX_WORD_LENGTH}.
	 */
	public int getMaxWordLen() {
		return maxWordLen;
	}

	/**
	 * Sets the maximum word length above which words will be ignored.
	 */
	public void setMaxWordLen(int maxWordLen) {
		if (maxWordLen<0)
			throw new IllegalArgumentException("maxWordLen must be 0 for no limit, >0 for a maximum word length when finding relevant terms");
		this.maxWordLen = maxWordLen;
	}

	/**
	 * Returns the maximum number of query terms that will be included in the rewritten query.
	 * The default is {@link #DEFAULT_MAX_QUERY_TERMS}.
	 */
	public int getMaxQueryTerms() {
		return maxQueryTerms;
	}

	/**
	 * Sets the maximum number of query terms that will be included in the
	 * rewritten query. A hard limit is {@link  BooleanQuery#getMaxClauseCount}.
	 */
	public void setMaxQueryTerms(int maxQueryTerms) {
		if (maxQueryTerms<0)
			throw new IllegalArgumentException("maxQueryTerms must be 0 for no limit, >0 for a maximum number of terms in the rewritten query");
		this.maxQueryTerms = maxQueryTerms;
	}

	/**
	 * Gets the percentage of terms to match in similar documents.
	 */
	public float getFractionTermsToMatch() {
		return fractionTermsToMatch;
	}
	
	/**
	 * Sets the percentage of terms to match in similar documents. Defaults to {@link #DEFAULT_FRACTION_TERMS_TO_MATCH}.
	 */
	public void setFractionTermsToMatch(float fractionTermsToMatch) {
		if (fractionTermsToMatch<=0.0f || fractionTermsToMatch>1.0f)
			throw new IllegalArgumentException("fractionTermsToMatch must be between 0.0 and 1.0");
		this.fractionTermsToMatch = fractionTermsToMatch;
	}

	@Override
	public Query rewrite(IndexReader reader) throws IOException {
		// retrieve the docNum for the docIdentifier from the index
		int docNum;
		Term idt=new Term(IndexConstants.FIELDNAME_IDENTIFIER,docIdentifier);
		TermDocs td=reader.termDocs(idt);
		try {
			if (td.next()) {
				docNum=td.doc();
			} else {
				log.warn("No document with identifier="+docIdentifier+" found in index for 'More like this' query.");
				return new TermQuery(idt); // should not match anything (and, if yes, the doc itsself)
			}
			if (td.next()) log.warn("More than one document with unique identifier="+docIdentifier+" found in index.");
		} finally {
			td.close();
		}

		// get term vectors
		TermFreqVector vector = reader.getTermFreqVector(docNum, matchingField);
		if (vector==null) throw new IllegalFieldConfigException("Field "+matchingField+" does not have term vectors.");

		// Adds terms and frequencies found in vector into the Map termFreqMap
		String[] terms = vector.getTerms();
		int freqs[]=vector.getTermFrequencies();
		int numDocs = reader.numDocs();
		FreqQ q = new FreqQ(terms.length); // will order words by score
		Similarity similarity=new DefaultSimilarity();

		for (int i=0; i<terms.length; i++) { // for every word
			if (minTermFreq > 0 && freqs[i] < minTermFreq) continue; // filter out words that don't occur enough times in the source

			// go through all the fields and find the largest document frequency
			int docFreq = reader.docFreq(new Term(matchingField, terms[i]));

			if (minDocFreq > 0 && docFreq < minDocFreq) continue; // filter out words that don't occur in enough docs
			if (docFreq == 0) continue; // index update problem?
			
			q.insert(new Freq(terms[i], freqs[i] * similarity.idf(docFreq, numDocs)));
		}

		int qterms = 0;
		float bestScore = 0;
		BooleanQuery query=new BooleanQuery();

		for (Freq ar=(Freq)(q.pop()); ar!=null; ) {
			TermQuery tq = new TermQuery(new Term(matchingField, ar.term));

			if (boostByScore) {
				if (qterms==0) bestScore=ar.score;
				tq.setBoost(ar.score / bestScore);
			}

			try {
				query.add(tq, BooleanClause.Occur.SHOULD);
			} catch (BooleanQuery.TooManyClauses ignore) {
				break;
			}

			qterms++;
			if (maxQueryTerms > 0 && qterms >= maxQueryTerms) break;
		}
		
		query.setMinimumNumberShouldMatch((int)(qterms*fractionTermsToMatch));
		query.setBoost(this.getBoost());
		return query.rewrite(reader);
	}
	
	@Override
	public String toString(String field) {
		StringBuilder sb=new StringBuilder("moreLikeThis{");
		sb.append(docIdentifier);
		if (!matchingField.equals(field)) sb.append(",matchingField=").append(matchingField);
		if (boostByScore) sb.append(",boostByScore");
		if (minTermFreq!=DEFAULT_MIN_TERM_FREQ) sb.append(",minTermFreq=").append(minTermFreq);
		if (minDocFreq!=DEFAULT_MIN_DOC_FREQ) sb.append(",minDocFreq=").append(minDocFreq);
		if (minWordLen!=DEFAULT_MIN_WORD_LENGTH) sb.append(",minWordLen=").append(minWordLen);
		if (maxWordLen!=DEFAULT_MAX_WORD_LENGTH) sb.append(",maxWordLen=").append(maxWordLen);
		if (maxQueryTerms!=DEFAULT_MAX_QUERY_TERMS) sb.append(",maxQueryTerms=").append(maxQueryTerms);
		if (fractionTermsToMatch!=DEFAULT_FRACTION_TERMS_TO_MATCH) sb.append(",fractionTermsToMatch=").append(fractionTermsToMatch);
		return sb.append('}').append(ToStringUtils.boost(getBoost())).toString();
	}

	@Override
	public final boolean equals(Object o) {
		if (o instanceof MoreLikeThisQuery) {
			MoreLikeThisQuery q=(MoreLikeThisQuery)o;
			return (this.matchingField==q.matchingField
				&& this.docIdentifier.equals(q.docIdentifier)
				&& this.minTermFreq==q.minTermFreq
				&& this.minDocFreq==q.minDocFreq
				&& this.boostByScore==q.boostByScore
				&& this.minWordLen==q.minWordLen
				&& this.maxWordLen==q.maxWordLen
				&& this.maxQueryTerms==q.maxQueryTerms
				&& this.fractionTermsToMatch==q.fractionTermsToMatch
				&& this.getBoost()==q.getBoost()
			);
		} else return false;
	}

	@Override
	public final int hashCode() {
		return matchingField.hashCode()+
			(docIdentifier.hashCode()^0x7af456bc)+
			(Integer.valueOf(minTermFreq).hashCode()^0x12345678)+
			(Integer.valueOf(minDocFreq).hashCode()^0x98742888)+
			(Boolean.valueOf(boostByScore).hashCode()^0xabfd34d)+
			(Integer.valueOf(minWordLen).hashCode()^0xffaa4598)+
			(Integer.valueOf(maxWordLen).hashCode()^0xab543980)+
			(Integer.valueOf(maxQueryTerms).hashCode()^0x334678aa)+
			(Float.valueOf(fractionTermsToMatch).hashCode()^0x645dae3f)+
			Float.floatToIntBits(getBoost());
	}

	/**
	 * PriorityQueue that orders words by score.
	 */
	private static final class FreqQ extends PriorityQueue {
		FreqQ(int s) {
			initialize(s);
		}

		protected boolean lessThan(Object a, Object b) {
			return ((Freq)a).score > ((Freq)b).score;
		}
	}    
	
	/**
	 * Frequency item
	 */
	private static final class Freq {
		Freq(String term, float score) {
			this.term=term;
			this.score=score;
		}

		public String term;
		public float score;
	}    
	
}