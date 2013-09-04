/*
 *   Copyright panFMP Developers Team c/o Uwe Schindler
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

package de.pangaea.metadataportal.utils;

import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.util.Bits;

import java.io.IOException;

/**
 * A collector that boosts by a indexed float NumericField value before passing to another collector.
 * @author Uwe Schindler
 */
public final class BoostingCollector extends Collector {

	private final Collector delegate;
	private final String boostField;
	
	Bits validDocs = null;
	float[] boosts = null;
	int doc;

	public BoostingCollector(Collector delegate, String boostField) {
		this.delegate = delegate;
		this.boostField = boostField;
	}

	public void collect(int doc) throws IOException {
		delegate.collect(this.doc = doc);
	}
	
	public void setNextReader(IndexReader reader, int docBase) throws IOException {
		boosts = FieldCache.DEFAULT.getFloats(reader, boostField, FieldCache.NUMERIC_UTILS_FLOAT_PARSER, true);
		validDocs = FieldCache.DEFAULT.getDocsWithField(reader, boostField);
		delegate.setNextReader(reader, docBase);
	}
	
	public void setScorer(final Scorer scorer) throws IOException {
		delegate.setScorer(new Scorer((Weight) null) {
			@Override
			public final float score() throws IOException {
				return validDocs.get(doc) ? 
					(scorer.score() * boosts[doc]) : scorer.score();
			}

			@Override
			public final int docID() { return doc; }

			@Override
			public final float freq() throws IOException { return scorer.freq(); }

			@Override
			public final int advance(int target) { throw new UnsupportedOperationException(); }

			@Override
			public final int nextDoc() { throw new UnsupportedOperationException(); }
		});
	}
	
	public boolean acceptsDocsOutOfOrder() {
		return delegate.acceptsDocsOutOfOrder();
	}
	
}
