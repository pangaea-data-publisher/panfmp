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

package de.pangaea.metadataportal.config;

import java.util.Arrays;
import de.pangaea.metadataportal.utils.*;
import org.apache.lucene.document.Field;

/**
 * Config element that contains the definition of a field. It contains its name and some
 * properties like name, datatype and Lucene indexing flags.
 * @author Uwe Schindler
 */
public class FieldConfig extends ExpressionConfig {

	public void setName(String v) { name=v; }
	public String getName() { return name; }
	
	/* NOT YET USED
	public void setDataType(DataType v) { datatype=v; }
	public DataType getDataType() { return datatype; }*/

	@PublicForDigesterUse
	@Deprecated
	public void setDataType(String v) {
		try {
			datatype=DataType.valueOf(v.toUpperCase());
		} catch (IllegalArgumentException e) {
			throw new IllegalArgumentException("Invalid value '"+v+"' for attribute datatype, valid ones are: "+Arrays.toString(DataType.values()));
		}
	}

	/* NOT YET USED
	public void setStorage(Field.Store v) { storage=v; }
	public Field.Store getStorage() { return storage; }*/

	@PublicForDigesterUse
	@Deprecated
	public void setStorage(String v) {
		if (v==null) return;
		v=v.toUpperCase();
		if ("COMPRESS".equals(v) || "COMPRESSED".equals(v)) storage=Field.Store.COMPRESS;
		else if ("TRUE".equals(v) || "YES".equals(v) || "ON".equals(v)) storage=Field.Store.YES;
		else if ("FALSE".equals(v) || "NO".equals(v) || "OFF".equals(v)) storage=Field.Store.NO;
		else throw new IllegalArgumentException("Attribute lucenestorage must be one of: [YES,TRUE,ON]; [NO,FALSE,OFF]; [COMPRESS,COMPRESSED]");
	}

	/* NOT YET USED
	public void setTermVectors(Field.TermVector v) { termVectors=v; }
	public Field.TermVector getTermVectors() { return termVectors; }*/

	@PublicForDigesterUse
	@Deprecated
	public void setTermVectors(String v) {
		if (v==null) return;
		v=v.toUpperCase();
		if ("TERMPOSITIONS".equals(v)) termVectors=Field.TermVector.WITH_POSITIONS_OFFSETS;
		else if ("TRUE".equals(v) || "YES".equals(v) || "ON".equals(v)) termVectors=Field.TermVector.YES;
		else if ("FALSE".equals(v) || "NO".equals(v) || "OFF".equals(v)) termVectors=Field.TermVector.NO;
		else throw new IllegalArgumentException("Attribute lucenetermvectors must be one of: [YES,TRUE,ON]; [NO,FALSE,OFF]; TERMPOSITIONS");
	}

	/* NOT YET USED
	public void setIndexed(boolean v) { indexed=v; }
	public boolean isIndexed() { return indexed; }*/

	@PublicForDigesterUse
	@Deprecated
	public void setIndexed(String v) {
		if (v==null) return;
		indexed=BooleanParser.parseBoolean(v);
	}

	public void setDefault(String v) { defaultValue=v; }
	public String getDefault() { return defaultValue; }

	@Override
	public String toString() {
		return name;
	}

	// members "the configuration"
	public String name=null;
	public String defaultValue=null;
	public DataType datatype=DataType.TOKENIZEDTEXT;
	public Field.Store storage=Field.Store.YES;
	public Field.TermVector termVectors=Field.TermVector.NO;
	public boolean indexed=true;

	public static enum DataType { TOKENIZEDTEXT,STRING,NUMBER,DATETIME,XML,XHTML };
}

