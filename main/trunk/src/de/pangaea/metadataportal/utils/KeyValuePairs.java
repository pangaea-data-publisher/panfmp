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

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

/** 
 * Key/Value pairs used to build JSON. This behaves like a {@code Map} where
 * you can add element, but duplicate elements make the values an array.
 * It also supports adding {@code KeyValuePairs} as value, in which case an object is created in the resulting JSON.
 * */
public final class KeyValuePairs {
  private final Map<String,Object[]> map = new LinkedHashMap<>();
  
  public KeyValuePairs() {
  }
  
  public void add(final String key, final Object value) {
    Object[] existingVals = map.get(key);
    if (existingVals == null) {
      map.put(key, new Object[] { value });
    } else {
      Object[] newVals = new Object[existingVals.length + 1];
      System.arraycopy(existingVals, 0, newVals, 0, existingVals.length);
      newVals[existingVals.length] = value;
      map.put(key, newVals);
    }
  }
  
  public void add(final String key, final Object... values) {
    Object[] existingVals = map.get(key);
    if (existingVals == null) {
      map.put(key, values);
    } else {
      Object[] newVals = new Object[existingVals.length + 1];
      System.arraycopy(existingVals, 0, newVals, 0, existingVals.length);
      System.arraycopy(values, 0, newVals, existingVals.length, values.length);
      map.put(key, newVals);
    }
  }
  
  public int size() {
    return map.size();
  }
  
  public boolean isEmpty() {
    return map.isEmpty();
  }
  
  /** 
   * Serializes the object to a sequence of fields. The empty object will be serailized as {@code null}.
   */
  public void serializeToJSON(final XContentBuilder builder) throws IOException {
    if (map.isEmpty()) {
      builder.nullValue();
    } else {
      builder.startObject();
      for (final Map.Entry<String,Object[]> e : map.entrySet()) {
        final Object[] val = e.getValue();
        switch (val.length) {
          case 0:
            break;
          case 1:
            builder.field(e.getKey(), val[0]);
            break;
          default:
            builder.startArray(e.getKey());
            for (final Object o : val) {
              if (o instanceof KeyValuePairs) {
                ((KeyValuePairs) o).serializeToJSON(builder);
              } else {
                builder.value(o);
              }
            }
            builder.endArray();
        }
      }
      builder.endObject();
    }
  }
  
  @Override
  public String toString() {
    try {
      final XContentBuilder builder = XContentFactory.jsonBuilder();
      serializeToJSON(builder);
      return builder.string();
    } catch (IOException ioe) {
      throw new AssertionError(ioe);
    }
  }
  
}
