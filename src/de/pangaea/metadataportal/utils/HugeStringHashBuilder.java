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

import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import java.util.Spliterator;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.BytesRefHash;

/**
 * A utility class that uses Lucene's {@link BytesRefHash} to create a {@link Set}.
 * 
 * @author Uwe Schindler
 */
public final class HugeStringHashBuilder {
  
  private BytesRefHash hash = new BytesRefHash();
  private final BytesRefBuilder scratch = new BytesRefBuilder();
  
  public HugeStringHashBuilder() {
  }
  
  /** Adds a String */
  public void add(String str) {
    Objects.requireNonNull(str,"str");
    if (hash == null) {
      throw new IllegalStateException("Cannot add additional elements once Set is built.");
    }
    scratch.copyChars(str);
    hash.add(scratch.get());
  }
  
  /** Builds a read only set. After calling this method, you cannot reuse the instance. */
  public Set<String> build() {
    if (hash == null) {
      throw new IllegalStateException("build() can only be called once.");
    }
    scratch.clear();
    final BytesRefHash privateHash = hash;
    hash = null; // hand over to the new set, so we don't produce memory leak:
    return new SetImpl(privateHash);
  }
  
  private static final class SetImpl extends AbstractSet<String> {
    private final BytesRefHash hash;
    private final BytesRefBuilder scratch = new BytesRefBuilder();
    
    SetImpl(BytesRefHash hash) {
      this.hash = hash;
    }
    
    @Override
    public Stream<String> stream() {
      final BytesRef scratch = new BytesRef();
      return IntStream.range(0, hash.size()).mapToObj(i -> hash.get(i, scratch).utf8ToString());
    }
    
    @Override
    public Iterator<String> iterator() {
      return stream().iterator();
    }
    
    @Override
    public Spliterator<String> spliterator() {
      return stream().spliterator();
    }
    
    @Override
    public boolean contains(Object o) {
      if (o instanceof String) {
        scratch.copyChars((String) o);
        return hash.find(scratch.get()) >= 0;
      }
      return false;
    }
    
    @Override
    public boolean remove(Object o) {
      throw new UnsupportedOperationException();
    }
    
    @Override
    public int size() {
      return hash.size();
    }
  }

}