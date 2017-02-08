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

import javax.xml.transform.ErrorListener;
import javax.xml.transform.TransformerException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Helper class to correctly log {@link TransformerException}s with Commons
 * Logging.
 * 
 * @author Uwe Schindler
 */
public final class LoggingErrorListener implements ErrorListener {
  
  /**
   * A error listener using the supplied Commons Logging instance as log target.
   */
  public LoggingErrorListener(final Log log) {
    this.log = log;
  }
  
  /** A error listener using the supplied class name as log target. */
  public LoggingErrorListener(final Class<?> c) {
    this(LogFactory.getLog(c));
  }
  
  /** Just throws <code>e</code>. */
  @Override
  public void error(TransformerException e) throws TransformerException {
    throw e;
  }
  
  /** Just throws <code>e</code>. */
  @Override
  public void fatalError(TransformerException e) throws TransformerException {
    throw e;
  }
  
  /** Logs message and location with WARN method. */
  @Override
  public void warning(TransformerException e) throws TransformerException {
    log.warn(e.getMessageAndLocation());
  }
  
  private Log log;
  
}