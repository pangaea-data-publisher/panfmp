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
import java.net.CookieHandler;
import java.net.CookieManager;
import java.net.CookiePolicy;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import de.pangaea.metadataportal.harvester.Harvester;

/**
 * A CookieHandler that can be enabled and used per thread.
 * @author Uwe Schindler
 */
public final class SimpleCookieHandler extends CookieHandler {
  
  static final org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory.getLog(SimpleCookieHandler.class);
  
  /**
   * Singleton instance of this class. Should be set with
   * {@link CookieHandler#setDefault} as default.
   */
  public static final SimpleCookieHandler INSTANCE = new SimpleCookieHandler();
  
  private SimpleCookieHandler() {}
  
  private final ThreadLocal<CookieManager> manager = new ThreadLocal<CookieManager>() {
    @Override
    protected CookieManager initialValue() {
      return new CookieManager(null, CookiePolicy.ACCEPT_ORIGINAL_SERVER);
    }
  };
  private final ThreadLocal<Boolean> enabled = new ThreadLocal<Boolean>() {
    @Override
    protected Boolean initialValue() {
      return Boolean.FALSE;
    }
  };
  
  /**
   * Resets all recorded cookies for the current thread. This method is called
   * from {@link Harvester#open} to have an empty cookie list.
   */
  public void enable() {
    manager.remove();
    enabled.set(true);
  }
  
  /**
   * Cleans up the cookie list and disables the handler.
   */
  public void disable() {
    enabled.set(false);
    manager.remove();
  }
  
  @Override
  public void put(URI uri, Map<String,List<String>> responseHeaders) throws IOException {
    if (enabled.get().booleanValue()) {
      manager.get().put(uri, responseHeaders);
    }
  }
  
  @Override
  public Map<String,List<String>> get(URI uri, Map<String,List<String>> requestHeaders) throws IOException {
    if (enabled.get().booleanValue()) {
      return manager.get().get(uri, requestHeaders);
    } else {
      return Collections.emptyMap();
    }
  }
  
}
