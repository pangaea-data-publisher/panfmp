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

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Simple static class to parse {@code host:port} combinations
 * 
 * @author Uwe Schindler
 */
public final class HostAndPort {
  
  private HostAndPort() {}
  
  /**
   * Parses the given string to a {@link InetSocketTransportAddress}. IPv6
   * addresses have to be put in {@code []} brackets.
   */
  public static InetSocketAddress parse(String v) {
    // TODO: Better way to parse host:port, with working IPv6
    try {
      URI uri = new URI("dummy://" + v + "/");
      String host = uri.getHost();
      if (host == null)
        throw new IllegalArgumentException("Missing hostname: " + v);
      int port = uri.getPort();
      if (port == -1) port = 9300;
      return new InetSocketAddress(host, port);
    } catch (URISyntaxException use) {
      throw new IllegalArgumentException("Invalid address: " + v);
    }
  }
  
}