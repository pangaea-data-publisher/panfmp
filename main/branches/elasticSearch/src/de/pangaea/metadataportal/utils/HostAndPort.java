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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.elasticsearch.common.transport.InetSocketTransportAddress;

/**
 * Simple static class to parse {@code host:port} combinations.
 * <p>Original code borrowed from Google Guava libraries.
 */
public final class HostAndPort {
  
  private HostAndPort() {}
  
  private static final Pattern BRACKET_PATTERN = Pattern.compile("^\\[(.*:.*)\\](?::(\\d*))?$");

  /**
   * Parses the given string to a {@link InetSocketTransportAddress}. IPv6
   * addresses have to be put in {@code []} brackets.
   */
  public static InetSocketAddress parse(String hostPortString, int defaultPort) {
    final String host, portString;

    if (hostPortString.startsWith("[")) {
      // Parse a bracketed host, typically an IPv6 literal.
      final Matcher matcher = BRACKET_PATTERN.matcher(hostPortString);
      if (!matcher.matches())
        throw new IllegalArgumentException("Invalid bracketed host/port: " + hostPortString);
      host = matcher.group(1);
      portString = matcher.group(2);  // could be null
    } else {
      int colonPos = hostPortString.lastIndexOf(':');
      if (colonPos >= 0) {
        host = hostPortString.substring(0, colonPos);
        if (host.indexOf(':') >= 0)
          throw new IllegalArgumentException("Multiple colons in host name: " + hostPortString);
        portString = hostPortString.substring(colonPos + 1);
      } else {
        host = hostPortString;
        portString = null;
      }
    }

    int port = defaultPort;
    if (portString != null) {
      try {
        port = Integer.parseInt(portString);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("Unparseable port number: " + hostPortString);
      }
    }

    return new InetSocketAddress(host, port);
  }
  
}