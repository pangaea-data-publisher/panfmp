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

import java.net.CookieHandler;
import java.net.URI;
import java.util.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * @author Uwe Schindler
 */
public final class SimpleCookieHandler extends CookieHandler {

	private static org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory.getLog(SimpleCookieHandler.class);

	/** Singleton instance of this class. Should be set with {@link CookieHandler#setDefault} as default. */
	public static final SimpleCookieHandler INSTANCE = new SimpleCookieHandler();
	
	private SimpleCookieHandler() {
	}
	
	private final ThreadLocal<List<Cookie>> cache = new ThreadLocal<List<Cookie>>();
	
	/** Resets all recorded cookies for the current thread. This method is called from {@link Harvester#open} to have an empty cookie list. */
	public void enable() {
		cache.set(new LinkedList<Cookie>());
	}

	public void disable() {
		cache.remove();
	}

	@Override
	public void put(URI uri, Map<String,List<String>> responseHeaders) {
		if (cache.get()==null) return;
		for (Map.Entry<String,List<String>> entry : responseHeaders.entrySet()) {
			if (!("Set-Cookie".equalsIgnoreCase(entry.getKey()) || "Set-Cookie2".equalsIgnoreCase(entry.getKey()))) continue;
			List<String> setCookieList = entry.getValue();
			for (String gItem : setCookieList) {
				for (String item : gItem.split(",")) try {
					final Cookie cookie = new Cookie(uri, item);
					if (log.isDebugEnabled()) log.debug("Received cookie: "+cookie);
					for (Iterator<Cookie> it = cache.get().iterator(); it.hasNext();) {
						final Cookie existingCookie=it.next();
						if (cookie.getName().equals(existingCookie.getName())) it.remove();
					}
					cache.get().add(cookie);
				} catch (Exception e) {
					log.warn("Parsing cookie failed: "+e);
				}
			}
		}
	}

	@Override
	public Map<String,List<String>> get(URI uri, Map<String,List<String>> requestHeaders) {
		if (cache.get()==null) return Collections.<String,List<String>>emptyMap();
		
		StringBuilder cookies = new StringBuilder();
		Date now=new Date();
		for (Iterator<Cookie> it = cache.get().iterator(); it.hasNext();) {
			final Cookie cookie=it.next();
			if (cookie.hasExpired(now)) {
				it.remove();
			} else if (cookie.matches(uri)) {
				if (cookies.length()>0) cookies.append(", ");
				cookies.append(cookie.toString());
			}
		}

		if (cookies.length()>0) {
			if (log.isDebugEnabled()) log.debug("Sending cookies: "+cookies);
			return Collections.singletonMap("Cookie", Collections.singletonList(cookies.toString()));
		} else {
			return Collections.<String,List<String>>emptyMap();
		}
	}

	private static final class Cookie {
		private String _name;
		private String _value;
		private String _domain;
		private Date _expires;
		private String _path;

		public Cookie(URI uri, String header) {
			String attributes[] = header.split(";");
			String nameValue = attributes[0].trim();
			this._name = nameValue.substring(0, nameValue.indexOf('='));
			this._value = nameValue.substring(nameValue.indexOf('=') + 1);
			this._path = "/";
			this._domain = uri.getHost();

			for (int i = 1; i < attributes.length; i++) {
				nameValue = attributes[i].trim();
				int equals = nameValue.indexOf('=');
				if (equals == -1) continue;
				String name = nameValue.substring(0, equals).trim();
				String value = nameValue.substring(equals + 1).trim();
				if (value.length()>=2 && value.charAt(0)=='"' && value.charAt(value.length()-1)=='"') {
					value=value.substring(1,value.length()-1);
				}
				if (name.length()==0 || value.length()==0) continue;
				if (name.equalsIgnoreCase("domain")) {
					String uriDomain = uri.getHost();
					if (uriDomain.equalsIgnoreCase(value)) {
						this._domain = value;
					} else {
						if (!value.startsWith(".")) {
							value = "." + value;
						}
						if (!uriDomain.toLowerCase().endsWith(value.toLowerCase())) {
							throw new IllegalArgumentException("Trying to set foreign cookie '"+toString()+"'");
						}
						this._domain = value;
					}
				} else if (name.equalsIgnoreCase("path")) {
					this._path = value;
				} else if (name.equalsIgnoreCase("max-age")) {
					this._expires = new Date(System.currentTimeMillis()+1000L*Integer.parseInt(value));
				} else if (name.equalsIgnoreCase("expires")) {
					try {
						synchronized(EXPIRES_FORMAT_1) {
							this._expires = EXPIRES_FORMAT_1.parse(value);
						}
					} catch (ParseException e1) {
						try {
							synchronized(EXPIRES_FORMAT_2) {
								this._expires = EXPIRES_FORMAT_2.parse(value);
							}
						} catch (ParseException e2) {
							log.warn("Bad date format in cookie header (ignoring expires value): "+value);
							this._expires=null;
						}
					}
				}
			}
		}

		public boolean hasExpired(Date now) {
			if (_expires == null) return false;
			return now.after(_expires);
		}

		public String getName() {
			return _name;
		}

		public boolean matches(URI uri) {
			String uriDomain = uri.getHost();
			final boolean ok;
			if (uriDomain.equalsIgnoreCase(this._domain)) {
				ok=true;
			} else {
				ok=(uriDomain.toLowerCase().endsWith(this._domain.toLowerCase()));
			}
			
			String path = uri.getPath();
			if (path == null) {
				path = "/";
			}

			return ok && path.startsWith(this._path);
		}

		public String toString() {
			return new StringBuilder(_name).append("=").append(_value).toString();
		}

		private static final DateFormat EXPIRES_FORMAT_1 = new SimpleDateFormat("E, dd MMM yyyy H:m:s z", Locale.US);
		private static final DateFormat EXPIRES_FORMAT_2 = new SimpleDateFormat("E, dd-MMM-yyyy H:m:s z", Locale.US);
	}

}
