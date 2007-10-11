/*
 *   Copyright 2007 panFMP Developers Team c/o Uwe Schindler
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

package de.pangaea.metadataportal.harvester;

import java.net.*;
import java.io.*;
import java.util.zip.*;
import org.xml.sax.*;
import java.util.Date;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Static helper class for downloading OAI documents.
 * @author Uwe Schindler
 */
public final class OAIDownload {

	private static org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory.getLog(OAIDownload.class);

	private OAIDownload() {}

	// ** public API **

	public static InputStream getInputStream(URL url, AtomicReference<Date> checkModifiedDate) throws IOException {
		return getInputSource(url,checkModifiedDate).getByteStream();
	}

	public static EntityResolver getEntityResolver(final EntityResolver parent) {
		return new EntityResolver() {
			public InputSource resolveEntity(String publicId, String systemId) throws IOException,SAXException {
				try {
				   URL url=new URL(systemId);
				   String proto=url.getProtocol().toLowerCase();
				   if ("http".equals(proto) || "https".equals(proto)) return getInputSource(url,null);
				   else return (parent==null)?null:parent.resolveEntity(publicId,systemId);
				} catch (MalformedURLException malu) {
					return (parent==null)?null:parent.resolveEntity(publicId,systemId);
				}
			}
		};
	}

	public static InputSource getInputSource(URL url, AtomicReference<Date> checkModifiedDate) throws IOException {
		String proto=url.getProtocol().toLowerCase();
		if (!("http".equals(proto) || "https".equals(proto)))
			throw new IllegalArgumentException("OAI only allows HTTP(S) as network protocol!");
		HttpURLConnection conn=(HttpURLConnection)url.openConnection();

		StringBuilder ua=new StringBuilder("Java/");
		ua.append(System.getProperty("java.version"));
		ua.append(" (").append(de.pangaea.metadataportal.Package.getProductName()).append('/');
		ua.append(de.pangaea.metadataportal.Package.getVersion()).append("; OAI downloader)");
		conn.setRequestProperty("User-Agent",ua.toString());

		conn.setRequestProperty("Accept-Encoding","gzip, deflate, identity;q=0.3, *;q=0");
		conn.setRequestProperty("Accept-Charset","utf-8, *;q=0.1");
		conn.setRequestProperty("Accept","text/xml, application/xml, *;q=0.1");
		
		if (checkModifiedDate!=null && checkModifiedDate.get()!=null) conn.setIfModifiedSince(checkModifiedDate.get().getTime());

		conn.setUseCaches(false);
		conn.setFollowRedirects(true);
		log.debug("Opening connection...");
		InputStream in=null;
		try {
			conn.connect();
			in=conn.getInputStream();
		} catch (IOException ioe) {
			int after,code;
			try {
				after=conn.getHeaderFieldInt("Retry-After",-1);
				code=conn.getResponseCode();
			} catch (IOException ioe2) {
				after=-1; code=-1;
			}
			if (code==HttpURLConnection.HTTP_UNAVAILABLE && after>0) throw new RetryAfterIOException(after,ioe);
			throw ioe;
		}
		
		if (checkModifiedDate!=null) {
			if (conn.getResponseCode()==HttpURLConnection.HTTP_NOT_MODIFIED) {
				log.debug("File not modified since "+checkModifiedDate.get());
				return null;
			}
			long d=conn.getLastModified();
			checkModifiedDate.set( (d==0) ? null : new Date(d) );
		}
		
		String encoding=conn.getContentEncoding();
		if (encoding==null) encoding="identity";
		encoding=encoding.toLowerCase();
		log.debug("HTTP server uses "+encoding+" content encoding.");
		if ("gzip".equals(encoding)) in=new GZIPInputStream(in);
		else if ("deflate".equals(encoding)) in=new InflaterInputStream(in);
		else if (!"identity".equals(encoding)) throw new IOException("Server uses an invalid content encoding: "+encoding);

		// get charset from content-type to fill into InputSource to prevent SAXParser from guessing it
		// if charset is superseded by <?xml ?> declaration, it is changed later by parser
		String contentType=conn.getContentType();
		String charset=null;
		if (contentType!=null) {
			contentType=contentType.toLowerCase();
			int charsetStart=contentType.indexOf("charset=");
			if (charsetStart>=0) {
				int charsetEnd=contentType.indexOf(";",charsetStart);
				if (charsetEnd==-1) charsetEnd=contentType.length();
				charsetStart+="charset=".length();
				charset=contentType.substring(charsetStart,charsetEnd).trim();
			}
		}
		log.debug("Charset from Content-Type: '"+charset+"'");

		InputSource src=new InputSource(in);
		src.setSystemId(url.toString());
		src.setEncoding(charset);
		return src;
	}

}