/*
 *   Copyright 2007-2008 panFMP Developers Team c/o Uwe Schindler
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

/* This class was borrowed from Nutch, a sub-project of Apache Lucene, and made conformant to Java 1.5 coding style. */

package de.pangaea.metadataportal.utils;

import java.io.ByteArrayOutputStream; 
import java.io.IOException; 
import java.io.PrintStream; 
import java.lang.reflect.Method; 
import org.apache.commons.logging.Log; 
import org.apache.commons.logging.LogFactory; 

public class LogUtil { 
	private static final Log log = LogFactory.getLog(LogUtil.class); 
	
	private static Method TRACE = null; 
	private static Method DEBUG = null; 
	private static Method INFO = null; 
	private static Method WARN = null; 
	private static Method ERROR = null; 
	private static Method FATAL = null; 
	static { 
		try { 
			TRACE = Log.class.getMethod("trace", Object.class, Throwable.class); 
			DEBUG = Log.class.getMethod("debug", Object.class, Throwable.class); 
			INFO = Log.class.getMethod("info", Object.class, Throwable.class); 
			WARN = Log.class.getMethod("warn", Object.class, Throwable.class); 
			ERROR = Log.class.getMethod("error", Object.class, Throwable.class); 
			FATAL = Log.class.getMethod("fatal", Object.class, Throwable.class); 
		} catch (Exception e) { 
			log.fatal("Cannot init log methods", e); 
		} 
	} 
	
	private LogUtil() {
		// no instance
	}
	
	public static PrintStream getTraceStream(final Log logger) { 
		return getLogStream(logger, TRACE); 
	} 
	
	public static PrintStream getDebugStream(final Log logger) { 
		return getLogStream(logger, DEBUG); 
	} 
	
	public static PrintStream getInfoStream(final Log logger) { 
		return getLogStream(logger, INFO); 
	} 
	
	public static PrintStream getWarnStream(final Log logger) { 
		return getLogStream(logger, WARN); 
	} 
	
	public static PrintStream getErrorStream(final Log logger) { 
		return getLogStream(logger, ERROR); 
	} 
	
	public static PrintStream getFatalStream(final Log logger) { 
		return getLogStream(logger, FATAL); 
	} 
	
	private static PrintStream getLogStream(final Log logger, final Method method) {
		try {
			return new PrintStream(new ByteArrayOutputStream(){ 
				private int scan = 0; 
				
				private synchronized boolean hasNewline() { 
					for (; scan<count; scan++) { 
						if (buf[scan]=='\n' || buf[scan]=='\r') return true; 
					} 
					return false; 
				} 
				
				@Override
				public synchronized void flush() throws IOException { 
					if (!hasNewline()) return; 
					try { 
						method.invoke(logger, new String(buf,0,scan,"US-ASCII"), null); 
					} catch (Exception e) { 
						log.fatal("Cannot log with method [" + method + "]", e); 
					}
					while (scan<count && (buf[scan]=='\n' || buf[scan]=='\r')) scan++;
					byte[] b=toByteArray();
					reset();
					write(b,scan,b.length-scan);
					scan=0;
					flush();
				} 
				
				@Override
				public void close() throws IOException {
					if (!hasNewline()) write('\n');
					flush();
				}
			}, true, "US-ASCII");
		} catch (java.io.UnsupportedEncodingException ue) {
			throw new RuntimeException(ue); // should never happen with US-ASCII
		}
	} 
}