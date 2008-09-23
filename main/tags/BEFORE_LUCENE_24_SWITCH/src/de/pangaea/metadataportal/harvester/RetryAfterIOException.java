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

package de.pangaea.metadataportal.harvester;

/**
 * Thrown when HTTP server responds with {@link java.net.HttpURLConnection#HTTP_UNAVAILABLE}.
 * @author Uwe Schindler
 */
public class RetryAfterIOException extends java.io.IOException {

	public RetryAfterIOException(int retryAfter, java.io.IOException ioe) {
		super();
		initCause(ioe);
		this.retryAfter=retryAfter;
	}

	public int getRetryAfter() {
		return retryAfter;
	}

	private int retryAfter;
}