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

/**
 * This exception is thrown on an OAI protocol error, which is triggered by an <code>&lt;error&gt;</code> response from the repository.
 * @author Uwe Schindler
 */
public class OAIException extends java.lang.Exception {

	public OAIException(String code, String message) {
		super("".equals(message)?null:message);
		this.code=("".equals(code)?null:code);
	}

	@Override
	public String getMessage() {
		StringBuilder sb=new StringBuilder((code==null)?"default":code);
		String s=super.getMessage();
		if (s!=null) {
			sb.append(": ");
			sb.append(s);
		}
		return sb.toString();
	}

	public String getCode() {
		return code;
	}

	private String code;
}