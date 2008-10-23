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

package de.pangaea.metadataportal.utils;

import java.security.*;

/**
 * Simple static class to create some hashes from string values.
 * @author Uwe Schindler
 */
public final class HashGenerator {

	private HashGenerator() {} // no instance

	/** Converts a byte array to a hexadecimal string. */
	public static String hex(byte[] array) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < array.length; i++) {
			sb.append(Integer.toHexString((array[i] & 0xFF) | 0x100).toLowerCase().substring(1,3));
		}
		return sb.toString();
	}

	/** Applies MD5 algorithm to the given message and returns it as a hex string. */
	public static String md5(String message) {
		try {
			MessageDigest md = MessageDigest.getInstance("MD5");
			return hex(md.digest(message.getBytes()));
		} catch (NoSuchAlgorithmException e) {
			//fallback
			return "hash"+message.hashCode();
		}
	}

	/** Applies SHA1 algorithm to the given message and returns it as a hex string. */
	public static String sha1(String message) {
		try {
			MessageDigest md = MessageDigest.getInstance("SHA1");
			return hex(md.digest(message.getBytes()));
		} catch (NoSuchAlgorithmException e) {
			//fallback
			return "hash"+message.hashCode();
		}
	}

}