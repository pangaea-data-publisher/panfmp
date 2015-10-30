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

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * This annotation is used to mark methods/classes that are only declared
 * <code>public</code> for use by <code>Digester</code> but are not intended to
 * be public. So <b>please do not use them in your Java code!</b> The annotation
 * should always be used together with <code>@Deprecated</code> to warn the user
 * if it is used in code outside of Digester.
 * 
 * @author Uwe Schindler
 */
@Documented
@Retention(value = RetentionPolicy.RUNTIME)
public @interface PublicForDigesterUse {}
