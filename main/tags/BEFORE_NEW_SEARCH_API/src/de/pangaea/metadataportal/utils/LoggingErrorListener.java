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

package de.pangaea.metadataportal.utils;

import javax.xml.transform.*;

public final class LoggingErrorListener implements ErrorListener {

    public LoggingErrorListener(Class c) {
        log=org.apache.commons.logging.LogFactory.getLog(c);
    }

    public void error(TransformerException e) throws TransformerException {
        throw e;
    }

    public void fatalError(TransformerException e) throws TransformerException {
        throw e;
    }

    public void warning(TransformerException e) throws TransformerException {
        log.warn(e.getMessage());
    }

    private org.apache.commons.logging.Log log;

}