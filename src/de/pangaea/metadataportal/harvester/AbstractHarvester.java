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

import de.pangaea.metadataportal.config.*;

public abstract class AbstractHarvester {

    // log
    protected org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory.getLog(this.getClass());

    // Object members
    public IndexBuilder index=null;
    public SingleIndexConfig iconfig=null;
    protected int harvestCount=0;
    protected int harvestMessageStep=100;
    protected XMLConverter xmlConverter=null;

    // construtors
    public AbstractHarvester() {}

    public void open(SingleIndexConfig iconfig) throws Exception {
        if (iconfig==null) throw new IllegalArgumentException("Missing index configuration");
        this.iconfig=iconfig;
        harvestMessageStep=Integer.parseInt(iconfig.harvesterProperties.getProperty("harvestMessageStep","100"));
        if (harvestMessageStep<=0) throw new IllegalArgumentException("Invalid value for harvestMessageStep: "+harvestMessageStep);
        index = new IndexBuilder(false,iconfig);
        xmlConverter=new XMLConverter(iconfig);
    }

    public void close() throws Exception {
        if (index==null) throw new IllegalStateException("Harvester must be opened before using");
        if (!index.isClosed()) index.close();
        index=null;

        log.info("Harvested "+harvestCount+" documents - finished.");
    }

    public void addDocument(MetadataDocument mdoc) throws Exception {
        if (index==null) throw new IllegalStateException("Harvester must be opened before using");
        index.addDocument(mdoc);
        harvestCount++;
        if (harvestCount%harvestMessageStep==0) log.info("Harvested "+harvestCount+" documents so far.");
    }

    public abstract void harvest() throws Exception;
}