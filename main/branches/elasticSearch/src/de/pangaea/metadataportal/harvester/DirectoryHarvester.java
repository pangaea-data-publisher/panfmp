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

package de.pangaea.metadataportal.harvester;

import java.io.File;
import java.io.FilenameFilter;
import java.util.Arrays;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.transform.stream.StreamSource;

import de.pangaea.metadataportal.config.HarvesterConfig;
import de.pangaea.metadataportal.processor.ElasticsearchConnection;
import de.pangaea.metadataportal.utils.BooleanParser;

/**
 * Harvester for traversing file system directories. Identifiers are build from
 * the relative path of files against the base directory.
 * <p>
 * This harvester supports the following additional <b>harvester properties</b>:
 * <ul>
 * <li><code>directory</code>: file system directory to harvest</li>
 * <li><code>recursive</code>: traverse in subdirs (default: false)</li>
 * <li><code>identifierPrefix</code>: This prefix is appended before all
 * relative file system pathes (that are the identifiers of the documents)
 * (default: "")</li>
 * <li><code>filenameFilter</code>: regex to match the filename (default: none)</li>
 * </ul>
 * 
 * @author Uwe Schindler
 */
public class DirectoryHarvester extends SingleFileEntitiesHarvester implements FilenameFilter {
  
  // Class members
  private File directory = null;
  private boolean recursive = false;
  private Pattern filenameFilter = null;
  private String identifierPrefix = "";
  
  public DirectoryHarvester(HarvesterConfig iconfig) {
    super(iconfig);
  }

  @Override
  public void open(ElasticsearchConnection es) throws Exception {
    super.open(es);
    
    String s = iconfig.harvesterProperties.getProperty("directory");
    if (s == null) throw new IllegalArgumentException(
        "Missing directory name to start harvesting (property \"directory\")");
    
    directory = new File(iconfig.parent.makePathAbsolute(s, false));
    recursive = BooleanParser.parseBoolean(iconfig.harvesterProperties
        .getProperty("recursive", "false"));
    identifierPrefix = iconfig.harvesterProperties.getProperty(
        "identifierPrefix", "");
    
    s = iconfig.harvesterProperties.getProperty("filenameFilter");
    filenameFilter = (s == null) ? null : Pattern.compile(s);
  }
  
  @Override
  public void harvest() throws Exception {
    processDirectory(directory);
  }
  
  @Override
  protected void enumerateValidHarvesterPropertyNames(Set<String> props) {
    super.enumerateValidHarvesterPropertyNames(props);
    props.addAll(Arrays.<String> asList("directory", "recursive",
        "identifierPrefix", "filenameFilter"));
  }
  
  public boolean accept(File dir, String name) {
    File file = new File(dir, name);
    if (file.isDirectory()) return (recursive && !".".equals(name) && !".."
        .equals(name));
    if (filenameFilter == null) return true;
    Matcher m = filenameFilter.matcher(name);
    return m.matches();
  }
  
  private void processFile(File file) throws Exception {
    String identifier = "file:"
        + identifierPrefix
        + directory.toURI().normalize().relativize(file.toURI().normalize())
            .toString();
    addDocument(identifier, file.lastModified(), new StreamSource(file));
  }
  
  private void processDirectory(File dir) throws Exception {
    StringBuilder logstr = new StringBuilder("Walking into directory \"")
        .append(dir).append("\" (recursive=").append(recursive);
    if (filenameFilter != null) logstr.append(",filter=\"")
        .append(filenameFilter).append("\"");
    logstr.append(")...");
    log.info(logstr);
    
    File[] files = dir.listFiles(this);
    if (files == null) return;
    for (File f : files) {
      if (f.isDirectory()) processDirectory(f);
      else if (f.isFile()) processFile(f);
    }
    
    log.info("Finished directory \"" + dir + "\".");
  }
  
}