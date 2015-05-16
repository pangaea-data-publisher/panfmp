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

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.Set;
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
public class DirectoryHarvester extends SingleFileEntitiesHarvester {
  
  static final String WRAPPED_MARKER = "###wrapped###";

  final boolean recursive;
  final Pattern filenameFilter;
  final String identifierPrefix;
  
  Path directory = null;
  
  public DirectoryHarvester(HarvesterConfig iconfig) {
    super(iconfig);
    
    recursive = BooleanParser.parseBoolean(iconfig.properties
        .getProperty("recursive", "false"));
    identifierPrefix = iconfig.properties.getProperty(
        "identifierPrefix", "");
    
    String s = iconfig.properties.getProperty("filenameFilter");
    filenameFilter = (s == null) ? null : Pattern.compile(s);
  }
  
  @Override
  public void open(ElasticsearchConnection es, String targetIndex) throws Exception {
    super.open(es, targetIndex);
    
    String directoryStr = iconfig.properties.getProperty("directory");
    if (directoryStr == null) throw new IllegalArgumentException(
        "Missing directory name to start harvesting (property \"directory\")");
    directory = iconfig.root.makePathAbsolute(directoryStr);
  }

  @Override
  public void harvest() throws Exception {
    try {
      Files.walkFileTree(directory, new SimpleFileVisitor<Path>() {
        @Override
        public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
          if (recursive || dir.equals(directory)) {
            StringBuilder logstr = new StringBuilder("Walking into directory \"").append(dir).append("\" (recursive=").append(recursive);
            if (filenameFilter != null) {
              logstr.append(",filter=\"").append(filenameFilter).append("\"");
            }
            logstr.append(")...");
            log.info(logstr);
            return super.preVisitDirectory(dir, attrs);
          } else {
            return FileVisitResult.SKIP_SUBTREE;
          }
        }
  
        @Override
        public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
          log.info("Finished directory \"" + dir + "\".");
          return super.postVisitDirectory(dir, exc);
        }
  
        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
          if (filenameFilter.matcher(file.getFileName().toString()).matches()) {
            try {
              processFile(file);
            } catch (Exception e) {
              throw new RuntimeException(WRAPPED_MARKER, e);
            }
          }
          return super.visitFile(file, attrs);
        }
      });
    } catch (RuntimeException e) {
      if (WRAPPED_MARKER.equals(e.getMessage())) {
        throw (Exception) e.getCause();
      }
      throw e;
    }
  }
  
  @Override
  protected void enumerateValidHarvesterPropertyNames(Set<String> props) {
    super.enumerateValidHarvesterPropertyNames(props);
    props.addAll(Arrays.asList("directory", "recursive", "identifierPrefix", "filenameFilter"));
  }
  
  void processFile(Path file) throws Exception {
    final URI relative = directory.toUri().relativize(file.toUri());
    final String identifier = "file:" + identifierPrefix + relative.toASCIIString();
    addDocument(identifier, Files.getLastModifiedTime(file).toMillis(), new StreamSource(file.toUri().toASCIIString()));
  }
  
}