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

/**
 * This interface must be implemented to receive harvester commit events.
 * Each time after the {@link IndexBuilder} commits changes to the Lucene index
 * it calls this interface's {@link #harvesterCommitted} method.
 * <P>To use commit events, the {@link Harvester} class must implement this interface
 * (which is not the default) and call {@link IndexBuilder#registerHarvesterCommitEvent}
 * on startup.
 * @author Uwe Schindler
 */
public interface HarvesterCommitEvent {

	/**
	 * Called with a {@link java.util.Set} of {@link String}s that are the commited document identifiers.
	 * Be warned, you should have some synchronization because this method is called from a different thread!
	 */
	public void harvesterCommitted(java.util.Set<String> docIds);

}