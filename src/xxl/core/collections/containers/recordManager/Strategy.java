/* XXL: The eXtensible and fleXible Library for data processing

Copyright (C) 2000-2011 Prof. Dr. Bernhard Seeger
                        Head of the Database Research Group
                        Department of Mathematics and Computer Science
                        University of Marburg
                        Germany

This library is free software; you can redistribute it and/or
modify it under the terms of the GNU Lesser General Public
License as published by the Free Software Foundation; either
version 3 of the License, or (at your option) any later version.

This library is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public
License along with this library;  If not, see <http://www.gnu.org/licenses/>. 

    http://code.google.com/p/xxl/

*/

package xxl.core.collections.containers.recordManager;

import java.util.SortedMap;

import xxl.core.collections.containers.recordManager.RecordManager.PageInformation;
import xxl.core.io.Convertable;

/**
 * An interface for the strategies used by the Recordmanager.
 * A strategy gets all calls to get, insert, update and remove methods 
 * from a record manager. This is so, because the strategy has to be informed
 * about changes (for example to update histograms). So, the strategie usually
 * gets a Container object inside the constructor.
 * <p>
 * A Strategy is not allowed to access the paged directly, because
 * the content of the Pages may be incomplete. The RecordManager
 * stores some information inside its internal state.
 * <p>
 * Every Strategy has to be convertable (serialize/deserialize the
 * state of the Strategy).
 */
public interface Strategy extends Convertable {
	/**
	 * Initializes the strategy. This call must be made, before the
	 * first real (other) operation is performed. The call can also
	 * be made multiple times. The PageInformation objects inside
	 * will not be changed until a page is removed (and realocated).
	 * So, a test with == is possible. But: to compare a pageId,
	 * the equals method must be used, because these identifyers must
	 * be regenerated when reread from seondary memory.
	 * @param pages SortedMap with key pageId and value of type PageInformation.
	 * @param pageSize size of each page in bytes.
	 * @param maxObjectSize Size of the largest record which can be stored
	 * 	inside the RecordManager.
	 */
	public void init(SortedMap pages, int pageSize, int maxObjectSize);
	
	/**
	 * Closes the strategy. After closing, the state of the strategy still
	 * has to be convertable.
	 */
	public void close();

	/**
	 * Finds a block with enough free space to hold the given number
	 * of bytes.
	 * @param bytesRequired The free space needed, in bytes.
	 * @return Id of the Page or null, if no such page exists.
	 */
	public Object getPageForRecord(int bytesRequired);

	/**
	 * Informs the strategy, that a new page has been inserted by the RecordManager.
	 * @param pageId identifyer of the page which has been inserted.
	 * @param pi PageInformation for the page.
	 */
	public void pageInserted(Object pageId, PageInformation pi);

	/**
	 * Informs the strategy, that a page has been deleted by the RecordManager.
	 * @param pageId identifyer of the page which has been removed.
	 * @param pi PageInformation for the page.
	 */
	public void pageRemoved(Object pageId, PageInformation pi);

	/**
	 * Informs the strategy, that the RecordManager has performed an update on a certain page.
	 * size==-1 means removal.
	 * @param pageId identifyer of the page where an update has occured.
	 * @param pi PageInformation for the page.
	 * @param recordNumber number of the record which has been changed.
	 * @param recordsAdded number of records which were added.
	 * @param bytesAdded number of added bytes inside the Page (can be negative).
	 * @param linkRecordsAdded number of link records added.
	 */
	public void recordUpdated(Object pageId, PageInformation pi, short recordNumber, 
		int recordsAdded, int bytesAdded, int linkRecordsAdded);
}
