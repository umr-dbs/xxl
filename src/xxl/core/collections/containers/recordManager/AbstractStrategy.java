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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.SortedMap;

import xxl.core.collections.containers.recordManager.RecordManager.PageInformation;
import xxl.core.io.Convertable;

/**
 * Abstract implementation of a Strategy which is useful for most
 * of the implemented strategies. Only the central method getPageForRecord
 * has to be implemented to get a non-abstract class.
 */
public abstract class AbstractStrategy implements Strategy, Convertable {
	/**
	 * SortedMap containing pageIds and PageInformations.
	 */
	protected SortedMap pages;

	/**
	 * Page size of each page in bytes.
	 */
	protected int pageSize;

	/**
	 * Size of the largest record which can be stored inside the 
	 * RecordManager.
	 */
	protected int maxObjectSize;

	/**
	 * Initializes the strategy. This call must be made, before the
	 * first real (other) operation is performed. The call can also
	 * be made multiple times.
	 * @param pages SortedMap with key pageId and value of type PageInformation.
	 * @param pageSize size of each page in bytes.
	 * @param maxObjectSize Size of the largest record which can be stored
	 * 	inside the RecordManager.
	 */
	public void init(SortedMap pages, int pageSize, int maxObjectSize) {
		this.pages = pages;
		this.pageSize = pageSize;
		this.maxObjectSize = maxObjectSize;
	}

	/**
	 * Closes the strategy. After closing, the state of the strategy still
	 * has to be convertable.
	 */
	public void close() {
	}

	/**
	 * Finds a block with enough free space to hold the given number
	 * of bytes.
	 * @param bytesRequired The free space needed, in bytes.
	 * @return Id of the Page or null, if no such page exists.
	 */
	public abstract Object getPageForRecord(int bytesRequired);

	/**
	 * Informs the strategy, that a new page has been inserted by the RecordManager.
	 * @param pageId identifyer of the page which has been inserted.
	 * @param pi PageInformation for the page.
	 */
	public void pageInserted(Object pageId, PageInformation pi) {
	}

	/**
	 * Informs the strategy, that a page has been deleted by the RecordManager.
	 * @param pageId identifyer of the page which has been removed.
	 * @param pi PageInformation for the page.
	 */
	public void pageRemoved(Object pageId, PageInformation pi) {
	}

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
			int recordsAdded, int bytesAdded, int linkRecordsAdded) {
	}

	/**
	 * For most of the strategy, the pages-map is sufficient to 
	 * work. If more state information has to be stored,
	 * then the methods read and write have to be overwritten.
	 * @param dataInput DataInput which is used to reconstruct the
	 *	strategy.
	 * @throws IOException
	 */
	public void read(DataInput dataInput) throws IOException {
	}

	/**
	 * For most of the strategy, the pages-map is sufficient to 
	 * work. If more state information has to be stored,
	 * then the methods read and write have to be overwritten.
	 * @param dataOutput DataOutput which is used to store the
	 *	state of the strategy.
	 * @throws IOException
	 */
	public void write(DataOutput dataOutput) throws IOException {
	}
}
