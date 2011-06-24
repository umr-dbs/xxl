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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;

import xxl.core.collections.MapEntry;
import xxl.core.collections.containers.recordManager.RecordManager.PageInformation;

/**
 * This class provides a last-to-first-fit-strategy for the record-manager.
 * It inserts a new record into the first block with enough free space it 
 * finds in linear search starting from the end of the record manager going
 * to the beginning.
 * <p>
 * This class does not have a state. So no information is written when
 * calling write.
 * 
 * @see RecordManager
 * @see Strategy
 */
public class LastToFirstFitStrategy extends AbstractStrategy {

	/**
	 * ArrayList for storing the pages a second time. This is
	 * necessary, because no reverse iterator is implemented inside
	 * the Java API.
	 */
	protected ArrayList pageList;

	/**
	 * Creates a new FirstFitStrategy object.
	 */
	public LastToFirstFitStrategy() {
		pageList = null;
	}

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
		super.init(pages, pageSize, maxObjectSize);
		pageList = new ArrayList();
		Iterator it = pages.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry me = (Map.Entry) it.next();
			pageList.add(new MapEntry(me.getKey(), me.getValue()));
		}
	}

	/**
	 * Informs the strategy, that a new page has been inserted by the RecordManager.
	 * @param pageId identifyer of the page which has been inserted.
	 * @param pi PageInformation for the page.
	 */
	public void pageInserted(Object pageId, PageInformation pi) {
		pageList.add(new MapEntry(pageId, pi));
	}

	/**
	 * Informs the strategy, that a page has been deleted by the RecordManager.
	 * @param pageId identifyer of the page which has been removed.
	 * @param pi PageInformation for the page.
	 */
	public void pageRemoved(Object pageId, PageInformation pi) {
		pageList.remove(new MapEntry(pageId, pi));
	}

	/**
	 * Finds a block with enough free space to hold the given number
	 * of bytes.
	 * @param bytesRequired The free space needed, in bytes.
	 * @return Id of the Page or null, if no such page exists.
	 */
	public Object getPageForRecord(int bytesRequired) {
		int size = pageList.size();
		Object pageId=null;
		PageInformation pi;
		
		for (int i = size-1; i>=0; i--) {
			MapEntry me = (MapEntry) pageList.get(i);
			pageId = me.getKey();
			pi = (PageInformation) me.getValue();
			if (pi.bytesFreeAfterPossibleReservation(bytesRequired)>=0)
				return pageId;
		}
		
		return null;
	}
}
