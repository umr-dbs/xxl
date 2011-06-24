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

import java.util.Iterator;
import java.util.LinkedList;
import java.util.SortedMap;

import xxl.core.collections.containers.recordManager.RecordManager.PageInformation;

public class LRUStrategy extends AbstractStrategy {
	/**
	 * Information which is stored for each of the last n
	 * allocated pages. This class implements the method equal
	 * (important for removal from a list).  
	 */
	protected class PageEntry  {
		/**
		 * The identifyer of a page.
		 */
		protected Object pageId;
		/**
		 * The additional information used for a page.
		 */
		protected PageInformation pi;

		/**
		 * Constructs a new entry.
		 * @param pageId The identifyer of a page.
		 * @param pi The additional information used for a page.
		 */
		protected PageEntry(Object pageId, PageInformation pi) {
			this.pageId = pageId;
			this.pi = pi;
		}

		/**
		 * Returns true iff the page identifyers are identical.
		 * @param o Object of type PageEntry with which the current
		 * 	Object is compared.
		 * @return true iff the page identifyers are identical.
		 */
		public boolean equals(Object o) {
			return pageId.equals(((PageEntry) o).pageId);
		}
	}

	/**
	 * Pages with the most free space. 
	 */
	protected LinkedList lastPages;

	/**
	 * Number of pages inside the queue (at most). 
	 */
	protected int n;

	/**
	 * Creates a BestFitOnNEmptiestPagesStrategy object.
	 * @param n Number of pages which are stored inside.
	 */
	public LRUStrategy(int n) {
		this.n = n;
		lastPages = new LinkedList();
	}

	/**
	 * Returns the number of pages inside the queue (at most). 
	 * @return Returns the number of pages inside the queue (at most).
	 */
	public int getN() {
		return n;
	}

	/**
	 * At the time of initialization, the data of the
	 * n emptiest pages are taken. Computation is
	 * done via a heap.
	 * @see xxl.core.collections.containers.recordManager.Strategy#init(java.util.SortedMap, int, int)
	 */
	public void init(SortedMap pages, int pageSize, int maxObjectSize) {
		super.init(pages, pageSize, maxObjectSize);
		
		lastPages.clear();
	}

	/**
	 * Finds a block with enough free space to hold the given number
	 * of bytes.
	 * @param bytesRequired The free space needed, in bytes.
	 * @return Id of the Page or null, if no such page exists.
	 */
	public Object getPageForRecord(int bytesRequired) {
		
		Object pageId=null;
		int bytesFree=Integer.MAX_VALUE;
		
		Iterator it = lastPages.iterator();
		while (it.hasNext()) {
			PageEntry sp = (PageEntry) it.next();
			int currentBytesFree = sp.pi.bytesFreeAfterPossibleReservation(bytesRequired);
			if (currentBytesFree>=0) {
				if (currentBytesFree<bytesFree) {
					pageId = sp.pageId;
					bytesFree = currentBytesFree;
				}
			}
		}
		return pageId;
	}

	/**
	 * @see xxl.core.collections.containers.recordManager.Strategy#pageRemoved(java.lang.Object, xxl.core.collections.containers.recordManager.RecordManager.PageInformation)
	 */
	public void pageRemoved(Object pageId, PageInformation pi) {
		// if it is inside the structures, remove it.
		lastPages.remove(new PageEntry(pageId, pi));
	}

	/**
	 * @see xxl.core.collections.containers.recordManager.Strategy#recordUpdated(java.lang.Object, xxl.core.collections.containers.recordManager.RecordManager.PageInformation, short, int, int, int)
	 */
	public void recordUpdated(Object pageId, PageInformation pi, short recordNumber, 
			int recordsAdded, int bytesAdded, int linkRecordsAdded) {
		
		PageEntry peNew = new PageEntry(pageId, pi);
		lastPages.remove(peNew); // Remove and insert at the beginning!
		lastPages.addLast(peNew);
		if (lastPages.size()>=n) // If there has not been removed a page...
			lastPages.removeFirst();
	}
}
