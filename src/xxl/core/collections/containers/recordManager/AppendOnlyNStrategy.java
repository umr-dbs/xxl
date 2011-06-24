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

/**
 * This class provides a append only strategy which searches 
 * inside the last n allocated pages.
 */
public class AppendOnlyNStrategy extends AbstractStrategy {
	/**
	 * Information which is stored for each of the last n
	 * allocated pages. This class implements the method equal
	 * (important for removal from a list).  
	 */
	protected class PageEntry {
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
	 * Last pages which were used. 
	 */
	protected LinkedList lastPages;

	/**
	 * Number of pages inside the queue (at most). 
	 */
	protected int n;

	/**
	 * Creates a AppendOnlyNStrategy object.
	 * @param n Number of pages which are stored inside.
	 */
	public AppendOnlyNStrategy(int n) {
		this.n = n;
	}

	/**
	 * Number of pages inside the queue (at most).
	 * @return Returns the n.
	 */
	public int getN() {
		return n;
	}

	/**
	 * Finds a block with enough free space to hold the given number
	 * of bytes.
	 * @param bytesRequired The free space needed, in bytes.
	 * @return Id of the Page or null, if no such page exists.
	 */
	public Object getPageForRecord(int bytesRequired) {
		Iterator it = lastPages.iterator();
		while (it.hasNext()) {
			PageEntry sp = (PageEntry) it.next();
			if (sp.pi.bytesFreeAfterPossibleReservation(bytesRequired)>=0)
				return sp.pageId;
		}
		return null;
	}

	/**
	 * @see xxl.core.collections.containers.recordManager.Strategy#pageInserted(java.lang.Object, xxl.core.collections.containers.recordManager.RecordManager.PageInformation)
	 */
	public void pageInserted(Object pageId, PageInformation pi) {
		if (lastPages.size()>=n)
			lastPages.removeFirst();
		lastPages.add(new PageEntry(pageId, pi));
	}

	/**
	 * @see xxl.core.collections.containers.recordManager.Strategy#pageRemoved(java.lang.Object, xxl.core.collections.containers.recordManager.RecordManager.PageInformation)
	 */
	public void pageRemoved(Object pageId, PageInformation pi) {
		lastPages.remove(new PageEntry(pageId,pi));
	}

	/**
	 * @see xxl.core.collections.containers.recordManager.Strategy#init(java.util.SortedMap, int, int)
	 */
	public void init(SortedMap pages, int pageSize, int maxObjectSize) {
		super.init(pages, pageSize, maxObjectSize);
		lastPages = new LinkedList();
	}
}
