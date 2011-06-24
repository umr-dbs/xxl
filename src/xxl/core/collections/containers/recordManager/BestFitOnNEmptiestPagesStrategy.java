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
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;

import xxl.core.collections.containers.recordManager.RecordManager.PageInformation;
import xxl.core.collections.queues.Heap;

/**
 * This class provides a strategy which makes a best fit
 * search inside pages which have the most free space.
 */
public class BestFitOnNEmptiestPagesStrategy extends AbstractStrategy {
	/**
	 * Information which is stored for each of the last n
	 * allocated pages. This class implements the method equal
	 * (important for removal from a list).  
	 */
	protected class PageEntry implements Comparable {
		/**
		 * The identifyer of a page.
		 */
		protected Object pageId;
		/**
		 * The additional information used for a page.
		 */
		protected PageInformation pi;
		/**
		 * Number of bytes used inside the page.
		 */
		protected int bytesUsed;

		/**
		 * Constructs a new entry.
		 * @param pageId The identifyer of a page.
		 * @param pi The additional information used for a page.
		 */
		protected PageEntry(Object pageId, PageInformation pi) {
			this.pageId = pageId;
			this.pi = pi;
			bytesUsed = pi.numberOfBytesUsedByRecords;
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

		/**
		 * @see java.lang.Comparable#compareTo(java.lang.Object)
		 */
		public int compareTo(Object o) {
			int bytesUsed2 = ((PageEntry)o).bytesUsed;
			return bytesUsed<bytesUsed2?-1:(bytesUsed==bytesUsed2?0:+1);
		}
	}

	/**
	 * Pages with the most free space. 
	 */
	protected ArrayList lastPages;

	/**
	 * Number of pages inside the queue (at most). 
	 */
	protected int n;
	
	/**
	 * Value which says how many bytes are used inside a stored
	 * page at most.
	 */
	protected int maxBytesUsed;
	
	/**
	 * Entry of the list, where the maximum bytes
	 * are used (this entry can be replaced next).
	 */
	protected PageEntry maxBytesUsedEntry;

	/**
	 * Entry of the list, where the minimum bytes
	 * are used (if a new object does not fit into
	 * this page, then a new one has to be allocated).
	 */
	protected PageEntry minBytesUsedEntry;

	/**
	 * Creates a BestFitOnNEmptiestPagesStrategy object.
	 * @param n Number of pages which are stored inside.
	 */
	public BestFitOnNEmptiestPagesStrategy(int n) {
		this.n = n;
		lastPages = new ArrayList();
		maxBytesUsed = -1;
		minBytesUsedEntry = null;
		maxBytesUsedEntry = null;
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
		
		Comparator comparator = new Comparator() {
			public int compare(Object o1, Object o2) {
				Map.Entry me1 = (Map.Entry) o1;
				Map.Entry me2 = (Map.Entry) o2;
				
				int free1 = ((PageInformation) me1.getValue()).numberOfBytesUsedByRecords;
				int free2 = ((PageInformation) me2.getValue()).numberOfBytesUsedByRecords;
				return free1<free2?+1:free1==free2?0:-1;
			}
		};
		
		Heap heap = new Heap(n, comparator);
		Iterator it = pages.entrySet().iterator();
		int i=0;
		while (it.hasNext() && i<n) {
			Map.Entry me = (Map.Entry) it.next();
			heap.enqueue(me);
			i++;
		}
		
		while (it.hasNext()) {
			Map.Entry me = (Map.Entry) it.next();
			
			if (comparator.compare(me, heap.peek())>0) {
				heap.dequeue();
				heap.enqueue(me);
			}
		}
		
		Map.Entry me;
		PageEntry pe;
		lastPages.clear();
		while (!heap.isEmpty()) {
			me = (Map.Entry) heap.dequeue();
			pe = new PageEntry(me.getKey(), (PageInformation) me.getValue());
			lastPages.add(pe);
		}
		
		if (lastPages.size()>=1) {
			maxBytesUsedEntry = (PageEntry) lastPages.get(0);
			minBytesUsedEntry = (PageEntry) lastPages.get(lastPages.size()-1);
		}
		else {
			minBytesUsedEntry = null;
			maxBytesUsedEntry = null;
		}
	}

	/**
	 * Finds a block with enough free space to hold the given number
	 * of bytes.
	 * @param bytesRequired The free space needed, in bytes.
	 * @return Id of the Page or null, if no such page exists.
	 */
	public Object getPageForRecord(int bytesRequired) {
		if (minBytesUsedEntry==null)
			return null;
		if (minBytesUsedEntry.pi.bytesFreeAfterPossibleReservation(bytesRequired)<0)
			return null;
		
		// There is a nice page for the object...
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
		if (pageId==null)
			throw new RuntimeException("There should have been a page (bytes required="+bytesRequired+")");
		return pageId;
	}

	/**
	 * Recalculates min and max completely.
	 */
	protected void recalculateStatistic() {
		maxBytesUsed = -1;
		minBytesUsedEntry = null;
		maxBytesUsedEntry = null;
		
		if (lastPages.size()>0) {
			minBytesUsedEntry = (PageEntry) lastPages.get(0);
			maxBytesUsedEntry = minBytesUsedEntry;
			maxBytesUsed = maxBytesUsedEntry.bytesUsed;
			
			for (int i=1; i<lastPages.size(); i++) {
				PageEntry pe = (PageEntry) lastPages.get(i);
				if (pe.bytesUsed>maxBytesUsed) {
					maxBytesUsed = pe.bytesUsed;
					maxBytesUsedEntry = pe;
				}
				if (pe.bytesUsed<minBytesUsedEntry.bytesUsed)
					minBytesUsedEntry = pe;
			}
		}
	}

	/**
	 * Updates the min/max statistic internally.
	 * @param pe The updated PageEntry (must be an element from the list).
	 */
	protected void updateStatistic(PageEntry pe) {
		if (pe==minBytesUsedEntry || pe==maxBytesUsedEntry)
			recalculateStatistic();
		else {
			if (pe.bytesUsed>maxBytesUsed) {
				maxBytesUsed = pe.bytesUsed;
				maxBytesUsedEntry = pe;
			}
			if (minBytesUsedEntry==null || pe.bytesUsed<minBytesUsedEntry.bytesUsed)
				minBytesUsedEntry = pe;
		}
	}

	/**
	 * Removes a page entry and adds the current page entry if
	 * the new entry has more free space than the page with the
	 * least free space inside the list.
	 * @param peNew The new PageEntry which may be included into
	 * 	the list of relatively empty pages.
	 */
	protected void changeEntryIfNecessary(PageEntry peNew) {
		if (peNew.bytesUsed<maxBytesUsed) {
			// search the PageEntry with the most bytes used
			// (least free space). This entry is removed.
			lastPages.remove(maxBytesUsedEntry);
			// add the current entry
			lastPages.add(peNew);
			
			recalculateStatistic();
		}
	}

	/**
	 * @see xxl.core.collections.containers.recordManager.Strategy#pageRemoved(java.lang.Object, xxl.core.collections.containers.recordManager.RecordManager.PageInformation)
	 */
	public void pageRemoved(Object pageId, PageInformation pi) {
		// if it is inside the structures, remove it.
		lastPages.remove(new PageEntry(pageId, pi));
		if (pageId.equals(minBytesUsedEntry.pageId) || pageId.equals(maxBytesUsedEntry.pageId))
			recalculateStatistic();
	}

	/**
	 * @see xxl.core.collections.containers.recordManager.Strategy#recordUpdated(java.lang.Object, xxl.core.collections.containers.recordManager.RecordManager.PageInformation, short, int, int, int)
	 */
	public void recordUpdated(Object pageId, PageInformation pi, short recordNumber, 
			int recordsAdded, int bytesAdded, int linkRecordsAdded) {
		PageEntry peNew = new PageEntry(pageId, pi);
		int index = lastPages.indexOf(peNew);
		if (index>-1) {
			PageEntry pe = (PageEntry) lastPages.get(index);
			pe.bytesUsed = pi.numberOfBytesUsedByRecords;
			updateStatistic(pe);
		}
		else {
			if (lastPages.size()>=n)
				changeEntryIfNecessary(peNew);
			else {
				lastPages.add(peNew);
				updateStatistic(peNew);
			}
		}
	}
}

/*
with heap ... dynamic is needed for changing the size of the page
and removal.

public void pageInserted(Object pageId, PageInformation pi) {
	// The page cannot be inside the structures so far,
	// because it is a new page.
	if (lastPages.size()>=n) {
		Object minObject = lastPages.get(0);
		
		for (int i=0; i<addList.size(); i++)
			minHeap.enqueue(addList.get(i));
		
		PageEntry pe = (PageEntry) minHeap.peek();
		if (pe.bytesUsed>pi.numberOfBytesUsedByRecords) {
			PageEntry peNew = new PageEntry(pageId, pi);
			minHeap.dequeue();
			lastPages.remove(pe);
			minHeap.enqueue(peNew);
			lastPages.add(peNew);
		}
	}
	else {
		PageEntry peNew = new PageEntry(pageId, pi);
		minHeap.enqueue(peNew);
		lastPages.add(peNew);
	}
}

public void pageRemoved(Object pageId, PageInformation pi) {
	PageEntry peRemoved = new PageEntry(pageId, pi);
	
	// if it is inside the structures, remove it.
	lastPages.remove(peRemoved);
		// Rebuild the heap...
		List addList = new ArrayList();
		while (true) {
			PageEntry pe = (PageEntry) minHeap.dequeue();
			if (pe.pageId==pageId)
				break;
			else
				addList.add(pe);
		}
		for (int i=0; i<addList.size(); i++)
			minHeap.enqueue(addList.get(i));
	}
}
*/
