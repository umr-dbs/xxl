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
import java.util.Map;
import java.util.SortedMap;

import xxl.core.collections.containers.recordManager.RecordManager.PageInformation;

/**
 * @author maschn
 *
 * To change the template for this generated type comment go to
 * Window - Preferences - Java - Code Generation - Code and Comments
 */
public class HybridAONFStrategy extends AbstractStrategy {
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

	protected NextFitWithHStrategy nf;
	protected int numberOfBytesInRecords;
	protected int n;
	protected double u;
	protected double v;
	protected LinkedList pageCache;

	public void recalcV() {
		if (pages.size()>0)
			v = ((double) numberOfBytesInRecords)/(pages.size()*pageSize); 
		else
			v = 0.0;
		// System.out.println(v);
	}

	public void insertPageEntry(PageEntry pe) {
		int insertPos = 0;
		Iterator it = pageCache.iterator();
		while (it.hasNext()) {
			PageEntry peTemp = (PageEntry) it.next();
			if (peTemp.compareTo(pe)==-1)
				insertPos++;
			else
				break;
		}
		pageCache.add(insertPos, pe);
		
		if (pageCache.size() > n)
			pageCache.removeLast();
	}

	public void addPage(Object pageId, PageInformation pi) {
		if (pageCache.size()>0) {
			if (((double) pi.numberOfBytesUsedByRecords)/pageSize < u) {
				PageEntry peNew = new PageEntry(pageId, pi);
				if (pageCache.indexOf(peNew)==-1) {
					PageEntry pe = (PageEntry) pageCache.getLast();
					if (pi.numberOfBytesUsedByRecords<pe.pi.numberOfBytesUsedByRecords)
						insertPageEntry(peNew);
						// pageCache.removeLast(); // is done automatically by insert
				}
			}
		}
		else
			pageCache.add(new PageEntry(pageId, pi));
	}

	public Object findPage(int bytesRequired) {
		if (pageCache.size()>0) {
			PageEntry pe = (PageEntry) pageCache.getFirst();
			if (pe.pi.bytesFreeAfterPossibleReservation(bytesRequired)>=0)
				return pe.pageId;
		}
		return null;
	}

	public HybridAONFStrategy(int n, double u) {
		this.u = u;
		this.n = n;
		nf = new NextFitWithHStrategy(n) {
			public Map.Entry testEntry(Map.Entry entry, int bytesRequired) {
				Object pageId = entry.getKey();
				PageInformation pi = (PageInformation) entry.getValue();
				addPage(pageId, pi);
				int bytesFree = pi.bytesFreeAfterPossibleReservation(bytesRequired);
				if (bytesFree>=0)
					return entry;
				else
					return null;
			}
		};
	}

	private int countNF=0, countAO=0;

	public int getN(){
	    return n;
	}
	
	public double getU(){
	    return u;
	}
	
	public Object getPageForRecord(int bytesRequired) {
		if (v>u) {
			countAO++;
			return findPage(bytesRequired);
		}
		else {
			countNF++;
			return nf.getPageForRecord(bytesRequired);
		}
	}
	public void close() {
		nf.close();
		super.close();
	}

	public void init(SortedMap pages, int pageSize, int maxObjectSize) {
		super.init(pages, pageSize, maxObjectSize);
		nf.init(pages, pageSize, maxObjectSize);
		pageCache = new LinkedList();
		
		numberOfBytesInRecords = 0;
		
		Iterator it = pages.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry me = (Map.Entry) it.next();
			PageInformation pi = (PageInformation) me.getValue();
			numberOfBytesInRecords += pi.numberOfBytesUsedByRecords;
		}
		recalcV();
	}
	public void pageInserted(Object pageId, PageInformation pi) {
		nf.pageInserted(pageId, pi);
		recalcV();
	}
	public void pageRemoved(Object pageId, PageInformation pi) {
		nf.pageRemoved(pageId, pi);
		numberOfBytesInRecords -= pi.numberOfBytesUsedByRecords;
		pageCache.remove(new PageEntry(pageId, pi));
		recalcV();
	}
	public void recordUpdated(Object pageId, PageInformation pi, short recordNumber, int recordsAdded, int bytesAdded, int linkRecordsAdded) {
		numberOfBytesInRecords += bytesAdded;
		nf.recordUpdated(pageId, pi, recordNumber, recordsAdded, bytesAdded, linkRecordsAdded);
		
		pageCache.remove(new PageEntry(pageId, pi));
		addPage(pageId, pi);
		
		recalcV();
	}
	public String toString() {
		return "HybridAONFStrategy: NF taken: "+countNF+", AO taken: "+countAO;
	}
}
