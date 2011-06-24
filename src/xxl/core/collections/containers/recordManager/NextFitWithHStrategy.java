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
import java.util.Map;
import java.util.SortedMap;

import xxl.core.collections.MapEntry;
import xxl.core.collections.containers.recordManager.RecordManager.PageInformation;

/** 
 * This class provides a next-fit strategy for the record manager.
 * A histogram is used to store the number of pages in each bucket,
 * thus saving one lookup in the index when the serach for
 * a page with enough free space is unsuccesfull.
 */
public class NextFitWithHStrategy extends AbstractStrategy {
	/**
	 * Stores the last entry where a reservation has been made.
	 */
	protected Map.Entry lastEntry;

	/**
	 * Histogram held inside.
	 * <pre>
	 * Example with 5 buckets: (percent of free space)
	 * bucket 0: [0.0,0.25)
	 * bucket 1: [0.25,0.5)
	 * bucket 2: [0.5,0.75)
	 * bucket 3: [0,75,1.0)
	 * bucket 4: [1.0,1.0]
	 * </pre>
	 */
	protected int[] histogram;

	/**
	 * Number of buckets inside the histogram.
	 */
	protected int numberOfBuckets;

	/**
	 * Number of full searches which were necessary.
	 */
	protected int numberOfFullSearches;

	/**
	 * Returns the numberOfBuckets.
	 * @return Returns the numberOfBuckets.
	 */
	public int getNumberOfBuckets() {
		return numberOfBuckets;
	}

	/**
	 * Creates a new NextFitStrategy Object.
	 * @param numberOfBuckets Number of buckets for the histogram.
	 */
	public NextFitWithHStrategy(int numberOfBuckets) {
		this.numberOfBuckets = numberOfBuckets;
		numberOfFullSearches = 0;
	}

	/**
	 * Determines the bucket, into which a page falls.
	 * @param bytesFree Bytes free inside the page for a new
	 * 	record.
	 * @return The bucket number.
	 */
	protected final int getHistogramClassForPage(int bytesFree) {
		return ((numberOfBuckets-1)*bytesFree)/maxObjectSize;
	}

	/**
	 * Gets the minimal bucket from which an appropriate record
	 * can be taken.
	 * @param bytesNeeded Bytes needed for the current record.
	 * @return The bucket number.
	 */
	protected final int getHistogramClass(int bytesNeeded) {
		return 1 + ((numberOfBuckets-1)*(bytesNeeded-1))/maxObjectSize;
	}

	/**
	 * @see xxl.core.collections.containers.recordManager.Strategy#init(java.util.SortedMap, int, int)
	 */
	public void init(SortedMap pages, int pageSize, int maxObjectSize) {
		super.init(pages, pageSize, maxObjectSize);
		histogram = calculateHistogram();
		lastEntry = null;
	}

	/**
	 * Adds a new entry to the histogram. The entry has a given
	 * page identifyer.
	 * @param pageId Identifyer of the page, for which a histogram entry
	 * 	is added.
	 * @param bucketNumber Number of the bucket of the histogram where
	 * 	the entry is added.
	 */
	protected void addHistogramEntry(Object pageId, int bucketNumber) {
		histogram[bucketNumber]++;
	}

	/**
	 * Removes an entry from the histogram. The entry has a given
	 * page identifyer.
	 * @param pageId Identifyer of the page, for which the histogram entry
	 * 	is removed.
	 * @param bucketNumber Number of the bucket of the histogram where
	 * 	the entry is removed.
	 */
	protected void removeHistogramEntry(Object pageId, int bucketNumber) {
		histogram[bucketNumber]--;
	}

	/**
	 * Recalculates the whole histogram from the pages and
	 * returns the array (does not overwrite the old histogram).
	 * The methods addHistogramEntry and removeHistogramEntry 
	 * are not used here!
	 * @return The calculated histogram.
	 */
	protected int[] calculateHistogram() {
		int h[] = new int[numberOfBuckets];
		
		Iterator it = pages.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry me = (Map.Entry) it.next();
			PageInformation pi = (PageInformation) me.getValue();
			h[getHistogramClassForPage(pi.bytesFreeAfterPossibleReservation(0))]++;
		}
		return h;
	}

	/**
	 * Checks the consistency of the current histogram with the
	 * real values. If the values does not match, then an Exception is
	 * thrown.
	 */
	protected void checkConsistency() {
		int h[] = calculateHistogram();
		for (int i=0; i<numberOfBuckets; i++) {
			if (h[i]!=histogram[i])
				throw new RuntimeException("Histogram not adequate in class "+i+" (is: "+h[i]+", should: "+histogram[i]+")");
		}
	}

	/**
	 * Tests an entry if there is enough space for a reservation.
	 * @param entry The entry to be tested.
	 * @param bytesRequired bytes needed for the Record.
	 * @return the entry if a reservation is possible else null.
	 */
	public Map.Entry testEntry(Map.Entry entry, int bytesRequired) {
		PageInformation pi = (PageInformation) entry.getValue();
		int bytesFree = pi.bytesFreeAfterPossibleReservation(bytesRequired);
		if (bytesFree>=0)
			return entry;
		else
			return null;
	}

	/**
	 * Tests a whole iterator of entries if there is enough space for a 
	 * reservation.
	 * @param it Iterator with entries.
	 * @param bytesRequired bytes needed for the Record.
	 * @return the entry if a reservation is possible else null.
	 */
	private Map.Entry findPlaceInIterator(Iterator it, int bytesRequired) {
		Map.Entry searchEntry;
		while (it.hasNext()) {
			searchEntry = testEntry((Map.Entry) it.next(),bytesRequired);
			if (searchEntry!=null)
				return searchEntry;
		}
		return null;
	}

	/**
	 * Consults the histogram and returns an entry which
	 * can hold the record. If there is an existing but
	 * unknown page where the record will fit the method
	 * will return null. If the method can say that 
	 * there will be no such page, then a MapEntry with 
	 * (null, null) is returned. 
	 * @param bytesRequired Number of bytes required for the
	 * 	current record.
	 * @return A valid Map.Entry, null or MapEntry(null, null).
	 */
	protected Map.Entry consultHistogram(int bytesRequired) {
		int minClass = getHistogramClass(bytesRequired);
		for (int i=minClass; i<numberOfBuckets; i++)
			if (histogram[i]>0)
				return null;
		return new MapEntry(null, null);
	}

	/**
	 * Finds a block with enough free space to hold the given number
	 * of bytes.
	 * @param bytesRequired The free space needed, in bytes.
	 * @return Id of the Page or null, if no such page exists.
	 */
	public Object getPageForRecord(int bytesRequired) {
		Map.Entry searchEntry=null;
		
		if (lastEntry!=null)
			searchEntry = testEntry(lastEntry, bytesRequired);
		
		if (searchEntry==null)
			searchEntry = consultHistogram(bytesRequired);
		
		if (searchEntry==null) {
			numberOfFullSearches++;
			if (lastEntry==null)
				searchEntry = findPlaceInIterator(pages.entrySet().iterator(),bytesRequired);
			else {
				searchEntry = findPlaceInIterator(
					pages.tailMap(lastEntry.getKey()).entrySet().iterator(),
					bytesRequired);
				if (searchEntry==null) {
					searchEntry = findPlaceInIterator(
						pages.headMap(lastEntry.getKey()).entrySet().iterator(),
						bytesRequired);
				}
			}
		}
		
		if (searchEntry==null)
			throw new RuntimeException("Full search was unsuccessful");

		if (searchEntry.getValue()!=null)
			lastEntry = searchEntry;
		return searchEntry.getKey();
	}

	/**
	 * @see xxl.core.collections.containers.recordManager.Strategy#recordUpdated(java.lang.Object, xxl.core.collections.containers.recordManager.RecordManager.PageInformation, short, int, int, int)
	 */
	public void recordUpdated(Object pageId, PageInformation pi, short recordNumber, 
			int recordsAdded, int bytesAdded, int linkRecordsAdded) {
		
		int newClass = getHistogramClassForPage(
			pi.bytesFreeAfterPossibleReservation(0)
		);
		
		// Page.getSize(pageSize,pi.numberOfRecords+pi.numberOfLinkRecords, pi.numberOfBytesUsedByRecords)
		int oldClass = getHistogramClassForPage(
			pi.bytesFreeAfterPossibleReservation(
				pi.numberOfRecords-recordsAdded,
				pi.numberOfLinkRecords-linkRecordsAdded,
				pi.numberOfBytesUsedByRecords-bytesAdded,
				0
			)
		);
		
		if (oldClass!=newClass) {
			removeHistogramEntry(pageId, oldClass);
			addHistogramEntry(pageId, newClass);
			
			if (histogram[oldClass]<0)
				throw new RuntimeException("The statistic was wrong");
		}
		// checkConsistency();
	}

	/**
	 * @see xxl.core.collections.containers.recordManager.Strategy#pageInserted(java.lang.Object, xxl.core.collections.containers.recordManager.RecordManager.PageInformation)
	 */
	public void pageInserted(Object pageId, PageInformation pi) {
		addHistogramEntry(pageId, numberOfBuckets-1);
	}

	/**
	 * Informs the strategy, that a page has been deleted by the RecordManager.
	 * @param pageId identifyer of the page which has been removed.
	 * @param pi PageInformation for the page.
	 */
	public void pageRemoved(Object pageId, PageInformation pi) {
		if (lastEntry!=null && pageId.equals(lastEntry.getKey()))
			lastEntry=null;
		removeHistogramEntry(pageId, numberOfBuckets-1);
	}

	/**
	 * Outputs the state of the Strategy.
	 * @return The String representation of the state of the Strategy.
	 */
	public String toString() {
		String s = "NextFitWithHStrategy, Number of full Searches: "+numberOfFullSearches+", currentPage: ";

		if (lastEntry==null)
			return s+"null";
		else
			return s+lastEntry.getKey();
	}
}
