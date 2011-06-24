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
public class NextFitWithHWStrategy extends NextFitWithHStrategy {
	/**
	 * Field where witnesses for each bucket are stored. 
	 */
	protected Object[] witness;
	
	/**
	 * Statistical information.
	 */
	protected int numberWitnessesUsed;

	/**
	 * Creates a new NextFitStrategy Object.
	 * @param numberOfBuckets Number of buckets for the histogram.
	 */
	public NextFitWithHWStrategy(int numberOfBuckets) {
		super(numberOfBuckets);
		numberWitnessesUsed = 0;
	}

	/**
	 * @see xxl.core.collections.containers.recordManager.Strategy#init(java.util.SortedMap, int, int)
	 */
	public void init(SortedMap pages, int pageSize, int maxObjectSize) {
		super.init(pages, pageSize, maxObjectSize);
		witness = new Object[numberOfBuckets];
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
		if (witness[bucketNumber]==null)
			witness[bucketNumber] = pageId;
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
		if (witness[bucketNumber]!=null && witness[bucketNumber].equals(pageId))
			witness[bucketNumber] = null;
	}

	/**
	 * Checks the consistency of the current histogram with the
	 * real values. If the values does not match, then an Exception is
	 * thrown.
	 */
	protected void checkConsistency() {
		super.checkConsistency();
		for (int i=0; i<numberOfBuckets; i++) {
			if (witness[i]!=null) {
				if (histogram[i]<=0) 
					throw new RuntimeException("Histogram witness cannot exist");
				PageInformation pi = (PageInformation) pages.get(witness[i]);
				int realBucket = getHistogramClassForPage(pi.bytesFreeAfterPossibleReservation(0)); 
				if (i!=realBucket)
					throw new RuntimeException("Histogram witness is not in correct class "+realBucket);
			}
		}
	}

	/**
	 * Tests an entry if there is enough space for a reservation.
	 * @param entry The entry to be tested.
	 * @param bytesRequired bytes needed for the Record.
	 * @return the entry if a reservation is possible else null.
	 */
	public Map.Entry testEntry(Map.Entry entry, int bytesRequired) {
		Map.Entry me = super.testEntry(entry, bytesRequired);
		
		// only use this as witness, if the entry will not hold
		// the wanted record.
		if (me==null) {
			PageInformation pi = (PageInformation) entry.getValue();
			int bucket = getHistogramClassForPage(pi.bytesFreeAfterPossibleReservation(0)); 
			if (witness[bucket]==null)
				witness[bucket] = entry.getKey();
		}
		return me;
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
		
		for (int i=minClass; i<numberOfBuckets; i++) {
			if (witness[i]!=null) {
				numberWitnessesUsed++;
				return new MapEntry(witness[i], null);
			}
		}
		
		for (int i=minClass; i<numberOfBuckets; i++)
			if (histogram[i]>0)
				return null;
		
		return new MapEntry(null, null);
	}

	/**
	 * Outputs the state of the Strategy.
	 * @return The String representation of the state of the Strategy.
	 */
	public String toString() {
		String s = "NextFitWithHWStrategy, Number of witnesses used: "+numberWitnessesUsed+", Number of full Searches: "+numberOfFullSearches+", currentPage: ";

		if (lastEntry==null)
			return s+"null";
		else
			return s+lastEntry.getKey();
	}
}
