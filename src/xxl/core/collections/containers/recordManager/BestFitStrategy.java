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

import xxl.core.collections.containers.recordManager.RecordManager.PageInformation;

/** 
 * This class provides the best-fit stratey for the record manager.
 * Best-fit searches the container until it finds a block that
 * has just enough space to store the new record. If there is no
 * exact match, then the strategy chooses the block which is closest
 * to the criteria.
 */
public class BestFitStrategy extends AbstractStrategy {

	/**
	 * A best fit is needed space <= free space < needed space + off
	 */
	protected int off;

	/**
	 * Creates a BestFitStrategy object.
	 * @param percentageFree If the algorithm finds a page that would have
	 *	this percentage free after inserting the Record, then the insertion 
	 *	operation is done although there might exist better fitting Pages.
	 *	Normal values are 0.0 or 0.05.
	 */
	public BestFitStrategy(double percentageFree) {
		super();
		off = (int) percentageFree * pageSize;
	}

	/**
	 * Returns the percentage free value with which the Strategy
	 * became initialized.
	 * @return The percentage free value.
	 */
	public double getPercentageFree() {
		return ((double) off)/pageSize;
	}

	/**
	 * Finds a block with enough free space to hold the given number
	 * of bytes.
	 * @param bytesRequired The free space needed, in bytes.
	 * @return Id of the Page or null, if no such page exists.
	 */
	public Object getPageForRecord(int bytesRequired) {
		Map.Entry searchEntry=null;
		Map.Entry bestEntry=null;
		PageInformation pi;
		int bestBytesFree=Integer.MAX_VALUE;

		Iterator it = pages.entrySet().iterator();
		while (it.hasNext()) {
			searchEntry = (Map.Entry) it.next();
			pi = (PageInformation) searchEntry.getValue();
			int bytesFree = pi.bytesFreeAfterPossibleReservation(bytesRequired);
			// does the record fit into the page?
			if (bytesFree>=0) {
				// if it is rather full then insert it
				if (bytesFree<=off)
					return searchEntry.getKey();
				// if it is better than the previous then it is the best
				if (bytesFree<bestBytesFree) {
					bestEntry = searchEntry;
					bestBytesFree = bytesFree;
				}
			}
		}
		if (bestEntry!=null)
			return bestEntry.getKey();
		else
			return null;
	}
}
