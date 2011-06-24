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
 * This class provides the first-fit-strategy for the record-manager.
 * Meaning it inserts a new record into the first block with enough free space it 
 * finds in linear search.
 * <p>
 * This class does not have a state. So no information is written when
 * calling write.
 * 
 * @see RecordManager
 * @see Strategy
 */
public class FirstFitStrategy extends AbstractStrategy {
	
	/**
	 * Creates a new FirstFitStrategy object.
	 */
	public FirstFitStrategy() {
	}

	/**
	 * Finds a block with enough free space to hold the given number
	 * of bytes.
	 * @param bytesRequired The free space needed, in bytes.
	 * @return Id of the Page or null, if no such page exists.
	 */
	public Object getPageForRecord(int bytesRequired) {
		Object pageId=null;
		PageInformation pi;
		
		Iterator it = pages.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry entry = (Map.Entry) it.next();
			pi = (PageInformation) entry.getValue();
			if (pi.bytesFreeAfterPossibleReservation(bytesRequired)>=0) {
				pageId = entry.getKey();
				break;
			}
		}
		
		return pageId;
	}
}
