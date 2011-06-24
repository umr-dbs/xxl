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

import java.util.SortedMap;

import xxl.core.collections.containers.recordManager.RecordManager.PageInformation;


/** 
 * This class provides a append only strategy. Each record
 * gets its own page if it does not fit into the last record
 * used.
 */
public class AppendOnlyStrategy extends AbstractStrategy {

	/**
	 * Last page which was used. 
	 */
	protected Object lastPageId;

	/**
	 * Information about the last page. 
	 */
	protected PageInformation lastPageInfo;

	/**
	 * Creates a AppendOnlyStrategy object.
	 */
	public AppendOnlyStrategy() {
	}

	/**
	 * Finds a block with enough free space to hold the given number
	 * of bytes.
	 * @param bytesRequired The free space needed, in bytes.
	 * @return Id of the Page or null, if no such page exists.
	 */
	public Object getPageForRecord(int bytesRequired) {
		if (lastPageId!=null && lastPageInfo.bytesFreeAfterPossibleReservation(bytesRequired)>=0)
			return lastPageId;
		else
			return null;
	}

	/**
	 * @see xxl.core.collections.containers.recordManager.Strategy#pageInserted(java.lang.Object, xxl.core.collections.containers.recordManager.RecordManager.PageInformation)
	 */
	public void pageInserted(Object pageId, PageInformation pi) {
		lastPageId = pageId;
		lastPageInfo = pi;
	}

	/**
	 * @see xxl.core.collections.containers.recordManager.Strategy#pageRemoved(java.lang.Object, xxl.core.collections.containers.recordManager.RecordManager.PageInformation)
	 */
	public void pageRemoved(Object pageId, PageInformation pi) {
		if (pageId.equals(lastPageId))
			lastPageId = null;
	}

	/**
	 * @see xxl.core.collections.containers.recordManager.Strategy#init(java.util.SortedMap, int, int)
	 */
	public void init(SortedMap pages, int pageSize, int maxObjectSize) {
		super.init(pages, pageSize, maxObjectSize);
		lastPageId = null;
	}
}
