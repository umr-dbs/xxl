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

package xxl.core.xxql;

import java.util.Collection;
import java.util.Iterator;

import xxl.core.cursors.AbstractCursor;
import xxl.core.cursors.Cursor;

/**
 * A Cursor that wraps any {@link Iterable}, like all those standard java {@link Collection} classes
 * like LinkedList etc, into a <b>resettable</b> {@link Cursor}.
 */
public class IterableCursor<E> extends AbstractCursor<E> {
	
	protected Iterable<? extends E> iterable = null;
	protected Iterator<? extends E> currIterator = null;
	
	/**
	 * Creates a resettable {@link Cursor} that iterates over iter's elements.
	 * @param iter {@link Iterable} that provides this cursors elements
	 */
	public IterableCursor(Iterable<? extends E> iter){
		iterable = iter;
		currIterator = iter.iterator();
	}
	
	
	/* (non-Javadoc)
	 * @see xxl.core.cursors.AbstractCursor#hasNextObject()
	 */
	@Override
	protected boolean hasNextObject() {
		return currIterator.hasNext();
	}

	/* (non-Javadoc)
	 * @see xxl.core.cursors.AbstractCursor#nextObject()
	 */
	@Override
	protected E nextObject() {
		return currIterator.next();
	}
	
	@Override
	public void reset() throws UnsupportedOperationException {
		// fetch new iterator, so iteration starts again from the beginning
		currIterator = iterable.iterator();
		super.reset();
	}
	
	@Override
	public boolean supportsReset() {
		return true;
	}

}
