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

package xxl.core.collections.sweepAreas;

import java.util.Iterator;

import xxl.core.util.Decorator;
import xxl.core.util.memory.MemoryMonitorable;

/**
 * 
 */
public class DecoratorSweepArea<I,E> extends AbstractSweepArea<I,E> implements Decorator<SweepArea<I,E>> {

	/**
	 * The underlying sweeparea. 
	 */
	protected final SweepArea<I,E> sweepArea;	

	public DecoratorSweepArea(SweepArea<I,E> sweepArea, int objectSize) {
		super(objectSize);
		this.sweepArea = sweepArea;
	}

	public DecoratorSweepArea(SweepArea<I,E> sweepArea) {
		this(sweepArea, MemoryMonitorable.SIZE_UNKNOWN);
	}	
	
	public void insert(I o) throws IllegalArgumentException {
		super.insert(o);
		sweepArea.insert(o);
	}
	
	public void clear() {
		sweepArea.clear();
	}
	
	public void close() {
		sweepArea.close();
	}
	
	public int size() {
		return sweepArea.size();
	}
	
	public Iterator<E> iterator() {
		return sweepArea.iterator();
	}
	
	public Iterator<E> query(I o, int ID) throws IllegalArgumentException {
		return sweepArea.query(o, ID);
	}
	
	public Iterator<E> query(I [] os, int [] IDs, int valid) throws IllegalArgumentException {
		return sweepArea.query(os, IDs, valid);
	}

	public Iterator<E> query(I [] os, int [] IDs) throws IllegalArgumentException {
		return sweepArea.query(os, IDs);
	}

	public Iterator<E> expire (I currentStatus, int ID) {
		return sweepArea.expire(currentStatus, ID);
	}
	
	public void reorganize(I currentStatus, int ID) throws IllegalStateException {
		sweepArea.reorganize(currentStatus, ID);
	}
	
	@Override
	public SweepArea<I,E> getDecoree() {
		return sweepArea;
	}

}
