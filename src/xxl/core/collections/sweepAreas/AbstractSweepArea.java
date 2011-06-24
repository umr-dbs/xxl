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

import xxl.core.functions.AbstractFunction;
import xxl.core.predicates.Equal;
import xxl.core.predicates.Predicate;
import xxl.core.util.XXLSystem;
import xxl.core.util.memory.MemoryMonitorable;
import xxl.core.util.metaData.AbstractMetaDataManagement;
import xxl.core.util.metaData.CompositeMetaData;
import xxl.core.util.metaData.MetaDataManagement;

public abstract class AbstractSweepArea<I,E> implements SweepArea<I,E> {

	/**
	 * A flag, signing that reflection must be used to determine the objectsize.
	 */
	protected int objectSize;
	
	/**
	 * The object size of elements in the sweeparea.
	 */
	protected boolean checkObjectSize;
	
	protected MetaDataManagement<Object,Object> metaDataManagement;
	
	
	public class AbstractSAMetaDataManagement extends AbstractMetaDataManagement<Object,Object> {

		// AVAILABLE METADATA
		public static final String OBJECT_SIZE = "OBJECT_SIZE";
		public static final String SIZE = "SIZE";
		public static final String MEMORY_USAGE = "MEMORY_USAGE";
				
		public AbstractSAMetaDataManagement() {
			super();
		}
		
		@Override
		protected boolean addMetaData(Object metaDataIdentifier) {
			if (metaDataIdentifier.equals(OBJECT_SIZE)) {
				metaData.add(metaDataIdentifier, new AbstractFunction<Object,Integer>() {					
					@Override
					public Integer invoke() {
						return getObjectSize();	
					}
				});
				return true;
			}
			if (metaDataIdentifier.equals(SIZE)) {
				metaData.add(metaDataIdentifier, new AbstractFunction<Object,Integer>() {					
					@Override
					public Integer invoke() {
						return size();
					}
				});
				return true;
			}
			if (metaDataIdentifier.equals(MEMORY_USAGE)) {
				metaData.add(metaDataIdentifier, new AbstractFunction<Object,Integer>() {					
					@Override
					public Integer invoke() {
						return getCurrentMemUsage();						
					}
				});
				return true;
			}
			return false;
		}

		@Override
		protected boolean removeMetaData(Object metaDataIdentifier) {			
			if (metaDataIdentifier.equals(OBJECT_SIZE) || 
				metaDataIdentifier.equals(SIZE) ||
				metaDataIdentifier.equals(MEMORY_USAGE)	) {
					metaData.remove(metaDataIdentifier);
					return true;
			}
			return false;
		}
				
	}
	
	protected Predicate<? super E> equals;

	public AbstractSweepArea(Predicate<? super E> equals, int objectSize) {
		this.equals = equals;
		this.objectSize = objectSize;
		this.checkObjectSize = objectSize == MemoryMonitorable.SIZE_UNKNOWN;
		createMetaDataManagement();
	}
	
	public AbstractSweepArea(Predicate<? super E> equals) {
		this(equals, SIZE_UNKNOWN);
	}

	public AbstractSweepArea(int objectSize) {
		this(new Equal<E>(), objectSize);
	}
	
	public AbstractSweepArea() {
		this(SIZE_UNKNOWN);
	}
	
	/* (non-Javadoc)
	 * Checks if the object size has been specified. Otherwise it is determined
	 * via reflection.
	 * @see xxl.core.collections.sweepAreas.SweepArea#insert(java.lang.Object)
	 */
	public void insert(I o) throws IllegalArgumentException {
		if (checkObjectSize)
			computeObjectSize(o);
	}

	/* (non-Javadoc)
	 * @see xxl.core.collections.sweepAreas.SweepArea#clear()
	 */
	public abstract void clear();

	/* (non-Javadoc)
	 * @see xxl.core.collections.sweepAreas.SweepArea#close()
	 */
	public abstract void close();

	/* (non-Javadoc)
	 * @see xxl.core.collections.sweepAreas.SweepArea#size()
	 */
	public abstract int size();

	/* (non-Javadoc)
	 * @see xxl.core.collections.sweepAreas.SweepArea#iterator()
	 */
	public abstract Iterator<E> iterator();

	/* (non-Javadoc)
	 * @see xxl.core.collections.sweepAreas.SweepArea#query(java.lang.Object, int)
	 */
	public abstract Iterator<E> query(I o, int ID) throws IllegalArgumentException;

	/* (non-Javadoc)
	 * @see xxl.core.collections.sweepAreas.SweepArea#query(java.lang.Object[], int[], int)
	 */
	public abstract Iterator<E> query(I[] os, int[] IDs, int valid) throws IllegalArgumentException;
	
	/* (non-Javadoc)
	 * @see xxl.core.collections.sweepAreas.SweepArea#query(java.lang.Object[], int[])
	 */
	public abstract Iterator<E> query(I[] os, int[] IDs) throws IllegalArgumentException;

	/* (non-Javadoc)
	 * @see xxl.core.collections.sweepAreas.SweepArea#expire(java.lang.Object, int)
	 */
	public abstract Iterator<E> expire(I currentStatus, int ID) throws UnsupportedOperationException, IllegalStateException;
	
	/* (non-Javadoc)
	 * @see xxl.core.collections.sweepAreas.SweepArea#reorganize(java.lang.Object, int)
	 */
	public abstract void reorganize(I currentStatus, int ID) throws UnsupportedOperationException, IllegalStateException;
	
	/* (non-Javadoc)
	 * @see xxl.core.pipes.memoryManager.MemoryMonitorable#getCurrentMemUsage()
	 */
	public int getCurrentMemUsage() {
		if ( objectSize != SIZE_UNKNOWN )
			return objectSize * size();
		return 0;
	}
	
	public int getObjectSize() {
		return objectSize;
	}
	
	protected void computeObjectSize(Object o) {		
		try {
			objectSize = XXLSystem.getObjectSize(o);
		} catch (IllegalAccessException e) {
			objectSize = MemoryMonitorable.SIZE_UNKNOWN;
		}
		checkObjectSize = false;		
	}
	
	public void createMetaDataManagement() {
		if (metaDataManagement != null)
			throw new IllegalStateException("An instance of MetaDataManagement already exists.");
		metaDataManagement = new AbstractSAMetaDataManagement();
	}
	
	public MetaDataManagement<Object,Object> getMetaDataManagement() {
		return metaDataManagement;
	}
	
	public CompositeMetaData<Object,Object> getMetaData() {
		return metaDataManagement.getMetaData();
	}
	
}
