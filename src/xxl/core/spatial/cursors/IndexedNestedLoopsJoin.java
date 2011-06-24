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
package xxl.core.spatial.cursors;

import java.util.Iterator;

import xxl.core.cursors.AbstractCursor;
import xxl.core.cursors.Cursor;
import xxl.core.indexStructures.RTree;
import xxl.core.spatial.KPE;
import xxl.core.spatial.rectangles.Rectangle;

/** A simple IndexedNestedLoopsJoin-operator: 
 *  The given {@link RTree}-index processes a window query for each {@link KPE}-object of the input iterator.  
 *  The join returns tuples of the current KPE query object and the leaf-entries of the tree return by the 
 *  window-queries.
 *  
 */
public class IndexedNestedLoopsJoin extends AbstractCursor<KPE[]> {

	/** The input iterator */
	protected Iterator<KPE> input;
	
	/** The index to query */
	protected RTree index;
	
	/** The next element of the iterator */
	protected KPE[] nextResult;
	
	/** The current query-object */
	protected KPE queryObject = null;
	
	/** Delivers the results of the current window-query */
	protected Cursor query;					

	/** The top-level constructor of this class. Creates a new Instance of IndexedNestedLoopsJoin.
	 * 
	 * @param input the first input iterator
	 * @param index the index on the second input
	 */
	public IndexedNestedLoopsJoin( Iterator<KPE> input, RTree index ){ 			
		this.input = input;
		this.index = index;
	}	
	
	@Override
	protected boolean hasNextObject() {
		nextResult = null;								
		while(nextResult==null){
			if( query == null || !query.hasNext()){
				// get next element of input1						
				if(input.hasNext()) queryObject = input.next();
					else break;
				query = index.query((Rectangle) queryObject.getData());
			}						
			nextResult = query.hasNext() ? new KPE[] { queryObject, ((KPE)query.next()) } : null; 
		}
		return nextResult!=null;
	}

	@Override
	protected KPE[] nextObject() {						
		return nextResult;
	}

	/** This method is not supported */
	public void remove() {					
		throw new UnsupportedOperationException();
	}		
}
