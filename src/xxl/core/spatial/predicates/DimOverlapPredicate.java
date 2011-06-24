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

package xxl.core.spatial.predicates;

import xxl.core.predicates.AbstractPredicate;
import xxl.core.spatial.rectangles.Rectangle;


/**
 *	A predicate that returns true if two Rectangles overlap
 *	in a user-specified dimension.
 *
 *  @see xxl.core.spatial.rectangles.Rectangle
 *	@see xxl.core.spatial.predicates.OverlapsPredicate
 *	@see xxl.core.spatial.cursors.PlaneSweep
 *	@see xxl.core.predicates.Predicate
 *
 */
public class DimOverlapPredicate extends AbstractPredicate{

	/** Default instance of this Object.
	*/
	public static final DimOverlapPredicate DEFAULT_INSTANCE = new DimOverlapPredicate();

	/** The dimension to consider for the overlap-operation.
	*/
	protected int dim;

	/** Creates a new DimOverlapPredicate-instance.
	 *  @param dim the dimension used to check for overlap
	 */
	public DimOverlapPredicate(int dim){
		this.dim = dim;
	}

	/** Creates a new DimOverlapPredicate-instance (sets dim to 1).
	*/
	public DimOverlapPredicate(){
		this(1);
	}

	/** Returns true if left.overlaps(right) holds.
	 * 
	 * @param left first rectangle
	 * @param right second rectangle
	 * @return returns true if left.overlaps(right) holds
	 * 
	*/
	public boolean invoke(Object left, Object right){
		return ((Rectangle)left).overlaps((Rectangle) right, dim);
	}
}
