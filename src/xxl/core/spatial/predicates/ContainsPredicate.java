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

import xxl.core.indexStructures.Descriptor;
import xxl.core.predicates.AbstractPredicate;

/**
 *	A predicate that returns true if object0 contains object1.
 *
 *  @see xxl.core.indexStructures.Descriptor
 *  @see xxl.core.predicates.Predicate
 *
 */
public class ContainsPredicate extends AbstractPredicate<Descriptor> {

	/** Returns true if object1 is contained in object0.
	 *
	 * The method is implemented as follows:
	 * <pre><code>
	 *
	 *   public boolean invoke(Descriptor desc1, Descriptor desc2){
	 *	   return desc1.contains(desc2);
	 *   }	 
	 * </pre></code>
	 * 
	 * @param desc1 first object
	 * @param desc2 second object
	 * @return returns true if object1 is contained in object0 
	 */
	@Override
	public boolean invoke(Descriptor desc1, Descriptor desc2){
		return desc1.contains(desc2);
	}
}
