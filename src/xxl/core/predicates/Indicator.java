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

package xxl.core.predicates;

import java.util.Collection;

/**
 * Implements an indicator for a given collection. An indicator is an unary
 * predicate that returns <code>true</code> exactly if the argument is
 * contained in the collection associated with the predicate.
 * 
 * @param <P> the type of the predicate's parameters.
 */
public class Indicator<P> extends AbstractPredicate<P> {

	/**
	 * The collection for which the indicator should be created.
	 */
	protected Collection<? super P> collection;
	
	/**
	 * Creates a new Indicator predicate.
	 * 
	 * @param collection the collection for which the indicator should be
	 *        created.
	 */
	public Indicator(Collection<? super P> collection){
		this.collection = collection;
	}

	/** Returns true iff the collection contains the object.
	 * 
	 * @param o object to be checked.
	 * @return returns true iff the collection contains the object.
	 */
	@Override
	public boolean invoke(P o) {
		return collection.contains(o);
	}
}
