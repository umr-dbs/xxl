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

/**
 * This abstract class provides a wrapper for a given predicate that provides
 * additional meta data. The meta data of this predicate can be accessed by
 * calling the <code>getMetaData</code> method. Concrete implementations of
 * this predicate must implement the <code>getMetaData</code> for meta data
 * access.
 *
 * @param <P> the type of the predicate's parameters.
 * @param <M> the type of the meta data provided by this predicate.
 * @see DecoratorPredicate
 * @see MetaDataPredicate
 * @see Predicate
 */
public abstract class PredicateMetaDataPredicate<P, M> extends DecoratorPredicate<P> implements MetaDataPredicate<P, M> {

	/**
	 * Creates a new meta data predicate that adds meta data to the specified
	 * predicate.
	 *
	 * @param predicate the predicate that should provide meta data.
	 */
	public PredicateMetaDataPredicate(Predicate<? super P> predicate) {
		super(predicate);
	}

	@Override
	public abstract M getMetaData();
	
}
