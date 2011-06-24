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

package xxl.core.spatial.geometries.predicates;

import java.util.Arrays;

import xxl.core.predicates.AbstractPredicate;
import xxl.core.predicates.Predicate;

/** A predicate applicable to arrays. This is usefull in cases where
 *  a tuple-candidate already has been built, but needs to be checked
 *  against a refinement-predicate. 
 *  <br>
 *  The invoke-Method simply calls the invoke method of the predicate
 *  given in the constructor:<br>
 *  <code><pre>
 *  public boolean invoke(T[] t){
 *		return predicate.invoke(Arrays.asList(t));
 *	}
 *  </pre></code>
 *  
 * @param <T> the type of the predicate's parameters.
 */
public class TuplePredicate<T> extends AbstractPredicate<T[]>{
	
	/**
	 * The predicate to verify on the tuple-elements
	 */
	protected Predicate<T> predicate;

	/** Constructs a new <code>TuplePredicate</code>. 
	 * 
	 * @param predicate the predicate to verify against the tuple
	 */
	public TuplePredicate(Predicate<T> predicate){
		this.predicate = predicate;
	}

	/** Calls the invoke-method of the underlying predicate with the elements
	 *  of the tuple as parametes.
	 * 
	 * @param t the tuple which the underlying predicate is verified against
	 * @return true if the called predicate returns true, otherwise <code> false</code>
	 */
	public boolean invoke(T[] t){
		return predicate.invoke(Arrays.asList(t));
	}	
}
