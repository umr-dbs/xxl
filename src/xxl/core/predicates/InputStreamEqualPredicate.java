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

import java.io.IOException;
import java.io.InputStream;

/**
 * This class provides a binary predicate that returns <code>true</code> if the
 * input stream given as the first argument is equal to the second.
 */
public class InputStreamEqualPredicate extends AbstractPredicate<InputStream> {

	/**
	 * This instance can be used for getting a default instance of
	 * InputStreamEqualPredicate. It is similar to the <i>Singleton Design
	 * Pattern</i> (for further details see Creational Patterns, Prototype in
	 * <i>Design Patterns: Elements of Reusable Object-Oriented Software</i> by
	 * Erich Gamma, Richard Helm, Ralph Johnson, and John Vlissides) except
	 * that there are no mechanisms to avoid the creation of other instances of
	 * InputStreamEqualPredicate.
	 */
	public static final InputStreamEqualPredicate DEFAULT_INSTANCE = new InputStreamEqualPredicate();

	/**
	 * Creates a new binary predicate which tests InputStreams for equality.
	 */
	public InputStreamEqualPredicate() {
		super();
	}

	/**
	 * Returns the result of the predicate as a primitive boolean value.
	 *
	 * @param s1 the first InputStream.
	 * @param s2 the second InputStream.
	 * @return true iff the InputStreams produce the same bytes.
	 */
 	@Override
	public boolean invoke(InputStream s1, InputStream s2) {
		try{
			int i1,i2;
			
			while (true) {
				i1 = s1.read();
				i2 = s2.read();
				if (i1 == i2) {
					if (i1 == -1) // i2 == -1 !
						return true;
				}
				else
					return false;
			}
		}
		catch (IOException e) {
			return false;
		}
	}
}
