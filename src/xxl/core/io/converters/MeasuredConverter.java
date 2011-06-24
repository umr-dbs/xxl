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

package xxl.core.io.converters;

/**
 * This is a converter which can convert objects with a known maximal size.
 * That is in many cases very useful (for example
 * {@link xxl.core.collections.containers.io.BlockFileContainer}). For example
 * the class {@link xxl.core.indexStructures.BPlusTree} uses normaly a 
 * {@link xxl.core.collections.containers.io.BlockFileContainer} to store its
 * nodes. I/O operations are executed by composite converters. To determine the
 * node size it is necessary to know the maximal size of the data objects. For
 * this purpose a measured converter is practical.
 * 
 * @param <T> the type to be converted.
 */
public abstract class MeasuredConverter<T> extends Converter<T> {
	
	/**
	 * Determines the maximal size of the objects for which this converter is
	 * used. In the case of an integer converter the result will be 4 bytes.
	 *  
	 * @return the maximal size of the objects for which this converter is
	 *         used.
	 */
	public abstract int getMaxObjectSize();
}
