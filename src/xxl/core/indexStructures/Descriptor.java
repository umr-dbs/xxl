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

package xxl.core.indexStructures;

/** A descriptor describes the region represented by a given
	subtree. The region of each data item of that subtree is
	completely covered by the region of the <tt>Descriptor</tt>.
	This means, that the <tt>Descriptor</tt> may have an arbitrary shape.
	Descriptors in a B-tree are simple integer values, whereas a {@link RTree}
	has descriptors which are {@link xxl.core.spatial.rectangles.Rectangle}.
*/
public interface Descriptor extends Cloneable {

	/** Returns <tt>true</tt> if this <tt>Descriptor</tt> overlaps the specified <tt>Descriptor</tt>.
	 * 
	 * @param descriptor the descriptor to check for overlap with this descriptor
	 * @return <tt>true</tt> if this <tt>Descriptor</tt> overlaps the specified <tt>Descriptor</tt>, 
	 * <tt>false</tt> otherwise
	 */
	public abstract boolean overlaps (Descriptor descriptor);

	/** Returns <tt>true</tt> if this <tt>Descriptor</tt> contains the specified <tt>Descriptor</tt>.
	 * 
	 * @param descriptor the descriptor which is checked if it is contained in this descriptor
	 * @return <tt>true</tt> if this <tt>Descriptor</tt> contains the specified <tt>Descriptor</tt>,
	 * <tt>false</tt> otherwise
	 */
	public abstract boolean contains (Descriptor descriptor);

	/** Unions this descriptor with the specified <tt>Descriptor</tt>.
	 * The Descriptor the method is invoked on will be changed to represent
	 * the union.
	 *  
	 * @param descriptor the descriptor to unite with this descriptor
	 */
	public abstract void union (Descriptor descriptor);

	/** Returns <tt>true</tt> if this <tt>Descriptor</tt> equals <tt>object</tt>.
	 * 
	 * @param object the object to be compared for equality with this decriptor 
	 * @return <tt>true</tt> if this <tt>Descriptor</tt> equals <tt>object</tt>,
	 * <tt>false</tt> otherwise
	 */
	public abstract boolean equals (Object object);	

	/* @see java.lang.Object#clone
	 */	
	public abstract Object clone();
}
