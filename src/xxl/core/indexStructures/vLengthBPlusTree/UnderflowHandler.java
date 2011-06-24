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
package xxl.core.indexStructures.vLengthBPlusTree;

import xxl.core.indexStructures.vLengthBPlusTree.VariableLengthBPlusTree.IndexEntry;
import xxl.core.indexStructures.vLengthBPlusTree.VariableLengthBPlusTree.Node;
import xxl.core.indexStructures.vLengthBPlusTree.VariableLengthBPlusTree.Node.MergeInfo;


/**
 * 
 * This is an interface which defines a method for creating own underflow handler for @see {@link VariableLengthBPlusTree}
 *
 */
public interface UnderflowHandler {
	/**
	 * 
	 * @param mergeInfo 
	 * @param indexEntry
	 * @param node
	 * @param siblingIndexEntry
	 * @param siblingNode
	 * @param min
	 * @param average
	 * @param left
	 * @return
	 */
	public Node.MergeInfo runUnderflowHandling(MergeInfo mergeInfo, 
			IndexEntry indexEntry, 
			Node  node, 
			IndexEntry siblingIndexEntry, 
			Node siblingNode, 
			int min, 
			int average, 
			boolean left);
}
