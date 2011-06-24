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

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Stack;

import xxl.core.cursors.Cursors;
import xxl.core.cursors.mappers.Mapper;
import xxl.core.cursors.sources.Enumerator;
import xxl.core.functions.AbstractFunction;
import xxl.core.spatial.rectangles.DoublePointRectangle;
import xxl.core.spatial.rectangles.Rectangle;

/** An <tt>RTree</tt> implementing the linear split-strategy proposed
 * in the original R-Tree paper.  
 * 
 * For a detailed discussion see Guttman, A.: "R-trees: a dynamic index structure
 * for spatial searching", Proc. ACM-SIGMOD Int. Conf. on Management of Data, 47-57, 1984.
 * 
 * @see Tree
 * @see ORTree
 * @see RTree
 */
public class LinearRTree extends RTree {
	
	/* (non-Javadoc)
	 * @see xxl.core.indexStructures.Tree#createNode(int)
	 */
	public Tree.Node createNode (int level) {
		return new Node().initialize(level, new LinkedList());
	}

	/** A modification of {@link RTree.Node node} implementing the linear split-algorithm.
	 *
	 * @see RTree.Node 
	 */
	public class Node extends RTree.Node {

		/** 
		 *	Performs an RTree split with complexity O(d*n).
		 *	First, the dimension is chosen by maximizing a normalized
		 *	separation distance that is computed for the rectangles
		 *	that have the greatest distance in a dimension.
		 *	Then, for the split dimension, the two most separated rectangles 
		 *	are the seeds for the distribution algorithm.
		 *	Then, each rectangle (one after the other) is added to the partition so
		 *	that the area enlargement of the MBRs of the partitions is minimized.
		 *	Furthermore, the algorithm has to make a distribution so that 
		 *	the number of objects in the new nodes is between the minimal and maximal
		 *	number.
		 *
		 * @param path the nodes already visited during this insert
		 * @return a <tt>SplitInfo</tt> containig all information needed about the split
		 */
		protected Tree.Node.SplitInfo split (final Stack path) {
			final Node node = (Node)node(path);
			final int dimensions = ((Rectangle)rootDescriptor()).dimensions();
			int number = node.number(), minNumber = node.splitMinNumber(), maxNumber = node.splitMaxNumber();
			Iterator seeds = new Mapper(
				new AbstractFunction() {
					Rectangle MBR = rectangle(indexEntry(path));

					public Object invoke (Object object) {
						final int dimension = ((Integer)object).intValue();
						Object e1 = Cursors.minima(node.entries(),
							new AbstractFunction() {
								public Object invoke (Object object) {
									return new Double(rectangle(object).getCorner(true).getValue(dimension));
								}
							}
						).getFirst();
						Object e2 = Cursors.maxima(node.entries(),
							new AbstractFunction() {
								public Object invoke (Object object) {
									return new Double(rectangle(object).getCorner(false).getValue(dimension));
								}
							}
						).getLast();
						Double normalizedSeparation = new Double(
							(rectangle(e2).getCorner(false).getValue(dimension)-rectangle(e1).getCorner(true).getValue(dimension))/
							(MBR.getCorner(true).getValue(dimension)-MBR.getCorner(false).getValue(dimension))
						);

						return new Object [] {normalizedSeparation, e1, e2};
					}
				}
			,new Enumerator(dimensions));
			Object [] seed = (Object[])Cursors.maxima(seeds,
				new AbstractFunction() {
					public Object invoke (Object seed) {
						return ((Object[])seed)[0];
					}
				}
			).getFirst();
			Rectangle oldNodesMBR = new DoublePointRectangle(rectangle(seed[1])), newNodesMBR = new DoublePointRectangle(rectangle(seed[2]));
			
			this.entries.add(seed[2]); // this is definitely in this partition!			
			// the following line has to be here, because if entries.remove is called
			// during iteration, it must be considered the case when seed[2] is the
			// last object in the iteration (not very nice for the if-condition).
			node.entries.remove(seed[2]); // object is already in the node's collection
			
			int distrNumber=number-2; // nodes that still have to be distributed
			double areaEnlargementDifference, areaDifference;
			Rectangle rectangle;
			Object entry;
			
			for (Iterator entries = node.entries(); entries.hasNext();) {
				entry = entries.next();

				if (entry!=seed[1]) {
					// now, entry is not one of the seed objects
					rectangle = rectangle(entry);

					if (
						(node.number()>minNumber) && // still enough objects in the node?
						(
							node.number()==maxNumber || 
							distrNumber+number()<=minNumber ||
							(
								number-number()!=minNumber && (
									(areaEnlargementDifference =
										(Descriptors.union(newNodesMBR, rectangle).area()-newNodesMBR.area())-
										(Descriptors.union(oldNodesMBR, rectangle).area()-oldNodesMBR.area())
									)<0 ||
									areaEnlargementDifference==0 && (
										(areaDifference = newNodesMBR.area()-oldNodesMBR.area())<0 ||
										areaDifference==0 && number()<node.number()
									)
								)
							)
						)
					) {
						// putting objects into the new partition
						this.entries.add(entry);
						// removing it from the old one
						entries.remove();
						// compute the new descriptor
						newNodesMBR.union(rectangle);
					}
					else
						// compute the new descriptor
						oldNodesMBR.union(rectangle);
					// element was distributed
					distrNumber--;
				}
			}
			((IndexEntry)indexEntry(path)).descriptor = oldNodesMBR;
			return new SplitInfo(path).initialize(newNodesMBR);
		}
	}
}
