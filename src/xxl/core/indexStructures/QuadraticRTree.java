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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Stack;

import xxl.core.cursors.Cursors;
import xxl.core.cursors.filters.Filter;
import xxl.core.cursors.mappers.Mapper;
import xxl.core.cursors.sources.Enumerator;
import xxl.core.cursors.unions.Sequentializer;
import xxl.core.functions.AbstractFunction;
import xxl.core.predicates.AbstractPredicate;
import xxl.core.spatial.rectangles.DoublePointRectangle;
import xxl.core.spatial.rectangles.Rectangle;

/** An <tt>RTree</tt> implementing the quadratic split-strategy proposed
 * in the original R-Tree paper.  
 * 
 * For a detailed discussion see Guttman, A.: "R-trees: a dynamic index structure
 * for spatial searching", Proc. ACM-SIGMOD Int. Conf. on Management of Data, 47-57, 1984.
 * 
 * @see Tree
 * @see ORTree
 * @see RTree
 */
public class QuadraticRTree extends RTree {
	
	/* (non-Javadoc)
	 * @see xxl.core.indexStructures.Tree#createNode(int)
	 */
	public Tree.Node createNode (int level) {
		return new Node().initialize(level, new LinkedList());
	}

	/** A modification of {@link RTree.Node node} implementing the quadratic split-algorithm.
	 *
	 * @see RTree.Node 
	 */
	public class Node extends RTree.Node {
		
		/* (non-Javadoc)
		 * @see xxl.core.indexStructures.Tree.Node#split(java.util.Stack)
		 */
		protected Tree.Node.SplitInfo split (final Stack path) {
			final Node node = (Node)node(path);
			int number = node.number(), minNumber = node.splitMinNumber(), maxNumber = node.splitMaxNumber();
			Iterator seeds = new Sequentializer(
				new Mapper(
					new AbstractFunction() {
						int index = 0;

						public Object invoke (final Object entry1) {
							return new Mapper(
								new AbstractFunction() {
									public Object invoke (Object entry2) {
										return new Object[] {entry1, entry2};
									}
								}
							,((List)node.entries).listIterator(++index));
						}
					}
				,node.entries())
			);
			final Object [] seed = (Object[])Cursors.maxima(seeds,
				new AbstractFunction() {
					public Object invoke (Object seed) {
						Rectangle rectangle0 = rectangle(((Object[])seed)[0]);
						Rectangle rectangle1 = rectangle(((Object[])seed)[1]);

						return new Double(Descriptors.union(rectangle0, rectangle1).area()
							-rectangle0.area()-rectangle1.area());
					}
				}
			).getFirst();
			final Rectangle [] nodesMBRs = new Rectangle[] {new DoublePointRectangle(rectangle(seed[0])), new DoublePointRectangle(rectangle(seed[1]))};
			final Collection [] nodesEntries = new Collection[] {node.entries, this.entries};
			final List remainingEntries = Cursors.toList(
				new Filter(node.entries(),
					new AbstractPredicate() {
						public boolean invoke (Object entry) {
							return entry!=seed[0] && entry!=seed[1];
						}
					}
				),
				new ArrayList(number-2)
			);

			node.entries.clear();
			for (int i=0; i<2; nodesEntries[i].add(seed[i++]));
			while (!remainingEntries.isEmpty() && node.number()!=maxNumber && number-number()!=minNumber) {
				int entryIndex = ((Integer)Cursors.maxima(new Enumerator(remainingEntries.size()),
					new AbstractFunction() {
						public Object invoke (Object object) {
							Rectangle rectangle = rectangle(remainingEntries.get(((Integer)object).intValue()));

							return new Double(Math.abs(
								(Descriptors.union(nodesMBRs[1], rectangle).area()-nodesMBRs[1].area())-
								(Descriptors.union(nodesMBRs[0], rectangle).area()-nodesMBRs[0].area())
							));
						}
					}
				).getFirst()).intValue();
				Object entry = remainingEntries.set(entryIndex, remainingEntries.get(remainingEntries.size()-1));
				Rectangle rectangle = rectangle(entry);
				double areaEnlargementDifference, areaDifference;
				int index = (areaEnlargementDifference =
						(Descriptors.union(nodesMBRs[1], rectangle).area()-nodesMBRs[1].area())-
						(Descriptors.union(nodesMBRs[0], rectangle).area()-nodesMBRs[0].area())
					)<0 ||
					areaEnlargementDifference==0 && (
						(areaDifference = nodesMBRs[1].area()-nodesMBRs[0].area())<0 ||
						areaDifference==0 && number()<node.number()
					)? 1: 0;

				nodesEntries[index].add(entry);
				nodesMBRs[index].union(rectangle);
				remainingEntries.remove(remainingEntries.size()-1);
			}
			if (!remainingEntries.isEmpty()) {
				int index = node.number()==maxNumber? 1: 0;

				for (Iterator entries = remainingEntries.iterator(); entries.hasNext();) {
					Object entry = entries.next();

					nodesEntries[index].add(entry);
					nodesMBRs[index].union(rectangle(entry));
				}
			}
			((IndexEntry)indexEntry(path)).descriptor = nodesMBRs[0];
			return new SplitInfo(path).initialize(nodesMBRs[1]);
		}
	}
}
