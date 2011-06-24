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
import java.util.Arrays;
import java.util.List;
import java.util.Stack;

import xxl.core.collections.ReversedList;
import xxl.core.collections.containers.Container;
import xxl.core.cursors.Cursors;
import xxl.core.cursors.filters.Taker;
import xxl.core.cursors.mappers.Mapper;
import xxl.core.cursors.sources.Enumerator;
import xxl.core.cursors.sources.Inductor;
import xxl.core.cursors.unions.Sequentializer;
import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;

/** This class implements a {@link BTree} which stores its leaf-nodes on multiple disks.
 */
public class MultiDiskBTree extends BTree {

	/** The containers used in this <tt>MultiDiskBTree</tt>. 
	 */
	protected Container [] leafContainers;

	/** Function returning the container of a leaf if invoked with an <tt>Level1IndexEntry</tt>.  
	 */
	protected Function multiDiskGetContainer = new AbstractFunction() {
		public Object invoke (Object indexEntry) {
			return leafContainers[((Level1IndexEntry)indexEntry).containersIndex];
		}
	};

	/** Function determining which Container to use if invoked with a <tt>SplitInfo</tt>. 
	 */
	protected Function multiDiskDetermineContainer = new AbstractFunction() {
		public Object invoke (Object object) {
			Node.SplitInfo splitInfo = (Node.SplitInfo)object;
			Stack path = splitInfo.path;
			if (path.isEmpty())
				return leafContainers[0];
			else {
				final Tree.IndexEntry indexEntry = indexEntry(path);
				final boolean [] excluded = new boolean[leafContainers.length];
				Function exclude = new AbstractFunction() {
					public Object invoke (Object indexEntry) {
						excluded[((Level1IndexEntry)indexEntry).containersIndex] = true;
						return null;
					}
				};
				Object pathElement = path.pop();
				if (path.isEmpty())
					exclude.invoke(indexEntry);
				else {
					final Node parentNode = (Node)node(path);
					final int index = ((List)parentNode.entries).indexOf(indexEntry)+1;
					for (int i=0; i<2; i++) {
						final int dir = i;
						Cursors.forEach(
								exclude,
							new Taker(
								new Sequentializer(
									new Mapper(
										new AbstractFunction() {
											public Object invoke (Object node) {
												List entryList = (List)((Node)node).entries;
												int from = node==parentNode? index: 0;
												int to = entryList.size();

												return (dir==1?
													entryList:
													new ReversedList(entryList)
												).subList(dir==1? from: to-from, to).iterator();
											}
										}
									 ,new Inductor(
												new AbstractFunction() {
											public Object invoke (Object node) {
												return ((Node)node).siblingsContainers[dir]!=null? Boolean.TRUE: Boolean.FALSE;
											}
										},
										new AbstractFunction() {
											public Object invoke (Object node) {
												return ((Node)node).siblingsContainers[dir].get(((Node)node).siblingsIds[dir]);
											}
										},parentNode
									))
								),
								(leafContainers.length-i)/2
							)
							
						);
					}
				}
				path.push(pathElement);
				return leafContainers[((Integer)Cursors.minima(new Enumerator(leafContainers.length),
					new AbstractFunction() {
						public Object invoke (Object object) {
							int containersIndex = ((Integer)object).intValue();

							return new Integer(
								excluded[containersIndex]?
									Integer.MAX_VALUE:
									leafContainers[containersIndex].size()
							);
						}
					}
				).getFirst()).intValue()];
			}
		}
	};

 /** Initializes the tree.
  * 
	* @param rootEntry the new {@link Tree#rootEntry}
	* @param getDescriptor the new {@link Tree#getDescriptor}
	* @param container the new {@link Tree#determineContainer}
	* @param leafContainers the new {@link #leafContainers}
	* @param minCapacity is used to define {@link Tree#underflows}, {@link Tree#getSplitMinRatio} 
	* 				and {@link Tree#getSplitMaxRatio}
	* @param maxCapacity is used to define {@link Tree#overflows}
	* @return the initialized tree 
	*/
	public MultiDiskBTree initialize (IndexEntry rootEntry, Function getDescriptor, Container container, Container [] leafContainers, final int minCapacity, final int maxCapacity) {
		super.initialize(rootEntry, getDescriptor, container, minCapacity, maxCapacity);
		return initialize(leafContainers, getContainer, determineContainer);
	}

	/** Initializes the tree without adding an entry. 
	 * 
   * @param getDescriptor the new {@link Tree#getDescriptor}
   * @param container the new {@link Tree#determineContainer}
   * @param leafContainers the new {@link #leafContainers}
   * @param minCapacity is used to define {@link Tree#underflows}, {@link Tree#getSplitMinRatio} 
   * 				and {@link Tree#getSplitMaxRatio}
   * @param maxCapacity is used to define {@link Tree#overflows}
   * @return the initialized tree 
   */
	public MultiDiskBTree initialize (Function getDescriptor, Container container, Container [] leafContainers, final int minCapacity, final int maxCapacity) {
		return initialize(null, getDescriptor, container, leafContainers, minCapacity, maxCapacity);
	}

	/** Initializes the tree with containers for non-leaf nodes. 
	 * 
	 * @param leafContainers the new {@link #leafContainers}  
	 * @param superGetContainer the new {@link Tree#getContainer} for level>0
	 * @param superDetermineContainer the new {@link Tree#determineContainer} for level>0
	 * @return the initialized tree 
	 */
	public MultiDiskBTree initialize (Container [] leafContainers, final Function superGetContainer, final Function superDetermineContainer) {
		this.leafContainers = leafContainers;
		this.getContainer =
			new AbstractFunction() {
				public Object invoke (Object indexEntry) {
					return (((IndexEntry)indexEntry).level()>0?
						superGetContainer:
						multiDiskGetContainer)
					.invoke(indexEntry);
				}
			};
		this.determineContainer =
			new AbstractFunction() {
				public Object invoke (Object object) {
					return (((Node.SplitInfo)object).newNode().level>0?
						superDetermineContainer:
						multiDiskDetermineContainer)
					.invoke(object);
				}
			};
		return this;
	}

	/* (non-Javadoc)
	 * @see xxl.core.indexStructures.Tree#createIndexEntry(int)
	 */
	public Tree.IndexEntry createIndexEntry (int parentLevel) {
		return parentLevel>2?
			super.createIndexEntry(parentLevel):
			parentLevel==2?
				(Tree.IndexEntry)new Level2IndexEntry(parentLevel):
				(Tree.IndexEntry)new Level1IndexEntry(parentLevel);
	}

	/** This class describes the normal index entries (i.e. the entries of the non-leaf nodes with level>1) 
	 * of a <tt>MultiDiskBTree</tt>. Each index entry refers to a {@link Tree.Node node} which is the root
	 * of the subtree. We call this node the subnode of the index entry.
	 * 
	 * @see ORTree.IndexEntry
	 * @see MultiDiskBTree.Level1IndexEntry
	 */ 
	public class Level2IndexEntry extends ORTree.IndexEntry {
		
		/** Creates a new <tt>Level2IndexEntry</tt> with a given {@link Tree.IndexEntry#parentLevel parent level}.
		 * 
		 * @param parentLevel the parent level of the new <tt>IndexEntry</tt>
		 */
		public Level2IndexEntry (int parentLevel)  {
			super(parentLevel);
		}

		/* (non-Javadoc)
		 * @see xxl.core.indexStructures.Tree.IndexEntry#initialize(xxl.core.collections.containers.Container, java.lang.Object, xxl.core.indexStructures.Tree.Node.SplitInfo)
		 */
		public Tree.IndexEntry initialize (Container container, Object id, Tree.Node.SplitInfo splitInfo) {
			super.initialize(container, id, splitInfo);
			if (!splitInfo.path.isEmpty()) {
				Node node = (Node)node(splitInfo.path);

				if (node.siblingsContainers[1]!=null) {
					Node siblingsNode = (Node)node.siblingsContainers[1].get(node.siblingsIds[1], false);

					siblingsNode.siblingsContainers[0] = container;
					siblingsNode.siblingsIds[0] = id;
					node.siblingsContainers[1].update(node.siblingsIds[1], siblingsNode);
				}
				node.siblingsContainers[1] = container;
				node.siblingsIds[1] = id;
			}
			return this;
		}
	}

	/** This class describes the index entries for level 1 (i.e. the entries of the non-leaf nodes with level=1) 
	 * of a <tt>MultiDiskBTree</tt>. Each index entry refers to a {@link Tree.Node node} which is the root
	 * of the subtree. We call this node the subnode of the index entry.
	 * 
	 * @see ORTree.IndexEntry
	 * @see MultiDiskBTree.Level2IndexEntry
	 */ 
	public class Level1IndexEntry extends ORTree.IndexEntry {
		
		/** The index of the container containing the Node this indexEntry points at.
		 */
		protected int containersIndex;

		/** Creates a new <tt>Level1IndexEntry</tt> with a given {@link Tree.IndexEntry#parentLevel parent level}.
		 * 
		 * @param parentLevel the parent level of the new <tt>IndexEntry</tt>
		 */
		public Level1IndexEntry (int parentLevel)  {
			super(parentLevel);
		}

		/* (non-Javadoc)
		 * @see xxl.core.indexStructures.Tree.IndexEntry#initialize(xxl.core.collections.containers.Container, java.lang.Object)
		 */
		public Tree.IndexEntry initialize (Container container, Object id) {
			super.initialize(container, id);
			this.containersIndex = Arrays.asList(leafContainers).indexOf(container);
			return this;
		}
	}

	/* (non-Javadoc)
	 * @see xxl.core.indexStructures.Tree#createNode(int)
	 */
	public Tree.Node createNode (int level) {
		return level==1? new Node().initialize(level, new ArrayList()): super.createNode(level);
	}

	/** <tt>Node</tt> is the class used to represent leaf- and non-leaf nodes of <tt>MultiDiskBTree</tt>.
	 *	Nodes are stored in containers.
	 *
	 *	@see Tree.Node
	 *  @see ORTree.Node
	 *  @see MultiDiskBTree.Node
	 */
	public class Node extends BTree.Node {

		/** The containers containing the siblings of this node.
		 */
		protected Container [] siblingsContainers = new Container[2];
		
		/** The ids of the containers containing the siblings of this node.
		 */
		protected Object [] siblingsIds = new Object[2];

		/* (non-Javadoc)
		 * @see xxl.core.indexStructures.Tree.Node#split(java.util.Stack)
		 */
		protected Tree.Node.SplitInfo split (Stack path) {
			Tree.Node.SplitInfo splitInfo = super.split(path);
			Tree.IndexEntry indexEntry = indexEntry(path);
			siblingsContainers[0] = indexEntry.container();
			siblingsIds[0] = indexEntry.id();
			siblingsContainers[1] = siblingsContainers[1];
			siblingsIds[1] = siblingsIds[1];
			return splitInfo;
		}
	}
}
