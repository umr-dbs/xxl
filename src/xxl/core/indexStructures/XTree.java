
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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Stack;

import xxl.core.collections.ReversedList;
import xxl.core.collections.containers.Container;
import xxl.core.comparators.ComparableComparator;
import xxl.core.comparators.FeatureComparator;
import xxl.core.cursors.Cursors;
import xxl.core.cursors.filters.Filter;
import xxl.core.cursors.identities.TeeCursor;
import xxl.core.cursors.mappers.Mapper;
import xxl.core.cursors.sources.ArrayCursor;
import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Constant;
import xxl.core.functions.Function;
import xxl.core.io.converters.ConvertableConverter;
import xxl.core.io.converters.Converter;
import xxl.core.io.converters.IntegerConverter;
import xxl.core.io.converters.LongConverter;
import xxl.core.io.converters.ShortConverter;
import xxl.core.predicates.AbstractPredicate;
import xxl.core.spatial.rectangles.DoublePointRectangle;
import xxl.core.spatial.rectangles.Rectangle;
import xxl.core.util.BitSet;


/** The <tt>XTree</tt> is a variant of the {@link RTree RTree} for
 * high dimensional data which avoids splits which would result in
 * high overlap.
 * 
 * For a detailed discussion see 
 * Stefan Berchtold, Daniel A. Keim and Hans-Peter Kriegel:
 * "The {X}-Tree: An Index Structure for High-Dimensional Data",
 * VLDB (1996), 28-39.
 * 
 * @see Tree
 * @see ORTree
 * @see RTree
 */
public class XTree extends RTree {

	/** Threshold overlap value. Explained in detail in the article about the X-Tree.
	 */
	protected double maxOverlap = 0.2;
	
	/** Minimum fanout. Explained in detail in the article about the X-Tree.
	 */
	protected double minFanout = 0.2;
	
	/** the number of nodes in the tree
	 */
	protected int numOfNode = 0;

	/** the number of dimensions
	 */
	protected int numberOfDimensions;


	/** Initializes a created XTree.
	 * 
	 * @param rootEntry the new {@link Tree#rootEntry}
	 * @param rootDescriptor the new {@link Tree#rootDescriptor}
	 * @param getDescriptor the new {@link Tree#getDescriptor}
	 * @param container container used for as constant for {@link Tree#getContainer}
	 * and {@link Tree#determineContainer}
	 * @param minCapacity is used to define {@link Tree#underflows}, {@link Tree#getSplitMinRatio} 
	 * 				and {@link Tree#getSplitMaxRatio}.
	 * @param maxCapacity is used to define {@link Tree#underflows} and {@link Tree#overflows}
	 * @param maxOverlap the new {@link XTree#maxOverlap}
	 * @param minFanout the new {@link XTree#minFanout}
	 * @param numberOfDimensions {@link XTree#numberOfDimensions}
	 * @return the initialized <tt>XTree</tt>
	 */
	public XTree initialize (IndexEntry rootEntry, Descriptor rootDescriptor, Function getDescriptor, Container container, final int minCapacity, final int maxCapacity,double maxOverlap,double minFanout,int numberOfDimensions) {
		this.numberOfDimensions = numberOfDimensions;
		this.maxOverlap = maxOverlap;
		this.minFanout  = minFanout;
		return (XTree)super.initialize(
			rootEntry,
			rootDescriptor,
			getDescriptor,
			new Constant(container),
			new Constant(container),
			new AbstractPredicate() {
				public boolean invoke (Object node) {
					return ((Tree.Node)node).number()<(maxCapacity*(((XTree.Node)node).getNodeSize()-1)+minCapacity);
				}
			},
			new AbstractPredicate() {
				public boolean invoke (Object node) {
					return ((Tree.Node)node).number()>(maxCapacity*((XTree.Node)node).getNodeSize());
				}
			},
			new AbstractFunction() {
				public Object invoke (Object node) {
					return new Double(minCapacity*((XTree.Node)node).getNodeSize()/(double)((Tree.Node)node).number());
				}
			},
			new AbstractFunction() {
				public Object invoke (Object node) {
					return new Double(1.0-minCapacity*((XTree.Node)node).getNodeSize()/(double)((Tree.Node)node).number());
				}
			}
		);
	}

	/** Initializes a created XTree qithoit root entry.
	 * 
	 * @param getDescriptor the new {@link Tree#getDescriptor}
	 * @param container container used for as constant for {@link Tree#getContainer}
	 * and {@link Tree#determineContainer}
	 * @param minCapacity is used to define {@link Tree#underflows}, {@link Tree#getSplitMinRatio} 
	 * 				and {@link Tree#getSplitMaxRatio}.
	 * @param maxCapacity is used to define {@link Tree#underflows} and {@link Tree#overflows}
	 * @param maxOverlap the new {@link XTree#maxOverlap}
	 * @param minFanout the new {@link XTree#minFanout}
	 * @param numberOfDimensions {@link XTree#numberOfDimensions}
	 * @return the initialized <tt>XTree</tt>
	 */
	public Tree initialize (Function getDescriptor, Container container, int minCapacity, int maxCapacity, double maxOverlap, double minFanout, int numberOfDimensions) {
		 return this.initialize (null,null,getDescriptor,container,minCapacity,maxCapacity,maxOverlap,minFanout,numberOfDimensions);
	}

	/** Initializes a created XTree without root entry and default values for
	 * XTree parameters.
	 * 
	 * @param getDescriptor the new {@link Tree#getDescriptor}
	 * @param container container used for as constant for {@link Tree#getContainer}
	 * and {@link Tree#determineContainer}
	 * @param minCapacity is used to define {@link Tree#underflows}, {@link Tree#getSplitMinRatio} 
	 * 				and {@link Tree#getSplitMaxRatio}.
	 * @param maxCapacity is used to define {@link Tree#underflows} and {@link Tree#overflows}
	 * @param numberOfDimensions {@link XTree#numberOfDimensions}
	 * @return the initialized <tt>XTree</tt>
	 */
	public Tree initialize (Function getDescriptor, Container container, final int minCapacity, final int maxCapacity, int numberOfDimensions) {
		return this.initialize (null,null,getDescriptor,container,minCapacity,maxCapacity,this.maxOverlap,this.minFanout,numberOfDimensions);
	}

	/* (non-Javadoc)
	 * @see xxl.core.indexStructures.Tree#createNode(int)
	 */
	public Tree.Node createNode(int level){
		return new Node().initialize(level, new LinkedList(),1);
	}

	/* (non-Javadoc)
	 * @see xxl.core.indexStructures.Tree#createIndexEntry(int)
	 */
	public Tree.IndexEntry createIndexEntry (int parentLevel) {
		return new IndexEntry(parentLevel);
	}

	/** Counts the number of normal directory nodes per level in the X-tree.
	 * 
	 * @param noOfDNodePerLevel array with a counter for each level
	 * @param bufferSize array containing the buffersize at index 0
	 */
	public void getNoOfDNode(int[] noOfDNodePerLevel,int[] bufferSize){
		((XTree.Node)rootEntry().get()).getNoOfDNode(noOfDNodePerLevel,bufferSize);
	}

	/** Counts the number of supernodes per level in the X-tree.
	 * 
	 * @param noOfSDNodePerLevel array with a counter for each level
	 * @param bufferSize array containing the buffersize at index 0
	 */
	public void getNoOfSDNode(int[] noOfSDNodePerLevel,int[] bufferSize){
		((XTree.Node)rootEntry().get()).getNoOfSDNode(noOfSDNodePerLevel,bufferSize);
	}


	/** This class describes the index entries (i.e. the entries of the non-leaf nodes) 
	 * of a x-tree. Each index entry refers to a {@link Tree.Node node} which is the root
	 * of the subtree. We call this node the subnode of the index entry.
	 * 
	 * @see ORTree.IndexEntry
	 */ 
	public class IndexEntry extends ORTree.IndexEntry {
		
		/** The split history of the son.
		 */
		protected BitSet splitHistory = new BitSet(numberOfDimensions);

		/** Creates a new <tt>IndexEntry</tt> with a given {@link Tree.IndexEntry#parentLevel parent level}.
		 * @param parentLevel the parent level of the new <tt>IndexEntry</tt>
		 */
		public IndexEntry(int parentLevel) {
			super(parentLevel);
		}

		/** This method is used to initialize a new <tt>IndexEntry</tt> with
		 * a split-history.
		 * 
		 * @param splitHistory the new {@link XTree.IndexEntry#splitHistory}
		 * @return the <tt>IndexEntry</tt>, i.e. <tt>this</tt>
		 */
		public IndexEntry initialize(BitSet splitHistory) {
			this.splitHistory = splitHistory;
			return this;
		}

		/** Returns the split-history of the node.
		 * 
		 * @return the split-history of the node
		 */
		public BitSet getHistory() {
			return splitHistory;
		}

		/** Sets the split-history of the node
		 * 
		 * @param splitHistory the new {@link XTree.IndexEntry#splitHistory}
		 */
		public void setHistory(BitSet splitHistory) {
			this.splitHistory = splitHistory;
		}

		/** Updates the split-history of this node by setting the bit at dimension 
		 * <tt>dim</tt>.
		 * 
		 * @param dim the dimension at which the split-history is updated
		 */
		public void updateHistory(int dim) {
			BitSet set = new BitSet(numberOfDimensions);
			set.set(dim);
			updateHistory(set);
		}

		/** Updates the split-history of this node by setting all bits set in
		 * <tt>bitSet</tt>. 
		 * 
		 * @param bitSet the bits to set in the split-history
		 */
		public void updateHistory(BitSet bitSet) {
			splitHistory.or(bitSet);
		}
	} //IndexEntry

	/** <tt>Node</tt> is the class used to represent leaf- and non-leaf 
	 *  nodes of <tt>XTree</tt>. Nodes are stored in containers.
	 * 
	 *	@see Tree.Node
	 *  @see ORTree.Node
	 *  @see RTree.Node
	 */
	public class Node extends RTree.Node {

		/* (non-Javadoc)
		 * @see xxl.core.indexStructures.ORTree.Node#chooseSubtree(xxl.core.indexStructures.Descriptor, java.util.Iterator)
		 */
		protected ORTree.IndexEntry chooseSubtree (Descriptor descriptor, Iterator minima) {
			final Rectangle dataRectangle = (Rectangle)descriptor;
			TeeCursor validEntries = new TeeCursor(minima);
			minima = new Filter(validEntries,
				new AbstractPredicate() {
					public boolean invoke (Object object) {
						return rectangle(object).contains(dataRectangle);
					}
				}
			);
			if (!minima.hasNext()) {
				minima = Cursors.minima(validEntries.cursor(),
					new AbstractFunction() {
						public Object invoke (Object object) {
							Rectangle oldRectangle = rectangle(object);
							Rectangle newRectangle = Descriptors.union(oldRectangle, dataRectangle);

							return new Double(newRectangle.area()-oldRectangle.area());
						}
					}
				).iterator();
			}
			minima = Cursors.minima(minima,
				new AbstractFunction() {
					public Object invoke (Object object) {
						return new Double(rectangle(object).area());
					}
				}
			).iterator();
			validEntries.close();
			return (IndexEntry)minima.next();
		}

		
		/** SplitInfo contains information about a split. The enclosing
		 * Object of this SplitInfo-Object (i.e. Node.this) is the new node
		 * that was created by the split.
		 */		
		public class SplitInfo extends RTree.Node.SplitInfo {

			/** Flag indicating if the split succeeded.
			 */
			protected boolean splitSuccess;
			
			/** Creates a new <tt>SplitInfo</tt> with a given path.
			 * @param path the path from the root to the splitted node
			 */
			public SplitInfo (Stack path) {
				super(path);
			}
			
			/** Initializes a new <tt>SplitInfo</tt> by setting <tt>splitSeccess</tt>.
			 * @param splitSuccess the new {@link XTree.Node.SplitInfo#splitSuccess}
			 * @return the initialized <tt>Splitinfo</tt>, i.e. <tt>this</tt>
			 */
			public SplitInfo initialize (boolean splitSuccess) {
				this.splitSuccess = splitSuccess;
				return this;
			}
			
			/* (non-Javadoc)
			 * @see xxl.core.indexStructures.RTree.Node.SplitInfo#initialize(xxl.core.indexStructures.RTree.Node.Distribution)
			 */
			public ORTree.Node.SplitInfo initialize( RTree.Node.Distribution distribution) {
				return super.initialize(distribution);
			}
			
			/** Returns if the split succeeded.
			 * @return <tt>true</tt> if the split succeeded
			 */
			public boolean isSucceeded() {
				return splitSuccess;
			}
			
			/** Sets the <tt>splitSuccess</tt> flag. 
			 * @param splitSuccess the new {@link XTree.Node.SplitInfo#splitSuccess}
			 */
			public void setSuccess(boolean splitSuccess) {
				this.splitSuccess = splitSuccess;
			}
		}

		/** The size if this node. If nodeSize>1, this is a supernode. 
		 */
		protected int nodeSize;

		/** Initializes a created node by setting its level, entries and size.
		 * 
		 * @param level the node's level
		 * @param entries the node's entries
		 * @param nodeSize the size of the node
		 * @return the initialized node
		 */
		public Node initialize(int level, Collection entries, int nodeSize) {
			super.initialize(level,entries);
			this.nodeSize = nodeSize;
			return this;
		}
		
		/** Gets the size of the node. 
		 * 
		 * @return the size of the node
		 */
		public int getNodeSize() {
		 	return nodeSize;
		}
		
		/** Sets the size of the node 
		 * 
		 * @param nodeSize the new {@link XTree.Node#nodeSize}
		 */
		public void setNodeSize(int nodeSize) {
			this.nodeSize = nodeSize;
		}

		/** Adapts {@link XTree.Node#nodeSize} to a suitable value by
		 * checking for under- and overflows.
		 */
		public void adaptNodeSize() {
			while (underflows()) {
				if (getNodeSize()==1)
					break;
				setNodeSize(getNodeSize()-1);
			}
			while (overflows())
				setNodeSize(getNodeSize()+1);
		}

		/** Adapts {@link XTree.Node#nodeSize} by setting it to <tt>mult</tt>.
		 * 
		 * @param mult the new {@link XTree.Node#nodeSize}
		 */
		public void adaptNodeSize(int mult) {
			setNodeSize(mult);
		}

		/** Counts the number of normal directory nodes per level.
		 * 
		 * @param noOfDNodePerLevel array with a counter for each level
		 * @param bufferSize array containing the buffersize at index 0
		 */
		public void getNoOfDNode(int[] noOfDNodePerLevel, int[] bufferSize) {
			if (getNodeSize()==1)
				noOfDNodePerLevel[level]++;
			if (level>1) {
				Object [] entryArray = entries.toArray();
				for (int i=0; i<entryArray.length && bufferSize[0]>0; i++) {
					(((IndexEntry)entryArray[i]).container()).get(((IndexEntry)entryArray[i]).id(),false);
					bufferSize[0]--;
				}
				for (int i=0; i<entryArray.length; i++) {
					((Node)((IndexEntry)entryArray[i]).get()).getNoOfDNode(noOfDNodePerLevel,bufferSize);
				}
			}
		}

		/** Counts the number of supernodes per level.
		 * 
		 * @param noOfSDNodePerLevel array with a counter for each level
		 * @param bufferSize array containing the buffersize at index 0
		 */
		public void getNoOfSDNode(int[] noOfSDNodePerLevel, int[] bufferSize) {
			if (getNodeSize()>1)
				noOfSDNodePerLevel[level]++;
			if (level>1) {
				Object [] entryArray = entries.toArray();
				for (int i =0; i<entryArray.length && bufferSize[0]>0; i++) {
					if (((Node)((IndexEntry)entryArray[i]).get()).getNodeSize()>1) {
						(((IndexEntry)entryArray[i]).container()).get(((IndexEntry)entryArray[i]).id(),false);
						bufferSize[0]--;
					}
				}
				for (int i =0; i<entryArray.length; i++) {
					((Node)((IndexEntry)entryArray[i]).get()).getNoOfSDNode(noOfSDNodePerLevel,bufferSize);
				}
			}
		}

		/** Treats overflows which occur during an insertion. 
		 * It tries to split the overflowing node in new nodes. If a split occurs 
		 * it creates new index-entries for the new Nodes. These 
		 * index-entries will be added to the given <tt>List</tt>. 
		 * The method {@link Tree.Node#post(Tree.Node.SplitInfo, Tree.IndexEntry)} 
		 * will be used to post the created index-entries to
		 * the parent Node. If no split occurs the node is added to a supernode.
		 *  
		 * @param path the path from the root to the overflowing node
		 * @param parentNode the parent node of the overflowing node
		 * @param newIndexEntries a <tt>List</tt> to carry the new index-entries created by the split
		 * @return splitinfo for the split 
		 */
		protected Tree.Node.SplitInfo redressOverflow (Stack path, Tree.Node parentNode, List newIndexEntries) {
			IndexEntry indexEntry = (IndexEntry)indexEntry(path);
			SplitInfo splitInfo = (SplitInfo)createNode(level).split(path);
			if(splitInfo.isSucceeded()){
				int dim = splitInfo.distribution().getDim();
				IndexEntry newIndexEntry =(IndexEntry) createIndexEntry(parentNode.level).initialize(splitInfo);
				Node newNode = (Node)splitInfo.newNode();
				// update histories of both IndexEntries
				//old node
				indexEntry.updateHistory(dim);
				//newNode
				newIndexEntry.updateHistory(indexEntry.getHistory());
				Container container = splitInfo.determineContainer();
				// adds new newIndexentry in entries f parentNode
				parentNode.post(splitInfo, newIndexEntry);
				if(level !=0){
					// old node
					if (overflows()){
						adaptNodeSize();
						indexEntry.update(this);
					}
					if (underflows()){
						adaptNodeSize();
						indexEntry.update(this);
					}
					//new node
					if (newNode.overflows())
						newNode.adaptNodeSize();
				}
				//new Node is inserted in container
				((Tree.IndexEntry)newIndexEntry).initialize(container, container.insert(newNode));
				numOfNode++;
				newIndexEntries.add(newIndexEntry);
			}//endif
			else{
				//extend old node to supernode,note: this method (redressover..) is called from old node
				// here implicit that level !=0, because for level = 0 topospilt aways OK
				adaptNodeSize();
				// update old node in Container
				indexEntry.update(this);
			}
			return splitInfo;
		}
		
		/** Split the node in the case of an overflow. First, the split method
		 * of the r*-tree is tried. If this produces too much overlap, the
		 * overlap minimal split is used instead. If this produces an unbalanced
		 * node, a supernode is created and no split is performed. 
		 * 
		 * @param path the nodes already visited during this insert
		 * @return a SplitInfo containig all needed information about the split
		 */
		protected Tree.Node.SplitInfo split (Stack path) {

			// node = node before split ,   this = new node)
			final Node node = (Node)node(path);
			// try topological split, i.e. use split method of R-Tree
			// Note : minEntries and maxEntries in RTree.split( Stack path) method are constants ?!

			Tree.Node.SplitInfo splitInfoTopo = topologicalsplit(path);
			RTree.Node.Distribution distribution = ((RTree.Node.SplitInfo)splitInfoTopo).distribution();
			// only change the type of SplitInfo
			SplitInfo splitInfo =new SplitInfo(path);
			splitInfo.initialize(distribution);
			// if node is not data node,  test further, else return, i.e. only use the topological split(RTree split)
			if (level() != 0) {
				double overlap = distribution.overlapValue();
				double area = distribution.areaValue();
				double relOverlap = overlap/(area-overlap);
				if (relOverlap > maxOverlap) {

					// topological split fails -> try overlap minimal split
					// but first we have to merge both new node and old node
					// it is because of the design of split of RTree and we have to use it !!
					// if we can change the end of the method split in RTree then we do not need to merge the nodes
					//i.e one must not .....
					node.entries.clear();
					node.entries.addAll(Arrays.asList(distribution.entries));
					(((IndexEntry)indexEntry(path)).descriptor).union(distribution.descriptor(true));

					// try overlap minimal split
					splitInfo = this.overlapminimalsplit(path);

					// if the split has not been successful, then use the topological
					// split from above instead.
					if (!splitInfo.isSucceeded())
						return splitInfo;

					// test for unbalanced nodes
					double minF = ((node.splitMinNumber()+node.splitMaxNumber()-1)/node.getNodeSize())*minFanout;
					Node.Distribution distr = splitInfo.distribution();
					overlap = distr.overlapValue();
					area =distr.areaValue();
					if (distr.entries(true).size()<minF||distr.entries(false).size()<minF||overlap/(area-overlap) > maxOverlap) {
						//overvalminimalsplit is not succesfull
						//extend old node to supernode, it is carried out in redressoverflow
						//node.adaptNodeSize();
						// we donot need to reunite the node , because overlap split do not divide old node into two nodes
						// it gives only splitInfo
						//System.out.println(" Ovlmsplit fails  size of node "+splitInfo.distribution().entries(true).size()+ " and    "+splitInfo.distribution().entries(false).size()+"  required not < "+minF);
						splitInfo.setSuccess(false);
						return splitInfo;
					}
					// overlapminimal split is successful
					distribution = splitInfo.distribution();

					//old node
					((IndexEntry)indexEntry(path)).descriptor = distribution.descriptor(false);
					//new node
					//entries.addAll(distribution.entries(true));
					// note : descriptor of new node is in splitInfo and will be set for new Indexentry in redressoverflow
				}//if
			}// end if level !=0

			//update new node
			entries.clear();
			entries.addAll(distribution.entries(true));

			//old node was updated in toplogicalsplit
			node.entries.clear();
			node.entries.addAll(distribution.entries(false));
			splitInfo.setSuccess(true);
			return splitInfo;
		}//split

		/** Performs the normal split of the superclass <tt>RTree</tt>.
		 * 
		 * @param path the nodes already visited during this insert
		 * @return a SplitInfo containig all needed information about the split
		 * 
		 * @see xxl.core.indexStructures.Tree.Node#split(java.util.Stack)
		 */
		protected Tree.Node.SplitInfo topologicalsplit(Stack path){
			return super.split(path);
		}

		/** Performs a node split minimizing the overlap of the nodes. 
		 * 
		 * @param path the nodes already visited during this insert
		 * @return a SplitInfo containig all needed information about the split
		 * 
		 * @see xxl.core.indexStructures.Tree.Node#split(java.util.Stack)
		 */
		protected SplitInfo overlapminimalsplit (Stack path) {
			int numberOfDimensions = ((Rectangle)rootDescriptor()).dimensions();
			// the node to be split
			final Node node = (Node)node(path);
			Distribution distribution;
			final int minEntries = 1;
			final int maxEntries = node.splitMaxNumber()+node.splitMinNumber()- minEntries;//node.number()+1-minEntries;//node.number()=maxCapacity
			Iterator entries = node.entries();
			BitSet splitSet = ((IndexEntry)entries.next()).getHistory();

			for(; entries.hasNext();)
				splitSet.and(((IndexEntry)entries.next()).getHistory() );

			if (splitSet.equals(new BitSet(numberOfDimensions))) {
				SplitInfo splitInfo = new SplitInfo(path);
				splitInfo.setSuccess(false);
				return splitInfo;
			}

			int numberOfCandidate = 0;
			Integer [] candidate = new Integer[splitSet.size()];

			for (int i =0; i<splitSet.size(); i++) {
				if( splitSet.get(i)){
					candidate[numberOfCandidate] =new Integer(i);
					numberOfCandidate++;
				}
			}

			Integer [] candidate2 = new Integer[numberOfCandidate];

			for (int j=0; j<numberOfCandidate; j++)
				candidate2[j]=candidate[j];

			Iterator<List<Distribution>> distributionLists = new Mapper<Integer, List<Distribution>>(
				new AbstractFunction<Integer, List<Distribution>>() {
					public List<Distribution> invoke(final Integer dim) {
						// Liste der Distributionen fuer diese Dimension
						ArrayList<Distribution> distributionList = new ArrayList<Distribution>(2*(maxEntries-minEntries+1));
						Rectangle [] [] rectangles = new Rectangle[2] [];

						// Betrachte pro Dimension die bzgl. linken bzw. rechten Raendern geordneten Eintraege
						for (int i=0; i<2; i++) {
							// Gewuenschte minimale Anzahl an Eintraegen im alten bzw. neuen Knoten
							Object [] entryArray = node.entries.toArray();
							final boolean right = i==1;

							// Sortierung der Eintraege nach linkem bzw. rechtem Rand der aktuellen Dimension
							Arrays.sort(entryArray, new FeatureComparator(new ComparableComparator(),
								new AbstractFunction() {
									public Object invoke (Object entry) {
										return new Double(rectangle(entry).getCorner(right).getValue(dim));
									}
								}
							));

							// Monotone Berechnung der Deskriptoren fuer alle Distributionen (linear!)
							for (int k = 0; k<2; k++) {
								List entryList = Arrays.asList(entryArray);
								Iterator entries2 = (k==0? entryList: new ReversedList(entryList)).iterator();
								Rectangle rectangle = new DoublePointRectangle(rectangle(entries2.next()));

								for (int l = (k==0? minEntries: node.number()-maxEntries); --l>0;)
									rectangle.union(rectangle(entries2.next()));
								(rectangles[k] = new Rectangle[maxEntries-minEntries+1])[0] = rectangle;
								for (int j=1; j<=maxEntries-minEntries; rectangles[k][j++] = rectangle)
									rectangle = Descriptors.union(rectangle, (rectangle(entries2.next())));
							}
							// Erzeugen der Distributionen dieser Dimension
							for (int j = minEntries; j<=maxEntries; j++)
								distributionList.add(new Distribution(entryArray, j, rectangles[0][j-minEntries], rectangles[1][maxEntries-j], dim));
						}
						return distributionList;
					}
				},
				new ArrayCursor<Integer>(candidate2)
			);
			// Gib die Distributionsliste einer der Dimensionen zurueck, fuer die die Summe der Margin-Werte
			// aller ihrer Distributionen minimal ist
			List<Distribution> distributionList = Cursors.minima(
				distributionLists,
				new AbstractFunction<Collection<Distribution>, Double> () {
					public Double invoke (Collection<Distribution> list) {
						double marginValue = 0.0;
						for (Distribution distribution : list)
							marginValue += distribution.marginValue();
						return marginValue;
					}
					
					
					
				}
			).getFirst();
			// Waehle die Distributionen der gewaehlten Dimension mit minimalem Overlap aus
			distributionList = Cursors.minima(
				distributionList.iterator(),
				new AbstractFunction<Distribution, Double>() {
					public Double invoke(Distribution distribution) {
						return distribution.overlapValue();
					}
				}
			);
			// Falls mehrere Distributionen in Frage kommen: Waehle eine mit minimalem Area-Wert aus
			distributionList = Cursors.minima(
				distributionList.iterator(),
				new AbstractFunction<Distribution, Double>() {
					public Double invoke(Distribution distribution) {
						return distribution.areaValue();
					}
				}
			);
			distribution = distributionList.get(0);
			return (XTree.Node.SplitInfo)((new SplitInfo(path)).initialize(true).initialize(distribution));
			// end choose one dimension
		}//overlapminimalsplit
	}//Node

	/* (non-Javadoc)
	 * @see xxl.core.indexStructures.ORTree#indexEntryConverter(xxl.core.io.converters.Converter)
	 */
	public Converter indexEntryConverter (Converter descriptorConverter) {
		return new IndexEntryConverter(descriptorConverter);
	}

	/** The instances of this class are converters to write index entries to the 
	 * external storage (or any other {@link java.io.DataOutput DataOutput}) or read 
	 * them from it (or any other {@link java.io.DataInput DataInput}).  
	 * @see xxl.core.io.converters.Converter
	 */
	public class IndexEntryConverter extends ORTree.IndexEntryConverter {

		/**
		 * @param descriptorConverter a converter for descriptors
		 */
		public IndexEntryConverter (Converter descriptorConverter) {
			super(descriptorConverter);
		}

		/* (non-Javadoc)
		 * @see xxl.core.io.converters.Converter#read(java.io.DataInput, java.lang.Object)
		 */
		public Object read (DataInput dataInput, Object object) throws IOException {
			IndexEntry indexEntry = (IndexEntry)object;
			indexEntry.container();
			indexEntry.id = LongConverter.DEFAULT_INSTANCE.read(dataInput, null);
			indexEntry.splitHistory.read(dataInput);
			indexEntry.descriptor = (Descriptor)descriptorConverter.read(dataInput, null);
			return indexEntry;
		}

		/* (non-Javadoc)
		 * @see xxl.core.io.converters.Converter#write(java.io.DataOutput, java.lang.Object)
		 */
		public void write (DataOutput dataOutput, Object object) throws IOException {
			IndexEntry indexEntry = (IndexEntry)object;
			indexEntry.container();
			LongConverter.DEFAULT_INSTANCE.write(dataOutput, (Long)indexEntry.id());
			indexEntry.splitHistory.write(dataOutput);
			descriptorConverter.write(dataOutput, indexEntry.descriptor());
		}
	}//IndexEntryConverter

	/* (non-Javadoc)
	 * @see xxl.core.indexStructures.ORTree#nodeConverter(xxl.core.io.converters.Converter, xxl.core.io.converters.Converter)
	 */
	public Converter nodeConverter (Converter objectConverter, Converter indexEntryConverter) {
		return new NodeConverter(objectConverter, indexEntryConverter);
	}
	
	/* (non-Javadoc)
	 * @see xxl.core.indexStructures.RTree#nodeConverter(xxl.core.io.converters.Converter, int)
	 */
	public Converter nodeConverter (Converter objectConverter, final int dimensions) {
		return nodeConverter(objectConverter,indexEntryConverter(
			new ConvertableConverter(
				new AbstractFunction() {
					public Object invoke () {
						return new DoublePointRectangle(dimensions);
					}
				}
			)
		));
	}

	/** The instances of this class are converters to write nodes to the 
	 * external storage (or any other {@link java.io.DataOutput DataOutput}) or read 
	 * them from it (or any other {@link java.io.DataInput DataInput}).  
	 * @see xxl.core.io.converters.Converter
	 */
	public class NodeConverter extends ORTree.NodeConverter {

		/**
		 * @param objectConverter
		 * @param indexEntryConverter
		 */
		public NodeConverter (Converter objectConverter, Converter indexEntryConverter) {
			super(objectConverter,indexEntryConverter);
		}

		/* (non-Javadoc)
		 * @see xxl.core.io.converters.Converter#read(java.io.DataInput, java.lang.Object)
		 */
		public Object read (DataInput dataInput, Object object) throws IOException {
			Node node = (Node)createNode(dataInput.readShort());
			int nodeSize = dataInput.readShort();
			node.setNodeSize(nodeSize);
			for (int i=dataInput.readInt(); --i>=0;)
				node.entries.add(node.level==0?
					objectConverter.read(dataInput, null):
					indexEntryConverter.read(dataInput, createIndexEntry(node.level))
			);
			return node;
		}

		/* (non-Javadoc)
		 * @see xxl.core.io.converters.Converter#write(java.io.DataOutput, java.lang.Object)
		 */
		public void write (DataOutput dataOutput, Object object) throws IOException {
			Node node = (Node)object;
			Converter converter = node.level==0? objectConverter: indexEntryConverter;
			ShortConverter.DEFAULT_INSTANCE.write(dataOutput, new Short((short)node.level));
			ShortConverter.DEFAULT_INSTANCE.write(dataOutput, new Short((short)node.nodeSize));
			IntegerConverter.DEFAULT_INSTANCE.write(dataOutput, new Integer(node.number()));
			for (Iterator entries = node.entries(); entries.hasNext();)
				converter.write(dataOutput, entries.next());
		}
	}//NodeConverter
}//XTree
