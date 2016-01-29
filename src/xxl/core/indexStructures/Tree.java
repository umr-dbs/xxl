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

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Stack;
import java.util.Map.Entry;

import xxl.core.collections.MapEntry;
import xxl.core.collections.containers.Container;
import xxl.core.collections.queues.Queue;
import xxl.core.collections.queues.Queues;
import xxl.core.cursors.AbstractCursor;
import xxl.core.cursors.Cursor;
import xxl.core.cursors.filters.Filter;
import xxl.core.cursors.mappers.Mapper;
import xxl.core.cursors.sources.EmptyCursor;
import xxl.core.cursors.sources.SingleObjectCursor;
import xxl.core.cursors.unions.Sequentializer;
import xxl.core.cursors.wrappers.IteratorCursor;
import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Constant;
import xxl.core.functions.Function;
import xxl.core.predicates.AbstractPredicate;
import xxl.core.predicates.Equal;
import xxl.core.predicates.Predicate;

/** The Tree-class is a generic and highly flexible super-class that
	implements features used by grow- and post-trees. It has the following inner 
	classes:
	<ul>
		<li>interface Descriptor</li>
		<li>class IndexEntry</li>
		<li>class Node</li>
		<li>class Query</li>
	</ul>
	A tree consists of nodes which store entries. The entries of inner 
	nodes (index nodes) are index entries which refer to (sub-)nodes. See 
	following figure:
	<br>
	<img src="Node.jpg">
	<br>
	Each index entry has got a descriptor describing the 
	data range represented by the subnode refered to by the index entry. 
	For instance, in a B-tree the descriptor is a simple separator key, 
	but in a R-tree the descriptor is a spatial rectangle. <br>
	To use this class you have to implement the inner interface 
	Descriptor and to inherit from the inner class {@link Node Node} 
	(which represents leaf- and non-leaf nodes).
	You may also have to inherit from other inner classes like {@link IndexEntry IndexEntry} 
	in order to align them with your application or to obtain more efficiency. 
	The structure of this class, including all abstract methods, is as
	follows:
	<pre><code>
*	public abstract class Tree {
*		public abstract Node createNode(int level);
* 
*		public static interface Descriptor extends Cloneable {
*			public abstract boolean overlaps (Descriptor descriptor);
*			public abstract boolean contains (Descriptor descriptor);
*			public abstract void union (Descriptor descriptor);
*			public abstract boolean equals (Object object);
*			public abstract Object clone ();
*		}
*
*		protected class IndexEntry {
*		}
*
*		protected abstract class Node {
*			public class SplitInfo {
*			}
*			protected abstract SplitInfo initialize (Object entry);
*			public abstract Iterator entries ();
*			protected abstract int number ();
*			public abstract Iterator descriptors (Descriptor nodeDescriptor);
*			public abstract Iterator query (Descriptor queryDescriptor);
*			protected abstract IndexEntry chooseSubtree (Descriptor descriptor, Stack path);
*			protected abstract void grow (Object data, Stack path);
*			protected abstract SplitInfo split (Stack path);
*			protected abstract void post (SplitInfo splitInfo, IndexEntry newIndexEntry);
*		}
*
*		public abstract class Query implements Iterator {
*			protected static class Candidate {
*			}
*			protected Iterator expand (Candidate parent, Node node){
*			}
*		}
*	}
	</pre></code>
*/

public abstract class Tree {

	/** Returns the <tt>Descriptor</tt> of a given data item.
		<br>
		Function: Object &rarr; Descriptor.
		<br>
	*/
	public Function getDescriptor;

	/** Returns the <tt>Container</tt>used to store a new <tt>Node</tt> created by a split operation.
		<br>
		Function: {@link Tree.Node.SplitInfo} &rarr; {@link Container},
		<br>
		{@link Tree.Node.SplitInfo}: information on the split.
		If the {@link Container} of a new root has to be determined, then 
		splitInfo.path.isEmpty() == true.
	*/
	public Function determineContainer;

	/** Returns the {@link Container} that is used to store the node 
	 * the <tt>IndexEntry</tt> is pointing to.
		<br>
		Function: {@link Tree.IndexEntry} &rarr; {@link Container}
		<br>
	*/
	public Function getContainer;

	/** Checks if a <tt>Node</tt> overflows. 
	 * 
	 */
	public Predicate overflows;

	/** Checks if a <tt>Node</tt> underflows.
	 * 
	 */
	public Predicate underflows;

	/** Returns the minimal relative number of entries which the <tt>Node</tt> may
		contain after a split. The point of reference is assumed to
		be the number of entries of the <tt>Node</tt> that was split (before the split).

		<br>
		Function: {@link Tree.Node} &rarr; double
		<br>
	*/
	public Function getSplitMinRatio;

	/** Returns the maximal relative number of entries which the <tt>Node</tt> may 
	 * contain after a split. The point of reference is assumed to 
	 * be the number of entries of the node that was split (before the split).
	 * <br>
	 * Function: {@link Tree.Node} &rarr; double
	 * <br>
	*/
	public Function getSplitMaxRatio;

	/** This is a reference on the root node of the tree.
		NOTE: if rootEntry contains a descriptor it will be ignored
		by the algorithms of Tree.
		@see Tree#rootDescriptor
	*/
	protected IndexEntry rootEntry = null;

	/** The Descriptor of the tree.
		@see Tree#rootEntry
	*/
	protected Descriptor rootDescriptor = null;

	/** This is the basic method to create a new tree. It is not sufficient to call the constructor. 
	 * Before the tree is usable, it must be initialized.
	 * 
	 * @param rootEntry the new {@link Tree#rootEntry}
	 * @param rootDescriptor the new {@link Tree#rootDescriptor}
	 * @param getDescriptor the new {@link Tree#getDescriptor}
	 * @param getContainer the new {@link Tree#getContainer}
	 * @param determineContainer the new {@link Tree#determineContainer}
	 * @param underflows the new {@link Tree#underflows}
	 * @param overflows the new {@link Tree#overflows}
	 * @param getSplitMinRatio the new {@link Tree#getSplitMinRatio}
	 * @param getSplitMaxRatio the new {@link Tree#getSplitMaxRatio}
	 * @return the initialized tree (i.e. return this)
	 */
	public Tree initialize (IndexEntry rootEntry, Descriptor rootDescriptor, Function getDescriptor, Function getContainer, Function determineContainer, Predicate underflows, Predicate overflows, Function getSplitMinRatio, Function getSplitMaxRatio) {
		this.rootEntry = rootEntry;
		this.rootDescriptor = rootDescriptor;
		this.getDescriptor = getDescriptor;
		this.getContainer = getContainer;
		this.determineContainer = determineContainer;
		this.underflows = underflows;
		this.overflows = overflows;
		this.getSplitMinRatio = getSplitMinRatio;
		this.getSplitMaxRatio = getSplitMaxRatio;
		return this;
	}
	
	/** Calls the method {@link Tree#initialize(Tree.IndexEntry, Descriptor, Function, Function, Function, Predicate, Predicate, Function, Function)}
	 * to initialize the tree. Both Function objects <tt>getContainer</tt> and <tt>determineContainer</tt> are 
	 * initialized with	constant functions always returning the given <tt>Container</tt>.
	 * The <tt>Predicate</tt> {@link Tree#underflows} is initialized with:
	 * <pre><code>
	  		new AbstractPredicate() {
	 			public boolean invoke (Object node) {
					return ((Tree.Node)node).number() < minCapacity; 
				}
			}
	   </code></pre> 
	 * The <tt>Predicate</tt> {@link Tree#overflows} is initialized with:
	 * <pre><code>
	  		new AbstractPredicate() {
	 			public boolean invoke (Object node) {
					return ((Tree.Node)node).number() > maxCapacity; 
				}
			}
	   </code></pre>    
	 *  {@link Tree#getSplitMinRatio} is defined as:
	 * <pre><code>
	  			new AbstractFunction() {
					public Object invoke (Object node) {
						return new Double(minCapacity/(double)((Tree.Node)node).number());
					}
				}
	   </code></pre>
	 * {@link Tree#getSplitMaxRatio} is defined as:
	 * <pre><code>
			new AbstractFunction() {
				public Object invoke (Object node) {
					return new Double(1.0-minCapacity/(double)((Tree.Node)node).number());
				}
			}
	   </code></pre>
	 * @param rootEntry the new {@link Tree#rootEntry}
	 * @param rootDescriptor the new {@link Tree#rootDescriptor}
	 * @param getDescriptor the new {@link Tree#getDescriptor}
	 * @param container used as constant for the new {@link Tree#determineContainer}
	 * @param minCapacity is used to define {@link Tree#underflows}, {@link Tree#getSplitMinRatio} 
	 * 				and {@link Tree#getSplitMaxRatio}
	 * @param maxCapacity is used to define {@link Tree#overflows}
	 * @return the initialized tree 
	 */
	public Tree initialize (IndexEntry rootEntry, Descriptor rootDescriptor, Function getDescriptor, Container container, final int minCapacity, final int maxCapacity) {
		return initialize(
			rootEntry,
			rootDescriptor,
			getDescriptor,
			new Constant(container),
			new Constant(container),
			new AbstractPredicate() {
				public boolean invoke (Object node) {
					return ((Tree.Node)node).number() < minCapacity;
				}
			},
			new AbstractPredicate() {
				public boolean invoke (Object node) {
					return ((Tree.Node)node).number() > maxCapacity;
				}
			},
			new AbstractFunction() {
				public Object invoke (Object node) {
					return new Double(minCapacity/(double)((Tree.Node)node).number());
				}
			},
			new AbstractFunction() {
				public Object invoke (Object node) {
					return new Double(1.0-minCapacity/(double)((Tree.Node)node).number());
				}
			}
		);
	}

	/** Calls the method {@link Tree#initialize(Tree.IndexEntry, Descriptor, Function, Function, Function, Predicate, Predicate, Function, Function)}
	 * to initialize the tree. Both Function objects <tt>getContainer</tt> and <tt>determineContainer</tt> are 
	 * initialized with	constant functions always returning the given <tt>Container</tt>.
	 * The <tt>Predicate</tt> {@link Tree#underflows} is initialized with:
	 * <pre><code>
			new AbstractPredicate() {
				public boolean invoke (Object node) {
					Tree.Node treeNode = (Node) node;
					return treeNode.number() < (treeNode.level()==0 ? leafMinCapacity : innerNodeMinCapacity);
				}
			}
	   </code></pre> 
	 * The <tt>Predicate</tt> {@link Tree#overflows} is initialized with:
	 * <pre><code>
			new AbstractPredicate() {
				public boolean invoke (Object node) {
					Tree.Node treeNode = (Node) node;
					return treeNode.number() > (treeNode.level()==0 ? leafMaxCapacity : innerNodeMaxCapacity);
				}
			}
	   </code></pre>    
	 *  {@link Tree#getSplitMinRatio} is defined as:
	 * <pre><code>
			new AbstractFunction() {
				public Object invoke (Object node) {
					Tree.Node treeNode = (Node) node;
					return new Double((treeNode.level()==0 ? leafMinCapacity : innerNodeMinCapacity)/(double)treeNode.number());
				}
			}
	   </code></pre>
	 * {@link Tree#getSplitMaxRatio} is defined as:
	 * <pre><code>
			new AbstractFunction() {
				public Object invoke (Object node) {
					Tree.Node treeNode = (Node) node;
					return new Double(1.0-(treeNode.level()==0 ? leafMinCapacity : innerNodeMinCapacity)/(double)treeNode.number());
				}
			}
	   </code></pre>
	 * 
	 * @param rootEntry the new {@link Tree#rootEntry}
	 * @param rootDescriptor the new {@link Tree#rootDescriptor}
	 * @param getDescriptor the new {@link Tree#getDescriptor}
	 * @param container used as constant for the new {@link Tree#determineContainer}
	 * @param innerNodeMinCapacity is used to define {@link Tree#underflows}, {@link Tree#getSplitMinRatio} 
	 * 				and {@link Tree#getSplitMaxRatio} for inner nodes
	 * @param innerNodeMaxCapacity is used to define {@link Tree#overflows} for inner nodes
	 * @param leafMinCapacity is used to define {@link Tree#underflows}, {@link Tree#getSplitMinRatio} 
	 * 				and {@link Tree#getSplitMaxRatio} for leaf nodes
	 * @param leafMaxCapacity is used to define {@link Tree#overflows} for leaf nodes
	 * @return the initialized tree 
	 * 
	 * @since 1.1
	 */
	public Tree initialize (IndexEntry rootEntry, Descriptor rootDescriptor, Function getDescriptor, Container container, final int innerNodeMinCapacity, final int innerNodeMaxCapacity, final int leafMinCapacity, final int leafMaxCapacity) {
		return initialize(
			rootEntry,
			rootDescriptor,
			getDescriptor,
			new Constant(container),
			new Constant(container),
			new AbstractPredicate() {
				public boolean invoke (Object node) {
					Tree.Node treeNode = (Node) node;
					return treeNode.number() < (treeNode.level()==0 ? leafMinCapacity : innerNodeMinCapacity);
				}
			},
			new AbstractPredicate() {
				public boolean invoke (Object node) {
					Tree.Node treeNode = (Node) node;
					return treeNode.number() > (treeNode.level()==0 ? leafMaxCapacity : innerNodeMaxCapacity);
				}
			},
			new AbstractFunction() {
				public Object invoke (Object node) {
					Tree.Node treeNode = (Node) node;
					return new Double((treeNode.level()==0 ? leafMinCapacity : innerNodeMinCapacity)/(double)treeNode.number());
				}
			},
			new AbstractFunction() {
				public Object invoke (Object node) {
					Tree.Node treeNode = (Node) node;
					return new Double(1.0-(treeNode.level()==0 ? leafMinCapacity : innerNodeMinCapacity)/(double)treeNode.number());
				}
			}
		);
	}
	
	/**
	 * Before the tree is usable, it must be initialized. This version of initialize
	 * takes basic informations about the size of the blocks and entries and calculates
	 * the capacity of inner and leaf nodes. 
	 * 
	 * @param rootEntry the new {@link Tree#rootEntry}
	 * @param rootDescriptor the new {@link Tree#rootDescriptor}
	 * @param getDescriptor the new {@link Tree#getDescriptor}
	 * @param blockSize the size of a block of the <code>container</code> in bytes
	 * @param container used as constant for the new {@link Tree#determineContainer}
	 * @param dataSize the size of an data object in bytes
	 * @param descriptorSize the size of a Descriptor returned by <code>getDescriptor</code> in bytes
	 * @param minMaxFactor the quotient between minimum and maximum number of entries in an node, e.g. 0.5
	 * @return the initialized tree 
	 * 
	 * @since 1.1
	 */	
	public Tree initialize (IndexEntry rootEntry, Descriptor rootDescriptor, Function getDescriptor,int blockSize,Container container, int dataSize, int descriptorSize, double minMaxFactor) {
		int overhead = 6;
		int payLoad = blockSize - overhead;
		int leafMaxCap = payLoad / dataSize;
		int innerNodeMaxCap = payLoad / (descriptorSize+8);
		int leafMinCap = (int) (leafMaxCap * minMaxFactor);
		int innerNodeMinCap = (int) (innerNodeMaxCap * minMaxFactor);			
		return initialize(rootEntry, rootDescriptor, getDescriptor, container, innerNodeMinCap, innerNodeMaxCap, leafMinCap, leafMaxCap);
	}
		
	/** Calls {@link #initialize(Tree.IndexEntry, Descriptor, Function, Container, int, int, int, int)}
	 * <pre><code>
	  			return initialize(null, null, getDescriptor, container, innerNodeMinCapacity, innerNodeMaxCapacity, leafMinCapacity, leafMaxCapacity);
	   </code></pre>
	 * 
	 * @param getDescriptor the new {@link Tree#getDescriptor}
	 * @param container the new {@link Tree#determineContainer}
	 * @param innerNodeMinCapacity is used to define {@link Tree#underflows}, {@link Tree#getSplitMinRatio} 
	 * 				and {@link Tree#getSplitMaxRatio} for inner nodes
	 * @param innerNodeMaxCapacity is used to define {@link Tree#overflows} for inner nodes
	 * @param leafMinCapacity is used to define {@link Tree#underflows}, {@link Tree#getSplitMinRatio} 
	 * 				and {@link Tree#getSplitMaxRatio} for leaf nodes
	 * @param leafMaxCapacity is used to define {@link Tree#overflows} for leaf nodes
	 * @return the initialized tree 
	 * 
	 * @since 1.1
	 */
	public Tree initialize (Function getDescriptor, Container container, final int innerNodeMinCapacity, final int innerNodeMaxCapacity, final int leafMinCapacity, final int leafMaxCapacity) {
		return initialize(null, null, getDescriptor, container, innerNodeMinCapacity, innerNodeMaxCapacity, leafMinCapacity, leafMaxCapacity);
	}

	/**
	 * Before the tree is usable, it must be initialized. This version of initialize
	 * takes basic informations about the size of the blocks and entries and calculates
	 * the capacity of inner and leaf nodes. 
	 * 
	 * @param getDescriptor the new {@link Tree#getDescriptor}
	 * @param blockSize the size of a block of the <code>container</code> in bytes
	 * @param container the new {@link Tree#determineContainer}
	 * @param dataSize the size of an data object in bytes
	 * @param descriptorSize the size of a Descriptor returned by <code>getDescriptor</code> in bytes
	 * @param minMaxFactor the quotient between minimum and maximum number of entries in an node, e.g. 0.5
	 * @return the initialized tree 
	 * 
	 * @since 1.1
	 */
	public Tree initialize (Function getDescriptor, int blockSize, Container container, int dataSize, int descriptorSize, double minMaxFactor) {
		return initialize(null, null, getDescriptor, blockSize, container, dataSize, descriptorSize, minMaxFactor);
	}
		
	/** Calls {@link #initialize(Tree.IndexEntry, Descriptor, Function, Container, int, int)}
	 * <pre><code>
	  			return initialize(null, null, getDescriptor, container, minCapacity, maxCapacity);
	   </code></pre>
	 * 
	 * @param getDescriptor the new {@link Tree#getDescriptor}
	 * @param container the new {@link Tree#determineContainer}
	 * @param minCapacity is used to define {@link Tree#underflows}, {@link Tree#getSplitMinRatio} 
	 * 				and {@link Tree#getSplitMaxRatio}.
	 * @param maxCapacity is used to define {@link Tree#overflows}
	 * @return the initialized tree 
	 */
	public Tree initialize (Function getDescriptor, Container container, final int minCapacity, final int maxCapacity) {
		return initialize(null, null, getDescriptor, container, minCapacity, maxCapacity);
	}
	
	/**
	 * sets container 
	 * for determineContainer function
	 * and for getConatainer 
	 * 
	 *  sets both variables as follows: 
	 *  
	 *  determineFunction = new Constant(container); 
	 *  getContainer = new Constant(container); 
	 *  
	 *  
	 *  @param container to store nodes
	 */
	public void setContainer(Container container){
		this.determineContainer = new Constant(container);
		this.getContainer = new Constant(container); 
	} 
	
	/**
	 * @return the {@link Tree#rootEntry}
	*/
	public IndexEntry rootEntry () {
		return rootEntry;
	}

	/** @return {@link Tree#rootDescriptor} (i.e. the tree's Descriptor).
	*/
	public Descriptor rootDescriptor () {
		return rootDescriptor;
	}

	/** @return the height of the tree.
	*/
	public int height () {
		return rootEntry==null? 0: rootEntry.parentLevel();
	}
	
	/** Creates a new <tt>IndexEntry</tt>.
	 * @param parentLevel the level of the node in which the new <tt>indexEntry</tt> is stored
	 * @return a new <tt>indexEntry</tt>
	 * @see Tree.IndexEntry
	 */
	public IndexEntry createIndexEntry (int parentLevel) {
		return new IndexEntry(parentLevel);
	}

	/** Creates a new <tt>Node</tt> which does not have any information except its level.
	 * @param level the level of this <tt>Node</tt> (i.e. the maximum distance 
	 * to the leaves of the subtree)
	 * @return a new <tt>Node</tt>
	 * @see Tree.Node
	 */
	public abstract Node createNode (int level);

	/** Returns the <tt>Descriptor</tt> of a given entry. If this entry itself is a <tt>descriptor</tt>
	 * it is returned, else the <tt>Function</tt> {@link Tree#getDescriptor} is used to determine the 
	 * <tt>descriptor</tt> of the entry
	 * <pre><code>
	  			getDescriptor.invoke(entry)
	   </code></pre> 
	 * @param entry the entry whose <tt>Descriptor</tt> is to be determined
	 * @return the <tt>descriptor</tt> of <tt>entry</tt>
	 */
	public Descriptor descriptor (Object entry) {
		return (Descriptor)((entry instanceof Descriptor)? entry: getDescriptor.invoke(entry));
	}

	/** Tests if a given object is contained in the tree. The method {@link Tree#query(Object)} 
	 * is used to find the pages that may contain this object.
	 * @param data the object to search for
	 * @return true if the tree contains the given object <tt>data</tt>
	 * @see Tree#query(Object)
	 */
	public boolean contains (Object data) {
		for (Cursor objects = query(descriptor(data)); objects.hasNext();)
			if (data.equals(objects.next()))
				return true;
		return false;
	}
	
	/** Searches for the objects stored in the tree with a given <tt>Descriptor</tt>.
	 *  
	 * @param descriptor specifies the query
	 * @return the first object returned by {@link #query(Descriptor)}.
	 * The result is <tt>null</tt> if the result of {@link #query(Descriptor)} is empty
	 * 
	 * @see Descriptor
	 * @see #query(Descriptor)
	 */
	public Object get(Descriptor descriptor) {
		Cursor objects = query(descriptor);
		return objects.hasNext() ? objects.next() : null;
	}

	/** Inserts an object into a given level of the tree. If level>0 <tt>data</tt> has 
	 * to be an index entry (This only makes sense for trees whose index nodes
	 * support Node.grow()).
	 *
	 * @param data the data to insert
	 * @param descriptor is used to find the node into which data must be inserted
	 * @param targetLevel is the tree-level into which <tt>data</tt> has to be inserted 
	 * 			(<tt>targetLevel==0</tt> means insert into the suitable leaf node)
	 */
	protected void insert (Object data, Descriptor descriptor, int targetLevel) {
		if (rootEntry()==null) {
			rootDescriptor = (Descriptor)descriptor.clone();
			grow(data);
		}
		else {
			Stack path = new Stack();
			chooseLeaf(descriptor, targetLevel, path).growAndPost(data, path);
		}
	}

	/** Inserts an object into a given level of the tree. This method 
	 * calls {@link Tree#insert(Object,int)} in the following way:
	 * <pre><code>
	 * 			insert(entry, descriptor(entry), level);
	 * </code></pre>
	 * @param entry the object that is to be inserted
	 * @param level the target level of the insertion operation (leaf-level= 0)
	 * @see Tree#insert(Object,Descriptor,int)
	 */
	public void insert (Object entry, int level) {
		insert(entry, descriptor(entry), level);
	}

	/** Inserts an object into the leaf-level of the tree. This method 
	 * calls {@link Tree#insert(Object,Descriptor,int)} in the following way:
	 * <pre><code>
	  			insert(data, (Descriptor)getDescriptor.invoke(data), 0);
	   </code></pre>
	 * @param data the object that is to be inserted
	 * @see Tree#insert(Object,Descriptor,int)
	 */
	public void insert (Object data) {
		insert(data, (Descriptor)getDescriptor.invoke(data), 0);
	}
	
	/** Overwrites the <tt>oldData</tt> in the tree by the Object <tt>newData</tt>.
	 * 
	 * @param oldData the object (in the tree) which has to be replaced
	 * @param newData the object which should replace <tt>oldData</tt>
	 */
	public void update (Object oldData, Object newData) {
		Cursor objects = query(descriptor(oldData)); 
		while(objects.hasNext())
			if (oldData.equals(objects.next())) {
				objects.update(newData);
				break;
			}
		objects.close();
	}

	/** This method is akin to {@link Tree#update(Object,Object)}. Here the descriptor 
	 * of the old data object is used to find the old object.
	 * @param descriptor the <tt>Descriptor</tt> of the object which has to be updated
	 * @param newData the object which should replace the old object
	 * @see Tree#update(Object,Object)
	 */
	public void update (Descriptor descriptor, Object newData) {
		Cursor objects = query(descriptor); 
		while(objects.hasNext())
			if (descriptor.equals(descriptor(objects.next()))) {
				objects.update(newData);
				break;
			}
		objects.close();
	}

	/** Removes the object which is specified by the given descriptor. 
	 * First query is called. 
	 * <pre> <code>
	  	Iterator objects = query(descriptor, targetLevel);
	   </pre></code>
	 * Then the first object contained of the Iterator which meets
	 * <tt>test</tt> is removed from the tree and returned. 
	 * @param descriptor specifies the query
	 * @param targetLevel the level on which the query has to stop
	 * @param test a predicate the object to remove has to fulfill
	 * @return the removed object or <tt>null</tt> if there was no such object 
	 */ 
	public Object remove (Descriptor descriptor, int targetLevel, Predicate test) {
		Cursor objects = query(descriptor, targetLevel);
		Object retValue=null;
		while (objects.hasNext()) {
			Object object = objects.next();
			if (test.invoke(object)) {
				objects.remove();
				retValue= object;
				break;
			}
		}
		objects.close();
		return retValue;
	}

	/** Calls {@link #remove(Descriptor, int, Predicate)} whereas 
	 * the target level is set to zero. The used <tt>Descriptor</tt> is the result of
	 * <pre><code>
	  			getDescriptor.invoke(data);
	   </code></pre> 
	 * @param data object to remove
	 * @param equals predicate determining if an object in the tree equals <tt>data</tt> 
	 * and should therefor be removed
	 * @return  the removed object or <tt>null</tt> if no object was removed
	 * @see Tree#remove(Descriptor, int, Predicate)
	 */
	public Object remove (final Object data, final Predicate equals) {
		return remove((Descriptor)getDescriptor.invoke(data), 0,
			new AbstractPredicate() {
				public boolean invoke (Object object) {
					return equals.invoke(object, data);
				}
			}
		);
	}
	
	/** Calls {@link Tree#remove(Object,Predicate)} whereas 
	 * {@link xxl.core.predicates.Equal#DEFAULT_INSTANCE the default Equal Predicate} 
	 * is used.
	 * @param data the object which has to be removed
	 * @return the removed object or <tt>null</tt> if no such object was found
	 * @see Tree#remove(Object,Predicate)
	 */
	public Object remove (Object data) {
		return remove(data, Equal.DEFAULT_INSTANCE);
	}
	
	/** Removes all entries of the tree.
	 */ 
	public void clear () {
		if (rootEntry != null)
			rootEntry.removeAll();
		rootEntry = null;
		rootDescriptor = null;
	}

	/** This method is used to find the node whose descriptor overlaps the 
	 * given descriptor. The search stops at the given level.
	 * 
	 * @param descriptor specifies the search target
	 * @param targetLevel the level on which the search operation has to be stopped
	 * @param path is a stack used to store the path from the root to the node
	 * NOTE: entries in the stack have to be instances of 
	 * {@link java.util.Map.Entry} whereas the nodes
	 * on the path are used as content objects and the index entries as keys. 
	 * @return the <tt>IndexEntry</tt> which references the mentioned node
	 * 
	 * @see Tree.IndexEntry#chooseSubtree(Descriptor, Stack)
	 */
	protected IndexEntry chooseLeaf (Descriptor descriptor, int targetLevel, Stack path) {
		IndexEntry indexEntry = rootEntry;
		rootDescriptor.union(descriptor);
		while (indexEntry.level()>targetLevel)
			indexEntry = indexEntry.chooseSubtree(descriptor, path);
		return indexEntry;
	}

	/** Finds the node on the given level, in whose subtree the given object should be contained.
	 * If the target level is zero the search process stops on the leaves level. 
	 * @param object the object to search for
	 * @param targetLevel the level at which the search process must stop
	 * @param path is a stack used to store the path from the root to the found node
	 * NOTE: entries in the stack have to be instances of 
	 * {@link java.util.Map.Entry} whereas the nodes
	 * on the path are used as content objects and the index entries as keys. 
	 * @return the index entry which refers to the mentioned node
	 * 
	 * @see Tree#chooseLeaf(Descriptor, int, Stack)
	 */	
	protected IndexEntry chooseLeaf (Object object, int targetLevel, Stack path) {
		return chooseLeaf(descriptor(object), targetLevel, path);
	}

	/** Finds the node on the given level, in whose subtree the given object should be contained.
	 * If the target level is zero the search process stops on the leaves level. 
	 * @param object the object searched for
	 * @param targetLevel the level on which the search process must stop
	 * @return the index entry which refers to the mentioned node
	 * 
	 * @see Tree#chooseLeaf(Descriptor,int)
	 */
	public IndexEntry chooseLeaf (Object object, int targetLevel) {
		return chooseLeaf(descriptor(object), targetLevel);
	}

	/** This method is used to find the node whose descriptor overlaps the 
	 * given descriptor. The search stops on the given level.
	 * 
	 * @param descriptor specifies the target of the search 
	 * @param targetLevel the level on which the search operation has to be stopped
	 * @return the IndexEntry which references the mentioned node
	 * 
	 * @see Tree#chooseLeaf(Descriptor, int, Stack)
	 */	
	public IndexEntry chooseLeaf (Descriptor descriptor, int targetLevel) {
		Stack path = new Stack();
		IndexEntry indexEntry = chooseLeaf(descriptor, targetLevel, path);
		while (!path.isEmpty())
			up(path);
		return indexEntry;
	}

	/** Finds the leaf in which the given object should be contained.
	 * 
	 * @param object the object searched for
	 * @return the index entry which refers to the mentioned leaf
	 * @see #chooseLeaf(Object,int)
	 */
	public IndexEntry chooseLeaf (Object object) {
		return chooseLeaf(object, 0);
	}

	/** This method repairs the tree if an overflow occurs.  
	 * 
	 * @param path the path from the root to the overflowing node
	 */
	protected void post (Stack path) {
		if (!path.isEmpty()) {
			while (!node(path).redressOverflow(path).isEmpty());
			while (!path.isEmpty())
				up(path);
		}
	}

	/** This method is called when the root node overflows. In this case 
	 * the root node has to be splitted and a new root node has to be created.
	 * The tree's height increases by one. Also the new root node is written into 
	 * the container.
	 * @param entry the entry which should be inserted to the new root node.
	 * This usually is the old rootEntry.
	 * @return a MapEntry whose key is the new rootEntry and whose value is the 
	 * new root node
	 */
	protected Entry grow (Object entry) {
		Node rootNode = createNode(height());
		Tree.Node.SplitInfo splitInfo = rootNode.initialize(entry);
		Container container = splitInfo.determineContainer();
		Object id = container.insert(rootNode, false);

		rootEntry = createIndexEntry(height()+1).initialize(container, id, splitInfo);
		return new MapEntry(rootEntry, rootNode);
	}

	/** Takes the next {@link java.util.Map.Entry Entry} of the Stack and 
	 *  returns the index entry which refers to the node.
	 * 
	 * NOTE: The method uses {@link java.util.Stack#peek() peek()} to get the next 
	 * object from the stack. 
	 * That means that the object is not removed from the stack.
	 * @param path a stack which stores a path of the tree
	 * NOTE: entries in the stack have to be instances of 
	 * {@link java.util.Map.Entry} whereas the nodes
	 * on the path are used as content objects and the index entries as keys. 
	 * @return the index entry which refers to the next node of the path 
	 * or <tt>null</tt> if path is empty
	 */
	protected IndexEntry indexEntry (Stack path) {
		return path.isEmpty()? null: (IndexEntry)((Entry)path.peek()).getKey();
	}

	/** Takes the next {@link java.util.Map.Entry Entry} of the Stack and gets its node.
	 * 
	 * NOTE: The method use {@link java.util.Stack#peek() peek()} to get the 
	 * next object from the stack. 
	 * That means that the object is not removed from the stack.
	 * @param path a stack which stores a path of the tree
	 * NOTE: entries in the stack have to be instances of 
	 * {@link java.util.Map.Entry} whereas the nodes
	 * on the path are used as content objects and the index entries as keys. 
	 * @return the node of the next entry of path or <tt>null</tt> if path is empty
	 */
	protected Node node (Stack path) {
		return path.isEmpty()? null: (Node)((Entry)path.peek()).getValue();
	}

	/** Determines the depth of the given path. This is the height of the tree 
	 * if the path is empty or the level of to top node on the stack.
	 * @param path the path whose depth is requested
	 * @return the level of the node on the top of the stack or 
	 * {@link Tree#height()} if the path is empty 
	 */
	protected int level (Stack path) {
		return path.isEmpty()? height(): node(path).level;
	}

	/** Adds the node to which <tt>indexEntry</tt> refers to the path. Let node be the
	 * node to which indexEntry refers. First a new {@link MapEntry} 
	 * is created: 
	 * <pre><code>
	 * 			new MapEntry(indexEntry, node),
	 * </code></pre>
	 * Then it is pushed into the Stack path.
	 * 
	 * @param path a Stack which stores a path of the tree.
	 * NOTE: The Stack's entries must be objects of the {@link java.util.Map.Entry} whereas the nodes
	 * on the path are used as content objects and the index entries as keys. 
	 * @param indexEntry the index entry which has to added to the path
	 * @return the node which is assigned to the indexEntry
	 */
	protected Node down (Stack path, IndexEntry indexEntry) {
		Node node = indexEntry.get(false);
		path.push(new MapEntry(indexEntry, node));
		return node;
	}

	/** Refreshes the top entry of the stack. This entry is an <tt>MapEntry</tt> (<tt>IndexEntry</tt>,<tt>Node</tt>). 
	 * This association is refreshed in the underlaying container by calling {Tree.IndexEntry#update(Node)}.
	 * The stack remains unchanged. 
	 * @param path the stack whose top entry has to be refreshed
	 * @see Tree.IndexEntry#update(Tree.Node)
	 */
	protected void update (Stack path) {
		indexEntry(path).update(node(path), false);
	}
	
	/** Is used to traverse the path bottom-up. One call causes the removal of the 
	 * first path entry from the stack by calling {@link Stack#pop()}. 
	 * If <tt>unfix</tt> is <tt>true</tt> the removed index entry will be unfixed in 
	 * the underlaying container. Finally, the index entry is returned.
	 * 
	 * @param path the path to traverse
	 * @param unfix signals if the node associated to the index entry 
	 * has to be unfixed in the underlaying container
	 * @return the index entry on the top of the path or <tt>null</tt> if the stack was empty
	 */
	protected IndexEntry up (Stack path, boolean unfix) {
		IndexEntry indexEntry = null;

		if (!path.isEmpty()) {
			indexEntry = indexEntry(path);
			if (unfix)
				indexEntry.container().unfix(indexEntry.id());
			path.pop();
		}
		return indexEntry;
	}

	/** Is used to traverse the path bottom-up. One call causes the removal of the 
	 * first path entry from the stack by calling {@link Stack#pop()}. The removed index entry 
	 * is unfixed in the underlaying container. Finally the index entry is returned.
	 * 
	 * @param path the path to traverse	 
	 * @return the index entry on top of the path or <tt>null</tt> if the stack was empty
	 * @see #up(Stack,boolean)
	 */	
	protected IndexEntry up (Stack path) {
		return up(path, true);
	}


	/** This class describes the index entries (i.e. the entries of the non-leaf nodes) 
	 * of a tree. Each index entry refers to a {@link Tree.Node node} which is the root
	 * of the subtree. We call this <tt>Node</tt> the subnode of the <tt>IndexEntry</tt>.
	 */ 
	public class IndexEntry {

		/** Handle used to lookup the node, which the index entry refers to, in its container.
		*/
		protected Object id;

		/** The level of the node this <tt>IndexEntry</tt> is stored in, 
		 *	or the height of the tree, respectively.
		 */
		protected int parentLevel;

		/** Creates a new <tt>IndexEntry</tt> with a given {@link Tree.IndexEntry#parentLevel parent level}.
		 * 
		 * @param parentLevel the parent level of the new <tt>IndexEntry</tt>.
		 */
		public IndexEntry (int parentLevel) {
			this.parentLevel = parentLevel;
		}

		/** Initializes a (new created) <tt>IndexEntry</tt>.
		 * 
		 * @param id the ID (in the underlaying container) of the <tt>Node</tt> 
		 * refered to by the <tt>IndexEntry</tt>
		 * @return the <tt>IndexEntry</tt> itself (i.e. return <tt>this</tt>).
		 */
		public IndexEntry initialize (Object id) {
			this.id = id;
			return this;
		}

		/** Initializes the (new created) <tt>IndexEntry</tt>. The default implementation 
		 * only calls the method {@link Tree.IndexEntry#initialize(Object id)}. 
		 * 
		 * @param id the ID (in the underlaying container) of the <tt>Node</tt> refered to by the <tt>IndexEntry</tt>
		 * @param container the container in which the <tt>Node</tt> referred to by the <tt>IndexEntry</tt> is stored. 
		 * In the default implementation the container is ignored because it is determined by 
		 * {@link Tree#getContainer}.
		 * @return the IndexEntry itself (i.e. return <tt>this</tt>).
		 * @see Tree#getContainer
		 */
		public IndexEntry initialize (Container container, Object id) {
			return initialize(id);
		}

		/** Initializes the (new created) <tt>IndexEntry</tt> using some split information. 
		 * The default implementation does not change the index entry and only 
		 * returns it.
		 * 
		 * @param splitInfo contains information about the split 
		 * which led to create this index entry 
		 * @return the index entry itself
		 */
		public IndexEntry initialize (Node.SplitInfo splitInfo) {
			return this;
		}

		/** Initializes the (new created) <tt>IndexEntry</tt>. The implementation: 
		 * <pre><code>
				initialize(splitInfo);
				return initialize(container, id);
			</code></pre>
		 * 
		 * @param container the container in which the node refered by the <tt>IndexEntry</tt> is stored
		 * @param id the ID (in the underlaying container) of the <tt>Node</tt> referred to by the <tt>IndexEntry</tt>
		 * @param splitInfo contains information about the split 
		 * which led to create this <tt>IndexEntry</tt>
		 * @return the index entry itself
		 * 
		 * @see Tree.IndexEntry#initialize(Tree.Node.SplitInfo)
		 * @see Tree.IndexEntry#initialize(Container,Object)
		 */
		public IndexEntry initialize (Container container, Object id, Node.SplitInfo splitInfo) {
			initialize(splitInfo);
			return initialize(container, id);
		}

		/** Checks if two instances of <tt>IndexEntry</tt> are equal, i.e. 
		 * if they have the same id and their Nodes are stored in the same container.
		 * @param object the <tt>IndexEntry</tt> to which the current <tt>IndexEntry</tt> 
		 * (<tt>this</tt>) has to be compared 
		 * @return <tt>true</tt> if <tt>object</tt> is an <tt>IndexEntry</tt> and fulfills the 
		 * mentioned conditions, <tt>false</tt> otherwise  
		 */
		public boolean equals (Object object) {
			IndexEntry indexEntry;

			if (object==null || !(object instanceof IndexEntry))
				return false;
			indexEntry = (IndexEntry)object;
			return id().equals(indexEntry.id()) && container().equals(indexEntry.container());
		}

		/** Returns the level of the parent of this <tt>IndexEntry</tt>.
		 *  
		 * @return the parent level of the <tt>IndexEntry</tt>
		 * @see Tree.IndexEntry#parentLevel
		 */
		public int parentLevel () {
			return parentLevel;
		}

		/** Returns the id.
		 * 
		 * @return the id of the <tt>IndexEntry</tt>
		 * @see Tree.IndexEntry#id
		 */
		public Object id () {
			return id;
		}

		/** Returns the hashCode of the id of this node. 
		 * 
		 * @return id.hashCode()
		*/
		public int hashCode () {
			return id.hashCode();
		}

		/** The level of the <tt>Node</tt> which the <tt>IndexEntry</tt> refers to. 
		 * That is {@link Tree.IndexEntry#parentLevel}-1.
		 * 
		 * @return {@link Tree.IndexEntry#parentLevel}-1.
		 */
		public int level () {
			return parentLevel-1;
		}

		/** Invokes the <tt>Function</tt> {@link Tree#getContainer} to determine the <tt>Container</tt>
		 * in which the <tt>Node</tt> referred to by this <tt>IndexEntry</tt> is stored.
		 * <pre><code>
		 				return (Container)getContainer.invoke(this);
		  </code></pre>
		 * 
		 * @return the <tt>Container</tt> in which the <tt>Node</tt> referred to by 
		 * this <tt>IndexEntry</tt> is stored
		 */
		public Container container () {
			return (Container)getContainer.invoke(this);
		}

		/** Is used to get the Node to which this IndexEntry refers (subnode). 
		 * 
		 * @param unfix signals whether the <tt>Node</tt> can be removed from the
	 	 *        underlying buffer
		 * @return the <tt>Node</tt> to which this <tt>IndexEntry</tt> refers
		 */
		public Node get(boolean unfix) {
			return (Node)container().get(id(), unfix);
		}

		/** The same as {@link Tree.IndexEntry#get(boolean) get(true)}. 
		 * That means that the node can be removed from the underlaying buffer. 
		 * @return the <tt>Node</tt> referred to by this <tt>IndexEntry</tt>
		 * @see Tree.IndexEntry#get(boolean)
		 */
		public Node get () {
			return get(true);
		}

		/** Overwrites the <tt>Node</tt> refered by the current <tt>IndexEntry</tt> 
		 * by a new <tt>Node</tt>.
		 * 
		 * @param node the new <tt>Node</tt>
		 * @param unfix signals whether the node can be removed from the
	 	 *        underlying buffer
		 */
		public void update (Node node, boolean unfix) {
			container().update(id(), node, unfix);
		}

		/** Overwrites the subnode of the <tt>IndexEntry</tt> by a new <tt>Node</tt>.
		 * The node is unfixed, i.e. it can be removed form the underlying buffer.
		 * 
		 * @param node the new node.
		 * @see Tree.IndexEntry#update(Tree.Node,boolean)
		 */
		public void update (Node node) {
			update(node, true);
		}
		
		/** Removes the node referred to by the current <tt>IndexEntry</tt> from the container.
		 * 
		 */
		public void remove () {
			container().remove(id());
		}

		/** Removes the complete subtree of this <tt>IndexEntry</tt> from the container.
		 * 
		 * @see Tree.IndexEntry#remove()
		 */
		public void removeAll () {
			if (level()>0)
				for  (Iterator entries = ((Node)container().get(id())).entries(); entries.hasNext();) {
					IndexEntry indexEntry = (IndexEntry)entries.next();
					indexEntry.removeAll();
				}
			remove();
		}

		/** Unfixes the node referred to by the current <tt>IndexEntry</tt>. 
		 * After one call of <tt>unfix</tt> the underlaying buffer is allowed to remove 
		 * this <tt>Node</tt>. 
		 */
		public void unfix () {
			container().unfix(id());
		}

		/** Chooses the subtree which should be followed during an insertion operation. 
		 * Let N be the node referred to by the current <tt>IndexEntry</tt>. The method returns the
		 * <tt>IndexEntry</tt> of N which refers to the subtree in which the search has to be continued.
		 * First {@link Tree#down(Stack,Tree.IndexEntry)} is called to put the MapEntry(this, N) 
		 * on the Stack (path). Then {@link Tree.Node#chooseSubtree(Descriptor, Stack)} is called 
		 * by N.chooseSubtree(descriptor,path) to choose the suitable <tt>IndexEntry</tt> of N.
		 * 
		 * @param object the object which is looked for
		 * @param path a stack to store the path from the <tt>IndexEntry</tt> 
		 * at which the search began to the returned one
		 * @return the next <tt>IndexEntry</tt> in whose subtree the search has to be continued
		 */		
		public IndexEntry chooseSubtree (Object object, Stack path) {
			return chooseSubtree(descriptor(object), path);
		}
		
		/** Chooses the subtree which should be followed during an search operation. 
		 * Let N be the subnode of the current <tt>IndexEntry</tt>. The method returns the
		 * <tt>IndexEntry</tt> of N which refers to the subtree in which the search has to be continued.
		 * First {@link Tree#down(Stack,Tree.IndexEntry)} is called to put the MapEntry(this, N) 
		 * on the Stack (path). Then {@link Tree.Node#chooseSubtree(Descriptor, Stack)} is called 
		 * by N.chooseSubtree(descriptor,path) to choose the suitable <tt>IndexEntry</tt> of N.
		 * 
		 * @param descriptor the descriptor which is looked for
		 * @param path a stack to store the path from the <tt>IndexEntry</tt>, 
		 * at which the search began, to the returned one
		 * @return the next <tt>IndexEntry</tt> in whose subtree the search has to be continued
		 */
		public IndexEntry chooseSubtree (Descriptor descriptor, Stack path) {
			return down(path, this).chooseSubtree(descriptor, path);
		}

		/** This method is used to insert a data object into the subnode of the current 
		 * <tt>IndexEntry</tt>. The insertion can lead to an overflow. The method 
		 * treats this problem by calling {@link Tree#post(Stack) Tree#post(path)}
		 * 
		 * @param data the object to insert
		 * @param path a Stack to store the path from the <tt>IndexEntry</tt>, 
		 * at which the operation began, to the subnode of this IndexEntry.
		 */
		public void growAndPost (Object data, Stack path) {
			down(path, this).grow(data, path);
			post(path);
		}
	}//class Index Entry

	/** <tt>Node</tt> is the class used to represent leaf- and non-leaf nodes.
		Nodes are stored in <tt>Container</tt>s.
		@see Tree#determineContainer
		@see Tree#getContainer
	*/
	public abstract class Node {
		
		/** <tt>SplitInfo</tt> contains information about a split. The enclosing
			object of this SplitInfo-Object (i.e. <tt>Node.this</tt>) is the new node
			that was created by the split.
		*/
		public class SplitInfo {

			/** The path including the split node.
			*/
			public Stack path;

			/** Creates a new SplitInfo.
			 * 
			 * @param path path to the split node
			 */
			public SplitInfo (Stack path) {
				this.path = path;
			}

			/** Returns the enclosing object (i.e. <tt>Node.this</tt>: the node that was			
			* created by the split).
			*
			* @return <code>Node.this</code>
			*/
			public Node newNode () {
				return Node.this;
			}
			
			/** Determines the container which the new <tt>Node</tt> ist stored in.
			 * 
			 * @return the <tt>Container</tt> which the new <tt>Node</tt> ist stored in
			 */
			public Container determineContainer () {
				return (Container)Tree.this.determineContainer.invoke(this);
			}
		}// class SplitInfo

		/** The maximum distance to the leaves of the subtree (i.e.
			for leaf level == 0).
		*/
		public int level;

		/** Initializes the <tt>Node</tt> by setting it's level. 
		 * 
		 * @param level the level of the <tt>Node</tt>
		 * @return the <tt>Node</tt> itself
		 */
		public Node initialize (int level) {
			this.level = level;
			return this;
		}

		/** Initializes the <tt>Node</tt> by inserting a new entry into it. 
		 * 
		 * @param entry the entry which has to be inserted
		 * @return SplitInfo which contains information about a possible split
		 */
		protected abstract SplitInfo initialize (Object entry);

		/** Treats overflows which occur during an insertion. 
		 * It splits the overflowing node in new nodes and creates new index-entries for 
		 * the new nodes. These index-entries will be returned as a <tt>Collection</tt>. 
		 * The top <tt>Node</tt> on stack will be written into the storage and unfixed from 
		 * the underlaying buffer. The method {@link Tree.Node#post(Tree.Node.SplitInfo, Tree.IndexEntry)} will 
		 * be used to post the created index-entries into the parent node. 
		 * @param path the path from the root to the overflowing node
		 * @return a Collection containing all index-entries created by the split
		 */
		protected Collection redressOverflow (Stack path) {
			return redressOverflow(path, true);
		}

		/** Treats overflows which occur during an insertion. 
		 * It splits the overflowing node in new nodes and creates new index-entries for 
		 * the new nodes. These index-entries will be returned as a <tt>Collection</tt>. 
		 * The method {@link Tree.Node#post(Tree.Node.SplitInfo, Tree.IndexEntry)} will 
		 * be used to post the created index-entries into the parent node. 
		 * @param path the path from the root to the overflowing node
		 * @param up signals whether the top <tt>Node</tt> on the stack will be written into the 
		 * storage by calling {@link Tree#update(Stack)} and unfixed from the underlaying buffer.  
		 * @return a Collection containing all index-entries created by the split
		 */
		protected Collection redressOverflow (Stack path, boolean up) {
			return redressOverflow(path, new LinkedList(), up);
		}

		/** Treats overflows which occur during an insertion. 
		 * It splits the overflowing node in new nodes and creates new index-entries for the new nodes. 
		 * These index-entries will be added to the given <tt>List</tt>. 
		 * The top node on stack will be written into the 
		 * storage and unfixed from the underlaying buffer.
		 * The method {@link Tree.Node#post(Tree.Node.SplitInfo, Tree.IndexEntry)} will be used to post the 
		 * created index-entries to the parent node. 
		 * @param path the path from the root to the overflowing node
		 * @param newIndexEntries a <tt>List</tt> to carry the new index-entries created by the split
		 * @return a <tt>Collection</tt> which accrues from the given collection and the created 
		 * index-entries during the split.
		 */
		protected Collection redressOverflow (Stack path, List newIndexEntries) {
			return redressOverflow(path, newIndexEntries, true);
		}

		/** Treats overflows which occur during an insertion. 
		 * It splits the overflowing node in new nodes. It creates new index-entries for the new Nodes. These 
		 * index-entries will be added to the given <tt>List</tt>. 
		 * The method {@link Tree.Node#post(Tree.Node.SplitInfo, Tree.IndexEntry)} will be used to post the created index-entries to
		 * the parent Node. 
		 * @param path the path from the root to the overflowing node
		 * @param parentNode the parent node of the overflowing node
		 * @param newIndexEntries a <tt>List</tt> to carry the new index-entries created by the split
		 * @return a <tt>SplitInfo</tt> containing requiered information about the split
		 */
		protected SplitInfo redressOverflow (Stack path, Node parentNode, List newIndexEntries) {
			IndexEntry indexEntry = indexEntry(path);
			SplitInfo splitInfo = createNode(level).split(path);
			IndexEntry newIndexEntry = createIndexEntry(parentNode.level).initialize(splitInfo);
			Node newNode = splitInfo.newNode();
			Container container = splitInfo.determineContainer();

			parentNode.post(splitInfo, newIndexEntry);

			if (overflows())
				redressOverflow(path, parentNode, newIndexEntries);

			if (newNode.overflows()) {
				MapEntry pathEntry = (MapEntry)path.peek();

				pathEntry.setKey(newIndexEntry);
				pathEntry.setValue(newNode);
				newNode.redressOverflow(path, parentNode, newIndexEntries);
				pathEntry.setKey(indexEntry);
				pathEntry.setValue(this);
			}
			newIndexEntry.initialize(container, container.insert(newNode));
			newIndexEntries.add(newIndexEntries.size(), newIndexEntry);
			return splitInfo;
		}

		/** Treats overflows which occur during an insertion. 
		 * It splits the overflowing node in new nodes. It creates new index-entries 
		 * for the new nodes. These index-entries will be be added to the given <tt>List</tt>. 
		 * The method {@link Tree.Node#post(Tree.Node.SplitInfo, Tree.IndexEntry)} will be used to post the 
		 * created index-entries to the parent node. 
		 * @param path the path from the root to the overflowing node
		 * @param newIndexEntries a <tt>List</tt> to carry the new index-entries created by the split
		 * @param up signals whether the top node on stack will be written into the 
		 * storage by calling {@link Tree#update(Stack path)} and unfixed from the underlaying buffer 
		 * by calling {@link Tree#up(Stack path)}. 
		 * @return a Collection which accrues from the given collection and the created 
		 * index-entries during the split.
		 */
		protected Collection redressOverflow (Stack path, List newIndexEntries, boolean up) {
			if (overflows()) {
				Node parentNode;
				MapEntry pathEntry = (MapEntry)path.pop();
				if (path.isEmpty())
					path.push(Tree.this.grow(rootEntry()));
				parentNode = node(path);
				path.push(pathEntry);
				redressOverflow(path, parentNode, newIndexEntries);
			}
			if (up) {
				update(path);
				up(path);
			}
			return newIndexEntries;
		}

		/** Gives the level of this Node.
		 * @return the level the node is located on
		 */
		public int level () {
			return level;
		}

		/** The number of the entries that are currently stored
		 * in this Node.
		 * @return the number of entries that are currently stored
		 * in this <tt>Node</tt>.
		 */
		public abstract int number ();

		/** Returns an iterator pointing to all entries stored in this node. 
		 * @return an iterator pointing to all entries stored in this node.
		 */
		public abstract Iterator entries ();

		/** Returns an iterator pointing to the descriptors of each entry in this node.
			@param nodeDescriptor the descriptor of this node
			@return an Iterator pointing to the descriptors of each entry in this node
		*/
		public abstract Iterator descriptors (Descriptor nodeDescriptor);

		/** Returns an Iterator pointing to entries whose descriptors overlap the queryDescriptor 
		 * @param queryDescriptor the descriptor describing the query
		 * @return an Iterator pointing to entries whose descriptors overlap the <tt>queryDescriptor</tt>
		*/
		public abstract Iterator query (Descriptor queryDescriptor);

		/** Chooses the subtree which is followed during an insertion.
		 * @param descriptor the <tt>Descriptor</tt> of data onject
		 * @param path the path from the root to the current node 
		 * @return the index entry refering to the root of the chosen subtree
		 */
		protected abstract IndexEntry chooseSubtree (Descriptor descriptor, Stack path);
	
		/** Chooses the subtree which is followed during the insertion of object.
		  * @param object the object to be inserted. Its descriptor is used to choose the subtree
		  * @param path the path from the root to the current node
		  * @see Tree.Node#chooseSubtree(Descriptor,Stack)
		  * @return the index entry refering to the root of the chosen subtree
		  */
		protected IndexEntry chooseSubtree (Object object, Stack path) {
			return chooseSubtree(descriptor(object), path);
		}

		/** Chooses the subtree which is followed during an insertion.
		  * @param descriptor the Descriptor of data object
		  * @param path the path from the root to this node
		  * @param isValidEntry Function (IndexEntry&rarr;Boolean) that determines whether 
		  * an <tt>indexEntry</tt> may be chosen.
		  * @return the index entry refering to the root of the chosen subtree
		  */
		protected IndexEntry chooseSubtree (Descriptor descriptor, Stack path, Function isValidEntry) {
			return chooseSubtree(descriptor, path);
		}

		/** Chooses the subtree which is followed during an insertion.
		 * @param object the object to be inserted. Its descriptor is used to choose the subtree
		 * @param path the path from the root to this node
		 * @param isValidEntry Function (IndexEntry&rarr;Boolean) that determines whether an 
		 * <tt>indexEntry</tt> may be chosen
		 * @return the index entry refering to the root of the chosen subtree  
		 */
		protected IndexEntry chooseSubtree (Object object, Stack path, Function isValidEntry) {
			return chooseSubtree(descriptor(object), path, isValidEntry);
		}

		/** Chooses the subtree which is followed during an insertion.
		  *	@param descriptor the Descriptor of the data object
		  *	@param path the path from the root to this node
		  *	@param isValidEntry <tt>Predicate</tt> (IndexEntry&rarr;boolean) that determines whether an 
		  * <tt>indexEntry</tt> may be chosen
		  * @return the index entry refering to the root of the chosen subtree
		  */
		protected IndexEntry chooseSubtree (Descriptor descriptor, Stack path, Predicate isValidEntry) {
			return chooseSubtree(descriptor, path);
		}

		/** Chooses the subtree which is followed during an insertion.
		  * @param object the object to be inserted. Its descriptor is used to choose the subtree
		  *	@param path the path from the root to this node
		  *	@param isValidEntry <tt>Predicate</tt> (IndexEntry&rarr;boolean) that determines whether an 
		  * <tt>indexEntry</tt> may be chosen
		  * @return The index-entry refering to the root of the chosen subtree
		  */
		protected IndexEntry chooseSubtree (Object object, Stack path, Predicate isValidEntry) {
			return chooseSubtree(descriptor(object), path, isValidEntry);
		}

		/** Inserts an entry into the current node. If level>0 <tt>data</tt> must be an 
		 * index entry.
		 * @param data the entry which has to be inserted into the node
		 * @param path the path from the root to the current node
		 */
		protected abstract void grow (Object data, Stack path);

		/** This method is used to split the next node on the stack in two nodes. 
		 * The current node should be the empty new <tt>Node</tt>. 
		 * The method distributes the entries of the overflowing <tt>Node</tt> to the both nodes.
		 * @param path the nodes already visited during this insert
		 * @return a SplitInfo containig all needed information about the split
		*/
		protected abstract SplitInfo split (Stack path);

		/** Updates the current node with the split information 
		 * e.g. inserts the new index entry of the new node.
		 * @param splitInfo the information created by split
		 * @param newIndexEntry the new index-entry refering to the 
		 * new node created during the split
		*/
		protected abstract void post (SplitInfo splitInfo, IndexEntry newIndexEntry);

		/** Returns <tt>true</tt> if the node contains fewer elements than the underflow conditions allow
		 * 
		 * @return <tt>true</tt> if the node contains fewer elements than the underflow conditions allow,
		 * <tt>false</tt> otherwise
		 */
		protected boolean underflows () {
			return Tree.this.underflows.invoke(this);
		}

		/** Returns <tt>true</tt> if the node contains more elements than the overflow conditions allow
		 * 
		 * @return <tt>true</tt> if the node contains more elements than the overflow conditions allow,
		 * <tt>false</tt> otherwise
		*/
		protected boolean overflows () {
			return Tree.this.overflows.invoke(this);
		}

		/** Calls the function <tt>getSplitMinRatio</tt> and cast the return value to <tt>double</tt>.
		 * 
		 * @return the minimal relative number of entries which the node may 
		 * contain after a split
		 * 
		 * @see Tree#getSplitMinRatio
		*/
		protected double splitMinRatio () {
			return ((Double)getSplitMinRatio.invoke(this)).doubleValue();
		}

		/** Calls the function <tt>getSplitMaxRatio</tt> and cast the return value to <tt>double</tt>.
		 * 
		 * @return the maximal relative number of entries which the node may 
		 * contain after a split.
		 * 
		 * @see Tree#getSplitMaxRatio
		*/
		protected double splitMaxRatio () {
			return ((Double)getSplitMaxRatio.invoke(this)).doubleValue();
		}

		/** Uses the method <tt>splitMinRatio</tt> to find out the minimal number 
		 * of entries which the node may contain after a split.
		 * 
		 * @return the minimal number of entries which the node may 
		 * contain after a split
		 */
		protected int splitMinNumber () {
			return (int)Math.ceil(number()*splitMinRatio());
		}

		/** Uses the method <tt>splitMaxRatio</tt> to find out the maximal number 
		 * of entries which the node may contain after a split.
		 * 
		 * @return the maximal number of entries which the node may 
		 * contain after a split
		 */
		protected int splitMaxNumber () {
			return (int)Math.floor(number()*splitMaxRatio());
		}

	}//class Node

	/** This method is an implemtation of an efficient querying algorithm. 
	 * The result is a lazy <tt>Cursor</tt> pointing to all objects whose descriptors 
	 * overlap the given <tt>queryDescriptor</tt>. 
	 * @param queryDescriptor describes the query in terms of a descriptor
	 * @param targetLevel the tree-level to provide the answer-objects
	 * @return a lazy Cursor pointing to all response objects
	*/
	public Cursor query (final Descriptor queryDescriptor, final int targetLevel) {
		final Iterator [] iterators = new Iterator[height()+1];

		Arrays.fill(iterators, EmptyCursor.DEFAULT_INSTANCE);
		if (height()>0 && queryDescriptor.overlaps(rootDescriptor()))
			iterators[height()] = new SingleObjectCursor(rootEntry());
		return new IteratorCursor(new Iterator () {
			public boolean hasNext () {
				for (int parentLevel = targetLevel;;)
					if (iterators[parentLevel].hasNext())
						if (parentLevel==targetLevel)
							return true;
						else {
							IndexEntry indexEntry = (IndexEntry)iterators[parentLevel].next();

							if (indexEntry.level()>=targetLevel) {
								Node node = indexEntry.get(true);

								Iterator queryIterator = node.query(queryDescriptor);

								iterators[parentLevel = node.level] =
									iterators[parentLevel].hasNext()?
										new Sequentializer(queryIterator, iterators[parentLevel]):
										queryIterator;
							}
						}
					else
						if (parentLevel==height())
							return false;
						else
							iterators[parentLevel++] = EmptyCursor.DEFAULT_INSTANCE;
			}

			public Object next () throws NoSuchElementException {
				if (!hasNext())
					throw new NoSuchElementException();
				return iterators[targetLevel].next();
			}

			public void remove () throws UnsupportedOperationException {
				throw new UnsupportedOperationException();
			}
		});
	}

	/** This method is an implemtation of an efficient querying algorithm. 
	 * The result is a lazy cursor pointing to all leaf entries whose descriptors 
	 * overlap with the given <tt>queryDescriptor</tt>. 
	 * @param queryDescriptor describes the query in terms of a descriptor
	 * @return a lazy <tt>Cursor</tt> pointing to all response objects
	 * @see Tree#query(Descriptor, int)
	*/	
	public Cursor query (Descriptor queryDescriptor) {
		return query(queryDescriptor, 0);
	}

	/** This method executes a query unsing the rootDescriptor on a given level. 
	 * That means, that the respose consists of all entries of the given level.
	 * @param level the target level of the query
	 * @return a lazy <tt>Cursor</tt> pointing to all response objects
	*/
	public Cursor query (int level) {
		return query(rootDescriptor(), level);
	}

	/** This method executes a query unsing the rootDescriptor on the leaf level. 
	 * That means, that the respose consists of all leaf entries (i.e. data objects) 
	 * stored in the tree.
	 * @return a lazy Cursor pointing to all response objects
	*/
	public Cursor query () {
		return query(rootDescriptor(), 0);
	}

	/** This method executes a query using the descriptor of the given 
	 * data object on the leaf-level. Thereafter the result of the query is filtered 
	 * by the following Predicate:
	 * <pre><code>
	  			new AbstractPredicate() {
	  				public boolean invoke (Object entry) {
	  					return entry.equals(data);
	  				}
	  			}
	  </code><pre>
	 * That means, that the respose consists of all leaf entries (i.e. data objects) which equal the
	 * given data object and whose descriptors overlap with its descriptor.
	 * 
	 * @param data object to search for
	 * @return a lazy <tt>Cursor</tt> pointing to all response objects
	*/	
	public Cursor query (final Object data) {
		return new Filter (query(descriptor(data)),
			new AbstractPredicate() {
				public boolean invoke (Object entry) {
					return entry.equals(data);
				}
			}
		);
	}
	
	/** Query to a given level using a priority queue. The method returns a new Query object.
	 * <pre><code>
	  			return new Query(queue,level);
	   </code></pre>
	 * @param queue the priority queue
	 * @param level the target level of the query
	 * @return a lazy <tt>Iterator</tt> pointing to all response objects. The order is defined by the queue
	 * @see Tree.Query
	 */
	public Iterator query(Queue queue,int level){
		return new Query(queue,level);
	}

	/** Query to the leaf level using a priority queue.
	 * 
	 * @param queue the priority queue
	 * @return a lazy <tt>Iterator</tt> pointing to all response objects. The order is defined by the queue.
	 * @see Tree.Query
	 */
	public Iterator query(Queue queue){
		return query(queue,0);
	}

	/** Queries on the Tree are evaluated lazily (ONC). This means when
		<tt>query</tt> is called a <tt>QueryIterator</tt> is returned. No work has
		been done yet.
		<br>
		Idea: start with the root-node: access all entries, select those
			entries that are important for the query and insert
			them into a priority queue. If the top-element of
			the priority queue is a data-page an answer has been
			found, else an index entry has been found. In that
			case the node referenced by that index entry has to
			be read and its nodes have to be assessed
			recursively.
		<br>
		This is a generalization of the incremental nearest-neighbor-search
		algorithm (Henrich 94, Hjaltason and Samet 95) and greatly
		facilitates the implementation of arbitrary query types on
		arbitrary trees.
		<br>
	*/
	public class Query extends AbstractCursor {

		/** A <tt>Candidate</tt> is an entry of the priority queue used for
			queries.
		*/
		public class Candidate {

			/** The entry of the <tt>Candidate</tt>.
			*/
			protected final Object entry;

			/** The descriptor of the entry
			*/
			protected final Descriptor descriptor;

			/** The level of the node the entry is stored in (height of the tree, respectively).
			*/
			protected final int parentLevel;

			/** Creates a new Candidate.
			 * 
			 * @param entry the new {link #entry}
			 * @param descriptor the new {link #descriptor}
			 * @param parentLevel the new {link #parentLevel}
			 */
			public Candidate (Object entry, Descriptor descriptor, int parentLevel) {
				this.entry = entry;
				this.descriptor = descriptor;
				this.parentLevel = parentLevel;
			}

			/** Returns the entry of the <tt>Candidate</tt>.
			 * 
			 * @return the entry of the <tt>Candidate</tt>
			 */
			public Object entry () {
				return entry;
			}

			/** Returns the descriptor of the <tt>Candidate</tt>.
			 * 
			 * @return the descriptor of the <tt>Candidate</tt>
			 */
			public Descriptor descriptor () {
				return descriptor;
			}

			/** Returns the level of the parent of the <tt>Candidate</tt>.
			 * 
			 * @return the level of the parent of the <tt>Candidate</tt>
			 */
			public int parentLevel () {
				return parentLevel;
			}

			/* (non-Javadoc)
			 * @see java.lang.Object#toString()
			 */
			public String toString(){
				StringBuffer sb =new StringBuffer(" Candidate \n");
				if(entry instanceof IndexEntry)sb.append("entry : id "+((IndexEntry)entry).id()+"  parentlevel "+((IndexEntry)entry).level());
				sb.append("\n  descriptor "+descriptor+"  \n level "+parentLevel);
				return new String(sb);
			}

		}// class Candidate

		/** The (priority-)queue used to store candidates that
		 *	passed the selection-function.
		 */
		protected Queue queue;

		/** The level on which the query must stop and return the qualified entries
		 * on this level. For example: targetLevel==0 &rarr; The query stops on the leaf level 
		 * and returns only the qualified leaf-entries.  
		*/
		protected int targetLevel;
		
		/**
		 * If <tt>true</tt> all entries until target level are returned. Otherwise, only target
		 * level entries are returned.
		 */
		protected boolean returnAllEntries;

		/** 
		 * Helper attribute.
		 */
		protected Candidate nextCandidate;
		
		public Query (Queue queue, int targetLevel, boolean returnIndexAndLeafEntries) {
			this.queue = queue;
			this.targetLevel = targetLevel;
			this.returnAllEntries = returnIndexAndLeafEntries;
			this.nextCandidate = null;

			//initialize the Queue: assess root-node and insert it into queue
			if (rootEntry()!=null)
				queue.enqueue(createCandidate(null, rootEntry(), rootDescriptor(), height()));
		}
			
		/**Creates a new <tt>Query</tt>.
		 * 
		 * @param queue the priority queue
		 * @param targetLevel the target level of the query
		 */
		public Query (Queue queue, int targetLevel) {
			this(queue, targetLevel, false);
		}
		
		/** Creates an new <tt>Candidate</tt>
		 * @param parent the parent of the <tt>Candidate</tt>
		 * @param entry the entry of the <tt>Candidate</tt>
		 * @param descriptor the descriptor of the <tt>Candidate</tt>
		 * @param level the level of the <tt>Candidate</tt>
		 * @return the new <tt>Candidate</tt>
		 */
		public Candidate createCandidate (Candidate parent, Object entry, Descriptor descriptor, int level) {
			return new Candidate(entry, descriptor, level);
		}

		/** Expands Candidates stored in a node.
		 *  
		 * @param parent the parant candidate
		 * @param node the node to expand
		 * @return an interator containing the new candidates 
		 */
		protected Iterator expand (final Candidate parent, final Node node) {
			return new Mapper(
				new AbstractFunction() {
					public Object invoke (Object entry, Object descriptor) {
						return createCandidate(parent, entry, (Descriptor)descriptor, node.level);
					}
				}
			,node.entries(), node.descriptors(parent.descriptor()));
		}


		/** Checks whether there exists a next result
		 * @return <tt>true</tt> if the Query contains more objects
		 */
		public boolean hasNextObject () {
			for (; !queue.isEmpty() && ((Candidate)queue.peek()).parentLevel!=targetLevel;) {
				Candidate candidate = (Candidate)queue.dequeue();
				Node node = ((IndexEntry)candidate.entry()).get(true);
				if (node.level>=targetLevel)
					Queues.enqueueAll(queue, expand(candidate, node));
				if (returnAllEntries) {
					nextCandidate = candidate;
					break;
				}
			}
			return !queue.isEmpty();
		}

		/** Returns the next result of the query.
		 * 
		 * @return the next result of the query
		 * @throws NoSuchElementException
		*/
		public Object nextObject() throws NoSuchElementException {
			if (nextCandidate != null) {
				Object res = nextCandidate;
				nextCandidate = null;
				return res;
			}
			return queue.dequeue();
		}

	}// class Query
}
