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
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Stack;

import xxl.core.collections.MapEntry;
import xxl.core.collections.containers.Container;
import xxl.core.cursors.AbstractCursor;
import xxl.core.cursors.Cursor;
import xxl.core.cursors.filters.Filter;
import xxl.core.cursors.mappers.Mapper;
import xxl.core.cursors.sources.EmptyCursor;
import xxl.core.cursors.sources.SingleObjectCursor;
import xxl.core.cursors.unions.Sequentializer;
import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;
import xxl.core.io.converters.Converter;
import xxl.core.io.converters.IntegerConverter;
import xxl.core.io.converters.ShortConverter;
import xxl.core.predicates.AbstractPredicate;
import xxl.core.predicates.Predicate;

/** The class <tt>ORTree</tt> (Overlapping Region Tree) is a generic and highly flexible 
 * super-class that implements features used by grow- and post-trees which stores
 * objects with descriptors which may overlap.
 * 
 * @see Tree
 */
public abstract class ORTree extends Tree {
	
	/** This is the basic method to create a new tree. It is not sufficient to call the constructor. 
	 * Before the tree is usable, it must be initialized.
	 * 
	 * @param rootEntry the new {@link Tree#rootEntry}
	 * @param getDescriptor the new {@link Tree#getDescriptor}
	 * @param getContainer the new {@link Tree#getContainer}
	 * @param determineContainer the new {@link Tree#determineContainer}
	 * @param underflows the new {@link Tree#underflows}
	 * @param overflows the new {@link Tree#overflows}
	 * @param getSplitMinRatio the new {@link Tree#getSplitMinRatio}
	 * @param getSplitMaxRatio the new {@link Tree#getSplitMaxRatio}
	 * @return the initialized tree (i.e. return this)
	 * @see Tree#initialize (Tree.IndexEntry, Descriptor, Function, Function, Function, Predicate, Predicate, Function, Function)
	 */
	public ORTree initialize (IndexEntry rootEntry, Function getDescriptor, Function getContainer, Function determineContainer, Predicate underflows, Predicate overflows, Function getSplitMinRatio, Function getSplitMaxRatio) {
		return (ORTree)super.initialize(rootEntry, rootEntry==null? null: rootEntry.descriptor(), getDescriptor, getContainer, determineContainer, underflows, overflows, getSplitMinRatio, getSplitMaxRatio);
	}
	
	/** Initializes the tree. Both Functions <tt>getContainer</tt> and <tt>determineContainer</tt> are 
	 * initialized with	constant functions: &rarr; <tt>Container</tt> which always return the given container.
	 * The predicate {@link Tree#underflows} is initialized with:
	 * <pre><code>
	  		new AbstractPredicate() {
	  			public boolean invoke (Object node) {
		  			return ((Tree.Node)node).number() &lt; minCapacity; //or &gt; in case of overflow
			  	}
				}
	 	 </code></pre> 
	 * {@link Tree#getSplitMinRatio} is defined as:
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
	 * @param getDescriptor the new {@link Tree#getDescriptor}
	 * @param container used as constant for {@link Tree#determineContainer}
	 * @param minCapacity is used to define {@link Tree#underflows}, {@link Tree#getSplitMinRatio} 
	 * 				and {@link Tree#getSplitMaxRatio}
	 * @param maxCapacity is used to define {@link Tree#overflows}
	 * @return the initialized tree 
	 */
	public ORTree initialize (IndexEntry rootEntry, Function getDescriptor, Container container, final int minCapacity, final int maxCapacity) {
		return (ORTree)super.initialize(rootEntry, rootEntry==null? null: rootEntry.descriptor(), getDescriptor, container, minCapacity, maxCapacity);
	}

	/**
	 * Calls {@link Tree#initialize(Tree.IndexEntry, Descriptor, Function, Container, int, int, int, int)} with
	 * the root descriptor.
	 * 
	 * @param rootEntry the new {@link Tree#rootEntry}
	 * @param getDescriptor the new {@link Tree#getDescriptor}
	 * @param container used as constant for {@link Tree#determineContainer}
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
	public ORTree initialize (IndexEntry rootEntry, Function getDescriptor, Container container, final int innerNodeMinCapacity, final int innerNodeMaxCapacity, final int leafMinCapacity, final int leafMaxCapacity) {
		return (ORTree)super.initialize(rootEntry, rootEntry==null? null: rootEntry.descriptor(), getDescriptor, container, innerNodeMinCapacity, innerNodeMaxCapacity, leafMinCapacity, leafMaxCapacity);
	}
	
	/**
	 * Before the tree is usable, it must be initialized. This version of initialize
	 * takes basic informations about the size of the blocks and entries and calculates
	 * the capacity of inner and leaf nodes. 
	 * 
	 * @param getDescriptor the new {@link Tree#getDescriptor}
	 * @param container the new {@link Tree#determineContainer}
	 * @param blockSize the size of a block of the <code>container</code> in bytes
	 * @param dataSize the size of an data object in bytes
	 * @param descriptorSize the size of a Descriptor returned by <code>getDescriptor</code> in bytes
	 * @param minMaxFactor the quotient between minimum and maximum number of entries in an node, e.g. 0.5
	 * @return the initialized tree 
	 * 
	 * @since 1.1
	 */
	public ORTree initialize (Function getDescriptor, Container container, int blockSize, int dataSize, int descriptorSize, double minMaxFactor) {
		return (ORTree)super.initialize(getDescriptor, blockSize, container, dataSize, descriptorSize, minMaxFactor);	
	}

	public ORTree initialize (IndexEntry rootEntry, Function getDescriptor, Container container, int blockSize, int dataSize, int descriptorSize, double minMaxFactor) {
		return (ORTree)super.initialize(rootEntry, rootEntry==null? null: rootEntry.descriptor(), getDescriptor, blockSize, container, dataSize, descriptorSize, minMaxFactor);	
	}
	
	/** Removes the object from the target level whose descriptor equals the given descriptor.
	 * 
	 * @param descriptor to specify the object
	 * @param targetLevel the level on which the search has to stop
	 * @return the removed object
	 */  
	public Object remove (final Descriptor descriptor, int targetLevel) {
		return remove(descriptor, targetLevel,
			new AbstractPredicate() {
				public boolean invoke (Object object) {
					return descriptor.equals(descriptor(object));
				}
			}
		);
	}
	
	/** Returns the descriptor of a given entry. If this entry is an index entry its descriptor
	 * is returned, else the method {@link Tree#descriptor(Object)} is used to determine the 
	 * descriptor of the entry.
	 * @param entry the entry whose descriptor is to be determined
	 * @return the descriptor of the entry
	 * @see Tree#descriptor(Object)
	 */
	public Descriptor descriptor (Object entry) {
		return (entry instanceof IndexEntry)?
			((IndexEntry)entry).descriptor():
			super.descriptor(entry);
	}

	/** Computes the union of the descriptors of the collection's entries.
	 * The method {@link ORTree#descriptor(Object)} is used to determine the 
	 * descriptors of these entries.
	 * 
	 * @param collection a collection of objects
	 * @return the union of the descriptors of the collection's entries
	 * @see ORTree#descriptor(Object)
	 */
	public Descriptor computeDescriptor (Collection collection) {
		Descriptor descriptor = null;
		Iterator entries = collection.iterator();

		if (entries.hasNext()) {
			descriptor = (Descriptor)descriptor(entries.next()).clone();
			while (entries.hasNext())
				descriptor.union(descriptor(entries.next()));
		}
		return descriptor;
	}
	
	/** Creates a new IndexEntry.
	 * @param parentLevel the level of the node in which the new indexEntry is stored
	 * @return a new indexEntry
	 * @see ORTree.IndexEntry
	 */	
	public Tree.IndexEntry createIndexEntry (int parentLevel) {
		return new IndexEntry(parentLevel);
	}
	
	/** Creates a new <tt>Node</tt> which does not have any information except its level.
	 * @param level the level of this node (i.e. the maximum distance 
	 * to the leaves of the subtree)
	 * @return a new <tt>Node</tt>
	 * @see ORTree.Node
	 */
	public abstract Tree.Node createNode (int level);

	/** This class describes the index entries (i.e. the entries of the non-leaf nodes) 
	 * of a tree. Each index entry refers to a {@link Tree.Node node} which is the root
	 * of the subtree. We call this node the subnode of the index entry.
	 * @see Tree.IndexEntry
	 */ 
	public class IndexEntry extends Tree.IndexEntry {
		
		/** The descriptor of the subtree of this index entry.
		 */
		protected Descriptor descriptor;
		
		/** Creates a new <tt>IndexEntry</tt> with a given {@link Tree.IndexEntry#parentLevel}.
		 * 
		 * @param parentLevel the parent level of the new <tt>IndexEntry</tt>
		 */
		public IndexEntry (int parentLevel) {
			super(parentLevel);
		}


		/** Initializes the (new) IndexEntry using some split information. 
		 * The default implementation calls 
		 * {@link Tree.IndexEntry#initialize(Tree.Node.SplitInfo) super.initialize(splitInfo)} 
		 * and initializes the descriptor of the index entry by 
		 * {ORTree.Node.SplitInfo#newDescriptor() the new descriptor of the splitInfo}.
		 * 
		 * @param splitInfo contains information about the split 
		 * which led to create this index entry
		 * @return the initialized index entry itself
		 * @see Tree.IndexEntry#initialize(Tree.Node.SplitInfo)
		 * @see ORTree.Node.SplitInfo#newDescriptor()
		 */
		public Tree.IndexEntry initialize (Tree.Node.SplitInfo splitInfo) {
			super.initialize(splitInfo);
			return initialize(((Node.SplitInfo)splitInfo).newDescriptor());
		}
		
		/** Initializes the descriptor of the index entry by the given descriptor.
		 * 
		 * @param descriptor the new descriptor of the index entry
		 * @return the initialized index entry itself
		 */
		public Tree.IndexEntry initialize (Descriptor descriptor) {
			this.descriptor = descriptor;
			return this;
		}
		
		/** Gets the descriptor of the index entry.
		 * 
		 * @return the descriptor of the index entry
		 */
		public Descriptor descriptor () {
			return descriptor;
		}
	}
	
	/** <tt>Node</tt> is the class used to represent leaf- and non-leaf nodes of <tt>ORTree</tt>.
	 *	Nodes are stored in containers.
	 *
	 *	@see Tree#determineContainer
	 *	@see Tree#getContainer
	 *	@see Tree.Node
	 */
	public abstract class Node extends Tree.Node {
		
		/** SplitInfo contains information about a split. The enclosing
		 * Object of this SplitInfo-Object (i.e. Node.this) is the new node
		 * that was created by the split.
		*/
		public class SplitInfo extends Tree.Node.SplitInfo {
			
			/**
			 * The descriptor of the new node created during the split.
			 */
			protected Descriptor newDescriptor = rootDescriptor();
			
			/** Creates a new <tt>SplitInfo</tt> with a given path.
			 * @param path the path from the root to the splitted node
			 */
			public SplitInfo (Stack path) {
				super(path);
			}
			
			/** Initializes the SplitInfo by setting the descriptor of 
			 * the new node.
			 * 
			 * @param newDescriptor the descriptor of the new node
			 * @return the initialized <tt>SplitInfo</tt>
			 */
			public SplitInfo initialize (Descriptor newDescriptor) {
				this.newDescriptor = newDescriptor;
				return this;
			}
			
			/** Gets the descriptor of the new node
			 * 
			 * @return the descriptor of the new node
			 */
			public Descriptor newDescriptor () {
				return newDescriptor;
			}
		}
		
		/** The entries collection of this node.
		 */
		protected Collection entries;
		
		/** Initializes the node with a level and new entries.
		 * 
		 * @param level the node's level
		 * @param entries the node's entries
		 * @return the initialized node
		 */
		public Node initialize (int level, Collection entries) {
			super.initialize(level);
			this.entries = entries;
			return this;
		}
		
		/** Initializes the node by inserting a new entry. 
		 * 
		 * @param entry the entry wto inserted
		 * @return SplitInfo contains information about a possible split
		 */
		public Tree.Node.SplitInfo initialize (Object entry) {
			Stack path = new Stack();
			if (height()>0) {
				IndexEntry indexEntry = (IndexEntry)entry;
				indexEntry.descriptor = (Descriptor)indexEntry.descriptor.clone();
			}
			grow(entry, path);
			return createSplitInfo(path);
		}

		/** Creates a new SplitInfo with a given path.
		 * 
		 * @param path the path from the root to the splitted node
		 * @return a new instance of <tt>SplitInfo</tt>
		 */
		protected SplitInfo createSplitInfo (Stack path) {
			return new SplitInfo(path);
		}
		
		/** The number of entries that are currently stored
		 * in this node.
		 * @return the number of entries that are currently stored
		 * in this node
		*/
		public int number () {
			return entries.size();
		}
		
		/** Gets an iterator pointing to all entries that are currently stored
		 * in this node.
		 * @return an Iterator pointing to all the entries of this node
		 */
		public Iterator entries () {
			return entries.iterator();
		}
		
		/** Gets an iterator pointing to the descriptors of each entry in this node.
		 * 
		 * @param nodeDescriptor the descriptor of this node. The default implementation 
		 * ignores this parameter
		 * @return an iterator pointing to the descriptors of each entry in this node
		 */		
		public Iterator descriptors (Descriptor nodeDescriptor) {
			return new Mapper(
				level==0 ?
					getDescriptor :
					new AbstractFunction() {
						public Object invoke (Object entry) {
							return ((IndexEntry)entry).descriptor;
						}
					},entries()
			);
		}
		
		/** Returns an iterator pointing to entries whose descriptors overlap <tt>queryDescriptor</tt>. 
		 * @param queryDescriptor the descriptor describing the query
		 * @return an iterator pointing to entries whose descriptors overlap with <tt>queryDescriptor</tt>
		*/
		public Iterator query (final Descriptor queryDescriptor) {
			return new Filter(entries(),
				new AbstractPredicate() {
					public boolean invoke (Object object) {
						return descriptor(object).overlaps(queryDescriptor);
					}
				}
			);
		}
		
		
		/** Chooses the subtree which is followed during an insertion.
		 * @param descriptor the descriptor of the data object to insert
		 * @param path the path from the root to the current node modified by this function
		 * @return the index entry refering to the root of the chosen subtree
		 */
		protected Tree.IndexEntry chooseSubtree (Descriptor descriptor, Stack path) {
			IndexEntry indexEntry = chooseSubtree(descriptor, entries());
			if (!indexEntry.descriptor().contains(descriptor)) {
				indexEntry.descriptor().union(descriptor);
				update(path);
			}
			return indexEntry;
		}
		
		/** Chooses the subtree which is followed during an insertion.
		  * @param descriptor the descriptor of the data object to insert
		  * @param path the path from the root to this node
		  * @param isValidEntry Function (IndexEntry&rarr;Boolean) that determines whether an indexEntry may be chosen
		  * @return the index entry refering to the root of the chosen subtree
		  */		
		protected Tree.IndexEntry chooseSubtree (Descriptor descriptor, Stack path, Function isValidEntry) {
			IndexEntry indexEntry = chooseSubtree(descriptor, new Filter(entries(), isValidEntry));
			if (!indexEntry.descriptor.contains(descriptor)) {
				indexEntry.descriptor.union(descriptor);
				update(path);
			}
			return indexEntry;
		}
		
		/** Chooses the subtree which is followed during an insertion.
		 * @param object the object to be inserted. Its descriptor is used to choose the subtree
		 * @param entries an iterator of the IndexEntrys which may be chosen
		 * @return the index entry refering to the root of the chosen subtree  
		 */
		protected Tree.IndexEntry chooseSubtree (Object object, Iterator entries) {
			return chooseSubtree(descriptor(object), entries);
		}
		
		/** Chooses the subtree which is followed during an insertion.
		 * 
		 * @param descriptor the Descriptor of data object
		 * @param entries an iterator of the IndexEntrys which may be chosen
		 * @return the index entry refering to the root of the chosen subtree
		 */
		protected abstract IndexEntry chooseSubtree (Descriptor descriptor, Iterator entries);
		
		/** Inserts data into the current node. If level>0 data must be an 
		 * index entry.
		 * @param data the entry to be inserted into the node
		 * @param path the path from the root to the current node
		 */
		protected void grow (Object data, Stack path) {
			entries.add(data);
		}
		
		/** Updates the current node with the split information, 
		 * e.g. inserts the new index entry of the new node
		 * @param splitInfo the information created by split
		 * @param newIndexEntry the new index entry refering to the 
		 * new node created during the split
		*/		
		protected void post (Tree.Node.SplitInfo splitInfo, Tree.IndexEntry newIndexEntry) {
			grow(newIndexEntry, splitInfo.path);
		}
		
		/** Splits the current node. A new node is created, but no handle is 
		 * reserved for this node in its container (i.e. 
		 * container.insert() or container.reserve() are not
		 * called at this stage). This means that no index
		 * entry for this node is passed to its parent-node.
		 * @param path the nodes already visited during this insert
		 * @return a <tt>SplitInfo</tt> containig all information needed about the split
		*/
		protected abstract Tree.Node.SplitInfo split (Stack path);
	}
	
	/** Gets a suitable converter to serialize the index entries.
	 * 
	 * @param descriptorConverter the converter to serialze the descriptors
	 * @return an <tt>IndexEntryConverter</tt>
	 * @see Converter
	 * @see ORTree.IndexEntryConverter
	 */
	public Converter indexEntryConverter (Converter descriptorConverter) {
		return new IndexEntryConverter(descriptorConverter);
	}
	
	/** The instances of this class are converters to write index entries to the 
	 * external storage (or any other {@link DataOutput}) or read 
	 * them from it (or any other {@link DataInput}).  
	 * @see Converter
	 */
	public class IndexEntryConverter extends Converter {
		
		/** Converter for descriptors used in the index entry.
		 */	
		protected Converter descriptorConverter;
		
		/** Creates a new index entry converter. 
		 * 
		 * @param descriptorConverter converter used to convert the descriptors
		 */
		public IndexEntryConverter (Converter descriptorConverter) {
			this.descriptorConverter = descriptorConverter;
		}
	
		/** Reads an index entry from the input.
		 * @param dataInput the data input stream
		 * @param object an index entry which is updated by the read data
		 * @return the read index entry 
		 * @see Converter#read(java.io.DataInput, java.lang.Object)
		 */
		public Object read (DataInput dataInput, Object object) throws IOException {
			IndexEntry indexEntry = (IndexEntry)object;
			Container container = indexEntry.container();
			indexEntry.id = container.objectIdConverter().read(dataInput, null);
			indexEntry.descriptor = (Descriptor)descriptorConverter.read(dataInput, null);
			return indexEntry;
		}
		
		/** Writes an index entry into the output.
		 * @param dataOutput the data output stream
		 * @param object an index entry to be written 
		 * @see Converter#write(java.io.DataOutput, java.lang.Object)
		 */
		public void write (DataOutput dataOutput, Object object) throws IOException {
			IndexEntry indexEntry = (IndexEntry)object;
			Container container = indexEntry.container();
			container.objectIdConverter().write(dataOutput, indexEntry.id());
			descriptorConverter.write(dataOutput, indexEntry.descriptor());
		}
	}
	
	/** Gets a suitable Converter to serialize the tree's nodes.
	 * 
	 * @param objectConverter a converter to convert the data objects stored in the tree
	 * @param indexEntryConverter a converter to convert the index entries
	 * @return a NodeConverter
	 * @see Converter
	 * @see ORTree.NodeConverter
	 */
	public Converter nodeConverter (Converter objectConverter, Converter indexEntryConverter) {
		return new NodeConverter(objectConverter, indexEntryConverter);
	}

	/** The instances of this class are converters to write nodes to the 
	 * external storage (or any other {@link DataOutput}) or read 
	 * them from it (or any other {@link DataInput}).
	 *   
	 * @see Converter
	 */
	public class NodeConverter extends Converter {
		
		/**
		 * A converter for index entries.
		 */
		protected Converter indexEntryConverter; 
		
		/**
		 * A converter for objects.
		 */
		protected Converter objectConverter;
		
		/** Creates a new NodeConverter.
		 * 
		 * @param objectConverter a converter to convert the data objects stored in the tree
		 * @param indexEntryConverter a converter to convert the index entries
		 */
		public NodeConverter (Converter objectConverter, Converter indexEntryConverter) {
			this.indexEntryConverter = indexEntryConverter;
			this.objectConverter = objectConverter;
		}

		/** Reads a node from the data input. 
		 * 
		 * @param dataInput the data input stream
		 * @param object is ignored
		 * @return the read node
		 * 
		 * @see Converter#read(java.io.DataInput, java.lang.Object)
		 */
		public Object read (DataInput dataInput, Object object) throws IOException {
			Node node = (Node)createNode(dataInput.readShort());
			for (int i=dataInput.readInt(); --i>=0;)
				node.entries.add(node.level==0?
					objectConverter.read(dataInput, null):
					indexEntryConverter.read(dataInput, createIndexEntry(node.level))
			);
			return node;
		}
		
		/** Writes a node into the data output.
		 * @param dataOutput the data output stream
		 * @param object the node to write
		 * @see xxl.core.io.converters.Converter#write(java.io.DataOutput, java.lang.Object)
		 */
		public void write (DataOutput dataOutput, Object object) throws IOException {
			Node node = (Node)object;
			Converter converter = node.level==0? objectConverter: indexEntryConverter;
			ShortConverter.DEFAULT_INSTANCE.write(dataOutput, new Short((short)node.level));
			IntegerConverter.DEFAULT_INSTANCE.write(dataOutput, new Integer(node.number()));
			for (Iterator entries = node.entries(); entries.hasNext();)
				converter.write(dataOutput, entries.next());
		}
	}

	
	public int leafsTouched = 0;
	
	/** This method is an implemtation of an efficient querying algorithm. 
	 * The result is a lazy Cursor pointing to all objects whose descriptors 
	 * overlap with the given queryDescriptor. 
	 * @param queryDescriptor describes the query in terms of a descriptor
	 * @param targetLevel the tree-level to provide the answer-objects
	 * @return a lazy cursor pointing to all response objects
	*/
	public Cursor query (final Descriptor queryDescriptor, final int targetLevel) {
		final Iterator [] iterators = new Iterator[height()+1];

		Arrays.fill(iterators, EmptyCursor.DEFAULT_INSTANCE);
		if (height()>0 && queryDescriptor.overlaps(rootDescriptor()))
			iterators[height()] = new SingleObjectCursor(rootEntry());
		leafsTouched=0;
		return new AbstractCursor () {
			int queryAllLevel = 0;
			Object toRemove = null;
			Stack path = new Stack();

			public boolean hasNextObject() {
				for (int parentLevel = targetLevel;;)
					if (iterators[parentLevel].hasNext())
						if (parentLevel==targetLevel)
							return true;
						else {
							IndexEntry indexEntry = (IndexEntry)iterators[parentLevel].next();

							if (indexEntry.level()>=targetLevel) {
								Tree.Node node = indexEntry.get(true);
								Iterator queryIterator;
								if(node.level() == 0){ // leaf node
									leafsTouched++;
								}
								if (parentLevel<=queryAllLevel || queryDescriptor.contains(indexEntry.descriptor())) {
									queryIterator = node.entries();
									if (parentLevel>queryAllLevel && !iterators[node.level].hasNext())
											queryAllLevel = node.level;
								}
								else{
									queryIterator = node.query(queryDescriptor);
									
								}
								iterators[parentLevel = node.level] =
									iterators[parentLevel].hasNext()?
										new Sequentializer(queryIterator, iterators[parentLevel]):
										queryIterator;
								path.push(new MapEntry(indexEntry, node));
							}
						}
					else
						if (parentLevel==height())
							return false;
						else {
							if (parentLevel==queryAllLevel)
								queryAllLevel = 0;
							if (level(path)==parentLevel)
								path.pop();
							iterators[parentLevel++] = EmptyCursor.DEFAULT_INSTANCE;
						}
			}

			public Object nextObject() {
				return toRemove = iterators[targetLevel].next();
			}

			public void update (Object object) throws UnsupportedOperationException, IllegalStateException, IllegalArgumentException {
				super.update(object);
				if (targetLevel > 0)
					throw new IllegalStateException();
				else
					if (targetLevel!=0 || !descriptor(object).equals(descriptor(toRemove)))
						throw new IllegalArgumentException();
					else {
						IndexEntry indexEntry = (IndexEntry)indexEntry(path);
						Node node = (Node)node(path);

						iterators[0].remove();
						node.grow(object, path);
						indexEntry.update(node, true);
					}
			}
			
			public boolean supportsUpdate() {
				return true;
			}

			public void remove () throws UnsupportedOperationException, IllegalStateException {
				super.remove();
				if (targetLevel<height()) {
					IndexEntry indexEntry = (IndexEntry)indexEntry(path);
					Node node = (Node)node(path);

					iterators[node.level].remove();
					for (;;) {
						if (indexEntry==rootEntry() && node.level>0 && node.number()==1) {
							rootEntry = (IndexEntry)node.entries().next();
							rootDescriptor = ((IndexEntry)rootEntry()).descriptor();
							indexEntry.remove();
							break;
						}
						if (node.number()==0) {
							up(path);
							indexEntry.remove();
							if (height()==1) {
								rootEntry = null;
								rootDescriptor = null;
								break;
							}
							else {
								indexEntry = (IndexEntry)indexEntry(path);
								node = (Node)node(path);
								iterators[node.level].remove();
							}
						}
						else if (indexEntry!=rootEntry() && node.underflows()) {
							Iterator entries = node.entries();

							indexEntry.descriptor = computeDescriptor(node.entries);
							up(path);
							indexEntry.remove();
							iterators[level(path)].remove();
							indexEntry = (IndexEntry)node(path).chooseSubtree(indexEntry.descriptor(), path);
							ORTree.this.update(path);
							node = (Node)down(path, indexEntry);
							while (entries.hasNext())
								node.grow(entries.next(), path);
							if (node.overflows())
								node.redressOverflow(path);
							else {
								ORTree.this.update(path);
								up(path);
							}
							indexEntry = (IndexEntry)indexEntry(path);
							node = (Node)node(path);
						}
						else {
							ORTree.this.update(path);
							while (up(path)!=rootEntry()) {
								if (!indexEntry.descriptor().equals(indexEntry.descriptor = computeDescriptor(node.entries)))
									ORTree.this.update(path);
								indexEntry = (IndexEntry)indexEntry(path);
								node = (Node)node(path);
							}
							((IndexEntry)rootEntry).descriptor = rootDescriptor = computeDescriptor(node.entries);
							break;
						}
					}
				}
				else {
					rootEntry = null;
					rootDescriptor = null;
				}
				if (targetLevel>0) {
					IndexEntry indexEntry = (IndexEntry)toRemove;

					indexEntry.removeAll();
				}
			}
			
			public boolean supportsRemove() {
				return true;
			}
			
			
		};
	}

	/*********************************************************************/
	/*                       DEBUG FUNCTIONALITY                         */
	/*********************************************************************/

	/** Checks all descriptors in the tree.
	 * 
	 * @return <tt>true</tt> if the check does not find errors, <tt>false</tt> otherwise
	 */
	public boolean checkDescriptors () {
		if (height()>0)
			return checkDescriptors((ORTree.IndexEntry)rootEntry());
		else
			return true;
	}

	/** Checks whether all descriptors of a subtree are correct.
	 * 
	 * @param indexEntry reference to the root node of the subtree
	 * @return true iff the descrptors are correct 
	 */
	public boolean checkDescriptors (IndexEntry indexEntry) {
		return checkDescriptors(indexEntry, new LinkedList());
	}

	/** Checks whether all descriptors of a subtree are correct.
	 * 
	 * @param indexEntry reference to the root node of the subtree
	 * @param descriptorList a list containing all descriptors on the 
	 * path from the start index entry to the current index entry
	 * @return true iff the descrptors are correct 
	 */
	public boolean checkDescriptors (IndexEntry indexEntry, List descriptorList) {
		boolean returnValue = true;
		Node node = (Node)indexEntry.get(true);
		// index node: node.level > 0
		if (node.level > 0)
			for (Iterator entries = node.entries(); entries.hasNext();) {
				IndexEntry next = (IndexEntry)entries.next();
				descriptorList.add(next.descriptor);
				if (!checkDescriptors(next, descriptorList))
					returnValue = false;
				descriptorList.remove(next.descriptor);
			}
		// leaf: node.level == 0
		else {
			Iterator entries = node.entries();
			while (entries.hasNext()) {
				Iterator descriptors = descriptorList.listIterator();
				Descriptor nextLeafEntryDescriptor = descriptor(entries.next());
				while (descriptors.hasNext()) {
					Descriptor nextIndexEntryDescriptor = (Descriptor)descriptors.next();
					if (!nextIndexEntryDescriptor.contains(nextLeafEntryDescriptor)) {
						System.out.println("Error concerning OR-property occurred: \n"
											+"Descriptor of leaf entry "+nextLeafEntryDescriptor+"\n"
											+"is not contained in the index entry "+nextIndexEntryDescriptor+"\n"
											+"lying on the path bottom up to root.");
						returnValue = false;
					}
				}
			}
		}
		return returnValue;
	}

	/** Checks for each node in the tree whether the number of its entries is correct.
	 * This means that no overflows or underflows are detected.
	 * @return the number of nodes which have over- respectively underflows. 
	 */ 
	public int checkNumberOfEntries () {
		if (height() > 0) {
			Stack path = new Stack();
			Node next = (Node)down(path, rootEntry());
			return checkNumberOfEntries(next, path);
		}
		else
			return 0;
	}

	/** Checks for each node in the subtree whose root is the given node, 
	 * whether the number of its entries is correct.
	 * This means that overflows or underflows are not detected.
	 * @param node the node by which the checking must start
	 * @param path the path from the tree's root to the given node 
	 * @return the number of nodes which have over- respectively underflows. 
	 */
	public int checkNumberOfEntries (Node node, Stack path) {
		int number=0;
		Iterator it = node.entries();
		while (it.hasNext()) {
			Object o = it.next();
			if (o instanceof IndexEntry) {
				Node next = (Node)down(path, (IndexEntry)o);
				if (next.underflows() || next.overflows()) {
					System.out.println("Number of actual node entries is not correct. Overflow or underflow detected.");
					System.out.println("Number of entries in this node: "+next.number());
					number++;
				}
				number += checkNumberOfEntries (next, path);
				((IndexEntry)o).update(next);
			}
		}
		return number;
	}
}
