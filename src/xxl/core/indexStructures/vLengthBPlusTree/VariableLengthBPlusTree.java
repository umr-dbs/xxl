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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Stack;

import xxl.core.collections.MapEntry;
import xxl.core.collections.MappedList;
import xxl.core.collections.containers.Container;
import xxl.core.cursors.Cursor;
import xxl.core.cursors.mappers.Mapper;
import xxl.core.cursors.sources.EmptyCursor;
import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Constant;
import xxl.core.functions.Function;
import xxl.core.indexStructures.BPlusTree;
import xxl.core.indexStructures.BPlusTree.KeyRange;
import xxl.core.indexStructures.vLengthBPlusTree.splitStrategy.ShortestKeyStrategy;
import xxl.core.indexStructures.vLengthBPlusTree.splitStrategy.SimplePrefixBPlusTreeSplit;
import xxl.core.indexStructures.vLengthBPlusTree.underflowHandlers.StandardUnderflowHandler;
import xxl.core.indexStructures.Descriptor;
import xxl.core.indexStructures.Separator;
import xxl.core.indexStructures.Tree;
import xxl.core.io.converters.BooleanConverter;
import xxl.core.io.converters.Converter;
import xxl.core.io.converters.IntegerConverter;
import xxl.core.io.converters.MeasuredConverter;
import xxl.core.predicates.AbstractPredicate;
import xxl.core.predicates.Predicate;

/**
 * This class implements a B+Tree. 
 * B+Tree handle keys and data as variable length with a max size defined by the MeasuredConverter 
 * @see {@link MeasuredConverter#getMaxObjectSize()}. 
 * With two functions @see {@link VariableLengthBPlusTree#getActualEntrySize} and @see {@link VariableLengthBPlusTree#getActualKeySize} the serialized
 * size of the object or key computed. The reorganization of the nodes are based not on the number of the elements but on the overall byte size of the node. 
 * The initialization of the B+Tree awaits a SplitStrategy and underflow handler as a parameter:
 * @see {@link SplitStrategy}, 
 * @see {@link UnderflowHandler}
 * @see {@link StandardUnderflowHandler}
 * @see {@link ShortestKeyStrategy}
 * @see {@link SimplePrefixBPlusTreeSplit}
 * 
 * For a detailed discussion see Douglas Comer: "The Ubiquitous B-Tree", ACM
 * Comput. Surv. 11(2), 121-137, 1979.
 */
public class VariableLengthBPlusTree extends Tree {
	/**
	 * This declares the block size of the underlying external storage. This
	 * field is initialized finally in the constructor by the value given from
	 * the user as a parameter. It is used to compute the maximal capacity of
	 * the tree's nodes.ert
	 * 
	 */
	public final int BLOCK_SIZE;
	/**
	 * 
	 * @author achakeye
	 *
	 */
	public static enum MergeState{
		MERGE_RIGHT,
		MERGE_LEFT,
		DISTRIBUTION_LEFT,
		DISTRIBUTION_RIGHT,
		POSTPONED_MERGE
	};
	public static final int MERGE = 0;
	public static final int DISTRIBUTION_LEFT = 1;
	public static final int DISTRIBUTION_RIGHT = 2;
	public static final int POSTPONED_MERGE = 3;
	/**
	 * function which computes the serialized byte size of the key 
	 */
	protected Function<Object, Integer> getActualKeySize;
	/**
	 * function which computes the serialized byte size of the object stored in leaf nodes
	 */
	protected Function<Object, Integer> getActualEntrySize;
	/**
	 * lower bound of node capacity in the number of bytes.
	 * servers as start point for computing the split in order to find a best split index according to the byte capacity  
	 */
	protected int minAllowedLoad;
	/**
	 * average load 
	 */
	protected int averageLoad;
	/**
	 * the byte size of the internal id (pointer) of the node which managed by the index entries  
	 */
	protected int containerIdSize;
	/**
	 * 
	 */
	protected double averageLoadRatio;
	/**
	 * lower bound of node capacity as a fraction of bytes. e.g 0.4 of 4096 bytes 
	 * servers as start point for computing the split in order to find a best split index according to the byte capacity  
	 */
	protected double minAllowedLoadRatio;
	/**
	 * SplitStrategy wich will be used by the split method @see {@link Node#split(Stack)}
	 */
	protected SplitStrategy splitSt;
	/**
	 * underflow handler
	 * @see {@link StandardUnderflowHandler} 
	 */
	protected UnderflowHandler underflowHandler;
	/**
	 * This is a Converter for data objects stored in the tree. It is used to
	 * read or write these data objects from or into the external storage.
	 */
	protected MeasuredConverter dataConverter;
	/**
	 * This is a <tt>Converter</tt> for keys used by the tree. It is used to
	 * read or write these keys from or into the external storage.
	 */
	protected MeasuredConverter keyConverter;
	/**
	 * This <tt>Converter</tt> is used to read or write nodes (i.e. blocks) from
	 * or into the external storage.
	 */
	protected NodeConverter nodeConverter;
	/**
	 * This <tt>Function</tt> is used to get the key of a data object. Example:
	 * <code><pre>
	 * 
	 * Comparable key = (Comparable) getKey.invoke(data);
	 * </pre></code>
	 */
	protected Function getKey;
	/**
	 * This <tt>Function</tt> is used by the method
	 * {BPlusTree#createSeparator(Comparable)} to create <tt>Separators</tt>.
	 * The user has to initialize this field with a suitable <tt>Function</tt>.
	 * 
	 * @see BPlusTree#createSeparator(Comparable)
	 */
	protected Function createSeparator;
	/**
	 * This <tt>Function</tt> is used by the method
	 * {BPlusTree#createKeyRange(Comparable, Comparable)} to create a
	 * <tt>KeyRange</tt>. The user has to initialize this field with a suitable
	 * <tt>Function</tt>.
	 * 
	 * @see BPlusTree#createKeyRange(Comparable, Comparable)
	 */
	protected Function createKeyRange;
	/**
	 * Flag indicating if a reorganization took place.
	 */
	protected boolean reorg = false;
	/**
	 * Contains last used Descriptor for a query.
	 */
	protected Descriptor lastQueryDisscriptor;
	/**
	 * Indicates whether a B+Tree can contain Duplicates or not.
	 */
	protected boolean duplicate;
	
	/**
	 * Creates a new empty B+Tree. Before running B+Tree it should be initialized. Key Duplicates are not allowed. 
	 * @see #initialize(IndexEntry, Descriptor, Function, Container, MeasuredConverter, MeasuredConverter, Function, Function, Function, Function, SplitStrategy, UnderflowHandler) 
	 * @param blockSize
	 * @param minAllowedLoad is a minimal fraction of page / node capacity in bytes that each node after the reorganization should fulfill. e.g. 0.4 This parameter is used for computing the suitable split index.  
	 */
	public VariableLengthBPlusTree(int blockSize, double minAllowedLoad) {
		this(blockSize, minAllowedLoad, false);
	}
	
	/**
	 * Creates a new empty B+Tree. Before running B+Tree it should be initialized. Managing of duplicate keys can be switched with parameter duplicates.  
	 * @see #initialize(IndexEntry, Descriptor, Function, Container, MeasuredConverter, MeasuredConverter, Function, Function, Function, Function, SplitStrategy, UnderflowHandler) 
	 * @param blockSize
	 * @param minAllowedLoad
	 * @param duplicates
	 */
	public VariableLengthBPlusTree(int blockSize, double minAllowedLoad,
			boolean duplicates) {
		this.BLOCK_SIZE = blockSize;
		this.minAllowedLoadRatio = minAllowedLoad;
		this.averageLoadRatio = 0.5;
		this.duplicate = duplicates;
		nodeConverter = createNodeConverter();
	}
	
	/**
	 * Initializes the <tt>BPlusTree</tt>.
	 * 
	 * @param getKey
	 *            the <tt>Function</tt> to get the key of a data object
	 * @param keyConverter
	 *            the <tt>Converter</tt> for the keys used by the tree
	 * @param dataConverter
	 *            the <tt>Converter</tt> for data objects stored in the tree
	 * @param createSeparator
	 *            a factory <tt>Function</tt> to create <tt>Separators</tt>
	 * @param createKeyRange
	 *            a factory <tt>Function</tt> to create <tt>KeyRanges</tt>
	 * @param getSplitMinRatio
	 *            a <tt>Function</tt> to determine the minimal relative number
	 *            of entries which the node may contain after a split
	 * @param getSplitMaxRatio
	 *            a <tt>Function</tt> to determine the maximal relative number
	 *            of entries which the node may contain after a split
	 * @return the initialized <tt>BPlusTree</tt> itself
	 */
	protected VariableLengthBPlusTree initialize(Function getKey,
			MeasuredConverter keyConverter, MeasuredConverter dataConverter,
			Function createSeparator, Function createKeyRange,
			Function getSplitMinRatio, Function getSplitMaxRatio) {
		this.getKey = getKey;
		this.keyConverter = keyConverter;
		this.dataConverter = dataConverter;
		// this.nodeConverter = createNodeConverter();
		this.createSeparator = createSeparator;
		this.createKeyRange = createKeyRange;
		// new code
		this.containerIdSize = VariableLengthBPlusTree.this.container()
				.getIdSize();
		final int maxNodePayLoad = this.BLOCK_SIZE
				- (2 * IntegerConverter.SIZE + BooleanConverter.SIZE + containerIdSize);
		// new code
		this.averageLoad = (int) ((double) maxNodePayLoad * this.averageLoadRatio);
		this.minAllowedLoad = (int) ((double) maxNodePayLoad * this.minAllowedLoadRatio);

		Function getDescriptor = new AbstractFunction() {
			public Object invoke(Object o) {
				if (o instanceof Separator)
					return (Separator) o;
				if (o instanceof IndexEntry)
					return ((IndexEntry) o).separator;
				return createSeparator(key(o));
			}
		};
		// new Code
		Predicate overflows = new AbstractPredicate() {
			public boolean invoke(Object o) {
				Node node = (Node) o;
				int load = node.getCurrentLoad();
				return load > maxNodePayLoad;
			}
		};
		// new code
		Predicate underflows = new AbstractPredicate() {
			public boolean invoke(Object o) {
				Node node = (Node) o;
				int load = node.getCurrentLoad();
				return load < minAllowedLoad;
			}
		};
		super.initialize((IndexEntry) null, (Descriptor) null, getDescriptor,
				getContainer, determineContainer, underflows, overflows,
				getSplitMinRatio, getSplitMaxRatio);
		return this;
	}
	/**
	 * 
	 * This method initializes the B+Tree. It also initializes SplitStrategy and UnderflowHanlder Objects. 
	 * 
	 * @param rootEntry rootEntry of the tree 
	 * @param rootDescriptor rootDescriptor of the tree; @see {@link KeyRange}
	 * @param getKey function which extract a key from the data which is tored in the leaf nodes 
	 * @param container container of the tree 
	 * @param keyConverter converter of the tree which is @see {@link MeasuredConverter} and provides information about maximal size in bytes 
	 * @param dataConverter converter of the tree which is @see {@link MeasuredConverter} and provides information about maximal size in bytes 
	 * @param createSeparator factory function for creating the separtor @see {@link Separator}
	 * @param createKeyRange factory function for creating key ranges @see {@link KeyRange}
	 * @param getActualKeySize function which gives the serialized size of the current key in bytes
	 * @param getActualEntrySize function which gives the serialized size of the current key in bytes
	 * @param splitStrategy strategy class for to run split. @see {@link Node#split(Stack) } 
	 * @param underflowHandler strategy for handling underflow on the nodes @see {@link Node#redressUnderflow(Stack)}
	 * @return
	 */
	public VariableLengthBPlusTree initialize(IndexEntry rootEntry,
			Descriptor rootDescriptor, final Function getKey,
			final Container container, MeasuredConverter keyConverter,
			MeasuredConverter dataConverter, final Function createSeparator,
			final Function createKeyRange,
			final Function<Object, Integer> getActualKeySize,
			final Function<Object, Integer> getActualEntrySize, 
			SplitStrategy splitStrategy,
			UnderflowHandler underflowHandler) {	
		// set handlers and splitter
		this.splitSt = splitStrategy;
		this.underflowHandler = underflowHandler;
		return this.initialize(rootEntry, rootDescriptor, getKey, container, keyConverter, dataConverter, createSeparator, createKeyRange, getActualKeySize, getActualEntrySize);
	}
	
	/**
	 * 
	 * @param rootEntry
	 * @param rootDescriptor
	 * @param getKey
	 * @param container
	 * @param keyConverter
	 * @param dataConverter
	 * @param createSeparator
	 * @param createKeyRange
	 * @param getActualKeySize
	 * @param getActualEntrySize
	 * @return
	 */
	protected VariableLengthBPlusTree initialize(IndexEntry rootEntry,
			Descriptor rootDescriptor, final Function getKey,
			final Container container, MeasuredConverter keyConverter,
			MeasuredConverter dataConverter, final Function createSeparator,
			final Function createKeyRange,
			final Function<Object, Integer> getActualKeySize,
			final Function<Object, Integer> getActualEntrySize ) {
		Function<Separator, Integer> getSeparatorSize = new AbstractFunction<Separator, Integer>() {
			public Integer invoke(Separator sep) {
				return getActualKeySize.invoke(sep.sepValue());
			}
		};
		this.splitSt.initialize(getActualEntrySize, getSeparatorSize, getKey,
				createSeparator, containerIdSize);
		// this.splitSt.initialize(getActualEntrySize, getActualKeySize, getKey,
		// createSeparator, containerIdSize);
		Function getSplitMinRatio = new Constant(this.minAllowedLoadRatio);
		Function getSplitMaxRatio = new Constant(1.0);
		Function getContainer = new Constant(container);
		Function determineContainer = new Constant(container);
		this.getContainer = getContainer;
		this.determineContainer = determineContainer;
		this.getActualKeySize = getActualKeySize;
		this.getActualEntrySize = getActualEntrySize;
		initialize(getKey, keyConverter, dataConverter, createSeparator,
				createKeyRange, getSplitMinRatio, getSplitMaxRatio);
		this.rootEntry = rootEntry;
		this.rootDescriptor = rootDescriptor;
		return this;
	}
	
	/**
	 * Checks whether the duplicates mode is enabled
	 * 
	 * @return
	 */
	public boolean isDuplicatesEnabled() {
		return this.duplicate;
	}

	/**
	 * Creates a new node on a given level.
	 * 
	 * @param level
	 *            the level of the new Node
	 * @see xxl.core.indexStructures.Tree#createNode(int)
	 */
	public Tree.Node createNode(int level) {
		Node node = new Node(level);
		return node;
	}

	/**
	 * Creates a new Index Entry.
	 * 
	 * @param parentLevel
	 *            the level of the node in which the new indexEntry is stored
	 * @return a new indexEntr
	 * @see xxl.core.indexStructures.Tree#createIndexEntry(int)
	 * @see xxl.core.indexStructures.BPlusTree.IndexEntry
	 */
	public Tree.IndexEntry createIndexEntry(int parentLevel) {
		return new IndexEntry(parentLevel);
	}

	/**
	 * This method invokes the Function {BPlusTree#createSeparator} with the
	 * given argument and casts the result to <tt>Separator</tt>.
	 * 
	 * @param sepValue
	 *            the separation value of the new <tt>Separator</tt>
	 * @return the new created <tt>Separator</tt>
	 */
	protected Separator createSeparator(Comparable sepValue) {
		return (Separator) this.createSeparator.invoke(sepValue);
	}

	/**
	 * This method invokes the Function {#createKeyRange} with the given
	 * arguments and casts the result to <tt>KeyRange</tt>.
	 * 
	 * @param min
	 *            the minimal boundary of the <tt>KeyRange</tt>
	 * @param max
	 *            the maximal boundary of the <tt>KeyRange</tt>
	 * @return the new created <tt>KeyRange</tt>.
	 */
	protected KeyRange createKeyRange(Comparable min, Comparable max) {
		return (KeyRange) createKeyRange.invoke(min, max);
	}

	/**
	 * This method invokes the Function {BPlusTree#getKey} with the given
	 * argument and casts the result to <tt>Comparable</tt>. It is used to get
	 * the key value of a data object.
	 * 
	 * @param data
	 *            the data object whose key is required
	 * @return the key of the given data object
	 */
	public Comparable key(Object data) {
		return (Comparable) getKey.invoke(data);
	}

	/**
	 * It is used to determine the <tt>Separator</tt> of a given entry (e.g.
	 * <tt>IndexEntry</tt>, data entry, etc.). It is equivalent to:
	 * 
	 * <pre>
	 * &lt;code&gt;
	 * return (Separator) descriptor(entry);
	 * &lt;/code&gt;
	 * </pre>
	 * 
	 * @param entry
	 *            the entry whose <tt>Separator</tt> is required
	 * @return the <tt>Separator</tt> of the given entry
	 */
	public Separator separator(Object entry) {
		return (Separator) getDescriptor.invoke(entry);
	}

	/**
	 * Creates a new <tt>NodeConverter</tt>.
	 * 
	 * @return a new <tt>NodeConverter</tt>
	 */
	protected NodeConverter createNodeConverter() {
		return new NodeConverter();
	}

	/**
	 * Gives the <tt>NodeConverter</tt> used by the <tt>BPlusTree</tt>.
	 * 
	 * @return the <tt>NodeConverter</tt> <tt>BPlusTree</tt>
	 * 
	 * @see #nodeConverter
	 */
	public NodeConverter nodeConverter() {
		return nodeConverter;
	}

	/**
	 * Gives the <tt>Container</tt> used by the <tt>BPlusTree</tt> to store the
	 * nodes. It is equivalent to: <code><pre>
	 * return (Container) this.getContainer.invoke(this);
	 * </pre></code> NOTE: This only makes sense if the whole Tree is stored in
	 * a single <tt>Container</tt>. This method is used by the default
	 * <tt>NodeConverter</tt>. Therefore the user has to overwrite the
	 * <tt>NodeConverter</tt> if the tree works in multidisk mode.
	 * 
	 * @return the <tt>Container</tt> of the <tt>BPlusTree</tt>
	 */
	public Container container() {
		return (Container) this.getContainer.invoke(this);
	}

	/**
	 * Checks whether the <tt>BPlusTree</tt> is empty. The <tt>BPlusTree</tt> is
	 * empty if the <tt>rootEntry</tt> is <tt>null</tt> or the root node is
	 * empty.
	 * 
	 * @return <tt>true</tt> if the <tt>BPlusTree</tt> is empty and
	 *         <tt>false</tt> otherwise
	 */
	public boolean isEmpty() {
		return (rootEntry == null) || (rootEntry.get().number() == 0);
	}

	/**
	 * Gives last used Descriptor for a query.
	 * 
	 * @return last used Descriptor for a query.
	 */
	public Descriptor getLastDescriptor() {
		return lastQueryDisscriptor;
	}

	/**
	 * 
	 * This method is used to remove a data object from the tree. It uses the
	 * method {@link BPlusTree#separator(Object)}to get the <tt>Separator</tt>
	 * of the given object. Thereafter it searches in the tree for an Object
	 * with the same <tt>Separator</tt>. The method {Separator#equals{Object)}
	 * is used to check whether two Separators are the same. NOTE: In duplicate
	 * mode the objects are compared with o1.equals(o2)
	 * 
	 * @param data
	 *            the object which has to be removed
	 * @return the removed object if the search was successful, and
	 *         <tt>null</tt> otherwise
	 */
	public Object remove(final Object data) {
		return remove(data, new AbstractPredicate() {
			public boolean invoke(Object o1, Object o2) {
				return (duplicate) ? o1.equals(o2)
						: key(o1).compareTo(key(o2)) == 0;
			}
		});
	}

	/**
	 * Removes the object which is specified by the given descriptor. First
	 * query is called:
	 * 
	 * <pre>
	 * &lt;code&gt;
	 * 	  	Iterator objects = query(descriptor, targetLevel);
	 * </pre>
	 * 
	 * </code> Then the first object which meets the <tt>test</tt>(Predicate) is
	 * removed from the tree and returned.
	 * 
	 * @param descriptor
	 *            specifies the query
	 * @param targetLevel
	 *            the level on which the query has to stop
	 * @param test
	 *            a predicate the object to remove has to fulfill
	 * @return the removed object or <tt>null</tt> if there was no such object
	 */
	public Object remove(Descriptor descriptor, int targetLevel, Predicate test) {
		Cursor objects = query(descriptor, targetLevel);
		Object retValue = null;
		while (objects.hasNext()) {
			Object object = objects.next();
			if (test.invoke(object)) {
				objects.remove();
				retValue = object;
				break;
			}
		}
		objects.close();
		return retValue;
	}

	/**
	 * Inserts an object into a given level of the tree. If level > 0
	 * <tt>data</tt> has to be an <tt>IndexEntry</tt> (this only makes sense for
	 * trees whose index nodes support Node.grow()).
	 * 
	 * @param data
	 *            the data to insert
	 * @param descriptor
	 *            is used to find the node into which data must be inserted
	 * @param targetLevel
	 *            is the tree-level into which <tt>data</tt> has to be inserted
	 *            (<tt>targetLevel==0</tt> means insert into the suitable leaf
	 *            node)
	 */
	protected void insert(Object data, Descriptor descriptor, int targetLevel) {
		if (rootEntry() == null) {
			Comparable key = ((Separator) descriptor).sepValue();
			rootDescriptor = createKeyRange(key, key);
			grow(data);
		} else {
			super.insert(data, descriptor, targetLevel);
		}
	}

	/**
	 * Searches for data objects with the given key stored in the
	 * <tt>BPlusTree</tt>. It calls <code> <pre>
	 * KeyRange range = createKeyRange(key, key);
	 * return query(range, 0);
	 * </pre></code>. Returns the set of results as a cursor. If the tree is not
	 * in duplicate mode {@link java.lang.UnsupportedOperationException} is
	 * thrown.
	 * 
	 * @param key
	 * @return cursor
	 * @throws UnsupportedOperationException
	 *             when the tree is not in duplicate mode
	 */
	public Cursor aloneKeyQuery(Comparable key) {
		if (!this.duplicate) {
			throw new UnsupportedOperationException(
					"B+Tree does not support duplicates!!! Please run "
							+ "exactMatchQuery(Comparable key) methode ");
		}
		KeyRange range = createKeyRange(key, key);
		return query(range, 0);
	}

	/**
	 * Searches the single data object with the given key stored in the
	 * <tt>BPlusTree</tt>.
	 * 
	 * @param key
	 *            the key of the data object which has to be searched
	 * @return the single data object with the given key or <tt>null</tt> if no
	 *         such object could be found
	 * @throws UnsupportedOperationException
	 *             when the tree is in duplicate mode
	 */
	public Object exactMatchQuery(Comparable key) {
		if (this.duplicate) {
			throw new UnsupportedOperationException(
					"B+Tree supports Duplicates!!! Please run aloneKeyQuery() method");
		}
		Predicate p = new AbstractPredicate() {
			public boolean invoke() {
				return true;
			}

			public boolean invoke(Object argument) {
				return invoke();
			}
		};
		return exactMatchQuery(key, p);
	}

	/**
	 * Searches for the single entry with the given key stored in the
	 * <tt>BPlusTree</tt> which fulfills the given Predicate. The method
	 * {@link #query(Descriptor queryDescriptor, int targetLevel)} takes over
	 * the search process.
	 * 
	 * @param key
	 *            the key of the data object which has to be searched
	 * @param test
	 *            a Predicate which the found entry has to fulfill
	 * @return the single data object with the given key or <tt>null</tt> if no
	 *         such object could be found
	 */
	protected Object exactMatchQuery(Comparable key, Predicate test) {
		KeyRange range = createKeyRange(key, key);
		Cursor results = query(range, 0);
		Object result = null;
		while (results.hasNext()) {
			Object o = results.next();
			if (test.invoke(o)) {
				result = o;
				break;
			}
		}
		results.close();
		return result;
	}

	/**
	 * Searches all data objects stored in the tree whose key lie in the given
	 * key range [minKey, maxKey]. The method
	 * {@link #query(Descriptor queryDescriptor, int targetLevel)} takes over
	 * the search process.
	 * 
	 * @param minKey
	 *            the minimal boundary of the query's key range
	 * @param maxKey
	 *            the maximal boundary of the query's key range
	 * @return a <tt>Cursor</tt> pointing all qualified entries
	 */
	public Cursor rangeQuery(Comparable minKey, Comparable maxKey) {
		KeyRange range = createKeyRange(minKey, maxKey);
		return query(range, 0);
	}

	/**
	 * This method calls
	 * {@link #query(IndexEntry subRootEntry, KeyRange queryInterval,int targetLevel)}
	 * which uses an implementation of an efficient querying algorithm. The
	 * result is a lazy <tt>Cursor</tt> pointing to all entries whose keys are
	 * contained in <tt>queryDescriptor</tt>. Initially query creates the
	 * correct parameters for the call.
	 * 
	 * @param queryDescriptor
	 *            describes the query in terms of a <tt>Descriptor</tt>. In this
	 *            case it has to an instance of <tt>Separator</tt> oder
	 *            <tt>KeyRange</tt>
	 * @param targetLevel
	 *            the tree-level to provide the answer-objects
	 * @return a lazy <tt>Cursor</tt> pointing to all response objects
	 */
	public Cursor query(Descriptor queryDescriptor, int targetLevel) {
		KeyRange queryRange;
		if (queryDescriptor == null)
			return new EmptyCursor();
		if (!(queryDescriptor instanceof KeyRange)) {
			Comparable key = ((Separator) queryDescriptor).sepValue();
			queryRange = createKeyRange(key, key);
		} else
			queryRange = (KeyRange) queryDescriptor;
		if ((targetLevel >= height()) || !(rootDescriptor).overlaps(queryRange))
			return new EmptyCursor();
		return query((IndexEntry) rootEntry, queryRange, targetLevel);
	}

	/**
	 * This method uses an implementation of an efficient querying algorithm.
	 * The result is a lazy <tt>Cursor</tt> pointing to all entries whose keys
	 * are contained in <tt>queryDescriptor</tt>. Method creates object of type
	 * {@link xxl.core.indexStructures.BPlusTree.QueryCursor}
	 * 
	 * @param subRootEntry
	 *            the root of the subtree in which the query is to execute
	 * @param queryInterval
	 *            describes the query in terms of a <tt>Descriptor</tt>. In this
	 *            case it has to be an instance of <tt>KeyRange</tt>
	 * @param targetLevel
	 *            the tree-level to provide the answer-objects
	 * @return a lazy <tt>Cursor</tt> pointing to all response objects
	 */
	protected Cursor query(IndexEntry subRootEntry, KeyRange queryInterval,
			int targetLevel) {
		return new QueryCursor(subRootEntry, queryInterval, targetLevel);
	}

	/**
	 * This method is called after an entry was removed in order to repair the
	 * index structure.
	 * 
	 * @param path
	 *            the path from the root to the <tt>Node</tt> from which the
	 *            entry was removed
	 */
	protected void treatUnderflow(Stack path) {
		while (((Node) node(path)).redressUnderflow(path))
			;
	}

	/**
	 * This method is called after a new entry was inserted in order to repair
	 * the index structure.
	 * 
	 * @param path
	 *            the path from the root to the <tt>Node</tt> into which the new
	 *            entry was inserted
	 */
	protected void treatOverflow(Stack path) {
		post(path);
	}

	/**
	 * Return the flag {@link #reorg}and resets it to <tt>false</tt>.
	 * 
	 * @return value of #reorg.
	 */
	public boolean wasReorg() {
		boolean ret = reorg;
		reorg = false;
		return ret;
	}
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	/**
	 * 
	 * Range Remove algorithm
	 * not efficient only for test purpose
	 * 
	 * @param <K>
	 * @param minKey
	 * @param maxKey
	 */
	public <K extends Comparable<K>> void removeRange(K minKey, K maxKey) {
		Stack leftPath = new Stack();
		Stack rightPath = new Stack();
		Stack commonPath = new Stack();
		Node branchingNode = null;
		IndexEntry leftRoot = (IndexEntry) this.rootEntry;
		down(commonPath, leftRoot);
		branchingNode = (Node) node(commonPath);
		List<IndexEntry> rootsToDelete = new ArrayList<IndexEntry>();
		IndexEntry rightRoot = null;
		// find branching
		while (branchingNode.level() > 0) {
			int leftBound = branchingNode.search(minKey);
			int rightBound = branchingNode.search(maxKey);
			int leftIndex = (leftBound >= 0) ? leftBound
					: (-leftBound - 1 == branchingNode.number()) ? -leftBound - 2
							: -leftBound - 1;
			int rightIndex = (rightBound >= 0) ? rightBound
					: (-rightBound - 1 == branchingNode.number()) ? -rightBound - 2
							: -rightBound - 1;
			if (leftIndex == rightIndex) {
				// go down
				leftRoot = (IndexEntry) branchingNode.getEntry(leftIndex);
				down(commonPath, leftRoot);
				branchingNode = (Node) node(commonPath);
			} else { // leftIndex < rightIndex
				leftRoot = (IndexEntry) branchingNode.getEntry(leftIndex);
				rightRoot = (IndexEntry) branchingNode.getEntry(rightIndex);
				if (rightIndex - leftIndex > 1) {
					List<IndexEntry> entries = branchingNode.getEntries()
							.subList(leftIndex + 1, rightIndex);
					rootsToDelete.addAll(entries); // add subroots
					entries.clear(); // delete
					branchingNode
							.setByteLoad(branchingNode.computeActualLoad());
				}
				break;
			}
		}
		// process left
		computePath(leftRoot, leftPath, minKey, rootsToDelete, true);
		// delete entries in leaf entry
		if (!leftPath.isEmpty()) {
			Node node = (Node) node(leftPath);
			removeRangeFromLeafNode(node, minKey, maxKey);
		}
		// TODO

		treatUnderflowRange(leftPath);
		if (rightRoot != null) {
			computePath(rightRoot, rightPath, minKey, rootsToDelete, false);
			if (!rightPath.isEmpty()) {
				Node node = (Node) node(rightPath);
				removeRangeFromLeafNode(node, minKey, maxKey);
			}
			// TODO
			treatUnderflowRange(rightPath);
		}
		this.treatUnderflow(commonPath);
		// delete from container
		for (IndexEntry subRoot : rootsToDelete) {
			subRoot.removeAll();
		}
	}

	/**
	 * 
	 * @param <K>
	 * @param subRootEntry
	 * @param path
	 * @param key
	 * @param left
	 * @return
	 */
	protected <K extends Comparable<K>> void computePath(
			IndexEntry subRootEntry, Stack path, K key,
			List<IndexEntry> rootsToDelete, boolean left) {
		down(path, subRootEntry);
		Node node = (Node) node(path);
		while (node.level() > 0) {
			int bound = node.search(key);
			int index = (bound >= 0) ? bound
					: (-bound - 1 == node.number()) ? -bound - 2 : -bound - 1;
			if (left) {
				if (index != node.number() - 1) {
					List<IndexEntry> entries = node.getEntries().subList(
							index + 1, node.number());
					rootsToDelete.addAll(entries);
					entries.clear();
					node.setByteLoad(node.computeActualLoad());
				}
			} else {
				if (index != 0) {
					List<IndexEntry> entries = node.getEntries().subList(0,
							index);
					rootsToDelete.addAll(entries);
					entries.clear();
					node.setByteLoad(node.computeActualLoad());
				}
			}
			IndexEntry subRoot = (IndexEntry) node.getEntry(index);
			down(path, subRoot);
			node = (Node) node(path);
		}
	}

	/**
	 * 
	 * @param <K>
	 * @param node
	 * @param minKey
	 * @param maxKey
	 */
	protected <K extends Comparable<K>> void removeRangeFromLeafNode(Node node,
			K minKey, K maxKey) {
		int leftBound = node.search(minKey);
		int rightBound = node.search(maxKey);
		int leftIndex = (leftBound >= 0) ? leftBound : -leftBound - 1;
		int rightIndex = (rightBound >= 0) ? rightBound
				: (-rightBound - 1 == node.number()) ? -rightBound - 2
						: -rightBound - 1;
		if (leftIndex == rightIndex) {
			node.entries.remove(leftIndex);
			node.setByteLoad(node.computeActualLoad());
		} else {
			node.getEntries().subList(leftIndex, rightIndex + 1).clear();
			node.setByteLoad(node.computeActualLoad());
		}
	}

	/**
	 * 
	 * @param path
	 */
	protected void treatUnderflowRange(Stack path) {
		boolean parentUpdated;
		do {
			parentUpdated = false;
			MapEntry pathEntry = (MapEntry) path.pop();
			IndexEntry indexEntry = (IndexEntry) pathEntry.getKey();
			Node node = (Node) pathEntry.getValue();
			if (!path.isEmpty()) {
				if (node.entries.isEmpty()) {
					Node parent = (Node) node(path);
					parent.remove(indexEntry);
					indexEntry.remove();
					parentUpdated = true;
				} else {
					if (!path.isEmpty() && this.underflows.invoke(node)) {
						parentUpdated = node.redressUnderflow(indexEntry, node,
								path, true);
					}
				}
			}
			if (!parentUpdated) {
				indexEntry.update(node, true);
				indexEntry.unfix();
			}
		} while (parentUpdated);
	}

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	/**
    * 
    */
	public void insertRange(Iterator dataIterator, Predicate overflows,
			Comparable minKey, Comparable maxKey) {
		// build main memory tree
		List leftFlankIds = new ArrayList();
		List<MapEntry<Object, VariableLengthBPlusTree.Node>> mmTree = new ArrayList<MapEntry<Object, VariableLengthBPlusTree.Node>>();
		List<IndexEntry> roots = writeRangeTree(dataIterator, overflows,
				leftFlankIds, mmTree);
		// compute path to the
		int leafLevel = 0;
		Stack pathToCommonLevel = new Stack();
		Node leftBoundNode = null;
		Node rightTopNode = null;
		down(pathToCommonLevel, this.rootEntry);
		Node tempNode = (Node) node(pathToCommonLevel);
		while (tempNode.level() > leafLevel) { // go down
			int leftBound = tempNode.search(minKey);
			int rightBound = tempNode.search(maxKey);
			int leftIndex = (leftBound >= 0) ? leftBound
					: (-leftBound - 1 == tempNode.number()) ? -leftBound - 2
							: -leftBound - 1;
			int rightIndex = (rightBound >= 0) ? rightBound
					: (-rightBound - 1 == tempNode.number()) ? -rightBound - 2
							: -rightBound - 1;
			if (leftIndex == rightIndex) {
				// go down
				down(pathToCommonLevel, (IndexEntry) tempNode
						.getEntry(leftIndex));
				tempNode = (Node) node(pathToCommonLevel);
			} else {
				// TODO
				throw new IllegalStateException();
			}
		}// NOTE: right flank is not written out only after linking
		insertRangeWriteBefore(roots, minKey, maxKey, pathToCommonLevel,
				leftFlankIds, mmTree);// insert
	}

	/**
	 * TODO Link with left and right flanks of newly created tree
	 * 
	 * @param mmTree
	 * @param minKey
	 * @param maxKey
	 * @param path
	 */
	protected void insertRangeWriteBefore(List<IndexEntry> roots,
			Comparable minKey, Comparable maxKey, Stack path,
			List leftFlankIds,
			List<MapEntry<Object, VariableLengthBPlusTree.Node>> mmTree) {
		int rootLevel = roots.get(roots.size() - 1).level();
		if (!path.isEmpty()) {
			if (path.size() >= rootLevel) {
				// left list
				List<MapEntry> leftFlank = new ArrayList<MapEntry>();
				List<MapEntry> rightFlank = new ArrayList<MapEntry>();
				IndexEntry newRightEntry = null;
				for (int i = 0; i < rootLevel; i++) { // split, and write nodes
														// on the path
					Node nodeToSplit = (Node) node(path); // create node on the
															// same level
					Node rightNewNode = (Node) this.createNode(nodeToSplit
							.level());
					MapEntry leftEntry = (MapEntry) path.pop();
					int index = nodeToSplit.search(maxKey);
					if (index > 0)
						throw new IllegalStateException(
								"Entry of the range is in the Node!!!");
					index = -index - 1;
					List cpyList = nodeToSplit.entries.subList(index,
							nodeToSplit.number());
					rightNewNode.entries.addAll(cpyList);
					cpyList.clear();
					rightNewNode.nextNeighbor = nodeToSplit.nextNeighbor(); // re-link
					if (newRightEntry != null) {
						rightNewNode.grow(newRightEntry, null);
					}
					Object newId = this.container().insert(rightNewNode); // write
																			// new
																			// node
					IndexEntry rightIndexEntry = (IndexEntry) this
							.createIndexEntry(rightNewNode.level());
					rightIndexEntry.initialize(newId, separator(rightNewNode
							.getLast())); // init index entry
					newRightEntry = rightIndexEntry;
					IndexEntry indexEntry = (IndexEntry) leftEntry.getKey();
					indexEntry.separator = (Separator) separator(
							nodeToSplit.getLast()).clone();
					leftFlank.add(leftEntry);
					rightFlank.add(new MapEntry(rightIndexEntry, rightNewNode));
				}// glue entries
				Node levelRoot = (Node) node(path); // insert roots
				roots.add(newRightEntry);
				for (IndexEntry root : roots) {
					levelRoot.grow(root, null); // insert roots
				}
				levelRoot.setByteLoad(levelRoot.computeActualLoad());
				// Glue
				for (int i = 0; i < leftFlank.size(); i++) {
					Node nodeLeft = (Node) leftFlank.get(i).getValue();
					nodeLeft.nextNeighbor = (IndexEntry) this
							.createIndexEntry(nodeLeft.level() + 1);
					nodeLeft.nextNeighbor.initialize(leftFlankIds.get(i));// write
																			// changes
					((IndexEntry) leftFlank.get(i).getKey()).update(nodeLeft);
				}
				for (int i = 0; i < rightFlank.size(); i++) {
					Node rightNode = mmTree.get(i).getValue();
					rightNode.nextNeighbor = (IndexEntry) this
							.createIndexEntry(rightNode.level());
					rightNode.nextNeighbor.initialize(((IndexEntry) rightFlank
							.get(i).getKey()).id());
					this.container().update(mmTree.get(i).getKey(), rightNode); // write
																				// changes
				}
				// Handle Overflow
				this.treatOverflow(path);
			}
		}
	}
	/**
	 * 
	 * @param mmTree
	 * @return
	 */
	protected List<IndexEntry> writeRangeTree(Iterator dataIterator,
			Predicate overflows, List leftFlankIds,
			List<MapEntry<Object, VariableLengthBPlusTree.Node>> mmTree) {
		// list that holds node entry pair for each level
		// List<MapEntry<Object,VariableLengthBPlusTree.Node>> mmTree
		// = new ArrayList<MapEntry<Object,VariableLengthBPlusTree.Node>>();
		int leafLevel = 0;
		while (dataIterator.hasNext()) {
			Object entry = dataIterator.next();
			insertAndWrite(entry, leafLevel, mmTree, overflows, leftFlankIds);
		}
		Node rootNode = null;
		List<IndexEntry> roots = new ArrayList<IndexEntry>();
		// go last nodes and add index entries
		for (int i = 0; i < mmTree.size();) {
			Node node = mmTree.get(i).getValue();
			i++;
			if (i < mmTree.size()) {
				Object id = this.container().insert(node); // TODO only reserve
															// do not write
				VariableLengthBPlusTree.IndexEntry indexEntry = (VariableLengthBPlusTree.IndexEntry) this
						.createIndexEntry(i + 1);
				indexEntry.initialize(id, this.separator(node.getLast()));// initialize
																			// with
																			// separator
				insertAndWrite(indexEntry, i, mmTree, overflows, leftFlankIds);
			} else {
				rootNode = node;
			}
		}
		if (this.underflows.invoke(rootNode)) {
			Iterator it = rootNode.entries();
			while (it.hasNext()) {
				roots.add((IndexEntry) it.next());
			}
		} else { // create root entry
			Object id = this.container().insert(rootNode);
			VariableLengthBPlusTree.IndexEntry indexEntry = (VariableLengthBPlusTree.IndexEntry) this
					.createIndexEntry(rootNode.level() + 1);
			indexEntry.initialize(id, this.separator(rootNode.getLast()));
			roots.add(indexEntry);
		}
		return roots;
	}

	/**
	 * 
	 * @param entry
	 * @param level
	 * @param mmTree
	 * @param overflows
	 */
	protected void insertAndWrite(Object entry, int level,
			List<MapEntry<Object, VariableLengthBPlusTree.Node>> mmTree,
			Predicate overflows, List leftFlankIds) {
		// check if the node exists on level
		if (level + 1 > mmTree.size()) { // first node left flank
			Node newNode = (VariableLengthBPlusTree.Node) this
					.createNode(level);
			Object idOfNode = this.container().reserve(new Constant(newNode));
			MapEntry mapEntry = new MapEntry(idOfNode, newNode);
			mmTree.add(mapEntry);
			leftFlankIds.add(idOfNode);
		}
		VariableLengthBPlusTree.Node node = mmTree.get(level).getValue();
		Object id = mmTree.get(level).getKey();
		node.grow(entry, null);
		if (overflows.invoke(node)) {
			// delete last
			node.remove(node.number() - 1);
			// add new node on level
			Node nNode = (VariableLengthBPlusTree.Node) this.createNode(level);
			nNode.grow(entry, null);
			Object newId = this.container().reserve(new Constant(nNode));
			node.nextNeighbor = (IndexEntry) this.createIndexEntry(level + 1);
			node.nextNeighbor.initialize(newId);
			this.container().update(id, node); // write
			mmTree.remove(level);
			mmTree.add(level, new MapEntry(newId, nNode));
			VariableLengthBPlusTree.IndexEntry indexEntry = (VariableLengthBPlusTree.IndexEntry) this
					.createIndexEntry(level + 1);
			indexEntry.initialize(id, this.separator(node.getLast()));// initialize
																		// with
																		// separator
			insertAndWrite(indexEntry, level + 1, mmTree, overflows,
					leftFlankIds);
		}
	}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////	
	
	
	/**
	 * 
	 * @author achakeye
	 * 
	 */
	public class IndexEntry extends Tree.IndexEntry {
		/**
		 * The <tt>Separator</tt> of the <tt>IndexEntry</tt>. It describes the
		 * key range of the subtree refered by this <tt>IndexEntry</tt>.
		 */
		public Separator separator;

		/**
		 * Creates a new <tt>IndexEntry</tt> with a given
		 * {@link xxl.core.indexStructures.Tree.IndexEntry#parentLevel}.
		 * 
		 * @param parentLevel
		 *            the parent level of the new <tt>IndexEntry</tt>
		 */
		public IndexEntry(int parentLevel) {
			super(parentLevel);
		}

		/**
		 * Initializes the (new created) <tt>IndexEntry</tt> using split
		 * information. The <tt>SplitInfo</tt> is used to determine the new
		 * <tt>Separator</tt> of the new <tt>IndexEntry</tt>.
		 * 
		 * @param splitInfo
		 *            contains information about the split which led to this
		 *            <tt>IndexEntry</tt> being created
		 * @return the <tt>IndexEntry</tt> itself
		 * 
		 * @see BPlusTree.Node.SplitInfo
		 * @see BPlusTree.Node.SplitInfo#separatorOfNewNode()
		 */
		public Tree.IndexEntry initialize(Tree.Node.SplitInfo splitInfo) {
			super.initialize(splitInfo);
			Separator sep = ((Node.SplitInfo) splitInfo).separatorOfNewNode();
			if (sep == null)
				return this;
			return initialize(sep);
		}

		/**
		 * Initializes the <tt>IndexEntry</tt> by the given <tt>Separator</tt>.
		 * 
		 * @param separator
		 *            the new <tt>Separator</tt> of the current
		 *            <tt>IndexEntry</tt>.
		 * @return the <tt>IndexEntry</tt> itself.
		 */
		public IndexEntry initialize(Separator separator) {
			this.separator = (Separator) separator.clone();
			return this;
		}

		/**
		 * Initializes the <tt>IndexEntry</tt> with the given ID and
		 * <tt>Separator</tt>.
		 * 
		 * @param id
		 *            the new ID of the current <tt>IndexEntry</tt>
		 * @param separator
		 *            the new <tt>Separator</tt> of the current
		 *            <tt>IndexEntry</tt>
		 * @return the <tt>IndexEntry</tt> itself
		 */
		public IndexEntry initialize(Object id, Separator separator) {
			return ((IndexEntry) this.initialize(id)).initialize(separator);
		}

		/**
		 * Gives the <tt>Separator</tt> of this <tt>IndexEntry</tt>.
		 * 
		 * @return the <tt>Separator</tt> of this <tt>IndexEntry</tt>
		 */
		public Separator separator() {
			return separator;
		}

		/**
		 * 
		 * @return
		 */
		public Object getId() {
			return this.id;
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see java.lang.Object#toString()
		 */
		public String toString() {
			return "(" + separator + ", ->" + id + ")";
		}

	}

	/**
	 * 
	 * @author achakeye
	 * 
	 */
	public class Node extends Tree.Node {
		/**
		 * 
		 */
		protected int byteLoad = 0;
		/**
		 * A <tt>List</tt> to hold the entries of the <tt>Node</tt>.
		 */
		protected List entries;
		/**
		 * This is a reference to the <tt>Node</tt> containing the smallest key
		 * which is larger than all keys stored in the subtree of this
		 * <tt>Node</tt>.
		 */
		public IndexEntry nextNeighbor;

		/**
		 * Creates a new <tt>Node</tt> on a given level.
		 * 
		 * @param level
		 *            the level of the new <tt>Node</tt>
		 * @param createEntryList
		 *            a factory <tt>Function</tt> to create the entries list
		 * 
		 * @see BPlusTree.Node#createEntryList
		 */
		public Node(int level, List entries) {
			super();
			initialize(level, entries);
		}

		/**
		 * Creates a new <tt>Node</tt> on a given level. An
		 * {@link java.util.ArrayList ArrayList}is used to store the entries.
		 * 
		 * @param level
		 *            the level of the new <tt>Node</tt>
		 */
		public Node(final int level) {
			super();
			initialize(level, new ArrayList());
		}

		/**
		 * Initializes the (new) <tt>Node</tt> with a level and a
		 * <tt>Function</tt> for creating the <tt>Entries List</tt>.
		 * 
		 * @param level
		 *            the level of the <tt>Node</tt>
		 * @param createEntryList
		 *            a factory <tt>Function</tt> to create the entries list
		 * @return the initialized <tt>Node</tt>
		 */
		private Node initialize(int level, List entries) {
			this.level = level;
			this.entries = entries;
			return this;
		}

		/**
		 * Initializes the <tt>Node</tt> and inserts a new entry into it.
		 * 
		 * @param entry
		 *            the entry which has to be inserted
		 * @return <tt>SplitInfo</tt> which contains information about a
		 *         possible split
		 */
		protected Tree.Node.SplitInfo initialize(Object entry) {
			Stack path = new Stack();
			if (level() > 0) {
				IndexEntry indexEntry = (IndexEntry) entry;
				// if (indexEntry == rootEntry )
				// System.out.println("Do Nothing");
				// indexEntry.separator = createSeparator(((KeyRange)
				// rootDescriptor).maxBound());
				// new code
				// 	 

			}
			grow(entry, path);
			return new SplitInfo(path).initialize((Separator) separator(
					this.getLast()).clone());
		}

		/**
		 * Gives the next neighbor of this <tt>Node</tt>.
		 * 
		 * @return the next neighbor of this <tt>Node</tt>
		 * 
		 * @see BPlusTree.Node#nextNeighbor
		 */
		public IndexEntry nextNeighbor() {
			return nextNeighbor;
		}

		/**
		 * 
		 * @param load
		 */
		public void setByteLoad(int load) {
			this.byteLoad = load;
		}

		/**
		 * Gives the entry stored on the given position in the <tt>Node</tt>.
		 * 
		 * @param index
		 *            the position of the required entry
		 * @return the entry stored on the given position in the <tt>Node</tt>
		 *         or <tt>null</tt> if the <tt>Node</tt> is empty
		 */
		public Object getEntry(int index) {
			if (entries.isEmpty())
				return null;
			return entries.get(index);
		}

		/**
		 * Gives the last entry of the <tt>Node</tt>.
		 * 
		 * @return the last entry of the <tt>Node</tt> or <tt>null</tt> if the
		 *         <tt>Node</tt> is empty
		 */
		public Object getLast() {
			if (entries.isEmpty())
				return null;
			return entries.get(entries.size() - 1);
		}

		/**
		 * Gives the first entry of the <tt>Node</tt>.
		 * 
		 * @return the first entry of the <tt>Node</tt> or <tt>null</tt> if the
		 *         <tt>Node</tt> is empty
		 */
		public Object getFirst() {
			if (entries.isEmpty())
				return null;
			return entries.get(0);
		}

		/**
		 * Gives the number of entries which are currently stored in this
		 * <tt>Node</tt>.
		 * 
		 * @return the number of entries which are currently stored in this
		 *         <tt>Node</tt>
		 */
		public int number() {
			return entries.size();
		}

		/**
		 * 
		 * @return
		 */
		public int getLevel() {
			return this.level;
		}

		/**
		 * Gives an <tt>Iterator</tt> pointing to all entries stored in this
		 * <tt>Node</tt>
		 * 
		 * @return an <tt>Iterator</tt> pointing to all entries stored in this
		 *         <tt>Node</tt>
		 */
		public Iterator entries() {
			return iterator();
		}

		/**
		 * 
		 * @return
		 */
		public List getEntries() {
			return this.entries;
		}

		/**
		 * 
		 * @return
		 */
		public int getEntryByteSize(Object entry, int nodeLevel) {
			return (nodeLevel == 0) ? getActualEntrySize.invoke(entry)
					: getActualKeySize.invoke(separator(entry).sepValue())
							+ containerIdSize;
		}

		/**
		 * 
		 * @param entry
		 * @return
		 */
		public int getKeyByteSize(Object entry) {
			return getActualKeySize.invoke(separator(entry).sepValue());
		}

		/**
		 * 
		 * @return
		 */
		public int getCurrentLoad() {
			return byteLoad;
		}

		/**
		 * if add = true, increase byteLoad else decerease byteload;
		 * 
		 * @param entry
		 * @return
		 */
		public void updateByteLoad(Object entry, boolean add) {
			byteLoad = (add) ? byteLoad + getEntryByteSize(entry, level)
					: byteLoad - getEntryByteSize(entry, level);
		}

		/**
		 * Gives an <tt>Iterator</tt> pointing to all entries stored in this
		 * <tt>Node</tt>
		 * 
		 * @return an <tt>Iterator</tt> pointing to all entries stored in this
		 *         <tt>Node</tt>
		 */
		protected Iterator iterator() {
			return entries.iterator();
		}
		/*
		 * (non-Javadoc)
		 * @see xxl.core.indexStructures.Tree.Node#overflows()
		 */
		@Override
		protected boolean overflows() {
			// TODO Auto-generated method stub
			return super.overflows();
		}
		/*
		 * (non-Javadoc)
		 * @see xxl.core.indexStructures.Tree.Node#underflows()
		 */
		@Override
		protected boolean underflows() {
			// TODO Auto-generated method stub
			return super.underflows();
		}
		
		/**
		 * Gives an <tt>Iterator</tt> pointing to the <tt>Descriptors</tt> of
		 * each entry in this <tt>Node</tt>.
		 * 
		 * @param nodeDescriptor
		 *            the descriptor of this <tt>Node</tt>
		 * @return an <tt>Iterator</tt> pointing to the <tt>Descriptors</tt> of
		 *         each entry in this <tt>Node</tt>
		 */
		public Iterator descriptors(Descriptor nodeDescriptor) {
			return new Mapper(new AbstractFunction() {
				public Object invoke(Object entry) {
					return separator(entry);
				}
			}, iterator());
		}

		/**
		 * Returns an <tt>Iterator</tt> of entries whose <tt>Separators</tt>
		 * overlap with the <tt>queryDescriptor</tt>. Initialization of minIndex
		 * and maxIndex similar to
		 * {@link BPlusTree.Node#chooseSubtree(Descriptor descriptor, Stack path)}
		 * 
		 * @param queryDescriptor
		 *            the <tt>KeyRange</tt> describing the query
		 * @return an <tt>Iterator</tt> of entries whose <tt>Separators</tt>
		 *         overlap with the <tt>queryDescriptor</tt>
		 */
		public Iterator query(Descriptor queryDescriptor) {
			KeyRange qInterval = (KeyRange) queryDescriptor;
			int minIndex = search(qInterval.minBound());
			int maxIndex = rightMostSearch(qInterval.maxBound());
			List response = null;
			minIndex = (minIndex >= 0) ? minIndex : (-minIndex - 1 == this
					.number()) ? -minIndex - 2 : -minIndex - 1;
			maxIndex = (maxIndex >= 0) ? maxIndex : (-maxIndex - 1 == this
					.number()) ? -maxIndex - 2 : -maxIndex - 1;
			maxIndex = Math.min(maxIndex + 1, number());
			response = entries.subList(minIndex, maxIndex);
			return response.iterator();
		}

		/**
		 * 
		 * @return
		 */
		public int computeActualLoad() {
			return computeLoad(0, this.number());
		}

		/**
		 * 
		 * [from, to) to index is not incuded
		 * 
		 * @return
		 */
		public int computeLoad(int from, int to) {
			if (from < 0 || to > this.number())
				throw new IllegalArgumentException("out of range!");
			int load = 0;
			for (int i = from; i < to; i++)
				load += getEntryByteSize(this.getEntry(i), this.level);
			return load;
		}

		/**
		 * Searches the given key in this <tt>Node</tt> using the binary search
		 * algorithm on a sorted list. NOTE: if running in duplicate mode left
		 * most duplicate value will be returned
		 * 
		 * @param key
		 *            the key which is to search in this <tt>Node</tt>
		 * @return the position of the key if it was found or its insertion
		 *         position in the list otherwise
		 */
		protected int binarySearch(Comparable key) {
			if (entries.size() == 0)
				return -1;
			List sepValues = new MappedList(entries, new AbstractFunction() {
				public Object invoke(Object entry) {
					return separator(entry).sepValue();
				}
			});
			int minIndex = Collections.binarySearch(sepValues, key);
			return (duplicate) ? leftMostSearch(sepValues, minIndex, key)
					: minIndex;
		}

		/**
		 * required for duplicate mode. Left most search of duplicate value.
		 * 
		 * @param entryList
		 * @param key
		 * @return the position of the left most duplicate value
		 */
		protected int leftMostSearch(List entryList, int index, Comparable key) {
			if (index >= 0) {
				while (!(index == 0) && entryList.get(index - 1).equals(key)) {
					index--;
				}
			}
			return index;
		}

		/**
		 * required for duplicate mode. Right most search of duplicate value.
		 * 
		 * @return
		 */
		protected int rightMostSearch(Comparable key) {
			List sepValues = new MappedList(entries, new AbstractFunction() {
				public Object invoke(Object entry) {
					return separator(entry).sepValue();
				}
			});
			int index = Collections.binarySearch(sepValues, key);
			if (index >= 0) {
				while (index != (sepValues.size() - 1)
						&& sepValues.get(index + 1).equals(key))
					index++;
			}
			return index;
		}

		/**
		 * Searches the given key in this <tt>Node</tt>. The default
		 * implementation simply calls the method
		 * {@link BPlusTree.Node#binarySearch(Comparable)}.
		 * 
		 * @param key
		 *            the key which is to search in this <tt>Node</tt>
		 * @return the position of the key if it was found or its insertion
		 *         position in the list otherwise
		 */
		public int search(Comparable key) {
			return binarySearch(key);
		}

		/**
		 * Searches the given <tt>Separator</tt> in this <tt>Node</tt>. The
		 * default implementation takes the separation value of the given
		 * <tt>Separator</tt> and searches for it using the method
		 * {@link BPlusTree.Node#search(Comparable)}.
		 * 
		 * @param separator
		 *            the <tt>Separator</tt> which is to search in this
		 *            <tt>Node</tt>
		 * @return the position of the <tt>Separator</tt> if it was found or its
		 *         insertion position in the list otherwise
		 */
		protected int search(Separator separator) {
			if (entries.size() == 0)
				return -1;
			if (!separator.isDefinite())
				return -this.number();
			return search(separator.sepValue());
		}

		/**
		 * Searches the given <tt>IndexEntry</tt> in this <tt>Node</tt>. The
		 * default implementation takes the <tt>Separator</tt> of the given
		 * <tt>IndexEntry</tt> and searches for it using the method
		 * {@link BPlusTree.Node#search(Separator)}.
		 * 
		 * @param indexEntry
		 *            the <tt>IndexEntry</tt> which is to search in this
		 *            <tt>Node</tt>
		 * @return the position of the <tt>IndexEntry</tt> if it was found or
		 *         its insertion position in the list otherwise
		 */
		protected int search(IndexEntry indexEntry) {
			return search(indexEntry.separator);
		}

		/**
		 * Chooses the subtree which is followed during an insertion.
		 * 
		 * In duplicate mode the minIndex is evaluated to positve when with a
		 * successful search or negative when the value separator is not found,
		 * this negative value points to the position in the list where the new
		 * item should be inserted. when this position lies at the end of the
		 * list a special case is used to correct the value. In normal mode for
		 * negative values of minIndex the item must be inserted before the item
		 * at the position pointed at by the returned value.
		 * {@link BPlusTree.Node#binarySearch(Comparable)}
		 * 
		 * @param descriptor
		 *            the <tt>Separator</tt> of the entry which is to insert
		 * @param path
		 *            the path from the root to this <tt>Node</tt>. The default
		 *            implementation does not use this path
		 * @return the <tt>IndexEntry</tt> pointing to the subtree which is
		 *         followed during an insertion
		 */
		protected Tree.IndexEntry chooseSubtree(Descriptor descriptor,
				Stack path) {
			Separator separator = (Separator) descriptor;
			int minIndex = search(separator);
			minIndex = (minIndex >= 0) ? minIndex : (-minIndex - 1 == this
					.number()) ? -minIndex - 2 : -minIndex - 1;
			IndexEntry subTreeEntry = (IndexEntry) entries.get(minIndex);
			return subTreeEntry;
		}

		/**
		 * Posts the <tt>SplitInfo</tt> from the child <tt>Nodes</tt> to the
		 * current <tt>Node</tt>. The method inserts the new <tt>IndexEntry</tt>
		 * into the <tt>Node</tt> using the method
		 * {@link BPlusTree.Node#grow(Object, Stack)}. It gets the path from the
		 * given <tt>SplitInfo</tt>.
		 * 
		 * @param splitInfo
		 *            contains the information about the split which led to
		 *            create the new <tt>IndexEntry</tt>
		 * @param newIndexEntry
		 *            the new <tt>IndexEntry</tt> created by the split
		 * 
		 * @see BPlusTree.Node#grow(Object, Stack)
		 */
		protected void post(Tree.Node.SplitInfo splitInfo,
				Tree.IndexEntry newIndexEntry) {
			if (!duplicate) { // update
				updateSeparatorOfTriggeredNode(splitInfo);
			}
			grow(newIndexEntry, splitInfo.path);
			if (duplicate) { // first insert beacuse it coukld be that index
								// entry was also duplicate
				// in order to place on the next position first palse then
				// change the separator
				updateSeparatorOfTriggeredNode(splitInfo);
			}
		}

		/**
         * 
         */
		private void updateSeparatorOfTriggeredNode(
				Tree.Node.SplitInfo splitInfo) {
			Object newIndexEntryOfTriggeredNode = ((SplitInfo) splitInfo)
					.separatorOfTriggeredNode();
			IndexEntry oldIndexEntryOfTriggeredNode = ((IndexEntry) indexEntry(splitInfo.path));
			int newSizeOfIndexEntry = getEntryByteSize(
					separator(newIndexEntryOfTriggeredNode), this.level);
			int oldSizeOfIndexEntry = getEntryByteSize(
					oldIndexEntryOfTriggeredNode, this.level);
			// update separator value
			Separator boundary = (Separator) separator(
					newIndexEntryOfTriggeredNode).clone();
			oldIndexEntryOfTriggeredNode.separator.updateSepValue(boundary
					.sepValue());
			// update load info of the parent node
			byteLoad -= oldSizeOfIndexEntry;
			byteLoad += newSizeOfIndexEntry;
		}

		/**
		 * Inserts an entry into the current <tt>Node</tt>.
		 * 
		 * @param entry
		 *            the entry which has to be inserted into the node
		 * @param path
		 *            the path from the root to the current node. The default
		 *            Implementation does not use this path
		 * 
		 * @see BPlusTree.Node#grow(Object)
		 */
		protected void grow(Object entry, Stack path) {
			grow(entry);
			// update byteload
			this.byteLoad += this.getEntryByteSize(entry, this.level);
			// check for test
//			if (this.computeActualLoad() != this.byteLoad) {
//				System.out.println("Entry to insert " + entry);
//				System.out.println("inconsistency found");
//				System.out.println(this);
//			}
		}

		/**
		 * Inserts a new entry into this <tt>Node</tt> at the suitable position.
		 * The position is found using a binary search.
		 * {@link BPlusTree.Node#binarySearch(Comparable)}
		 * 
		 * @param entry
		 *            the new entry which has to be inserted
		 * @exception IllegalArgumentException
		 *                in normal mode if the key already exists in the node.
		 * 
		 */
		protected void grow(Object entry) {
			int index;
			if (entries.isEmpty())
				index = 0;
			else {
				index = search((separator(entry)));
				if (duplicate) {
					if (index >= 0)
						index = (level != 0) ? index + 1 : index;
					else
						index = -index - 1;
				} else {
					if (index >= 0) {
						throw new IllegalArgumentException(
								"Insertion failed: An entry having the same key "
										+ "was found");
					}
					index = -index - 1;
				}
			}
			entries.add(index, entry);
		}

		/**
         * 
         */
		protected Collection redressOverflow(Stack path, List newIndexEntries,
				boolean up) {
			if (overflows()) {
				super.redressOverflow(path, newIndexEntries, up);
			}// check whether the last node affected
			else {
				// get Index Entry of the node
				if (!path.isEmpty()) {
					// check separator
					IndexEntry indexEntry = (VariableLengthBPlusTree.IndexEntry) indexEntry(path);
					// check
					if (indexEntry.separator().compareTo(
							separator(this.getLast())) < 0) {
						// get Old size
						int size = getEntryByteSize(indexEntry, this.level + 1);
						// update parentNode
						Separator sep = (Separator) separator(this.getLast())
								.clone();
						indexEntry.separator = sep;
						size -= getEntryByteSize(indexEntry, this.level + 1);
						// get parent node if exist
						MapEntry pathEntry = (MapEntry) path.pop();
						if (!path.isEmpty()) {
							Node parentNode = (VariableLengthBPlusTree.Node) node(path);
							parentNode.byteLoad -= size;
							indexEntry(path).update(parentNode, false);
							newIndexEntries.add(newIndexEntries.size(),
									indexEntry);
						}
						path.push(pathEntry);
					}
				}
				if (up) {
					update(path);
					up(path);
				}
			}
			return newIndexEntries;
		}

		/**
		 * Treats overflows which occur during an insertion. The method
		 * {@link xxl.core.indexStructures.Tree.Node#redressOverflow(Stack, xxl.core.indexStructures.Tree.Node, List)}
		 * of the super class is used to split the overflowing <tt>Node</tt> in
		 * two <tt>Nodes</tt>. It creates a new <tt>IndexEntry</tt> for the new
		 * <tt>Node</tt>. This <tt>IndexEntry</tt> will be added to the given
		 * List. Finally the new <tt>Node</tt> is linked from the split
		 * <tt>Node</tt> as {@link BPlusTree.Node#nextNeighbor}.
		 * 
		 * @param path
		 *            the path from the root to the overflowing <tt>Node</tt>
		 * @param parentNode
		 *            the parent <tt>Node</tt> of the overflowing <tt>Node</tt>
		 * @param newIndexEntries
		 *            a List to carry the new <tt>IndexEntries</tt> created by
		 *            the split
		 * @return a <tt>SplitInfo</tt> containing required information about
		 *         the split
		 */
		protected Tree.Node.SplitInfo redressOverflow(Stack path,
				Tree.Node parentNode, List newIndexEntries) {
			Node node = (Node) node(path);
			SplitInfo splitInfo = (SplitInfo) super.redressOverflow(path,
					parentNode, newIndexEntries);
			// link triggered node to new created node
			IndexEntry newIndexEntry = (IndexEntry) newIndexEntries
					.get(newIndexEntries.size() - 1);
			node.nextNeighbor = newIndexEntry;

			return splitInfo;
		}

		/**
		 * Splits the overflowed node. In non duplicate mode both the leaf and
		 * index nodes are splited in the middle. In duplicate mode the leaf
		 * nodes are splited according following strategy: with leaf nodes the
		 * last element is selected and checked against the the element at 75%
		 * position, when both are equal search for further duplicates until 25%
		 * reached or no more duplicates exists, then split at found index
		 * position, otherwise split in the middle.
		 * 
		 * @param path
		 * @return a <tt>SplitInfo</tt> containing all needed information about
		 *         the split
		 */
		protected Tree.Node.SplitInfo split(Stack path) {
			reorg = true;
			SplitInfo splitInfo = null;
			Node node = (Node) node(path);
			List newEntries = null;
			// strategy

			splitInfo = VariableLengthBPlusTree.this.splitSt.runSplit(path,
					this, node, minAllowedLoad, averageLoad
							+ (averageLoad - minAllowedLoad));
			newEntries = node.entries.subList(splitInfo.distributionIndex, node
					.number());
			// Separator sepNewNode = (Separator)
			// separator(newEntries.get(newEntries.size()-1)).clone();
			entries.addAll(newEntries);
			newEntries.clear();
			nextNeighbor = node.nextNeighbor;
			// compute Load
			node.byteLoad = (splitInfo.loadOfSplitTriggeredNode > 0) ? splitInfo.loadOfSplitTriggeredNode
					: node.computeActualLoad();
			byteLoad = (splitInfo.loadOfNewNode > 0) ? splitInfo.loadOfNewNode
					: this.computeActualLoad();
			return splitInfo;
		}

		/**
		 * Searches the given <tt>IndexEntry</tt> and removes it from this
		 * <tt>Node</tt>.
		 * 
		 * @param indexEntry
		 *            the <tt>IndexEntry</tt> which is to remove
		 * @return the removed <tt>IndexEntry</tt> if it was found or
		 *         <tt>null</tt> otherwise
		 */
		protected IndexEntry remove(IndexEntry indexEntry) {
			if (level == 0)
				return null;
			int index = 0;
			if (duplicate) { // 
				while (!getEntry(index).equals(indexEntry)
						&& index < this.number()) {
					index++;
				}
			} else {
				index = search(indexEntry);
			}
			return (IndexEntry) remove(index);
		}

		/**
		 * Removes the entry stored in the <tt>Node</tt> on the given position.
		 * 
		 * @param index
		 *            the position of the entry which is to be removed
		 * @return the removed entry if the given position is valid or
		 *         <tt>null</tt> otherwise
		 */
		protected Object remove(int index) {
			if ((index < 0) || (index >= entries.size()))
				return null;
			Object entry = entries.get(index);
			entries.remove(index);
			// Update byte load info
			this.byteLoad -= getEntryByteSize(entry, this.level);
			return entry;
		}

		/**
		 * Treats underflows which occur during a remove operation. If the given
		 * path is empty an <tt>IllegalStateException</tt> is thrown, otherwise
		 * the method {@link BPlusTree.Node#redressUnderflow(Stack, boolean)}is
		 * called with the parameter path and <tt>true</tt> to repair the
		 * underflow.
		 * 
		 * @param path
		 *            the path from the root to the underflowing <tt>Node</tt>
		 * @return a boolean which indicates whether the parent <tt>Node</tt>
		 *         was updated during the operation
		 */
		protected boolean redressUnderflow(Stack path) {
			if (path.isEmpty())
				throw new IllegalStateException();
			return redressUnderflow(path, true);
		}

		/**
		 * Treats underflows which occur during a remove operation. In case of
		 * underflow the method
		 * {@link BPlusTree.Node#redressUnderflow (BPlusTree.IndexEntry, BPlusTree.Node, Stack, boolean)}
		 * is called to repair the underflow. When the root node has only one
		 * entry the root entry pointer is deleted and replaced with the child
		 * node.
		 * 
		 * @param path
		 *            the path from the root to the underflowing <tt>Node</tt>
		 * @param up
		 *            signals whether the treated <tt>Nodes</tt> have to be
		 *            updated in the underlying <tt>Container</tt>
		 * @return a boolean which indicates whether the parent <tt>Node</tt>
		 *         was been updated during the operation
		 */
		protected boolean redressUnderflow(Stack path, boolean up) {
			MapEntry pathEntry = (MapEntry) path.pop();
			IndexEntry indexEntry = (IndexEntry) pathEntry.getKey();
			Node node = (Node) pathEntry.getValue();
			boolean parentUpdated = false;
			if (path.isEmpty() && (node.number() == 1) && (node.level > 0)) { // root
																				// node...
				IndexEntry newRootEntry = (IndexEntry) node.getLast();
				rootEntry.remove();
				rootEntry = newRootEntry;
				//XXX Why should be done this ??? check!!!
				((IndexEntry) rootEntry).separator = null;
				
			} else {
				if (!path.isEmpty() && node.underflows()) {
					parentUpdated = redressUnderflow(indexEntry, node, path, up);
				}
				if (!parentUpdated && up) {
					indexEntry.update(node, true);
					indexEntry.unfix();
				}
			}
			// update load Info in
			return parentUpdated;
		}

		/**
		 * Treats underflows which occur during a remove operation. It calls the
		 * method {@link BPlusTree.Node#merge(BPlusTree.IndexEntry, Stack)}to
		 * redistribute the elements of the underflowing <tt>Node</tt> an a
		 * suitable sibling or to merge them so that the underflow is repaired.
		 * 
		 * @param indexEntry
		 *            the <tt>IndexEntry</tt> referring to the underflowing
		 *            <tt>Node</tt>
		 * @param node
		 *            the underflowing <tt>Node</tt>
		 * @param path
		 *            the path from the root to parent of the underflowing
		 *            <tt>Node</tt>
		 * @param up
		 *            signals whether the treated <tt>Nodes</tt> have to be
		 *            updated in the underlying <tt>Container</tt>
		 * @return a boolean which indicate whether the parent <tt>Node</tt> was
		 *         been updated during the operation
		 */
		protected boolean redressUnderflow(IndexEntry indexEntry, Node node,
				Stack path, boolean up) {
			Node parentNode = (Node) node(path);
			// The merge...
			MergeInfo mergeInfo = node.merge(indexEntry, path);
			// Post the changes to the parent Node...
			boolean update = !mergeInfo.isPostponedMerge();
			if (update) {
				// TODO recompute Load for parent Nodes
				if (mergeInfo.isMerge()) {
					if (mergeInfo.isRightSide()) {
						((IndexEntry) parentNode.getEntry(mergeInfo.index())).separator = mergeInfo
								.newSeparator();
						mergeInfo.indexEntry().update(mergeInfo.node(), true);
						((IndexEntry) parentNode.remove(mergeInfo.index() + 1))
								.remove();
					} else {
						((IndexEntry) parentNode
								.getEntry(mergeInfo.index() - 1)).separator = mergeInfo
								.siblingNewSeparator();
						mergeInfo.siblingIndexEntry().update(
								mergeInfo.siblingNode(), true);
						((IndexEntry) parentNode.remove(mergeInfo.index()))
								.remove();
					}
				} else {
					((IndexEntry) parentNode.getEntry(mergeInfo.index())).separator = mergeInfo
							.newSeparator();
					int sibIndex = mergeInfo.isRightSide() ? mergeInfo.index() + 1
							: mergeInfo.index() - 1;
					((IndexEntry) parentNode.getEntry(sibIndex)).separator = mergeInfo
							.siblingNewSeparator();
					if (up) {
						mergeInfo.indexEntry().update(mergeInfo.node(), true);
						mergeInfo.siblingIndexEntry().update(
								mergeInfo.siblingNode(), true);
					}
				}
				// Update load info
				parentNode.byteLoad = parentNode.computeActualLoad();
			} else {
				// release nodes sibling nodes
				mergeInfo.siblingIndexEntry().unfix();

			}
			return update;
		}

		/**
		 * Used to redistribute the elements of the underflowing <tt>Node</tt>
		 * from a suitable sibling or to merge them so that the underflow is
		 * repaired.
		 * 
		 * @param indexEntry
		 *            the <tt>IndexEntry</tt> referring to the underflowing
		 *            <tt>Node</tt>
		 * @param path
		 *            the path from the root to parent of the underflowing
		 *            <tt>Node</tt>
		 * @return a <tt>MergeInfo</tt> containing some information about the
		 *         merge (or redistribution)
		 */
		protected MergeInfo merge(IndexEntry indexEntry, Stack path) {
			reorg = true;
			// Get the neighbor node with the most entries...
			Node parentNode = (Node) node(path);
			int index = parentNode.search(indexEntry);
			index = Math.max(0, index);
			boolean left = (index > 0);
			IndexEntry siblingIndexEntry = (IndexEntry) parentNode
					.getEntry((left) ? index - 1 : index + 1);
			Node siblingNode = (Node) siblingIndexEntry.get(false);
			MergeInfo mergeInfo = new MergeInfo(indexEntry, this, index, path);
			underflowHandler.runUnderflowHandling(mergeInfo, indexEntry, this,
					siblingIndexEntry, siblingNode, minAllowedLoad,
					averageLoad, left);
			return mergeInfo;
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see java.lang.Object#toString()
		 */
		public String toString() {
			String nextNeighborString = (nextNeighbor == null) ? "none"
					: nextNeighbor.toString();
			return "{" + number() + ": " + entries + "}" + " NN: "
					+ nextNeighborString + "ByteLoad:  " + this.byteLoad
					+ " ComputedLoad  " + this.computeActualLoad();
		}

		/**
		 * A <tt>SplitInfo</tt> contains information about a split. The
		 * enclosing object of this SplitInfo-Object (i.e. <tt>Node.this</tt>)
		 * is the new node which was created by the split.
		 */
		public class SplitInfo extends Tree.Node.SplitInfo {
			/**
			 * The <tt>Separator</tt> of the <tt>IndexEntry</tt> of the new
			 * <tt>Node</tt>.
			 */
			protected Separator separatorOfNewNode;

			protected Separator separatorOfTriggeredNode;

			protected int loadOfNewNode;

			protected int loadOfSplitTriggeredNode;

			protected int distributionIndex;

			protected List entries;

			/**
			 * Creates a new <tt>SplitInfo</tt>.
			 * 
			 * @param path
			 *            the current path
			 */
			public SplitInfo(Stack path) {
				super(path);
			}

			/**
			 * 
			 * @param path
			 * @param loadOfNewNode
			 * @param loadOfSplitTriggeredNode
			 * @param distributionIndex
			 */
			public SplitInfo(Stack path, int loadOfNewNode,
					int loadOfSplitTriggeredNode, int distributionIndex) {
				this(path);
				this.loadOfNewNode = loadOfNewNode;
				this.loadOfSplitTriggeredNode = loadOfSplitTriggeredNode;
				this.distributionIndex = distributionIndex;
			}

			/**
			 * 
			 * @param path
			 * @param loadOfNewNode
			 * @param loadOfSplitTriggeredNode
			 * @param distributionIndex
			 */
			public SplitInfo(Stack path, int distributionIndex) {
				this(path);
				this.distributionIndex = distributionIndex;
			}

			/**
			 * Initializes the <tt>SplitInfo</tt> with the <tt>Separator</tt> of
			 * the <tt>IndexEntry</tt> of the new <tt>Node</tt>.
			 * 
			 * @param newSeparator
			 *            the <tt>Separator</tt> of the <tt>IndexEntry</tt> of
			 *            the new <tt>Node</tt>
			 * @return the initialized <tt>SplitInfo</tt> itself
			 */
			public SplitInfo initialize(Separator newSeparator) {
				this.separatorOfNewNode = newSeparator;
				return this;
			}

			/**
			 * Initializes the <tt>SplitInfo</tt> with the <tt>Separator</tt> of
			 * the <tt>IndexEntry</tt> of the new <tt>Node</tt>.
			 * 
			 * @param newSeparator
			 *            the <tt>Separator</tt> of the <tt>IndexEntry</tt> of
			 *            the new <tt>Node</tt>
			 * @return the initialized <tt>SplitInfo</tt> itself
			 */
			public SplitInfo initialize(Separator newSeparator,
					Separator sepratorOfTriggeredNode) {
				this.separatorOfNewNode = newSeparator;
				this.separatorOfTriggeredNode = sepratorOfTriggeredNode;
				return this;
			}

			/**
			 * Gives <tt>Separator</tt> of the <tt>IndexEntry</tt> of the new
			 * <tt>Node</tt>.
			 * 
			 * @return <tt>Separator</tt> of the <tt>IndexEntry</tt> of the new
			 *         <tt>Node</tt>
			 */
			public Separator separatorOfNewNode() {
				return separatorOfNewNode;
			}

			/**
			 * 
			 * @return
			 */
			public Separator separatorOfTriggeredNode() {
				return separatorOfTriggeredNode;
			}

			/**
			 * 
			 * @return
			 */
			public int getLoadOfNewNode() {
				return loadOfNewNode;
			}

			/**
			 * 
			 * @return
			 */
			public int getLoadOfSplitTriggeredNode() {
				return loadOfSplitTriggeredNode;
			}

			/**
			 * 
			 * @return
			 */
			public int getDistributionIndex() {
				return distributionIndex;
			}

			/**
			 * 
			 */
			public boolean isSplit() {
				return separatorOfNewNode != null
						&& separatorOfTriggeredNode != null;
			}

		}

		/**
		 * A <tt>MergeInfo</tt> contains information about a merge (or
		 * redistribution).
		 */
		public class MergeInfo {
			/**
             * 
             */
			int redressOverflowState;
			/**
			 * The State of the merge 
			 * @see VariableLengthBPlusTree.MergeState
			 */
			protected MergeState mergeState;
			/**
			 * The path including the underflowing node.
			 */
			protected Stack path;
			/**
			 * The position of the <tt>IndexEntry</tt> of the underflowing
			 * <tt>Node</tt> in the parent <tt>Node</tt>.
			 */
			protected int index;
			/**
			 * The <tt>IndexEntry</tt> of the underflowing <tt>Node</tt>.
			 */
			protected IndexEntry indexEntry;
			/**
			 * The underflowing <tt>Node</tt>.
			 */
			protected Node node;
			/**
			 * The <tt>IndexEntry</tt> of the sibling <tt>Node</tt>.
			 */
			protected IndexEntry siblingIndexEntry;
			/**
			 * The sibling <tt>Node</tt>.
			 */
			protected Node siblingNode;

			/**
			 * Creates a new <tt>MergeInfo</tt>.
			 * 
			 * @param indexEntry
			 *            the <tt>IndexEntry</tt> of the underflowing
			 *            <tt>Node</tt>
			 * @param node
			 *            the underflowing <tt>Node</tt>
			 * @param index
			 *            the position of the <tt>IndexEntry</tt> of the
			 *            underflowing <tt>Node</tt> in the parent <tt>Node</tt>
			 * @param path
			 *            the path including the underflowing node
			 */
			public MergeInfo(IndexEntry indexEntry, Node node, int index,
					Stack path) {
				this.indexEntry = indexEntry;
				this.node = node;
				this.index = index;
				this.path = path;
			}
			/**
			 * 
			 */
			public MergeInfo initialize(IndexEntry siblingIndexEntry,
					Node sibilingNode, int redressOverflowState) {
				this.siblingIndexEntry = siblingIndexEntry;
				this.siblingNode = sibilingNode;
				this.redressOverflowState = redressOverflowState;
				return this;
			}
			/**
			 * 
			 */
			public MergeInfo initialize(IndexEntry siblingIndexEntry,
					Node sibilingNode, MergeState mergeState) {
				this.siblingIndexEntry = siblingIndexEntry;
				this.siblingNode = sibilingNode;
				this.mergeState = mergeState;
				return this;
			}
			/**
			 * Computes the new <tt>Separator</tt> of the <tt>IndexEntry</tt>
			 * after the merge. The result is as follows:
			 * <ul>
			 * <li>Merge with the left sibling:
			 * <ul>
			 * <li>Real merge &rarr; <tt>IllegalStateException</tt></li>
			 * <li>Non real merge &rarr; a copy of the <tt>Separator</tt> of the <tt>Node's</tt>
			 * last entry</li>
			 * 
			 * @return the new <tt>Separator</tt> of the <tt>IndexEntry</tt>
			 */
			public Separator newSeparator() {
				if (!isRightSide() && isMerge())
					throw new IllegalStateException();
				return (Separator) separator(node.getLast()).clone();
			}

			/**
			 * Computes the new <tt>Separator</tt> of the sibling
			 * <tt>IndexEntry</tt> after the merge. The result is as follows:
			 * <ul>
			 * <li>It is the right sibling of the merged node &rarr; a copy of the
			 * <tt>Separator</tt> of the first entry of the sibling
			 * <tt>Node</tt></li>
			 * <li>It is the right sibling
			 * <ul>
			 * <li>Real merge &rarr; <tt>IllegalStateException</tt></li>
			 * <li>Non real merge &rarr; a copy of the <tt>Separator</tt> of the
			 * first entry of the sibling <tt>Node</tt></li>
			 * </ul>
			 * </li>
			 * </ul>
			 * 
			 * @return the new <tt>Separator</tt> of the sibling
			 *         <tt>IndexEntry</tt>
			 */
			public Separator siblingNewSeparator() {
				if (isRightSide()) {
					if (isMerge())
						throw new IllegalStateException();
					return siblingIndexEntry.separator;
				} else
					return (Separator) separator(siblingNode.getLast()).clone();
			}

			/**
			 * Indicates whether this MergeInfo describes a real merge or an
			 * element redistribution.
			 * 
			 * @return <tt>true</tt> if it is a real merge or <tt>false</tt>
			 *         otherwise
			 */
			public boolean isMerge() {
				return (this.redressOverflowState == MERGE);
//				return this.mergeState == MergeState.MERGE_LEFT || this.mergeState == MergeState.DISTRIBUTION_RIGHT;
			}

			/**
			 * Indicates whether the sibling <tt>Node</tt> lies on the right
			 * side of the underflowing <tt>Node</tt>.
			 * 
			 * @return <tt>true</tt> if the sibling <tt>Node</tt> lies on the
			 *         right side of the underflowing <tt>Node</tt> and
			 *         <tt>false</tt> otherwise
			 */
			public boolean isRightSide() {
				return index == 0;
			}

			/**
			 * Gives the position of the <tt>IndexEntry</tt> of the underflowing
			 * <tt>Node</tt> in the parent <tt>Node</tt>.
			 * 
			 * @return the position of the <tt>IndexEntry</tt> of the
			 *         underflowing <tt>Node</tt> in the parent <tt>Node</tt>
			 */
			public int index() {
				return index;
			}

			/**
			 * Gives the path including the underflowing node.
			 * 
			 * @return the path including the underflowing node
			 */
			public Stack path() {
				return path;
			}

			/**
			 * Gives the <tt>IndexEntry</tt> of the sibling <tt>Node</tt>.
			 * 
			 * @return the <tt>IndexEntry</tt> of the sibling <tt>Node</tt>
			 */
			public IndexEntry siblingIndexEntry() {
				return siblingIndexEntry;
			}

			/**
			 * Gives the parent <tt>Node</tt> of the underflowing <tt>Node</tt>.
			 * 
			 * @return the parent <tt>Node</tt> of the underflowing
			 *         <tt>Node</tt>
			 */
			public Node parentNode() {
				return (Node) VariableLengthBPlusTree.this.node(path);
			}

			/**
			 * Gives the underflowing <tt>Node</tt>.
			 * 
			 * @return the underflowing <tt>Node</tt>
			 */
			public Node node() {
				return node;
			}

			/**
			 * Gives the sibling <tt>Node</tt>.
			 * 
			 * @return the sibling <tt>Node</tt>
			 */
			public Node siblingNode() {
				return siblingNode;
			}

			/**
			 * Gives the <tt>IndexEntry</tt> of the underflowing <tt>Node</tt>.
			 * 
			 * @return the <tt>IndexEntry</tt> of the underflowing <tt>Node</tt>
			 */
			public IndexEntry indexEntry() {
				return indexEntry;
			}
			/**
			 * 
			 * @return
			 */
			public int getRedressOverflowState() {
				return this.redressOverflowState;
			}
			/**
			 * 
			 * @return
			 */
			public MergeState getMergeState(){
				return this.mergeState;
			}
			/**
			 * 
			 * @return
			 */
			public boolean isPostponedMerge() {
			 	return (this.redressOverflowState == POSTPONED_MERGE);
			//	return this.mergeState == MergeState.POSTPONED_MERGE;
			}

		}

	}

	/**
	 * This class uses an efficient algorithm to perform queries on the
	 * <tt>BPlusTree</tt>. It does not traverse the <tt>BPlusTree</tt> in
	 * TOP-DOWN fashion to find the response set, but it uses the known
	 * efficient querying algorithm of B+-Tree. It only traverses the path to
	 * the first <tt>Node</tt>, which can contain answers, and then traverses
	 * the <tt>Nodes</tt> on the same level cross the <tt>BPlusTree</tt> from
	 * left to right, using the <tt>nextNeighbor</tt> references of
	 * <tt>BPlusTree.Node</tt>.
	 */
	protected class QueryCursor extends xxl.core.indexStructures.QueryCursor {
		/**
		 * The position of the current entry in the current <tt>Node</tt>.
		 */
		private int index;
		/**
		 * The position of the last entry in the last <tt>Node</tt>.
		 */
		private int lastIndex;
		/**
		 * The <tt>IndexEntry</tt> of the last <tt>Node</tt>.
		 */
		private IndexEntry lastIndexEntry;
		/**
		 * The last <tt>Node</tt>, i.e. the <tt>Node</tt> which contains the
		 * last entry returned by the <tt>QueryCursor</tt>. It is used as a
		 * history information to support remove an update.
		 */
		private Node lastNode;
		/**
		 * A counter which counts how many <tt>Nodes</tt> on the target level
		 * was already visited. The number of the visited <tt>Nodes</tt> equals
		 * <tt>nodeChangeover+1</tt>.
		 */
		private int nodeChangeover;
		/**
		 * The previous Separator of the lastIndexEntry. Needed to support the
		 * remove-operation.
		 */
		private Separator prevSeparator;
		/**
		 * subRootEntry is the <tt>IndexEntry</tt> which is the root of the
		 * subtree in which the query has to be executed.
		 */
		private IndexEntry subRootEntry;
		/**
		 * counts occurrence of the elements with the same separator key in the
		 * cursor
		 */
		private int counterRightShiftDup;

		/**
		 * Creates a new <tt>QueryCursor</tt>.
		 * 
		 * @param subRootEntry
		 *            the <tt>IndexEntry</tt> which is the root of the subtree
		 *            in which the query has to be executed
		 * @param qInterval
		 *            a <tt>Descriptor</tt> which specifies the query
		 * @param targetLevel
		 *            the target level of the query
		 */
		public QueryCursor(IndexEntry subRootEntry, KeyRange qInterval,
				int targetLevel) {
			super(VariableLengthBPlusTree.this, subRootEntry, qInterval,
					targetLevel);
			this.subRootEntry = subRootEntry;
			lastQueryDisscriptor = qInterval;
			path = new Stack();
			index = -1;
			lastIndex = index;
			lastNode = null;
			lastIndexEntry = null;
			nodeChangeover = 0;
			counterRightShiftDup = 1;
		}

		/**
		 * Checks whether a next element exists. First builds the path down to
		 * the target level, using only the left element from the Iterator
		 * returned by {@link BPlusTree.Node#query(Descriptor)} Method searches
		 * through target level from left to right within the query range,
		 * looking through each node in turn. At the end of each node it
		 * switches to the next node using
		 * {@link BPlusTree.QueryCursor#nodeChangeover}, It searches until it
		 * has found an item right of min bound then tests this item if it is
		 * left from max bound and returns this boolean.
		 * 
		 * @see xxl.core.cursors.AbstractCursor#hasNextObject();
		 * @return <tt>true</tt> if there is a next element and <tt>false</tt>
		 *         otherwise
		 */
		public boolean hasNextObject() {
			if (index == -1) {// if is empty
				down(path, indexEntry);// look at root of the tree, place in
										// stack.
				while (level(path) > targetLevel) {// search down the tree until
													// past target level
					Iterator entries = node(path).query(queryRegion);// finds a
																		// region
																		// but
																		// only
																		// uses
																		// the
																		// first(left)
																		// element
					if (entries.hasNext())// if the first element exists , place
											// it in the path. otherwise
											// hasNextObject is false
						down(path, (IndexEntry) entries.next());
					else
						return false;
				}
				indexEntry = (IndexEntry) indexEntry(path);
				currentNode = (Node) node(path);
				lastIndexEntry = (IndexEntry) indexEntry;
				lastNode = (Node) currentNode;
				for (int i = 0; i < currentNode.number(); i++) {
					lastIndex = index;
					index = i;
					if (separator(((Node) currentNode).getEntry(i)).isRightOf(
							((KeyRange) queryRegion).minBound())) {
						break;
					}
					if (index == currentNode.number() - 1
							&& ((Node) currentNode).nextNeighbor != null) {
						nodeChangeOver();
						i = -1;
					}
					if (index == currentNode.number() - 1
							&& ((Node) currentNode).nextNeighbor == null) {
						return false;
					}
				}
			}
			if (index == -1)
				return false;
			if (index >= currentNode.number()) {
				if (((Node) currentNode).nextNeighbor == null) { // right flank
					return false;
				}
				nodeChangeOver();
			}
			return separator(((Node) currentNode).getEntry(index)).sepValue()
					.compareTo(((KeyRange) queryRegion).maxBound()) <= 0;
		}

		/**
		 * Computes the next element of the <tt>QueryCursor</tt>.
		 * 
		 * @return the next element
		 */
		public Object nextObject() {
			if (!indexEntry.equals(lastIndexEntry)) {
				abolishPath();
				lastIndexEntry.unfix();
			}
			Separator currentSeapartor = separator(((Node) lastNode)
					.getEntry(index));
			if (prevSeparator != null
					&& prevSeparator.sepValue().compareTo(
							currentSeapartor.sepValue()) == 0) {
				counterRightShiftDup++;
			} else {
				counterRightShiftDup = 1;
			}
			prevSeparator = currentSeapartor;
			lastIndex = index;
			lastIndexEntry = (IndexEntry) indexEntry;
			lastNode = (Node) currentNode;
			return ((Node) currentNode).getEntry(index++);
		}

		/**
		 * Removes the last element returned by the method next() and calls the
		 * method {@link BPlusTree#treatUnderflow(Stack)}to repair underflows
		 * which can occur. It is only supported on the level 0. If the path is
		 * not correct (due to nodeChangover) then it will be calculated. The
		 * root descriptor is updated if the min or max object is deleted.
		 */
		protected void removeObject() {
			if (!hasPath()) {
				path = new Stack(); // compute new path when the cursor moved to
									// the right
				Separator range = separator(((Node) lastNode)
						.getEntry(index - 1));
				backToFirstEntry(path, range, false);
				prevSeparator = range;
			}
			Object oldData = lastNode.remove(lastIndex);
			// TODO what if the different size are swapped???
			if (duplicate) { // swap deleted element with duplicate value in
								// computed left most duplicate node
				IndexEntry startEntry = (IndexEntry) indexEntry(path);
				Node startNode = (Node) node(path);
				if (!startEntry.equals(lastIndexEntry)) {
					Object changeData = startNode
							.remove(startNode.number() - 1); //
					lastNode.entries.add(0, changeData);
				}
			}
			lastIndexEntry.update(lastNode, false);
			KeyRange root = (KeyRange) rootDescriptor(); // update root
															// descriptor
			Separator old = separator(oldData);
			Container rootContainer = null;
			Object rootID = null;
			if (old.sepValue().compareTo(root.minBound()) == 0) {
				if (lastNode.number() != 0) {
					Comparable newMinbound = separator(lastNode.getFirst())
							.sepValue();
					root.updateMinBound(newMinbound);
				}else {// last Element was removed
					rootContainer = rootEntry.container();
					rootID = rootEntry.id();
					rootDescriptor = null;
					rootEntry = null;
				}
			} else {
				if (old.sepValue().compareTo(root.maxBound()) == 0
						&& lastNode.nextNeighbor() == null) {
					if (lastNode.number() != 0) {
						Comparable newMaxbound = separator(lastNode.getLast())
								.sepValue();
						root.updateMaxBound(newMaxbound);
					} else {// last Element was removed
						rootContainer = rootEntry.container();
						rootID = rootEntry.id();
						rootDescriptor = null;
						rootEntry = null;
					}
				}
			}
			treatUnderflow(path); // UNDERFLOW
			if (rootDescriptor != null && rootEntry != null) {
				backToFirstEntry(path, prevSeparator, true);
				if (counterRightShiftDup > 0)
					counterRightShiftDup--; // count down returned duplicate
											// Objects
			} else {
				rootContainer.remove(rootID);
			}
		}

		/**
		 * Searches for the left most element that has the same key as the
		 * deleted item, constructs the path in queryCursor.
		 * 
		 * @param path
		 * @param prevSep
		 */
		protected void backToFirstEntry(Stack path, Separator prevSep,
				boolean computeIndex) {
			KeyRange prev = createKeyRange(prevSep.sepValue(), prevSep
					.sepValue());
			if (path == null) {
				path = new Stack();
			}
			if (path.isEmpty()) {
				down(path, (IndexEntry) rootEntry);
			}
			while (level(path) > targetLevel) {
				Iterator entries = node(path).query(prev);
				if (entries.hasNext()) {
					down(path, (IndexEntry) entries.next());
				} else
					return;
			}
			this.path = path;
			if (computeIndex) {
				indexEntry = (IndexEntry) indexEntry(path);
				currentNode = (Node) node(path);
				lastIndexEntry = (IndexEntry) indexEntry;
				lastNode = (Node) currentNode;
				for (int i = 0, j = 1; i < currentNode.number(); i++) {
					index = i;
					if (separator(((Node) currentNode).getEntry(i)).isRightOf(
							((KeyRange) prev).minBound())) {
						j++;
						if (j > counterRightShiftDup)
							break;
					}
					if (index == currentNode.number() - 1
							&& ((Node) currentNode).nextNeighbor != null) {
						nodeChangeOver();
						i = -1;
					}
					if (index == currentNode.number() - 1
							&& ((Node) currentNode).nextNeighbor == null) {
						index = currentNode.number();
						break;
					}
				}
			}
		}

		/**
		 * Replaces the last element returned by the method next() by a new data
		 * object. It is only supported on the level 0.
		 * 
		 * @param newData
		 *            the new data object
		 */
		protected void updateObject(Object newData) {
			if (targetLevel != 0)
				throw new UnsupportedOperationException(
						"update(Object) is to use only on leaf level.");
			if (!hasPath()) {
				path = new Stack(); // compute new path when the cursor moved to
									// the right
				Separator range = separator(((Node) lastNode)
						.getEntry(index - 1));
				backToFirstEntry(path, range, false);
				prevSeparator = range;
			}
			Object oldDat = lastNode.getEntry(lastIndex);
			if (!key(oldDat).equals(key(newData)))
				throw new IllegalArgumentException(
						"The updated new data have to be the different key like the old one.");

			Object oldData = lastNode.remove(lastIndex);
			// update weigth og the node
			lastNode.grow(newData, path);
			// check the node after if the size was updated
			if (lastNode.overflows()){
				treatOverflow(path); // UNDERFLOW
			}else{
				lastIndexEntry.container().update(lastIndexEntry.id(), lastNode,
						true);
				// what if underflows do nothing
				// FIXME underflow handling
			}
			
		
		}

		/**
		 * It unfixes all <tt>Nodes</tt> loaded in the underlying buffer then
		 * calls super.close().
		 */
		public void close() {
			if (isClosed)
				return;
			abolishPath();
			try {
				if (!lastIndexEntry.equals(indexEntry))
					lastIndexEntry.unfix();
			} catch (NoSuchElementException e) {
			}
			try {
				indexEntry.unfix();
			} catch (NoSuchElementException e) {
			}
			super.close();
		}

		/**
		 * Moves the <tt>QueryCursor</tt> to next right neighbor.
		 */
		protected void nodeChangeOver() {
			indexEntry = ((Node) currentNode).nextNeighbor;
			currentNode = (Node) indexEntry.get(false);
			index = 0;
			abolishPath();
			lastIndexEntry.unfix();
			lastIndexEntry = (IndexEntry) indexEntry;
			lastNode = (Node) currentNode;
			nodeChangeover++;
		}

		/**
		 * @see xxl.core.cursors.Cursor#supportsReset()
		 */
		public boolean supportsReset() {
			return true;
		}

		/**
		 * @see xxl.core.cursors.Cursor#reset()
		 */
		public void reset() throws UnsupportedOperationException {
			super.reset();
			this.indexEntry = subRootEntry;
			path = null;
			currentNode = null;
			path = new Stack();
			index = -1;
			lastIndex = index;
			lastNode = null;
			lastIndexEntry = null;
			nodeChangeover = 0;
		}
	}

	/**
	 * 
	 * @author achakeye
	 * 
	 */
	public class NodeConverter extends Converter {

		/**
		 * Reads a <tt>Node</tt> from the given <tt>DataInput</tt>.
		 * 
		 * @param dataInput
		 *            the <tt>DataInput</tt> from which the <tt>Node</tt> has to
		 *            be read
		 * @param object
		 *            is not used
		 * @return the read <tt>Node</tt>
		 * @throws IOException
		 */
		public Object read(DataInput dataInput, Object object)
				throws IOException {
			int payLoad = 0; // new code
			int level = dataInput.readInt();
			Node node = (Node) createNode(level);
			int number = dataInput.readInt();
			boolean readNext = dataInput.readBoolean();
			if (readNext) {
				node.nextNeighbor = (IndexEntry) createIndexEntry(level + 1);
				node.nextNeighbor.initialize(readID(dataInput));
			} else
				node.nextNeighbor = null;
			// new code compute actual load

			for (int i = 0; i < number; i++) {
				Object entry;
				int entrySize = 0;
				if (node.getLevel() == 0) {
					entry = dataConverter.read(dataInput, null);
					entrySize = getActualEntrySize.invoke(entry);
				} else {
					entry = readIndexEntry(dataInput, node.getLevel());
					entrySize = containerIdSize;
				}
				node.getEntries().add(i, entry);
				payLoad += entrySize;
			}
			// init
			if (node.getLevel() != 0) {
				for (int i = 0; i < node.number(); i++) {
					Comparable sepValue = (Comparable) keyConverter.read(
							dataInput, null);
					((IndexEntry) node.getEntry(i))
							.initialize(createSeparator(sepValue));
					// new code
					payLoad += getActualKeySize.invoke(sepValue);
				}
			}
			// new code
			node.byteLoad = payLoad;
			int sum = 0;
			for (int i = 0; i < node.number(); i++) {
				int load = node
						.getEntryByteSize(node.getEntry(i), node.level());
				sum += load;
			}
			if (payLoad != sum) {
				System.out.println("Warning!");
			}
			return node;
		}

		/**
		 * Writes a given <tt>Node</tt> into a given <tt>DataOutput</tt>.
		 * 
		 * @param dataOutput
		 *            the <tt>DataOutput</tt> which the <tt>Node</tt> has to be
		 *            written to
		 * @param object
		 *            the <tt>Node</tt> which has to be written
		 * @throws IOException
		 */
		public void write(DataOutput dataOutput, Object object)
				throws IOException {
			Node node = (Node) object;
			// 2x Integer
			dataOutput.writeInt(node.getLevel());
			dataOutput.writeInt(node.number());
			// Boolean
			dataOutput.writeBoolean(node.nextNeighbor != null);
			// ID
			if (node.nextNeighbor != null) {
				Converter idConverter = VariableLengthBPlusTree.this
						.container().objectIdConverter();
				idConverter.write(dataOutput, node.nextNeighbor.id());
			}
			// Entries
			writeEntries(dataOutput, node);

			// Separators
			// edit
			if (node.getLevel() != 0)
				for (int i = 0; i < node.number(); i++)
					keyConverter.write(dataOutput, separator(node.getEntry(i))
							.sepValue());
			if (((DataOutputStream) dataOutput).size() > VariableLengthBPlusTree.this.BLOCK_SIZE) {
				// System.out.println("Problem");
			}
		}

		/**
		 * Read the entries of the given <tt>Node</tt> from the
		 * <tt>DataInput</tt>. If the <tt>Node</tt> is a leaf the
		 * <tt>dataConverter</tt> is used to read its data objects. Otherwise
		 * the method {@link #readIndexEntry(DataInput, int)}is used to read its
		 * <tt>IndexEntries</tt>.
		 * 
		 * @param input
		 *            the <tt>DataInput</tt>
		 * @param node
		 *            the <tt>Node</tt>
		 * @param number
		 *            the number of the entries which have to be read
		 * @throws IOException
		 */
		protected void readEntries(DataInput input, Node node, int number)
				throws IOException {
			for (int i = 0; i < number; i++) {
				Object entry;
				if (node.getLevel() == 0)
					entry = dataConverter.read(input, null);
				else
					entry = readIndexEntry(input, node.getLevel());
				node.entries.add(i, entry);
			}
		}

		/**
		 * Writes the entries of the given <tt>Node</tt> into the
		 * <tt>DataOutput</tt>. If the <tt>Node</tt> is a leaf the
		 * <tt>dataConverter</tt> is used to write its data objects. Otherwise
		 * the method {@link #writeIndexEntry(DataOutput, BPlusTree.IndexEntry)}
		 * is used to write its <tt>IndexEntries</tt>.
		 * 
		 * @param output
		 *            the <tt>DataOutput</tt>
		 * @param node
		 *            the <tt>Node</tt>
		 * @throws IOException
		 */
		protected void writeEntries(DataOutput output, Node node)
				throws IOException {
			Iterator entries = node.entries();
			while (entries.hasNext()) {
				Object entry = entries.next();
				if (node.getLevel() == 0)
					dataConverter.write(output, entry);
				else
					writeIndexEntry(output, (IndexEntry) entry);
			}
		}

		/**
		 * Reads an <tt>IndexEntry</tt> from the given <tt>DataInput</tt>. In
		 * the default implementation only the ID of the <tt>IndexEntry</tt> is
		 * read.
		 * 
		 * @param input
		 *            the <tt>DataInput</tt>
		 * @param parentLevel
		 *            the parent level of the <tt>IndexEntry</tt>
		 * @return the read <tt>IndexEntry</tt>
		 * @throws IOException
		 */
		protected IndexEntry readIndexEntry(DataInput input, int parentLevel)
				throws IOException {
			IndexEntry indexEntry = new IndexEntry(parentLevel);
			Object id = readID(input);
			indexEntry.initialize(id);
			return indexEntry;
		}

		/**
		 * Writes an <tt>IndexEntry</tt> into the given <tt>DataOutput</tt>. In
		 * the default implementation only the ID of the <tt>IndexEntry</tt> is
		 * written.
		 * 
		 * @param output
		 *            the <tt>DataOutput</tt>
		 * @param entry
		 *            the <tt>IndexEntry</tt> which has to be written
		 * @throws IOException
		 */
		protected void writeIndexEntry(DataOutput output, IndexEntry entry)
				throws IOException {
			writeID(output, entry.getId());
		}

		/**
		 * Reads an ID from the given <tt>DataInput</tt>.
		 * 
		 * @param input
		 *            the <tt>DataInput</tt>
		 * @return the read ID
		 * @throws IOException
		 */
		protected Object readID(DataInput input) throws IOException {
			Converter idConverter = VariableLengthBPlusTree.this.container()
					.objectIdConverter();
			return idConverter.read(input, null);
		}

		/**
		 * Writes an ID into the given <tt>DataOutput</tt>.
		 * 
		 * @param output
		 *            the <tt>DataOutput</tt>
		 * @param id
		 *            the ID which has to be written
		 * @throws IOException
		 */
		protected void writeID(DataOutput output, Object id) throws IOException {
			Converter idConverter = VariableLengthBPlusTree.this.container()
					.objectIdConverter();
			idConverter.write(output, id);
		}

	}
}
