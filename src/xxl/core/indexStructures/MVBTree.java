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
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Stack;
import java.util.Map.Entry;

import xxl.core.collections.MapEntry;
import xxl.core.collections.MappedList;
import xxl.core.collections.containers.Container;
import xxl.core.collections.queues.DynamicHeap;
import xxl.core.collections.queues.ListQueue;
import xxl.core.collections.queues.Queue;
import xxl.core.collections.queues.Queues;
import xxl.core.cursors.Cursor;
import xxl.core.cursors.Cursors;
import xxl.core.cursors.filters.Filter;
import xxl.core.cursors.mappers.Mapper;
import xxl.core.cursors.sources.EmptyCursor;
import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Constant;
import xxl.core.functions.Function;
import xxl.core.io.converters.BooleanConverter;
import xxl.core.io.converters.IntegerConverter;
import xxl.core.io.converters.MeasuredConverter;
import xxl.core.predicates.AbstractPredicate;
import xxl.core.predicates.Predicate;

/**
 * This Class is an implementation of MVBT index structure
 * "An asymptotically optimal multiversion B-tree"
 * Becker Bruno, Gschwind Stephan, Ohler Thomas,  Seeger Bernhard and Widmayer, Peter
 *  VLDB 1996
 * 
 */
public class MVBTree extends BPlusTree {
	/** Defalut value for KeyDomain minValue*/
	public static final Integer DEAFUALT_KEYDOMAIN_MINVALUE = Integer.MIN_VALUE;
	
	/** Is used to define the bounds of the <tt>strong version condition</tt>.*/
	protected final float EPSILON;
	
	/**A <tt>BPlusTree</tt> to store the roots of the <tt>MVBTree</tt>.*/
	public BPlusTree roots;
	
	/** The current version of the <tt>MVBTree</tt>.*/
	public Version currentVersion;
	
	/**A <tt>MeasuredConverter</tt> to convert <tt>Version-Objects</tt>.*/
	protected MeasuredConverter versionConverter;
	
	/**A <tt>MeasuredConverter</tt> to convert <tt>Lifespans</tt>.*/
	protected MeasuredConverter lifespanConverter;
	
	/**A <tt>MeasuredConverter</tt> to convert <tt>Roots</tt>.*/
	protected MeasuredConverter rootConverter;
	
	/** Oldest relevant version - all elements before that version can be purged */
	protected Version cutoffVersion;
	
	
	public Comparable keyDomainMinValue;
	/**
	 * Contains information about one block of the MVBTree, i.e. the block's
	 * ID in the underlying container and the block's deletion version.
	 */
	protected static class BlockDelInfo implements Comparable {
		protected Object id;
		protected Version del;
		
		public BlockDelInfo(Object id, Version del) {
			this.id = id;
			this.del = del;
		}
		
		public Object getId() {
			return id;
		}

		public Version getDeletionVersion() {
			return del;
		}
		
		public int compareTo(Object obj) {
			return del.compareTo(((BlockDelInfo)obj).del);
		}
	}

	/**
	 * This queue contains an entry for each dead block of the MVBTree. The heap's
	 * root is the next block which will be purged.
	 */
	protected Queue<BlockDelInfo> purgeQueue = new ListQueue<BlockDelInfo>();
	
	/**Creates a new <tt>MVBTree</tt>.
	 * KeyDomainMinValue set to default value @see {@link MVBTree#DEAFUALT_KEYDOMAIN_MINVALUE}
	 * @param blockSize the block size of the underlaying <tt>Container</tt>.
	 * @param minCapRatio the minimal capacity ratio of the tree's nodes.
	 * @param e the epsilon of the <tt>strong version condition</tt>.
	 */	
	public MVBTree(int blockSize, float minCapRatio, float e) {
		super(blockSize, minCapRatio);
		EPSILON=e;
		roots=new BPlusTree(blockSize);
		this.keyDomainMinValue = MVBTree.DEAFUALT_KEYDOMAIN_MINVALUE;
	}
	
	/**Creates a new <tt>MVBTree</tt>.  The minimal capacity ratio of the tree's nodes is set ot 50%.
	 * @param blockSize the block size of the underlaying <tt>Container</tt>.
	 * @param e the epsilon of the <tt>strong version condition</tt>.
	 */	
	public MVBTree(int blockSize, float e) {
		this(blockSize, 0.5f, e);
	}
	/**Creates a new <tt>MVBTree</tt>.  The minimal capacity ratio of the tree's nodes is set ot 50%.
	 * @param blockSize the block size of the underlaying <tt>Container</tt>.
	 * @param e the epsilon of the <tt>strong version condition</tt>.
	 */	
	public MVBTree(int blockSize, float e, Comparable keyDomainMinValue) {
		this(blockSize, 0.5f, e, keyDomainMinValue);
	}
	
	public MVBTree(int blockSize, float minCapRatio, float e, Comparable keyDomainMinValue ) {
		super(blockSize, minCapRatio);
		EPSILON=e;
		roots=new BPlusTree(blockSize);
		this.keyDomainMinValue = keyDomainMinValue;
	}
	/** Initializes the <tt>MVBTree</tt> with the given parameters.
	 * @param getKey a <tt>Function</tt> to extract the key of a data object.
	 * @param rootsContainer the <tt>Container</tt> of the <tt>BPlusTree</tt> which is used to store the historical 
	 * roots of the <tt>MVBTree</tt>.
	 * @param treeContainer the <tt>Container</tt> which is used by the <tt>MVBTree</tt> to store its <tt>Nodes</tt>. 
	 * @param versionConverter a <tt>Converter</tt> to convert <tt>Versions</tt>.
	 * @param keyConverter a <tt>Converter</tt> to convert keys.
	 * @param dataConverter a <tt>Converter</tt> to convert the data objects stored in the <tt>MVBTree</tt>.
	 * @param createMVSeparator a <tt>Function</tt> to create <tt>MVSeparators</tt>.
	 * @param createMVRegion a <tt>Function</tt> to create <tt>MVRegion</tt>.
	 * @return the initialized <tt>MVBTree</tt>.
	 */	
	public MVBTree initialize(	Function getKey,
								final Container rootsContainer, 
								final Container treeContainer, 
								MeasuredConverter versionConverter, 
								MeasuredConverter keyConverter, 
								MeasuredConverter dataConverter,
								Function createMVSeparator,
								Function createMVRegion) {
		Function getRootsContainer = new Constant(rootsContainer);
		Function determineRootsContainer= getRootsContainer;
		Function getContainer = new Constant(treeContainer);
		Function determineContainer= new Constant(treeContainer);
		return initialize(getKey, getRootsContainer, determineRootsContainer, 
							getContainer, determineContainer, versionConverter, keyConverter, 
							dataConverter, createMVSeparator, createMVRegion);
	}
	/**
	 * 
	 * @param rootEntry
	 * @param rootsRootEntry
	 * @param getKey
	 * @param rootsContainer
	 * @param treeContainer
	 * @param versionConverter
	 * @param keyConverter
	 * @param dataConverter
	 * @param createMVSeparator
	 * @param createMVRegion
	 * @return
	 */
	public MVBTree initialize(IndexEntry rootEntry, 
			Descriptor liveRootDescriptor, 
			IndexEntry rootsRootEntry,	
			Descriptor rootsRootDescriptor, 
			final Function getKey,
			final Container rootsContainer, 
			final Container treeContainer, 
			MeasuredConverter versionConverter, 
			MeasuredConverter keyConverter, 
			MeasuredConverter dataConverter,
			Function createMVSeparator,
			Function createMVRegion){
		this.versionConverter=versionConverter;
		this.lifespanConverter=lifespanConverter();
		Function getSplitMinRatio= new Constant(new Float((1+EPSILON)*minCapacityRatio));
		Function getSplitMaxRatio= new Constant(new Float(1- EPSILON*minCapacityRatio));
		Function newGetKey=	new AbstractFunction() {
				public Object invoke(Object entry) {
					if(entry instanceof LeafEntry) return getKey.invoke(((LeafEntry)entry).data());
					return getKey.invoke(entry);	
				}
			};
		super.initialize(rootEntry, liveRootDescriptor, newGetKey, treeContainer, keyConverter, 
								dataConverter, createMVSeparator, createMVRegion, getSplitMinRatio, getSplitMaxRatio);
		this.getDescriptor= new AbstractFunction() {
				public Object invoke(Object entry) {
					if(entry instanceof MVSeparator) return entry;
					if(entry instanceof IndexEntry) return ((IndexEntry)entry).separator;
					if(entry instanceof LeafEntry) {
						LeafEntry leafEntry=(LeafEntry)entry;
						return createMVSeparator(	leafEntry.lifespan.beginVersion(), 
													leafEntry.lifespan.endVersion(),
													key(leafEntry.data));
					}
					throw new IllegalArgumentException(entry.toString());
				}
			};
		this.rootConverter=rootConverter(lifespanConverter);
		roots.initialize(rootsRootEntry,rootsRootDescriptor, new AbstractFunction() {
									public Object invoke(Object object) {
										// Roots have always bounded intervals
										return ((Root)object).lifespan().endVersion();
									}
								},
								rootsContainer,
								//lifespanConverter,
								versionConverter,
								this.rootConverter, 
								LifeSpanSeparator.FACTORY_FUNCTION, Lifespan.FACTORY_FUNCTION);
		// old code 
		// due to changes in BPlusTree 
//		   roots.getDescriptor=new AbstractFunction() {
//									public Object invoke(Object object) {
//										if(object instanceof Root){
//											//TODO new Code
//											LifeSpanSeparator separator = 
//												new LifeSpanSeparator(((Root)object).lifespan().beginVersion(),
//														((Root)object).lifespan().endVersion());
//											return separator;
//										}
//										return ((IndexEntry)object).separator();
//									}
//								};
			this.underflows = new AbstractPredicate() {
				public boolean invoke(Object object) {
					Node node = (Node)object;
					return node.countCurrentEntries() < (node.level() == 0 ? D_LeafNode : D_IndexNode);
				}
			};
			if (!checkFactors(B_LeafNode, D_LeafNode, EPSILON, B_LeafNode / D_LeafNode)) throw new IllegalArgumentException();
			if (!checkFactors(B_IndexNode, D_IndexNode, EPSILON, B_IndexNode / D_IndexNode)) throw new IllegalArgumentException();
			return this;						
	}
	/** Initializes the <tt>MVBTree</tt> with the given parameters.
	 * @param getKey a <tt>Function</tt> to extract the key of a data object.
	 * @param getRootsContainer the new @link{BPlusTree#getContainer <tt>Function "getContainer"</tt>} of the 
	 * <tt>BPlusTree</tt> which is used to store the historical roots of the <tt>MVBTree</tt>.
	 * @param determineRootsContainer the new @link{BPlusTree#determineContainer <tt>Function "determineContainer"</tt>} 
	 * of the <tt>BPlusTree</tt> which is used to store the historical roots of the <tt>MVBTree</tt>.
	 * @param getContainer a <tt>Function</tt> which gives the <tt>Container</tt> in which a particular <tt>Node</tt> 
	 * of the <tt>MVBTree</tt> is sotred. 
	 * @param determineContainer a <tt>Function</tt> which gives the <tt>Container</tt> in which a new created <tt>Node</tt> 
	 * has to be stored. 
	 * @param versionConverter a <tt>Converter</tt> to convert <tt>Versions</tt>.
	 * @param keyConverter a <tt>Converter</tt> to convert keys.
	 * @param dataConverter a <tt>Converter</tt> to convert the data objects stored in the <tt>MVBTree</tt>.
	 * @param createMVSeparator a <tt>Function</tt> to create <tt>MVSeparators</tt>.
	 * @param createMVRegion a <tt>Function</tt> to create <tt>MVRegion</tt>.
	 * @return the initialized <tt>MVBTree</tt>.
	 */	
	public MVBTree initialize(	final Function getKey,
								Function getRootsContainer,
								Function determineRootsContainer,
								Function getContainer,
								Function determineContainer,
								MeasuredConverter versionConverter, 
								MeasuredConverter keyConverter, 
								MeasuredConverter dataConverter,
								Function createMVSeparator,
								Function createMVRegion) {
		this.versionConverter=versionConverter;
		this.lifespanConverter=lifespanConverter();
		Function getSplitMinRatio= new Constant(new Float((1+EPSILON)*minCapacityRatio));
		Function getSplitMaxRatio= new Constant(new Float(1- EPSILON*minCapacityRatio));
		Function newGetKey=	new AbstractFunction() {
			public Object invoke(Object entry) {
				if(entry instanceof LeafEntry) return getKey.invoke(((LeafEntry)entry).data());
				return getKey.invoke(entry);	
			}
		};
		super.initialize(newGetKey, getContainer, determineContainer, keyConverter, 
							dataConverter, createMVSeparator, createMVRegion, getSplitMinRatio, getSplitMaxRatio);
		this.getDescriptor= new AbstractFunction() {
			public Object invoke(Object entry) {
				if(entry instanceof MVSeparator) return entry;
				if(entry instanceof IndexEntry) return ((IndexEntry)entry).separator;
				if(entry instanceof LeafEntry) {
					LeafEntry leafEntry=(LeafEntry)entry;
					return createMVSeparator(	leafEntry.lifespan.beginVersion(), 
												leafEntry.lifespan.endVersion(),
												key(leafEntry.data));
				}
				throw new IllegalArgumentException(entry.toString());
			}
		};
		this.rootConverter=rootConverter(lifespanConverter);
		roots.initialize(	new AbstractFunction() {
								public Object invoke(Object object) {
									// Roots have always bounded intervals
									return ((Root)object).lifespan().endVersion();
								}
							},
							getRootsContainer,
							determineRootsContainer,
							//lifespanConverter,
							versionConverter,
							this.rootConverter, 
							LifeSpanSeparator.FACTORY_FUNCTION, Lifespan.FACTORY_FUNCTION);
		// old code 
		// due to changes in BPlusTree 
//	 roots.getDescriptor=new AbstractFunction() {
//				public Object invoke(Object object) {
//					if(object instanceof Root){
//						//
//						LifeSpanSeparator separator = 
//							new LifeSpanSeparator(((Root)object).lifespan().beginVersion(),
//									((Root)object).lifespan().endVersion());
//						return separator;
//					}
//					return ((IndexEntry)object).separator();
//				}
//			};
		this.underflows = new AbstractPredicate() {
			public boolean invoke(Object object) {
				Node node = (Node)object;
				return node.countCurrentEntries() < (node.level() == 0 ? D_LeafNode : D_IndexNode);
			}
		};
		if (!checkFactors(B_LeafNode, D_LeafNode, EPSILON, B_LeafNode / D_LeafNode)) throw new IllegalArgumentException();
		if (!checkFactors(B_IndexNode, D_IndexNode, EPSILON, B_IndexNode / D_IndexNode)) throw new IllegalArgumentException();
		return this;									
	}
	
	
	
	private static boolean checkFactors(int b,int d,double e,int k) {
		return	(k>= (2+(3*e)-(1/(double)d))) && (e<=1-1/(double)d);
	}
	
	/** Gives the <tt>Converter</tt> used by the <tt>MVBTree</tt> to convert the <tt>Lifespans</tt>.
	 * @return the <tt>Converter</tt> used by the <tt>MVBTree</tt> to convert the <tt>Lifespans</tt>.
	 */
	protected MeasuredConverter lifespanConverter() {
		return new MeasuredConverter() {
			public Object read(DataInput input, Object object) throws IOException {
				Version in=(Version) versionConverter.read(input,null);
				Lifespan life=new Lifespan(in);
				boolean isDead= BooleanConverter.DEFAULT_INSTANCE.readBoolean(input);
				if(isDead) {
					Version del=(Version) versionConverter.read(input,null);
					life.delete(del);
				}
				return life;
			}
			public void write(DataOutput output, Object object) throws IOException {
				Lifespan lifespan=(Lifespan)object;
				//Version
				versionConverter.write(output,lifespan.beginVersion());
				//Boolean
				BooleanConverter.DEFAULT_INSTANCE.writeBoolean(output, lifespan.isDead());
				//Version
				if(lifespan.isDead())versionConverter.write(output,lifespan.endVersion());
			}
			public int getMaxObjectSize() {
				return 2* versionConverter.getMaxObjectSize()+ BooleanConverter.SIZE;
			}
		};
	}

	/** Gives the <tt>Converter</tt> used by the <tt>MVBTree</tt> to convert the historical <tt>Roots</tt>.
	 * @return the <tt>Converter</tt> used by the <tt>MVBTree</tt> to convert the historical <tt>Roots</tt>.
	 */	
	protected MeasuredConverter rootConverter(final MeasuredConverter lifespanConverter) {
		return new MeasuredConverter() {
			public Object read(DataInput input, Object object) throws IOException {
				Lifespan lifespan=(Lifespan)lifespanConverter.read(input,null);
				Comparable minBound=(Comparable) keyConverter.read(input,null);
				Comparable maxBound=(Comparable) keyConverter.read(input,null);
				MVRegion mvReg= createMVRegion(lifespan.beginVersion(), lifespan.endVersion(), 
													minBound, maxBound);
				Object rootId= MVBTree.this.container().objectIdConverter().read(input,null);
				int parentLevel= IntegerConverter.DEFAULT_INSTANCE.readInt(input);
				return new Root(mvReg, rootId, parentLevel);
			}
			public void write(DataOutput output, Object object) throws IOException {
				Root root=(Root)object;
				//Lifespan
				lifespanConverter.write(output, root.lifespan());
				//Key Range
				keyConverter.write(output, root.getRegion().minBound());
				keyConverter.write(output, root.getRegion().maxBound());
				//Id
				container().objectIdConverter().write(output, root.rootNodeId());
				//parent Level
				IntegerConverter.DEFAULT_INSTANCE.writeInt(output, root.parentLevel());
			}
			public int getMaxObjectSize() {
				return lifespanConverter.getMaxObjectSize()+2*keyConverter.getMaxObjectSize()+roots.container().getIdSize()+IntegerConverter.SIZE;
				//return 2*keyConverter.getMaxObjectSize()+roots.container().getIdSize()+IntegerConverter.SIZE;
			}
		};
	}
	
	/** Creates a new <tt>Node</tt>.
	 * @return the new created <tt>Node</tt>.
	 */		
	public Tree.Node createNode(int level) {
		return new Node(level);
	}
	
	/** Creates a new <tt>LeafEntry</tt>.
	 * @param lifespan the <tt>Lifespan</tt> of the new <tt>LeafEntry</tt>.
	 * @param data the data object which is to store in the new <tt>LeafEntry</tt>.
	 * @return the new created <tt>LeafEntry</tt>.
	 */
	protected LeafEntry createLeafEntry(Lifespan lifespan, Object data) {
		return new LeafEntry(lifespan, data);
	}
	
	/** Creates a new <tt>NodeConverter</tt>.
	 * @return the new created <tt>NodeConverter</tt>.
	 */
	protected BPlusTree.NodeConverter createNodeConverter() {
		return new NodeConverter();
	}
	
	/** Creates a new <tt>MVSeparator</tt> with the given parameters.
	 * @param insertVersion the insertion <tt>Version</tt> of the associated entry.
	 * @param deleteVersion the deletion <tt>Version</tt> of the associated entry.
	 * @param sepValue the key used as a separator.
	 * @return the new created <tt>MVSeparator</tt>.
	 */
	public MVSeparator createMVSeparator(Version insertVersion, Version deleteVersion, Comparable sepValue) {
		// CHANGE: convert array to list
		return (MVSeparator)createSeparator.invoke(java.util.Arrays.asList(
				new Object[] {	insertVersion, 
								deleteVersion, 
								sepValue}));
	}
	
	/** Creates a new <tt>MVRegion</tt>.
	 * @param beginVersion the begin <tt>Version</tt> of the <tt>MVRegion</tt>.
	 * @param endVersion the end <tt>Version</tt> of the <tt>MVRegion</tt>.
	 * @param minBound the minimal bound of the <tt>MVRegion</tt>.
	 * @param maxBound the maximal bound of the <tt>MVRegion</tt>.
	 * @return the new created <tt>MVRegion</tt>.
	 */
	public MVRegion createMVRegion(Version beginVersion, Version endVersion, 
											Comparable minBound, Comparable maxBound) {
		// CHANGE: convert array to list
		return (MVRegion)createKeyRange.invoke(java.util.Arrays.asList(
				new Object[] {	beginVersion, 
								endVersion, 
								minBound,
								maxBound}));
	}
 
 	/** Gives the <tt>BPlusTree</tt> used to manage the historical <tt>Roots</tt> of the <tt>MVBTree</tt>.
 	 * @return the <tt>BPlusTree</tt> used to manage the historical <tt>Roots</tt> of the <tt>MVBTree</tt>.
 	 */
	public BPlusTree rootsTree() {
		return roots;
	}
	
	/** Determines the <tt>Root</tt> which is appropriate to the given <tt>Version</tt>.
	 * @param version the <tt>Version</tt> whose <tt>Root</tt> is required.
	 * @return an <tt>IndexEntry</tt> pointing to the appropriate <tt>root Node</tt>.
	 */
	public IndexEntry determineRootEntry(Version version) {
		Version lastRootSplitVersion= ((MVSeparator)((IndexEntry)rootEntry).separator()).insertVersion();
		if((version==null)||(version.compareTo(lastRootSplitVersion)>=0)){ 
			return (IndexEntry)rootEntry;
		}
		// Daniar: new code 
		IndexEntry indexEntry = null;
		Root root = null;
		Cursor rootsCursor = roots.rangeQuery(version, lastRootSplitVersion);
		if(rootsCursor.hasNext()){
			 root = (Root) rootsCursor.next();
			 indexEntry = root.toIndexEntry();
		}
		// old code 
		//	((Root)roots.exactMatchQuery(version)).toIndexEntry();
		return indexEntry;
		
		
	}
	
	/** Gives the current <tt>Version</tt> of the <tt>MVBTree</tt>, i.e. the <tt>Version</tt> of the last update 
	 * operation (insertion, deletion or update).
	 * @return the current <tt>Version</tt> of the <tt>MVBTree</tt>.
	 */
	public Version currentVersion() {
		return currentVersion;
	}
	
	/** This method is normaly used during an update operation (insertion, deletion or update) to update the current 
	 * <tt>Version</tt> of the <tt>MVTree</tt>. Also it checks whether this chage is valid, i.e. the new 
	 * <tt>Version</tt> has to be greater than the old current <tt>Version</tt>.
	 * @param version the new current <tt>Version</tt>.
	 */
	public void setCurrentVersion(Version version) {
		if((currentVersion!=null)&&(version.compareTo(currentVersion)<0)) 
			throw new UnsupportedOperationException("Update Operations into a past version " +
														"are not supported by MVBTree.");
		currentVersion=(Version)version.clone();
	}
	
	
	
	
	
	/**
	 * Sets the cutoff version. The cutoff version is the oldest version whose
	 * elements are to be kept. All versions before the cutoff version can be purged.
	 * 
	 * @param version cutoff version
	 */
	public void setCutoffVersion(Version version) {
		if (version == null)
			throw new IllegalArgumentException();
		if (cutoffVersion != null && version.compareTo(cutoffVersion) < 0)
			throw new IllegalArgumentException("New cutoff version must be greater than or equal to the old cutoff version.");
		if (version.compareTo(currentVersion) > 0)
			throw new IllegalArgumentException("Cutoff version must be smaller than or equal to the current version.");
		cutoffVersion = (Version)version.clone();
		purge();
	}
	
	/**
	 * Removes all blocks from the container which died before the cutoff version.
	 */
	protected void purge() {
		if (cutoffVersion != null)
			while (!purgeQueue.isEmpty() && purgeQueue.peek().getDeletionVersion().compareTo(cutoffVersion) <= 0) {
				BlockDelInfo info = purgeQueue.dequeue();
				((Container)getContainer.invoke()).remove(info.getId());
			}
	}
	
	/** Converts a <tt>MVSeparator</tt> into a <tt>MVRegion</tt> on the following wise: 
	 * <ul>
	 * <li> mvSeparator.insertVersion() &rarr; begin <tt>Version</tt> of the <tt>MVRegion</tt> </li>
	 * <li> mvSeparator.deleteVersion() &rarr; end <tt>Version</tt> of the <tt>MVRegion</tt> </li>
	 * <li> mvSeparator.sepValue() &rarr; begin key of the <tt>MVRegion</tt> </li>
	 * <li> mvSeparator.sepValue() &rarr; end key of the <tt>MVRegion</tt> </li>
	 * </ul>
	 * @param mvSeparator the <tt>MVSeparator</tt> which is to convert.
	 * @return the new <tt>MVRegion</tt>.
	 */
	public MVRegion toMVRegion(MVSeparator mvSeparator) {
		MVRegion mvReg=createMVRegion(mvSeparator.insertVersion(), mvSeparator.deleteVersion(), 
										mvSeparator.sepValue(), mvSeparator.sepValue());
		return mvReg;
	}

	/** Converts a <tt>MVRegion</tt> into a <tt>MVSeparator</tt> on the following wise: 
	 * <ul>
	 * <li> region.beginVersion() &rarr; insertion <tt>Version</tt> of the <tt>MVSeparator</tt> </li>
	 * <li> region.endVersion() &rarr; deletion <tt>Version</tt> of the <tt>MVSeparator</tt> </li>
	 * <li> region.minBound() &rarr; separation key of the <tt>MVSeparator</tt> </li>
	 * <li> region.maxBound() &rarr; will be ignored. </li>
	 * </ul>
	 * @param region the <tt>MVRegion</tt> which is to convert.
	 * @return the new <tt>MVSeparator</tt>.
	 */
	public MVSeparator toMVSeparator(MVRegion region) {
		return createMVSeparator(region.beginVersion(), region.endVersion(), region.minBound());
	}
	
	/** Computes the region of the subtree pointed by the given entry.
	 * @param entry the root of the subtree.
	 * @param path the path from the root to the <tt>Node</tt> in which the given entry is stored.
	 * @return the region of the subtree pointed by the given entry.
	 */
	protected MVRegion computeRegion(IndexEntry entry, Stack path) {
		IndexEntry indexEntry=entry;
		MVRegion region=toMVRegion((MVSeparator)indexEntry.separator());
		if(!path.isEmpty()) {
			Node parentNode=(Node)node(path);
			int index= parentNode.entries.indexOf(indexEntry);
			boolean adjusted=false;
			if(index<parentNode.number()-1) {
				MVSeparator nextSep= (MVSeparator)separator(parentNode.getEntry(index+1));
				if(nextSep.lifespan().overlaps(region.lifespan())
				&& nextSep.sepValue().compareTo(region.minBound())>0) {
					region.updateMaxBound(nextSep.sepValue());
					adjusted=true;
				}
			}
			if(!adjusted) {
				IndexEntry parent=(IndexEntry)indexEntry(path);
				Object pathEntry= path.pop();
				MVRegion parentRegion= computeRegion(parent, path);
				path.push(pathEntry);
				region.updateMaxBound(parentRegion.maxBound());
			}
		}
		else region.union(rootDescriptor, false);
		return region;
	}
	
	/**
	 * 
	 */
	protected Stack pathToNode(KeyRange nodeRegion, int level) {
		MVRegion region=(MVRegion)nodeRegion;
		IndexEntry indexEntry= determineRootEntry(region.beginVersion());
		Comparable parentMax= ((MVRegion)rootDescriptor).maxBound(); //indexEntry.separator.maxBound();
		Stack path= new Stack();
		down(path, indexEntry);
		Node pathNode=(Node)node(path);
		while(pathNode.level() > level) {
			boolean found=false;
			for(int i = 0; !found && (i < pathNode.number()); i++) {
				MVRegion subRegion = toMVRegion((MVSeparator)separator(pathNode.getEntry(i)));
				subRegion.updateMaxBound(parentMax);
				if(i < pathNode.number() - 1) {
					MVSeparator nextSep = (MVSeparator)separator(pathNode.getEntry(i+1));
					if (nextSep.lifespan.overlaps(subRegion.lifespan())
						&& (nextSep.sepValue().compareTo(subRegion.minBound()) > 0))
						subRegion.updateMaxBound(separator(pathNode.getEntry(i+1)).sepValue());
				}
				if(subRegion.contains((Descriptor)region)) {
					indexEntry=(IndexEntry)pathNode.getEntry(i);
					found=true;
				} 
			}
			if(!found) throw new IllegalStateException();
			down(path, indexEntry);
			pathNode=(Node)node(path);
		}
		return path;
	}
	
	/**
	 * Builds a path from the appropriate root node to the node which
	 * contains the given key in the given version.
	 * 
	 * @param key key to query for.
	 * @param version version to query for.
	 * @param level tree level at which the search should stop.
	 * @return path from a root node to the correct node at the given level.
	 */
	protected Stack pathToNode(Object key, Version version, int level) {
		IndexEntry indexEntry = determineRootEntry(version);
		if (indexEntry == null) throw new IllegalStateException();

		Stack path = new Stack();
		down(path, indexEntry);
		Node node = (Node)node(path);

		while (node.level() > level) {
			MVSeparator lastSep = null;
			indexEntry = null;
			for (int i = 0; i < node.number(); i++) {
				MVSeparator sep = (MVSeparator)separator(node.getEntry(i));
				if (sep.lifespan().contains(version) && sep.sepValue().compareTo(key) <= 0
					&& (lastSep == null || sep.sepValue().compareTo(lastSep.sepValue()) > 0)) {
						lastSep = sep;
						indexEntry = (IndexEntry)node.getEntry(i);
					}
			}
			if (indexEntry == null)
				throw new IllegalStateException();

			down(path, indexEntry);
			node = (Node)node(path);
		}

		return path;
	}

	/** This method is called when the root node overflows and has been splitted. 
	 * A new root node must be created. The tree's height increases by one.
	 * NOTE: In the opposite to {Tree#grow(Object entry)} this method does not 
	 * write the new root node into the container. This redurces one access to the underlaying <tt>Container</tt>.
	 * @param entry the entry which should be inserted to the new root node.
	 * It is normally the old rootEntry.
	 * @return a MapEntry whose key is the new rootEntry and whose value is the 
	 * new root node.
	 */
	protected Entry grow (Object entry) {
		boolean virgin= rootEntry==null;
		Node rootNode = (Node)createNode(height());
		Tree.Node.SplitInfo splitInfo = rootNode.initialize(entry);
		rootEntry = createIndexEntry(height()+1).initialize(splitInfo);
		((IndexEntry)rootEntry).initialize(toMVSeparator((MVRegion)rootDescriptor));
		if(virgin) {
			Object id= rootEntry.container().insert(rootNode);
			rootNode.onInsert(id);
			rootEntry.initialize(id);
		}
		return new MapEntry(rootEntry, rootNode);
	}
	
	/** Unsupported Operation.
	 * @throws UnsupportedOperationException
	 */
	public void insert(Object data) {
		throw new UnsupportedOperationException("You have to define the insertion version. \n" +
												"Please use the method: insert(Version, Object)");
	}
	/** Unsupported Operation.
	 * @throws UnsupportedOperationException
	 */
	public void update (Object oldData, Object newData) {
		throw new UnsupportedOperationException("You have to define the update version. \n" +
												"Please use the method: update(Version, Object, Object)");

	}
	/** Unsupported Operation.
	 * @throws UnsupportedOperationException
	 */
	public Object remove(Object data) {
		throw new UnsupportedOperationException("You have to define the deletion version. \n" +
												"Please use the method: remove(Version, Object)");
	}
	
	/** Inserts a data object into the <tt>MVTree</tt>. 
	 * @param insertVersion the insertion <tt>Version</tt>.
	 * @param data the data object which is to insert.
	 */
	public void insert(Version insertVersion, Object data) {
		setCurrentVersion(insertVersion);
		LeafEntry leafEntry = createLeafEntry(new Lifespan(insertVersion,null), data);
		insert(insertVersion, leafEntry, (MVSeparator)separator(leafEntry), 0);
	}
	/** Replaces the <tt>oldObject</tt> by the <tt>newObject</tt>.
	 * @param updateVersion the <tt>Version</tt> of this Operation.
	 * @param oldData the data object stored in the <tt>MVBTree</tt> which is to replace.
	 * @param newData the new data object.
	 */
	public void update (Version updateVersion, Object oldData, Object newData) {
		Version inVer=currentVersion();
		setCurrentVersion(updateVersion);
		LeafEntry oldEntry=createLeafEntry(new Lifespan(inVer, null), oldData);
		super.update(oldEntry, newData);
	}
	
	/**
	 * Removes a data object from the <tt>MVBTree</tt>.
	 * 
	 * @param removeVersion the <tt>Version</tt> of this Operation.
	 * @param data the data object which is to remove.
	 * @return the removed data object or null if no such element is stored in the <tt>MVBTree</tt>.
	 */
	public Object remove_old(Version removeVersion, Object data) {
		setCurrentVersion(removeVersion);
		Object obj = super.remove(data);	
		return obj;
	}
	
	/**
	 * Removes a data object from the <tt>MVBTree</tt>.
	 * 
	 * @param removeVersion the <tt>Version</tt> of this Operation.
	 * @param data the data object which is to remove.
	 * @return the removed data object or null if no such element is stored in the <tt>MVBTree</tt>.
	 */
	public Object remove(Version removeVersion, Object data) {
		setCurrentVersion(removeVersion);
		Object key = getKey.invoke(data);
		Stack path = pathToNode(key, currentVersion(), 0);
		Iterator it = ((MVBTree.Node)node(path)).iterator();
		LeafEntry removed = null;
		while (it.hasNext()) {
			LeafEntry obj = (LeafEntry)it.next();
			if (obj.data().equals(data) && obj.getLifespan().isAlive()) {
				it.remove();
				removed = obj;
				break;
			}
		}
		treatUnderflow(path);
		return removed;
	}

	/** Inserts a data object into <tt>MVBTree</tt>. 
	 * @param insertVersion the <tt>Version</tt> of this Operation.
	 * @param data the data object which is to store.
	 * @param mvSep the <tt>MVSeparator</tt> of the data object. 
	 * @param targetLevel the target level on which the insertion has to stop (leaf level=0).
	 */
	protected void insert(Version insertVersion, Object data, MVSeparator mvSep, int targetLevel) {
		if (rootEntry()==null) {
//		rootDescriptor = createMVRegion(mvSep.insertVersion(), null, mvSep.sepValue(), mvSep.sepValue());
			// TODO: set separator value to minimum of key domain	
			
			//new code test
			rootDescriptor = createMVRegion(mvSep.insertVersion(), null, this.keyDomainMinValue , mvSep.sepValue());
	//		rootDescriptor = createMVRegion(mvSep.insertVersion(), null, mvSep.sepValue(), mvSep.sepValue());
			grow(data);
		}
		else {
//			super.insert(data, mvSep, targetLevel);
			Stack path = new Stack();
			chooseLeaf(mvSep, targetLevel, path).growAndPost(data, path);
			((IndexEntry)rootEntry).separator().union(mvSep);
		}
	}
	
	/**
	 * Removes a data object form the <tt>MVBTree</tt>.
	 * 
	 * @param data the data object which is to remove.
	 * @param equals a <tt>Predicate</tt> which is used to check whether an element of the <tt>MVBTree</tt> equals 
	 * the given data object.
	 * @return the removed data object or null if no such element is stored in the <tt>MVBTree</tt>.
	 */
	public Object remove(final Object data, final Predicate equals) {
		Comparable key=key(data);
		Version inVer=currentVersion();
		MVRegion querySep= createMVRegion(inVer, null, key, key);
		return super.remove(querySep, 0,
			new AbstractPredicate() {
				public boolean invoke (Object object) {
					return equals.invoke(object, data);
				}
			}
		);
	}
	/** Gives the maximal number of entries which a new created <tt>Node</tt> may contain after a split.
	 * @return the maximal number of entries which a new created <tt>Node</tt> may contain after a split.
	 */
	protected int strongUpperBound(int level) {
		float maxRatio= ((Float) getSplitMaxRatio.invoke()).floatValue();
		return (int)(maxRatio*(level == 0 ? getLeafNodeB() : getIndexNodeB()));
	}
	
	/** Gives the minimal number of entries which a new created <tt>Node</tt> may contain after a split.
	 * @return the minimal number of entries which a new created <tt>Node</tt> may contain after a split.
	 */	
	protected int strongLowerBound(int level) {
		float maxRatio= ((Float) getSplitMinRatio.invoke()).floatValue();
		return (int)(maxRatio*(level == 0 ? getLeafNodeB() : getIndexNodeB()));
	}
	
	/**
	 * Searches the data object stored in the given <tt>Version</tt> of the <tt>MVBTree</tt> with the given key.
	 * @param key the key of the element which is to search.
	 * @param version the <tt>Version</tt> in which the element is to search.
	 * @return the found object if the seach is successful and null otherwise.
	 */
	public Object exactMatchQuery_old(Comparable key, Version version) {
		Cursor result= rangePeriodQuery(key,key,version, version);
		if (result.hasNext()) {
			Object obj = result.next();
			result.close();
			return obj;
		}
		else return null;
	}
	
	/**
	 * Searches the data object stored in the given <tt>Version</tt> of the <tt>MVBTree</tt> with the given key.
	 * 
	 * @param key the key of the element which is to search.
	 * @param version the <tt>Version</tt> in which the element is to search.
	 * @return the found object if the search is successful and null otherwise.
	 */
	public Object exactMatchQuery(Comparable key, Version version) {
		Stack path = pathToNode(key, version, 0);
		Iterator it = ((MVBTree.Node)node(path)).iterator();
		LeafEntry found = null;
		while (it.hasNext()) {
			LeafEntry obj = (LeafEntry)it.next();
			if (getKey.invoke(obj.data()).equals(key)) {
				found = obj;
				break;
			}
		}
		while (!path.isEmpty())
			up(path);
		return found;
	}

	/** Searches all elements which are stored in the <tt>MVBTree</tt> with the given key and valid in the version 
	 * range <tt>[beginVersion, endVersion]</tt>.
	 * @param key the key of the elements.
	 * @param beginVersion the begin of the version range of the query.
	 * @param endVersion the end of the version range of the query.
	 * @return a lazy <tt>Cursor</tt> pointing to all query responses.
	 */
	public Cursor timePeriodQuery(Comparable key, Version beginVersion, Version endVersion) {
		return rangePeriodQuery(key,key,beginVersion, endVersion);
	}

	/** Searches all elements stored in the given <tt>Version</tt> of the <tt>MVBTree</tt> and whose keys lie in the 
	 * key range <tt>[min, max]</tt>.
	 * @param min the minimal bound of the key range of the query.
	 * @param max the maximal bound of the key range of the query.
	 * @param version the <tt>Version</tt> in which the query is to execute.
	 * @return a lazy <tt>Cursor</tt> pointing to all query responses.
	 */
	public Cursor keyRangeQuery(Comparable min, Comparable max, Version version) {
		return rangePeriodQuery(min,max,version,version);
//		return ( version.compareTo(currentVersion())>= 0)?  treequery(createMVRegion(version, version, min, max)) :
//			rangePeriodQuery(min,max,version,version);
//		return treequery(createMVRegion(version, version, min, max));
	}
	
	/** Searches all elements stored in the  <tt>CurrentVersion</tt> of the <tt>MVBTree</tt> and whose keys lie in the 
	 * key range <tt>[min, max]</tt>.
	 * @param min the minimal bound of the key range of the query.
	 * @param max the maximal bound of the key range of the query.
	 * @param version the <tt>Version</tt> in which the query is to execute.
	 * @return a lazy <tt>Cursor</tt> pointing to all query responses.
	 */
	public Cursor rangeQuery(Comparable min, Comparable max) {
		return rangePeriodQuery(min,max,currentVersion(),currentVersion());
	}
	/**
     * Searches the single data object with the given key stored in the 
     * in the  <tt>CurrentVersion</tt> of the <tt>MVBTree</tt>.
     * 
     * @param key
     *            the key of the data object which has to be searched
     * @return the single data object with the given key or <tt>null</tt> if
     *         no such object could be found
     * @throws UnsupportedOperationException
   * 			when the tree is in duplicate mode 
     */
	public Object  exactMatchQuery(Comparable key) {
		return exactMatchQuery(key,currentVersion()) ;
	}
	
	/** Searches all elements stored in the <tt>MVBTree</tt> which are valid in the version range 
	 * <tt>[beginVersion, endVersion]</tt> and whose keys lie in the key range <tt>[min, max]</tt>.
	 * @param min the minimal bound of the key range of the query.
	 * @param max the maximal bound of the key range of the query.
	 * @param beginVersion the begin of the version range of the query.
	 * @param endVersion the end of the version range of the query.
	 * @return a lazy <tt>Cursor</tt> pointing to all query responses.
	 */
	public Cursor rangePeriodQuery(Comparable min, Comparable max, Version beginVersion, Version endVersion) {
		MVRegion queryRegion= createMVRegion(beginVersion, endVersion, min, max);
		IndexEntry root = null;
		root = determineRootEntry(endVersion);
		return query(root, queryRegion);
	}
	/**
	 * Test Method
	 * @param min
	 * 
	 * @param max
	 * @param beginVersion
	 * @param endVersion
	 * @param keyComparator
	 * @return
	 */
	public Cursor rangePriorityQuery(Comparable min, Comparable max, Version beginVersion, Version endVersion, 
			Comparator keyComparator) {
		MVRegion queryRegion= createMVRegion(beginVersion, endVersion, min, max);
		return new PriorityQueryCursor(new DynamicHeap(keyComparator), 0, queryRegion);
	}
	/** Searches all elements stored in the <tt>MVBTree</tt> that lie in the given <tt>MVRegion</tt>.
	 * @param subRootEntry the root of subtree in which the query is to execute.
	 * @param queryRegion the <tt>MVRegion</tt> which specifies the query.
	 * @return a lazy <tt>Cursor</tt> pointing to all query responses.
	 */
	public Cursor query(IndexEntry subRootEntry, final MVRegion queryRegion) {
		return query(subRootEntry, queryRegion, 0);
	}

	/** Searches all elements stored in the <tt>MVBTree</tt> that lie in the given <tt>MVRegion</tt>.
	 * @param subRootEntry the root of subtree in which the query is to execute.
	 * @param queryRegion the <tt>MVRegion</tt> which specifys the query.
	 * @param targetLevel has to be zero (leaf level).
	 * @return a lazy <tt>Cursor</tt> pointing to all query responses.
	 */
	public Cursor query(IndexEntry subRootEntry, final BPlusTree.KeyRange queryRegion, final int targetLevel) {
		if(targetLevel!=0) throw new IllegalArgumentException("Target level must be zero!");
		return linkReferenceQuery(subRootEntry, (MVRegion)queryRegion, new Stack());
	}
	
	public Cursor query(Descriptor queryDescriptor, int targetLevel) {
		MVRegion region;
		if(queryDescriptor instanceof MVSeparator) region=toMVRegion((MVSeparator)queryDescriptor);
		else region=(MVRegion)queryDescriptor;
		return rangePeriodQuery(region.minBound(),region.maxBound(), region.beginVersion(), region.endVersion());
	}
	
	/** Uses the so called "Link Reference Method" to execute the query specified by the given <tt>MVRegion</tt>.
	 * @param indexEntry the root of the subtree in which the query is to execute.
	 * @param queryRegion the <tt>MVRegion</tt> which specifys the query.
	 * @param path the path from the root to the <tt>Node</tt> in which the given <tt>IndexEntry</tt> is stored.
	 * @return a lazy <tt>Cursor</tt> pointing to all query responses.
	 */ 
	protected Cursor linkReferenceQuery(IndexEntry indexEntry, MVRegion queryRegion, Stack path) {
		if (cutoffVersion != null && queryRegion.lifespan().beginVersion().compareTo(cutoffVersion) < 0)
			throw new IllegalArgumentException("Cannot query versions before cutoff version.");
		MVRegion nodeRegion=computeRegion(indexEntry, path);
		return linkReferenceQuery(indexEntry, nodeRegion, queryRegion, path);
	}

	/** Uses the so called "Link Reference Method" to execute the query specified by the given <tt>MVRegion</tt>.
	 * @param indexEntry the root of the subtree in which the query is to execute.
	 * @param nodeRegion the <tt>MVRegion</tt> of the <tt>Node</tt> pointed by the given <tt>IndexEntry</tt>.
	 * @param queryRegion the <tt>MVRegion</tt> which specifys the query.
	 * @return a lazy <tt>Cursor</tt> pointing to all query responses.
	 */ 	
	protected Cursor linkReferenceQuery(IndexEntry indexEntry, MVRegion nodeRegion, MVRegion queryRegion) {
		if(!nodeRegion.overlaps(queryRegion)){ 
			return new EmptyCursor();
		}
		return new MVBTreeQueryCursor(indexEntry, nodeRegion, queryRegion);
	}

	/** Uses the so called "Link Reference Method" to execute the query specified by the given <tt>MVRegion</tt>.
	 * @param indexEntry the root of the subtree in which the query is to execute.
	 * @param nodeRegion the <tt>MVRegion</tt> of the <tt>Node</tt> pointed by the given <tt>IndexEntry</tt>.
	 * @param queryRegion the <tt>MVRegion</tt> which specifys the query.
	 * @param path the path from the root to the <tt>Node</tt> in which the given <tt>IndexEntry</tt> is stored.
	 * @return a lazy <tt>Cursor</tt> pointing to all query responses.
	 */ 		
	protected Cursor linkReferenceQuery(IndexEntry indexEntry, MVRegion nodeRegion, MVRegion queryRegion, Stack path) {
		if(!nodeRegion.overlaps(queryRegion)) return new EmptyCursor();
		return new MVBTreeQueryCursor(indexEntry, nodeRegion, queryRegion, path);
	}

	/** Treats the underflow which can accrue after a deletion.	
	 * @param path the path from the root to the <tt>Node</tt> from which the element has been removed.
	 */
	protected void treatUnderflow(Stack path) {
		treatOverflow(path);
	}
	
	/** Chooses the suitable <tt>IndexEntry</tt> from the given <tt>Iterator</tt> in which an insertion	should be 
	 * continued.
	 * @param mvSeparator the <tt>MVSeparator</tt> of the data object which is to insert.
	 * @param entries an <tt>Iterator</tt> pointing to the <tt>IndexEntries</tt> from which a suitable on is to 
	 * choose.
	 * @return the suitable <tt>IndexEntry</tt> in which an insertion should be continued.
	 */
	protected IndexEntry chooseEntry(MVSeparator mvSeparator, Iterator entries) {
		List currentEntries = new ArrayList(Math.max(MVBTree.this.B_LeafNode, MVBTree.this.B_IndexNode));
		while(entries.hasNext()) { 
			currentEntries.add(entries.next());
		}
		int index= searchMinimumKey(currentEntries);
		if(index==-1) {
			return null;
		}
		IndexEntry subtree= (IndexEntry)currentEntries.get(index);
		Iterator iterator = currentEntries.iterator();
		while(iterator.hasNext()) {
			IndexEntry entry= (IndexEntry)iterator.next();
			if((entry.separator.compareTo(mvSeparator)<=0)&&(entry.separator.compareTo(subtree.separator)>0)){	
				subtree=entry; 
			}
		}
		return subtree;
	}
	
	/** Creates a copy of the given entry.
	 * @param entry a <tt>IndexEntry</tt> or <tt>LeafEntry</tt> which is to copy.
	 * @return a copy of the given entry.
	 */
	protected Object copyEntry(Object entry) {
		if(entry instanceof LeafEntry) {
			LeafEntry leafEntry=(LeafEntry)entry;
			return createLeafEntry((Lifespan)leafEntry.lifespan.clone(), leafEntry.data());
		}
		IndexEntry indexEntry=(IndexEntry)entry;
		IndexEntry cpy=(IndexEntry)createIndexEntry(indexEntry.parentLevel);
		cpy.initialize(indexEntry.id(), (Separator)indexEntry.separator.clone());
		return cpy;
	}

	/**Searchs the entry from the given <tt>List</tt> with the smallest key.
	 * @param entries a <tt>List</tt> containing the entries.
	 * @return the position of the entry with the smallest key in the <tt>List</tt>.
	 */
	protected int searchMinimumKey(List entries) {
		if(entries.isEmpty()) return -1;
		int index=0;
		for(int i=1; i<entries.size(); i++) {
			index=separator(entries.get(i)).compareTo(separator(entries.get(index)))<0 ?i:index; 
		}
		return index;
	}
	/**
	 * 
	 * @param c1
	 * @param c2
	 * @return
	 */
	protected Comparable max(Comparable c1, Comparable c2) {
		if(c1==null) return c2;
		if(c2==null) return c1;
		if(c1.compareTo(c2)>=0) return c1;
		return c2;
	}
	
	/*
	 * 
	 * FIXME remove in productive version: only for test purpose 
	 */
	public void updateRootDescriptor(Descriptor newRootDescriptor){
		this.rootDescriptor = newRootDescriptor;
	}

	/*
	 * FIXME remove in productive version: only for test purpose 
	 */
	public void updateRootEntry(Tree.IndexEntry entry){
		this.rootEntry = entry;
	}
	
	/**
	 * 
	 * @param c1
	 * @param c2
	 * @return
	 */
	protected Comparable min(Comparable c1, Comparable c2) {
		if(c1==null) return c2;
		if(c2==null) return c1;
		if(c1.compareTo(c2)<=0) return c1;
		return c2;
	}
	/**
	 * 
	 */
	private Version minVersion = new Version() {
		public Object clone() {
			return this;
		}

		public int compareTo(Object obj) {
			return -1;
		}
		
	};
	/** This class represents the entries which can be stored in the leaves of the <tt>MVBTree</tt>. A 
	 * <tt>LeafEntry</tt> has a <tt>Lifespan</tt> and capsules a data object.
	 * @author Husain Aljazzar
	 */
	public class LeafEntry {
			
		/** The <tt>Lifespan</tt> of this <tt>LeafEntry</tt>.*/
		protected Lifespan lifespan;
		
		/** The data object of the <tt>LeafEntry</tt>.*/
		protected Object data;
						
		/** Creates a new Leaf Entry. The created Leaf Entry conatains the given Separator and data Object. 
		 * @param separator the separator of the new Leaf Entry
		 * @param data the data object of the new Leaf Entry.
		 */		
		public LeafEntry(Lifespan lifespan, Object data) {
			this.initialize(lifespan, data);
		}
		
		/** Initializes the Leaf Entry by the given Separator and data Object. 
		 * @param separator the new separator of the Leaf Entry
		 * @param data the new data object of the Leaf Entry.
		 * @return the initialized current Leaf Entry itself.
		 */				
		public LeafEntry initialize(Lifespan lifespan, Object data) {
			this.lifespan=lifespan;
			this.data=data;
			return this;
		}
		
		/** Gives the enclosed data object of this Leaf Entry.
		 * @return the enclosed data object of this Leaf Entry.
		 * @see IndexEntry#data()
		 */
		public Object data() {
			return data;
		}
		
		/**
		 * MVBT TODO Specific
		 * @return
		 */
		 
		public Comparable getKey(){
			return key(data);
		}
		
		public Lifespan getLifespan() {
			return lifespan;
		}
		
		/**Computes the <tt>MVSeparator</tt> of this <tt>LeafEntry</tt>.
		 * @return the <tt>MVSeparator</tt> of this <tt>LeafEntry</tt>.
		 */
		public MVSeparator getMVSeparator() {
			 return createMVSeparator(lifespan.beginVersion(), lifespan.endVersion(), key(data));
		}
		
		public boolean equals(Object object) {
			return data.equals(((LeafEntry)object).data);
		}
		
		/** Converts this Leaf Entry to a String. It is used to show the Leaf Entry on the display. 
		 * @return the String display of this Leaf Entry.
		 */ 				
		public String toString() {
			StringBuffer sb=new StringBuffer("(");
			sb.append(lifespan.toString());
			sb.append(", ");
			sb.append(data());
			sb.append(")");
			return sb.toString();
		}
	}
	
	
	public int counter= 0;
	
	public List<IndexEntry> leafentries = new ArrayList<IndexEntry>();
	
	/**This class represents the <tt>Nodes</tt> of the <tt>MVBTree</tt>.
	 * @author Husain Aljazzar
	 */
	public class Node extends BPlusTree.Node {

		/** A <tt>List</tt> containing the temporal predecessors of this <tt>Node</tt>. Note the every <tt>Node</tt> 
		 * has maximal two temporal predecessors.
		 */
		protected List predecessors=new ArrayList(2);
	
		/**Creates a new <tt>Node</tt>.
		 * @param level the level of the <tt>Node</tt>.
		 * @param createEntryList a Function to create the entry list.
		 */
		public Node(int level, Function createEntryList) {
			super(level, createEntryList);
		}
		
		/**Creates a new <tt>Node</tt>.
		 * @param level the level of the <tt>Node</tt>.
		 */		
		public Node(int level) {
			super(level);
		}
		
		/**
		 * This method needs to be called when the node is inserted into the container
		 * and assigned an id. 
		 * 
		 * @param id ID of this node.
		 */
		public void onInsert(Object id) {
		}

		/**Indicates whether a new splitted <tt>Node</tt> violates the strong version condetion.
		 * @return true if the <tt>Node</tt> has too many entries and false otherwise.
		 */
		protected boolean strongOverflows() {
		//	System.out.println("strong max l:" + level() +" "  + strongUpperBound(level()) + " num : "  + number() );
			return number()> strongUpperBound(level());
		}
		/**Inicates whether a new splitted <tt>Node</tt> violates the strong version condetion.
		 * @return true if the <tt>Node</tt> has too few entries and false otherwise.
		 */
		protected boolean strongUnderflows() {
			//System.out.println("l:" + level() +" "  + strongLowerBound(level()) + " num : "  + number() );
			return number()< strongLowerBound(level());
		}
		
		/**Gives the <tt>List</tt> of the <tt>Node's</tt> temporal predecessors.
		 * @return the <tt>List</tt> of the <tt>Node's</tt> temporal predecessors.
		 */		
		public List predecessors() {
			return predecessors;
		}
		/**
		 * hack for bulk loading
		 * @return
		 */
		public List getEntries(){
			return this.entries;
		}
		
		/** Chooses the subtree which is followed during an insertion.
		 * @param descriptor the <tt>MVSeparator</tt> of data object
		 * @param path the path from the root to the current node 
		 * @return the index entry refering to the root of the chosen subtree
		 */
		protected Tree.IndexEntry chooseSubtree (Descriptor descriptor, Stack path) {
			final MVSeparator mvSeparator= (MVSeparator) descriptor;
			Iterator iterator = getCurrentEntries();
			return chooseEntry(mvSeparator, iterator);
		}
		// test method for bulk loading		
		public Tree.IndexEntry chooseSubtree(MVSeparator mvSeparator){
			Iterator iterator = getCurrentEntries();
			return chooseEntry(mvSeparator, iterator);
		}
		
		
		/** Searches the position of the given version. If the version is found its position is returned, 
		 * else (the negation of the insertion position of the version)-1.
		 * Example:
		 * <pre>
		 * 				Search for 7 in [3, 5, 5, 5, 7, 7, 11] returns 4
		 * 				Search for 6 in [3, 5, 5, 5, 7, 7, 11] returns -5
		 * <pre>
		 * @param version the version to search.
		 * @return If the version is found its position is returned, else 
		 * (the negation of its insertion position)-1.
		 */
		protected int search(Version version) {
			return binarySearch(version);
		}
		
		/**Searches a given key in a given range of the <tt>Node</tt>.
		 * @param key the key which is to search.
		 * @param from the begin position of the search range.
		 * @param to the end position of the search range.
		 * @return If the key is found its position is returned, else 
		 * (the negation of its insertion position)-1.
		 */
		protected int search(Comparable key, int from, int to) {
			List minima= new MappedList(entries.subList(from,to),
										new AbstractFunction() {	
											public Object invoke(Object entry) {
												return separator(entry).sepValue();
											}
										});
			int index= Collections.binarySearch(minima, key);
			index=(index>=0)? index+from : index-from;
			return index;
		}
		
		/**Searches the minimal key in the <tt>Node</tt>.
		 * @return the position of the entry with the smallest key.
		 */
		protected int searchMinimumKey() {
			return MVBTree.this.searchMinimumKey(this.entries);
		}
		
		/**Sorts the entries of the <tt>Node</tt> in respect of their <tt>Versions</tt>.
		 */
		protected void sortEntries() {
			sortEntries(false);
		}

		/**Sorts the entries of the <tt>Node</tt> in respect of their <tt>Versions</tt> or keys.
		 * @param keySort indicates whether the entries have to sorted in respect of their keys.
		 */		
		protected void sortEntries(boolean keySort) {
			Comparator comp;
			if(keySort) comp= new Comparator() {
								public int compare(Object o1, Object o2) {
									MVSeparator mvSep1=(MVSeparator)separator(o1);
									MVSeparator mvSep2=(MVSeparator)separator(o2);
									int x= mvSep1.compareTo(mvSep2);
									if(x==0) x=mvSep1.lifespan().compareTo(mvSep2.lifespan());
									return x;
								}
							};
			else comp=  new Comparator() {
							public int compare(Object o1, Object o2) {
								MVSeparator mvSep1=(MVSeparator)separator(o1);
								MVSeparator mvSep2=(MVSeparator)separator(o2);
								int x=mvSep1.lifespan().compareTo(mvSep2.lifespan());
								if(x==0) x= mvSep1.compareTo(mvSep2);
								return x;
							}
						};
						
			Collections.sort(entries,comp);
		}
		
		/**Searches the begining of a given <tt>Version</tt>. 
		 * @param version
		 * @return If the version is found its position is returned, else 
		 * (the negation of its insertion position)-1.
		 */
		protected int binarySearch(Version version) {
			List insertVersionList= 
					new MappedList(entries,
									new AbstractFunction() {
										public Object invoke(Object entry) {
											return ((MVSeparator)separator(entry)).insertVersion();
										}
									});
			return Collections.binarySearch(insertVersionList,version);
		}
		
		/**
		 * 
		 * @param queryRegion
		 * @param nodeRegion
		 * @return
		 */
		private Cursor referenceLeafQuery(final MVRegion queryRegion, final MVRegion nodeRegion) {
			if(level()>0) throw new UnsupportedOperationException("The node is not a leaf.");
			Predicate test=new AbstractPredicate() {
				public boolean invoke(Object entry) {//Umgestellt auf halboffene Intervale
					MVSeparator entrySeparator = (MVSeparator)separator(entry);
					Lifespan entryLifeSpan = entrySeparator.lifespan;
					Comparable entryKey = entrySeparator.sepValue; 
					if (queryRegion.contains(entryKey) && queryRegion.lifespan.overlaps(entryLifeSpan) ){
						Comparable referenceKey= max(queryRegion.minBound(), entryKey);
						Version referenceTime=(Version) min(queryRegion.endVersion(), entryLifeSpan.endVersion());
						if (nodeRegion.contains(referenceKey) ){
							if (nodeRegion.isAlive()) return true;
							else if ((nodeRegion.endVersion().compareTo(referenceTime)>0)){
								return true;
							}//sonder fall EntryIntervall endet in rechte Grenze von NodeIntervall 
							else if ((entryLifeSpan.isDead()) 
									&& entryLifeSpan.endVersion().compareTo(nodeRegion.endVersion()) == 0){
								return true;
							}
						}
					}
					return false;
				}
			};
			return new Filter(this.iterator(), test);
		}

		/** Searches all entries of this <tt>Node</tt> which are alive at the given <tt>Version</tt>.
		 * @param version the <tt>Version</tt> of the query.
		 * @return a <tt>Iterator</tt> pointing to all responses (i.e. all entries of this <tt>Node</tt> 
		 * which are alive at the given <tt>Version</tt>).
		 */		
		public Iterator query(Version version) {
			return query(new Lifespan(version,version));
		}
		
		/** Searches all entries of this <tt>Node</tt> whose <tt>Lifespans</tt> overlap the given <tt>Lifespan</tt>.
		 * @param lifespan the <tt>Lifespan</tt> of the query.
		 * @return a <tt>Iterator</tt> pointing to all responses (i.e. all entries of this <tt>Node</tt> whose 
		 * <tt>Lifespans</tt> overlap the given <tt>Lifespan</tt>).
		 */
		public Iterator query(final Lifespan lifespan) {
			return new Filter(	iterator(),
								new AbstractPredicate() {
									public boolean invoke(Object entry) {									 	
										return ((MVSeparator)separator(entry)).lifespan().overlaps(lifespan);
									}
								});
		}
		
		/** Gives an <tt>Iterator</tt> pointing to all entries of this <tt>Node</tt>.
		 * @return an <tt>Iterator</tt> pointing to all entries of this <tt>Node</tt>.
		 */
		public Iterator iterator() {
			return new Iterator() {
				private int index=0;
				private boolean removeable=false;
				public boolean hasNext() {
					
					return index<MVBTree.Node.this.number();
				}
				
				public Object next() {
					if(!hasNext()) throw new NoSuchElementException();
					removeable=true;
					return MVBTree.Node.this.getEntry(index++);
				}
				
				public void remove() {
					if(removeable) {
						removeable=false;
						MVBTree.Node.this.remove(index-1);
					}
					else throw new NoSuchElementException();
				}
			};
		}
		
		/** Gives an <tt>Iterator</tt> pointing to all entries of this <tt>Node</tt> which have not been deleted yet.
		 * @return an <tt>Iterator</tt> pointing to all entries of this <tt>Node</tt> which have not been deleted yet.
		 */
		public Iterator getCurrentEntries() {
			return new Filter( iterator(),
								new AbstractPredicate() {
									public boolean invoke(Object entry) {
										return ((MVSeparator)separator(entry)).isAlive();
									}
								});
		}
		
		/** Gives all <tt>Node's</tt> entries which have been inserted into the <tt>Node</tt> at or after the given 
		 * <tt>Version</tt>.
		 * @return a <tt>List</tt> containting all such entries.
		 */ 
		protected List entriesFromVersion(Version version) {
			int index= search(version);
			index=(index>=0)?index:-index-1;
			return entries.subList(index, number());			
		}
		
		/** Counts all alive entries stored in this <tt>Node</tt>.
		 * @return the number of alive entries stored in this <tt>Node</tt>.
		 */
		public int countCurrentEntries() {
			return Cursors.count(getCurrentEntries());
		}

		/** Gives all <tt>Node's</tt> entries which have been inserted into the <tt>Node</tt> at the given 
		 * <tt>Version</tt>.
		 * @return a <tt>List</tt> containting all such entries.
		 */ 		
		protected List entriesOf(Version insertVersion) {
			int[] coord= coordinatesOf(insertVersion);
			if(coord[0]<0) return entries.subList(0,0);
			return entries.subList(coord[0], coord[1]);
		}
		
		/**
		 * 
		 */
		private Comparator separatorComp = new Comparator() {
			public int compare(Object o1, Object o2) {
				return separator(o1).compareTo(separator(o2));
			}
		};

		/**
		 * Returns an Iterator pointing to entries whose descriptors overlap the queryDescriptor 
		 * Required in and called from 
		 *  {@link MVBTree#keyRangeQuery(Comparable, Comparable, xxl.core.indexStructures.MVBTree.Version)} query.
		 *  Does not support Lifespan range. Supports only point Lifespans. 
		 * @param queryDescriptor the descriptor describing the query
		 * @return an Iterator pointing to entries whose descriptors overlap the <tt>queryDescriptor</tt>
		 * @throws IllegalArgumentException 
		*/
		public Iterator query(Descriptor queryDescriptor) {
			MVRegion region = (MVRegion)queryDescriptor;
			if (!region.beginVersion().equals(region.endVersion()))
				throw new IllegalArgumentException("Version interval not supported.");
			Version version = region.beginVersion();
			List list = new ArrayList();		
			for (int i = 0; i < number(); i++) {
				Object entry = getEntry(i);
				MVSeparator sep = (MVSeparator)separator(entry);
				if (sep.lifespan().contains(version))
					list.add(entry);
			}
			Collections.sort(list, separatorComp);
            int minIndex = Collections.binarySearch(list, createMVSeparator(null, null, region.minBound()), separatorComp);
            int maxIndex = Collections.binarySearch(list, createMVSeparator(null, null, region.maxBound()), separatorComp);
            minIndex = (minIndex >= 0) ? minIndex : (minIndex == -1 )? 0 :  -minIndex - 2;
       		maxIndex = (maxIndex >= 0) ? maxIndex : (maxIndex == -1 )?  0 :  -maxIndex - 2;
            minIndex = Math.max(minIndex, 0);
            maxIndex = Math.min(maxIndex + 1, list.size());
            List response = list.subList(minIndex, maxIndex);
			return response.iterator();
		}
		
		/** Inserts an entry into this <tt>Node</tt>. If level>0 <tt>data</tt> must be an 
		 * <tt>IndexEntry</tt>.
		 * @param data the entry which has to be inserted into the node
		 * @param path the path from the root to the current node
		 */
		protected void grow(Object entry, Stack path) {
			
			
			
			if (level() != 0)
				removePointEntries();
			int index;
			if(number()==0) index=-1;
			else {
				MVSeparator mvSep=(MVSeparator)separator(entry);
				int[] coord= coordinatesOf(mvSep.insertVersion());
				index= coord[0];
				if(index>=0) {
					index= search(mvSep.sepValue(), coord[0], coord[1]);
				}
			}
			if(index>=0)  {
				throw new RuntimeException("Insertion failed: An entry having the same key was found");
			}
			index=-index-1;
			entries.add(index, entry);
			
			// test code check if the entry separator key the new min key ist ???
			// 
			if(!path.isEmpty()){
				// get entry pointing to this node
				BPlusTree.IndexEntry indexEntry = (BPlusTree.IndexEntry)indexEntry(path); 
				// check descriptor
				if(indexEntry.separator().compareTo(separator(entry)) > 0){
					MVSeparator sepIndex = ((MVSeparator)indexEntry.separator()); 
					MVSeparator entrySep = (MVSeparator)(separator(entry)).clone();
					sepIndex.updateSepValue(entrySep.sepValue());
					// get parent node if exists
					MapEntry pathEntry = (MapEntry)path.pop();
					if(!path.isEmpty()){
						update(path);
					}
					path.push(pathEntry);
				}
			}
		}

		
		public void simpleInsert(Object entry){
			int index;
			if(number()==0) index=-1;
			else {
				MVSeparator mvSep=(MVSeparator)separator(entry);
				int[] coord= coordinatesOf(mvSep.insertVersion());
				index= coord[0];
				if(index>=0) {
					index= search(mvSep.sepValue(), coord[0], coord[1]);
				}
			}
			if(index>=0)  {
				throw new RuntimeException("Insertion failed: An entry having the same key was found");
			}
			index=-index-1;
			entries.add(index, entry);
		}
		
		/**
		 * Removes entries from this node that have an empty lifespan. Such entries
		 * can result from a repeated version split of a node within one version (this
		 * can only happen if multiple updates in the tree within one version are
		 * allowed).
		 */
		protected void removePointEntries() {
			Iterator it = entries.iterator();
			while (it.hasNext())
				if (((MVSeparator)separator(it.next())).lifespan().isPoint())
					it.remove();
		}
		/**
		 * 
		 * @param version
		 * @return
		 */
		private int[] coordinatesOf(Version version) {
			int from=search(version);
			int to=from;
			if(from>=0) {
				to=from+1;
				while(to<number()){
					if(((MVSeparator)separator(getEntry(to))).insertVersion().compareTo(version)>0) break;
					to++;
				}
			}
			return new int[]{from,to};
		}
		
		protected void removeEntriesFromVersion(Version version) {
			int index= search(version);
			index=(index>=0)?index:-index-1;
			entries.subList(index, number()).clear();
		}
		
		protected Object remove(int index) {
			if((index<0)||(index>= entries.size())) return null;
			Object entry=getEntry(index); 
			Lifespan life;
			if(level>0) {
				((MVSeparator)((IndexEntry)entry).separator).delete(currentVersion());
				life=((MVSeparator)((IndexEntry)entry).separator).lifespan();
			}
			else {
				((LeafEntry)entry).lifespan.delete(currentVersion());
				life=((LeafEntry)entry).lifespan;
			}
			if(life.endVersion().compareTo(life.beginVersion())<=0) super.remove(index);
			return entry;
		}
		/**
		 * 
		 */
		protected Collection redressOverflow (Stack path, List newIndexEntries, boolean up) {
			if (overflows() || underflows()) {
				if((path.size()==1)&& underflows() && !overflows()) {
					// root 
					Node rootNode=(Node)node(path);
					if((rootNode.level()!=0)&&(rootNode.countCurrentEntries()==1)) {
						reorg=true;
						Object oldRootId= rootEntry.id();
						int rootParentLevel=rootEntry.parentLevel();
						MVRegion oldRootReg= toMVRegion((MVSeparator)((IndexEntry)rootEntry).separator());
						Version delVersion=oldRootReg.beginVersion();
						Iterator iter= rootNode.entries();
						while(iter.hasNext()) {
							MVSeparator mvSep=(MVSeparator)((IndexEntry)iter.next()).separator;
							if(mvSep.isDead() && mvSep.deleteVersion().compareTo(delVersion)>0) 
								delVersion=mvSep.deleteVersion();
						}
						Iterator current=rootNode.getCurrentEntries();
						if(current.hasNext()) rootEntry=(IndexEntry)current.next();
						else throw new IllegalStateException();
						//oldRootReg.updateMaxBound(((MVRegion)rootDescriptor).maxBound());
						oldRootReg.updateEndVersion(delVersion); 
						Root oldRoot= new Root(oldRootReg, oldRootId, rootParentLevel);
						//	new code 
						// if multiple operations are allowed in one version, a root with an
						// empty lifespan may be generated. It has to be discarded.
						if (!oldRootReg.lifespan().isPoint()) {
							roots.insert(oldRoot);
							//Daniar: new code 
							KeyRange newRange = roots.createKeyRange(oldRootReg.beginVersion(), currentVersion); 
							roots.rootDescriptor.union(newRange);
//							((Lifespan)roots.rootDescriptor).updateMaxBound(currentVersion);
//							.updateMaxBound(currentVersion);
						}
					}
				}	
				else {
					reorg=true;
					Node versionSplitNode=(Node)MVBTree.this.createNode(level());
					SplitInfo splitInfo= (SplitInfo)versionSplitNode.split(path);
					IndexEntry indexEntryOfVersionSplitNode=(IndexEntry) createIndexEntry(level()+1);
					Object idOfVersionSplitNode= 
								indexEntryOfVersionSplitNode.container().insert(splitInfo.versionSplitNode());
					splitInfo.versionSplitNode().onInsert(idOfVersionSplitNode);
					indexEntryOfVersionSplitNode.initialize(idOfVersionSplitNode, 
															splitInfo.getMVSeparatorOfVersionSplitNode());
					IndexEntry indexEntryOfKeySplitNode=null;
					// Test
					if(splitInfo.isKeySplit()) {
						indexEntryOfKeySplitNode=(IndexEntry)createIndexEntry(indexEntryOfVersionSplitNode.parentLevel());
						Object idOfKeySplitNode= indexEntryOfKeySplitNode.container().insert(splitInfo.keySplitNode);
						splitInfo.keySplitNode.onInsert(idOfKeySplitNode);
						indexEntryOfKeySplitNode.initialize(idOfKeySplitNode, 
															splitInfo.getMVSeparatorOfKeySplitNode());
					}
					//A root overflow...
					if (splitInfo.isRootSplit()) {
						int oldRootParentLevel= rootEntry.parentLevel();
						Object oldRootId= rootEntry.id();
						MVRegion oldRootReg= toMVRegion((MVSeparator)((IndexEntry)rootEntry).separator());
						if(splitInfo.isKeySplit()) {
							Entry newRootTuple = MVBTree.this.grow(indexEntryOfVersionSplitNode);
							Node newRootNode= (Node)newRootTuple.getValue();
							newRootNode.entries.add(indexEntryOfKeySplitNode);
							MVSeparator rootSeparator= (MVSeparator)indexEntryOfVersionSplitNode.separator;
							Object newRootNodeId= MVBTree.this.container().insert(newRootNode);
							newRootNode.onInsert(newRootNodeId);
							((IndexEntry)rootEntry).initialize(newRootNodeId,rootSeparator);
						}
						else rootEntry = indexEntryOfVersionSplitNode;
						oldRootReg.updateMaxBound(((MVRegion)rootDescriptor).maxBound());
						Root oldRoot= new Root(oldRootReg, oldRootId, oldRootParentLevel);
						// if multiple operations are allowed in one version, a root with an
						// empty lifespan may be generated. It has to be discarded.
						if (!oldRootReg.lifespan().isPoint()) {
							roots.insert(oldRoot);
							//Daniar: new code bug fix 
							KeyRange newRange = roots.createKeyRange(oldRootReg.beginVersion(), currentVersion); 
							roots.rootDescriptor.union(newRange);
//							((Lifespan)roots.rootDescriptor).updateMaxBound(currentVersion);
						}
					}
					else {
						MapEntry pathEntry = (MapEntry)path.pop();
						Node parentNode = (Node)node(path);
						// new code first grow
						parentNode.grow(indexEntryOfVersionSplitNode, path);
						
						newIndexEntries.add(newIndexEntries.size(), indexEntryOfVersionSplitNode);
						if(splitInfo.isKeySplit()) {
							parentNode.grow(indexEntryOfKeySplitNode, path);
							newIndexEntries.add(newIndexEntries.size(), indexEntryOfKeySplitNode);
						} 
						parentNode.sortEntries();
						// new code first grow
						path.push(pathEntry);
					}
				}
			}
			if (up) {
				update(path);
				up(path);
			}	
			if (level==0){
				counter+=newIndexEntries.size();
				leafentries.addAll(newIndexEntries);
			}
			return newIndexEntries;
		}
				
		protected Tree.Node.SplitInfo split(Stack path) {
			return split(currentVersion, path);
		}
		
		protected SplitInfo split(Version splitVersion, Stack path) {
			SplitInfo splitInfo = new SplitInfo(path);	
			splitInfo = this.versionSplit(splitVersion, path, splitInfo);
			if (strongOverflows())
				splitInfo = this.treatStrongOverflow(path, splitInfo);
			else if (strongUnderflows())
				splitInfo = this.treatStrongUnderflow(path, splitInfo);
			return splitInfo;
		}
		
		/*****************************************************************************************
		 * For bulk loading 
		 ******************************************************************************************/
		
		public List getDataEntries(){
			return entries;
		}
		
		public void sortKeyDimension(){
			sortEntries(true);
		}
		
		public void sortTimeDimension(){
			sortEntries(false);
		}
		
		
		
		/*****************************************************************************************
		 * !!!
		 ******************************************************************************************/
		
		
		/**
		 * 
		 * @param splitVersion
		 * @param path
		 * @param splitInfo
		 * @return
		 */
		protected SplitInfo versionSplit(Version splitVersion, Stack path, SplitInfo splitInfo) {
			Node node = (Node)node(path);
			IndexEntry indexEntry = (IndexEntry)indexEntry(path);
			splitInfo.initVersionSplit(splitVersion, (MVSeparator)indexEntry.separator());
			if (splitVersion.compareTo(((MVSeparator)indexEntry.separator).insertVersion()) < 0){
				throw new  IllegalArgumentException("Split of:"+ indexEntry+" at Version:"+splitVersion);
			}
			((MVSeparator)indexEntry.separator).delete(splitVersion);
			Iterator currentEntries = node.query(splitVersion);
			while (currentEntries.hasNext()) {
				Object entry = currentEntries.next();
				Object cpy = copyEntry(entry);
				entries.add(cpy);
			}
			if (level()==0) {
				this.predecessors.clear();
				IndexEntry predEntry = (IndexEntry)createIndexEntry(indexEntry.parentLevel);
				MVSeparator predSep = (MVSeparator)separator(indexEntry).clone();
				predSep.delete(splitVersion);
				predEntry.initialize(indexEntry.id(), predSep); 
				this.predecessors.add(predEntry);
			}
			node.removeEntriesFromVersion(splitVersion);
			purgeQueue.enqueue(new BlockDelInfo(indexEntry.id(), splitVersion));
			return splitInfo;
		}
				
		protected SplitInfo versionSplit(Version splitVersion, Stack path) {
			SplitInfo splitInfo = new SplitInfo(path);
			return versionSplit(splitVersion, path, splitInfo);
		}
		
		protected SplitInfo treatStrongOverflow(Stack path, SplitInfo splitInfo) {
			splitInfo = keySplit(path, splitInfo);
			return splitInfo;
		}
		
		protected SplitInfo treatStrongUnderflow(Stack path, SplitInfo splitInfo) {
			if (!splitInfo.isRootSplit()) {
				splitInfo = strongMerge(path,splitInfo);
				if (strongOverflows()){
					treatStrongOverflow(path,splitInfo);
					//Fall 
					if (this.level == 0) adjustPredecessors(splitInfo);
					
				}
			}
			return splitInfo;
		}
		/**
		 * 
		 * @param splitInfo
		 */
		protected  void adjustPredecessors(SplitInfo splitInfo){
			if(!(splitInfo.isMergePerformed && splitInfo.isKeySplit())) throw new IllegalStateException();
			Comparable minSplitBound = splitInfo.separatorOfNewNode().sepValue;
			IndexEntry predecessor1 = (IndexEntry)predecessors.get(0);
			IndexEntry predecessor2 = (IndexEntry)predecessors.get(1);
			int index = (predecessor1.separator.sepValue.compareTo( predecessor2.separator.sepValue ) > 0) ?
					0: 1;
			Comparable mergeSiblingBound = ((IndexEntry)predecessors.get(index)).separator.sepValue;
			if ( mergeSiblingBound.compareTo(minSplitBound) > 0){
					int minIndex = (index == 0)? 1: 0;
					IndexEntry predecessorMin = (IndexEntry)predecessors.get(minIndex);
					predecessors.remove(index);
					splitInfo.keySplitNode.predecessors.add(predecessorMin);
			}else if (mergeSiblingBound.compareTo(minSplitBound) == 0){
				predecessors.remove(index);
			}
		}

		protected SplitInfo keySplit(Stack path, SplitInfo splitInfo) {
			this.sortEntries(true);
			List cpyEntries = this.entries.subList((number()+1)/2, number());
			Node newNode = (Node)MVBTree.this.createNode(level());
			newNode.entries.addAll(cpyEntries);
			cpyEntries.clear();
			this.sortEntries();
			
			MVSeparator newSeparator = (MVSeparator)separator(newNode.getFirst()).clone();
			newSeparator.union(separator(newNode.getLast()));
			newSeparator.setInsertVersion(splitInfo.splitVersion());
			newNode.sortEntries();
			splitInfo.initKeySplit(newSeparator, newNode);
			if (level()==0)
				newNode.predecessors.add(this.predecessors.get(0));
			return splitInfo;
		}

		protected SplitInfo strongMerge(Stack path, SplitInfo splitInfo) {
			if (path.size() <= 1)
				throw new IllegalStateException("There is no parent on the stack.");
			Node node = splitInfo.versionSplitNode(); // created node
			IndexEntry mergeSibling = determineStrongMergeSibling(path, splitInfo);
			//test new code test
			splitInfo.setSiblingMergeSeparator((MVSeparator)mergeSibling.separator);
			MapEntry pathEntry = (MapEntry)path.pop();
			down(path, mergeSibling);
			Node tempNode = (Node)MVBTree.this.createNode(level());
			tempNode.versionSplit(splitInfo.splitVersion(), path);
			node.entries.addAll(tempNode.entries);
			node.sortEntries();
			if (node.level() == 0)
				node.predecessors.add(tempNode.predecessors.get(0));
			update(path);
			up(path);
			path.push(pathEntry);
			splitInfo.setMergePerformed();
			assert node.countCurrentEntries() >= (level() == 0 ? getLeafNodeD() : getIndexNodeD());
			return splitInfo; 
		}
				
		protected MVRegion computeLeafMVRegion() {
			if(level!=0) throw new UnsupportedOperationException();
			MVRegion mvRegion= toMVRegion((MVSeparator)separator(getFirst()));
			for(int i=0; i<number(); i++) {
				MVSeparator mvSep=(MVSeparator)separator(getEntry(i));
				if(mvSep.sepValue().compareTo(mvRegion.minBound())<0) mvRegion.updateMinBound(mvSep.sepValue());
				if(mvSep.sepValue().compareTo(mvRegion.maxBound())>0) mvRegion.updateMaxBound(mvSep.sepValue());
				if(mvSep.insertVersion().compareTo(mvRegion.beginVersion())<0) 
					mvRegion.updateBeginVersion(mvSep.insertVersion());
				if(mvSep.isAlive()||
					mvRegion.isDead() && (mvSep.deleteVersion().compareTo(mvRegion.endVersion())>0)) 
					mvRegion.updateEndVersion(mvSep.deleteVersion());
			}
			return mvRegion;		
		}
		
		protected IndexEntry determineStrongMergeSibling(Stack path, SplitInfo splitInfo) {
			IndexEntry indexEntry= (IndexEntry)indexEntry(path);
			MapEntry pathEntry=(MapEntry) path.pop();
			Node parentNode=(Node)node(path);
			IndexEntry mergeEntry=(IndexEntry)parentNode.chooseSubtree(indexEntry.separator, path);
			path.push(pathEntry);
			return mergeEntry;
		}
		
		
		public class SplitInfo extends BPlusTree.Node.SplitInfo {
			
			protected Version splitVersion = null;
			protected MVSeparator oldSeparator = null;
			protected MVSeparator mergeSiblingSeparator = null;
			protected Node keySplitNode = null;
			protected boolean isRootSplit;
			protected boolean isMergePerformed;

			public SplitInfo(Stack path) {
				super(path);
				isRootSplit=(path.size()==1);
			}

			protected SplitInfo initVersionSplit(Version splitVersion, MVSeparator oldSeparator) {
				this.splitVersion=(Version)splitVersion.clone();
				this.oldSeparator=oldSeparator;
				return this;
			}
				
			protected SplitInfo initKeySplit(MVSeparator separatorOfNewNode, Node newNode) {
				keySplitNode=newNode;
				separatorOfNewNode.setInsertVersion(splitVersion);
				separatorOfNewNode.setDeleteVersion(null);
				return (SplitInfo) super.initialize(separatorOfNewNode);
			}

			protected Node versionSplitNode() {
				return (Node) super.newNode();
			}

			boolean isKeySplit() {
				return this.keySplitNode!=null;
			}
			
			//test
			protected void setSiblingMergeSeparator(MVSeparator mergeSiblingSeparator){
				this.mergeSiblingSeparator = mergeSiblingSeparator;
			}  
			
			protected boolean isMergeSiblingBelow(){
				if (!isMergePerformed) throw new IllegalStateException("Merge was not performed!" );
				return  this.oldSeparator.sepValue.compareTo(this.mergeSiblingSeparator.sepValue) > 0 ;
			}  
			
			
			/*
			 * 3 Faelle 
			 * 1. Unterlauf+Merge
			 * 2. Unterlauf+Merge+KeySplit
			 * 3. Ueberlauf+KeySplit
			 */
			MVSeparator getMVSeparatorOfVersionSplitNode() {
				MVSeparator mvSeparator = (MVSeparator)oldSeparator.clone();
				if (isMergePerformed  && isMergeSiblingBelow()){ 
					mvSeparator = (MVSeparator)mergeSiblingSeparator.clone();
				}
				mvSeparator.setInsertVersion(splitVersion);
				mvSeparator.setDeleteVersion(null);
				return mvSeparator;
			}
			/*
			 *
			 */
			MVSeparator getMVSeparatorOfKeySplitNode() {
				MVSeparator mvSeparator = (MVSeparator)separatorOfNewNode;
				return mvSeparator; 
			}
			Version splitVersion() {
				return splitVersion;
			}

			boolean isRootSplit() {
				return isRootSplit;
			}
			
			boolean isMergePerformed() {
				return isMergePerformed;
			}
			
			void setMergePerformed() {
				isMergePerformed = true;
			}
		}
		
		
		
	}
	/**
	 * 
	 * Need to handle root in the bplus tree with max key 
	 *
	 */
	public static class LifeSpanSeparator extends Separator {
		
		/**A factury <tt>Function</tt> to create <tt>Lifespans</tt>.*/
		public static final Function FACTORY_FUNCTION= 
				new AbstractFunction<Object,Object>() {
					public Object invoke(Object version) {
						return new LifeSpanSeparator((Version)version);
					}		
		};
		
		public LifeSpanSeparator(Version beginVersion) {
			super(beginVersion);
		}
		
		public Object clone() {		
			return new LifeSpanSeparator((Version)((Version)this.sepValue).clone()); 
		}
		
	}
	
	

	/** The instances of this class are refernces pointing to a root node 
	 * of the MVBTree. Also they are leaf entries stored in the BTree "roots". 
	 * @author Husain Aljazzar
	 * @version Aug 19, 2003
	 */
	public class Root {
		protected MVRegion region;
		protected Object rootId;
		protected int parentLevel;
		
		public Root(MVRegion region, Object rootId, int parentLevel) {
			this.region=region;
			this.rootId=rootId;
			this.parentLevel=parentLevel;
		}
		public Lifespan lifespan() {
			return region.lifespan();
		}
		public MVRegion getRegion() {
			return region;
		}
		public Object rootNodeId() {
			return rootId;
		}
		public int parentLevel() {
			return parentLevel;
		}
		public Node get(boolean unfix) {
			return (Node) MVBTree.this.container().get(rootId, unfix);
		}
		public IndexEntry toIndexEntry() {
			IndexEntry indexEntry=(IndexEntry)createIndexEntry(parentLevel);
			indexEntry.initialize(rootId, toMVSeparator(region));
			return indexEntry;
		}
		public String toString() {
			StringBuffer sb= new StringBuffer("R(");
			sb.append(lifespan().toString());
			sb.append(" ->");
			sb.append(rootNodeId());
			sb.append(")");
			return sb.toString();
		}
	}
	
	public static abstract class MVSeparator extends Separator {
		
		protected Lifespan lifespan;
		
		public MVSeparator(Version insertVersion, Comparable sepValue) {
			this(insertVersion, null, sepValue);
		}
		
		protected MVSeparator( Version insertVersion, Version deleteVersion, 
								Comparable sepValue) {
			super(sepValue);
			this.lifespan=new Lifespan(insertVersion, deleteVersion);						
		}
		
		public void setInsertVersion(Version newInsertVersion) {
			lifespan.updateMinBound(newInsertVersion);
		}
		public void setDeleteVersion(Version newDeleteVersion) {
			lifespan.updateMaxBound(newDeleteVersion);
		}
		public void delete(Version deleteVersion) {
			lifespan.delete(deleteVersion);
		}
		public Lifespan lifespan() {
			return lifespan;
		}
		public Version insertVersion() {
			return lifespan.beginVersion();
		}
		public Version deleteVersion() {
			return lifespan.endVersion();
		}
		public boolean isAlive() {
			return lifespan.isAlive();
		}
		public boolean isDead() {
			return lifespan.isDead();
		}

		public boolean contains(Version version) {
			return lifespan.contains(version);
		}
		
		public String toString() {
			StringBuffer sb= new StringBuffer("[");
			sb.append(sepValue()+" ");
			sb.append(lifespan().toString());
			sb.append("[");
			return sb.toString();
		}
	}
	
	public static abstract class MVRegion extends BPlusTree.KeyRange {
		
		protected Lifespan lifespan;
		
		public MVRegion(Version beginVersion, Version endVersion, Comparable beginKey, Comparable endKey) {
			super(beginKey, endKey);
			this.lifespan=new Lifespan(beginVersion, endVersion, true);
		}

		public Lifespan lifespan() {
			return lifespan;
		}

		public Version beginVersion() {
			return lifespan.beginVersion();
		}
		
		public Version endVersion() {
			return lifespan.endVersion();
		}

		public void updateBeginVersion(Version newBeginVersion) {
			lifespan.updateMinBound((Version)newBeginVersion.clone());
		}
		
		public void updateEndVersion(Version newEndVersion) {
			Version vers=newEndVersion==null? null:(Version)newEndVersion.clone();
			lifespan.updateMaxBound(vers);
		}

		public boolean isAlive() {
			return lifespan.isAlive();
		}
		public boolean isDead() {
			return lifespan.isDead();
		}
		
		public void union(Descriptor descriptor, boolean time) {
			super.union(descriptor);
			if(time) {
				Lifespan life=(descriptor instanceof MVRegion)? ((MVRegion)descriptor).lifespan()
															: ((MVSeparator)descriptor).lifespan();
				this.lifespan().union((Descriptor)life);
			} 
		}

		public void union(Descriptor descriptor) {
			union(descriptor, true);
		}
		
		public boolean overlaps(Descriptor descriptor) {
			if(!(descriptor instanceof MVRegion)) return false;			
			MVRegion mvReg=(MVRegion) descriptor;
			return keyOverlaps(mvReg)&& versionOverlaps(mvReg);
		}
		
		public boolean keyOverlaps(MVRegion mvReg) {
			return super.overlaps(mvReg);
		}
		
		public boolean versionOverlaps(MVRegion mvReg) {
			return lifespan.overlaps(mvReg.lifespan);
		}
		
		public boolean contains(Descriptor descriptor) {
			if (descriptor instanceof MVSeparator)
				return super.contains(descriptor)&&this.lifespan.contains((Descriptor)((MVSeparator)descriptor).lifespan);
			if (descriptor instanceof MVRegion)
				return super.contains(descriptor)&&this.lifespan.contains((Descriptor)((MVRegion)descriptor).lifespan);
			return false;				
		}
		
		public boolean contains(Version version) {
			return lifespan.contains(version);
		}
				
		public String toString() {
			StringBuffer sb= new StringBuffer("[");
			sb.append("[" + minBound() + " " + (maxBound()== null  ? "--": maxBound)  );
			sb.append("]");
			sb.append(" ");
			sb.append(lifespan().toString());
			sb.append("[");
			return sb.toString();
		}
	}
	
	/** Objects of this class are version intervals and represent life spans of the entries sotred in the 
	 * <tt>MVBTree</tt>. A <tt>Lifespan</tt> consists of a begin and end versions and has one the following forms:
	 * <ul>
	 * <li><tt>[beginVersion, null[</tt> the entry became valid startig from <tt>beginVersion</tt> and it is still 
	 * alive.</li>
	 * <li><tt>[beginVersion, endVersion[</tt> the entry became valid startig from <tt>beginVersion</tt> and it was 
	 * deleted at <tt>endVersion</tt>.</li>
	 * <li><tt>[beginVersion, endVersion]</tt> this form is used to express query or root regions.</li>
	 * 
	 * @author Husain Aljazzar
	 * @version Aug 19, 2003
	 */
	public static class Lifespan extends BPlusTree.KeyRange {
				
		/**Indicates whether the <tt>endVersion</tt> is contained in the life interval.*/
		protected boolean isRightClose=false;
		
		/**A factury <tt>Function</tt> to create <tt>Lifespans</tt>.*/
		public static final Function FACTORY_FUNCTION= 
				new AbstractFunction<Object,Object>() {
					public Object invoke(Object beginVersion) {
						return new Lifespan((Version)beginVersion);
					}
					public Object invoke(Object beginVersion, Object endVersion) {
						return new Lifespan((Version)beginVersion, (Version)endVersion);
					}
					public Object invoke(List<? extends Object> args) {
						switch(args.size()) {
							case 1: return invoke(args.get(0));
							case 2: return invoke(args.get(0), args.get(1));
							case 3: return new Lifespan((Version)args.get(0), (Version)args.get(1), 
														((Boolean)args.get(2)).booleanValue()); 
						}
						throw new IllegalArgumentException();
					}
				};
				
		/**Creates a new <tt>Lifespan</tt> from the form <tt>[beginVersion, endVersion[</tt> with the given begin 
		 *and end version.
		 *@param beginVersion the begin version of the <tt>Lifespan</tt>.
		 *@param endVersion the end version of the <tt>Lifespan</tt>. 
		 */
		public Lifespan(Version beginVersion, Version endVersion) {
			 this(beginVersion, endVersion, false);
		}

		/**Creates a new <tt>Lifespan</tt> from the form <tt>[beginVersion, null[</tt> with the given begin version.
		 *@param beginVersion the begin version of the <tt>Lifespan</tt>. 
		 */
		public Lifespan(Version beginVersion) {
			this(beginVersion, null, false);
		}

		/**Creates a new <tt>Lifespan</tt> with the given begin and end version.
		 *@param beginVersion the begin version of the <tt>Lifespan</tt>.
		 *@param endVersion the end version of the <tt>Lifespan</tt>.
		 *@param isRightClose indicates whether the new <tt>Lifespan</tt> is a right close interval. 
		 */		
		public Lifespan(Version beginVersion, Version endVersion, boolean isRightClose) {
			super(beginVersion == null ? null : (Version)beginVersion.clone(),(endVersion==null)? null: (Version)endVersion.clone());
			this.isRightClose=endVersion()!=null && isRightClose;
		}
		
		/** Checks whether the given <tt>Descriptor</tt> overlaps this <tt>Lifespan</tt>.  
		 * @param descriptor the  <tt>Lifespan</tt> to check.
		 * @return true if the given <tt>Descriptor</tt> an instance of of <tt>Lifespan</tt> and overlaps this 
		 * <tt>Lifespan</tt> and false otherwise. 
		 * @see xxl.indexStructures.Separator#overlaps(xxl.indexStructures.Tree.Descriptor)
		 */
		
		public boolean overlaps(Descriptor descriptor) {
			if(descriptor instanceof Lifespan) {
				Lifespan lifespan=(Lifespan) descriptor;
				return contains(lifespan.beginVersion())|| lifespan.contains(this.beginVersion());
			}
			return false;
		}

		/** Builds the union of two <tt>Lifespans</tt>. The current <tt>Lifespan</tt> will be changed and returned.
		 * @param descriptor the <tt>Lifespan</tt> which is to unite with the current <tt>Lifespan</tt>.
		 * @return the current <tt>Lifespan</tt> is changed then returned. 
		 * @see xxl.indexStructures.Separator#union(xxl.indexStructures.Tree.Descriptor)
		 */
		public void union(Descriptor descriptor) {
			if(!(descriptor instanceof Lifespan)) return;
			Lifespan lifespan=(Lifespan) descriptor;
			if(beginVersion().compareTo(lifespan.beginVersion())>0) updateMinBound(lifespan.beginVersion());
			if(isAlive()||lifespan.isAlive()) updateMaxBound(null);
			else {
				int x=endVersion().compareTo(lifespan.endVersion());
				if(x==0) isRightClose=this.isRightClose()||lifespan.isRightClose();
				if(x<0) {
					updateMaxBound(lifespan.endVersion());
					isRightClose=lifespan.isRightClose();
				}
			}
		}

		public int compareTo(Object lifespan) {
			return beginVersion().compareTo(((Lifespan)lifespan).beginVersion());
		}
		
		public boolean isRightOf(Comparable version) { 
			return isAlive() || endVersion().compareTo(version)>0;
		}
		
		/** Checks whether the given <tt>Descriptor</tt> is totally contained in this <tt>Lifespan</tt>.  
		 * @param descriptor the <tt>Lifespan</tt> to check.
		 * @return true if the given <tt>Descriptor</tt> an instance of <tt>Lifespan</tt> and lies totally in this 
		 * <tt>Lifespan</tt> and false otherwise. 
		 * @see xxl.indexStructures.Separator#contains(xxl.indexStructures.Tree.Descriptor)
		 */
		/*
		public boolean contains(Descriptor descriptor) {
			if(!(descriptor instanceof Lifespan)) return false;
			Lifespan lifespan=(Lifespan) descriptor;
			return contains(lifespan.beginVersion())&& contains(lifespan.endVersion());
		}*/

		/**Checks whether this <tt>Lifespan</tt> contains the given <tt>version</tt>.
		 * @param version the <tt>Version</tt> to check. 
		 * @return true if this <tt>Lifespan</tt> contains the given <tt>version</tt> and false otherwise.
		 */ 
		// protected
		public boolean contains(Version version) {
			if(version==null) return isAlive();
			return (beginVersion().compareTo(version)<=0)
				&& (isAlive()|| endVersion().compareTo(version)>0 
							|| isRightClose()&& endVersion().compareTo(version)==0);
		}
		
		/**Creates a physical copy of this <tt>Lifespan</tt>.
		 */
		public Object clone() {
			Version in=isDefinite()? (Version)beginVersion().clone(): null;
			Version del= isDead()? (Version)endVersion().clone(): null; 
			return new Lifespan(in, del, isRightClose()); 
		}
		
		/**Gives the <tt>begin version</tt> of this <tt>Lifespan</tt>.
		 * @return the <tt>begin version</tt> of this <tt>Lifespan</tt>.
		 */
		public Version beginVersion() {
			return (Version)sepValue();
		}

		/**Gives the <tt>end version</tt> of this <tt>Lifespan</tt>.
		 * @return the <tt>end version</tt> of this <tt>Lifespan</tt>. NOTE: The <tt>end version</tt> may be 
		 * <tt>null</tt>.
		 */
		public Version endVersion() {
			return (Version)maxBound();
		}
		
		/**Deletes the <tt>Lifespan</tt> virually by setting the <tt>end version</tt> to the given 
		 * <tt>deleteVersion</tt>.
		 * @param deleteVersion the </tt>Version</tt> at which the <tt>Lifespan</tt> is to delete.
		 */
		public void delete(Version deleteVersion) {
			if(isDead()&& (endVersion().compareTo(deleteVersion)!=0)) 
				throw new UnsupportedOperationException("Delete a dead entry: Lifespan="+this+
						" \t the deletion version="+deleteVersion);
			updateMaxBound(deleteVersion);
			isRightClose=false;
		}
				
		/**Checks whether this <tt>Lifespan</tt> is already dead, i.e. the <tt>end version</tt> is not null.
		 * @return false if the <tt>end version</tt> is null and true otherwise.
		 */
		public boolean isDead() {
			return endVersion()!=null;
		}

		/**Checks whether this <tt>Lifespan</tt> is alive, i.e. the <tt>end version</tt> is null.
		 * @return true if the <tt>end version</tt> is null and false otherwise.
		 */
		public boolean isAlive() {
			return endVersion()==null;
		}
		
		/** Checks whether the separation value of this <tt>Separator</tt> is definite (not null).
		 * 
		 * @return <tt>true</tt> if the separation value is not <tt>null</tt>, <tt>false</tt> otherwise.
		 */
		public boolean isDefinite() {
			return sepValue!=null;
		}

		/**Gives whether this <tt>Lifespan</tt> is a right close interval, i.e. it is already dead and the flag 
		 * {@link Lifespan#isRightClose} is true. 
		 * @return true if this <tt>Lifespan</tt> is already dead and the flag {@link Lifespan#isRightClose} is true 
		 * and false otherwise.
		 */
		public boolean isRightClose() {
			return isDead()&& isRightClose;
		}
		
		@Override
		public boolean isPoint() {
			return isDead() && super.isPoint();
		}

		/** Converts this <tt>Lifespan</tt> to a String. It is used to show the <tt>Lifespan</tt> on the 
		 * display. 
		 * @return the String display of this <tt>Lifespan</tt>.
		 */ 				

		public String toString() {
			String del= isAlive()? "*": endVersion().toString();
			return "<"+(beginVersion() == null ? "NULL" : beginVersion().toString())+", "+	del+">";
		}
		
		public boolean equals(Object obj) {
			Lifespan lifespan = (Lifespan)obj;
			return this.beginVersion().equals(lifespan.beginVersion()) &&
				(this.endVersion() == null ? lifespan.endVersion() == null : this.endVersion().equals(lifespan.endVersion())) &&
				this.isRightClose() == lifespan.isRightClose();
		}
	}
	
	/** This is an interface for version instances used by the MVBTree. 
	 * The <tt>MVBTree</tt> can work with any version objects. 
	 */
	
	public static interface Version extends Comparable {
		public Object clone();
		public int hashCode();
	}
	
	/**
	 * 
	 * 
	 *
	 */
	protected class MVBTreeQueryCursor extends xxl.core.indexStructures.QueryCursor {
		
		protected MVRegion nodeRegion; 
		private Cursor nodeQuery;
		private int index;
		private int indexPrevious;
		private boolean routed;
		protected LeafEntry currentEntry;
		/**
		 * List with entries which overlap right bound of query  time interval, sorted according the key
		 */
		private List keySortedEntries; 
		/**
		 * 
		 * @param indexEntry
		 * @param nodeRegion
		 * @param queryRegion
		 */
		public MVBTreeQueryCursor(IndexEntry indexEntry, MVRegion nodeRegion, MVRegion queryRegion) {
			super(MVBTree.this, indexEntry, queryRegion, 0);
			this.indexEntry=indexEntry;
			this.nodeRegion=nodeRegion;
			currentNode=  indexEntry.get(false);
			//if regio is alive
			if (queryRegion.endVersion() == null){
				keySortedEntries = keySortCurrentNodeEntries(((Node)currentNode).getCurrentEntries());
			}else{
//				keySortedEntries = computeQueryCandidates();
				keySortedEntries = keySortCurrentNodeEntries(
						((Node)currentNode).query(queryRegion.endVersion()));
			}
//			System.out.println("Node Region " + nodeRegion);
//			System.out.println("Cursor Current Node " + currentNode);
//		
//			
			index=0;
			indexPrevious=0;
			routed=false;
		}
		/**
		 * 
		 * @param indexEntry
		 * @param nodeRegion
		 * @param queryRegion
		 * @param path
		 */
		public MVBTreeQueryCursor(IndexEntry indexEntry, MVRegion nodeRegion, MVRegion queryRegion, Stack path) {
			this(indexEntry, nodeRegion, queryRegion);
			this.path=path;
			path.push(new MapEntry(indexEntry, currentNode));
		}
		/**
		 * Test
		 * @param entriesIterator
		 * @return
		 */
		private List keySortCurrentNodeEntries(Iterator entriesIterator){
			List sortedList = new ArrayList();
			Comparator comparator = new Comparator() { // key +  time sort
				public int compare(Object o1, Object o2) {
					MVSeparator mvSep1=(MVSeparator)separator(o1);
					MVSeparator mvSep2=(MVSeparator)separator(o2);
					int x= mvSep1.compareTo(mvSep2);
					if(x==0) x=mvSep1.lifespan().compareTo(mvSep2.lifespan());
					return x;
				}
			};
			while(entriesIterator.hasNext()){
				sortedList.add(entriesIterator.next());
			}
			Collections.sort(sortedList, comparator);
			
			return sortedList;
		}
		
		/**
		 * 
		 * @param entriesIterator
		 * @return
		 */
		protected List computeQueryCandidates(){
			// entries which are cut the right region bound
			List sortedList = keySortCurrentNodeEntries(
					((Node)currentNode).query(((MVRegion)queryRegion).endVersion()));
			// all entries which are overlap the query time interval
			Iterator entriesIterator = ((Node)currentNode).query(((MVRegion)queryRegion).lifespan());
			while(entriesIterator.hasNext()){
				Object item = entriesIterator.next();
				MVSeparator itemSeparator = (MVSeparator)separator(item);
				Comparable key = itemSeparator.sepValue();
				if(((MVRegion)queryRegion).lifespan().overlaps(itemSeparator.lifespan()) ){
					// check if the key is already 
					Comparable lastKey = ((MVSeparator)separator(sortedList.get(sortedList.size()-1))).sepValue();
					Comparable firstKey = ((MVSeparator)separator(sortedList.get(0))).sepValue();
					if (lastKey.compareTo(key) < 0 ){
						// append key 
						sortedList.add(item);
					}else if (firstKey.compareTo(key) > 0 ){
						sortedList.add(0, item);
					}
				}
			}
			
			return sortedList;
		}
		
		
		
		/**
		 * 
		 */	
		public boolean hasNextObject() {
			if(nodeQuery==null)
				if(currentNode.level()==0){
					nodeQuery= ((Node)currentNode).referenceLeafQuery((MVRegion)queryRegion, nodeRegion);
					}
				else {
					nodeQuery= recursivQuery();
					
				}
			while(!nodeQuery.hasNext()) { 
				nodeQuery.close();      
				if(currentNode.level()==0) { 
					if((indexPrevious>=((Node)currentNode).predecessors.size()) 
							|| (((MVRegion)queryRegion).beginVersion().compareTo(nodeRegion.beginVersion())>=0)) {
						return false; 
					}
					else{ 
						nodeQuery=queryTemporalPred(); 
					}
				}
				else {
					if(index>=keySortedEntries.size()) return false;
					else {
						nodeQuery=recursivQuery();	
					} 
				}
			}
			return true;
		}
		/**
		 * 
		 */	
		public Object nextObject() {
			return currentEntry=(LeafEntry) nodeQuery.next();
		}
		/**
		 * 
		 */	
		public void removeObject() {
			nodeQuery.remove();
			if(!routed) {
				if(!hasPath()) path= pathToNode(nodeRegion, 0);
				if(!indexEntry.equals(indexEntry(path))) throw new IllegalStateException("Wrong path!");
				treatUnderflow(path);
			} 
		}
		/**
		 * 
		 */
		public void updateObject(Object newData) {
			if(!routed){
				nodeQuery.remove();
				Lifespan lifespan=new Lifespan(currentVersion());
				LeafEntry newEntry= new LeafEntry(lifespan, newData);
				if(!hasPath()) path= pathToNode(nodeRegion, 0);
				((Node)currentNode).grow(newEntry, path);
				treatOverflow(path);
			}
			else nodeQuery.update(newData);
		}
		/**
		 * 
		 */
		public void close() {
			nodeQuery.close();
			if(hasPath()&& !path.isEmpty()) up(path);
			indexEntry.unfix();
			super.close();
		}	
		/**
		 * 
		 * @return
		 */		
		private Cursor queryTemporalPred() {
			IndexEntry predecessor=(IndexEntry)((Node)currentNode).predecessors.get(indexPrevious);
			indexPrevious++;
			if (((Node)currentNode).predecessors.size() == 2){
				IndexEntry predecessor1 = (IndexEntry)((Node)currentNode).predecessors.get(0);
				IndexEntry predecessor2 = (IndexEntry)((Node)currentNode).predecessors.get(1);
				Comparable upperBound = max(((MVSeparator)predecessor1.separator).sepValue(), 
						((MVSeparator)predecessor2.separator).sepValue());
				Comparable minBoundQueryRegion = ((MVRegion)queryRegion).minBound();
				if (upperBound.compareTo(((MVSeparator)predecessor.separator).sepValue()) > 0 
						&& minBoundQueryRegion.compareTo(upperBound)  > 0){
					return new EmptyCursor();
				}
			}
			Comparable referenceKey= max(((MVSeparator)predecessor.separator).sepValue(), 
											((MVRegion)queryRegion).minBound());
			Comparable predecessorMinBound = ((MVSeparator)predecessor.separator).sepValue();
			if(referenceKey.compareTo(nodeRegion.minBound())>=0){
				if (referenceKey.compareTo(nodeRegion.minBound()) == 0 //new code
						&& predecessorMinBound.compareTo(nodeRegion.minBound()) < 0
					){
					return new EmptyCursor();
				}
				if (((MVRegion)queryRegion).maxBound().compareTo(predecessorMinBound) < 0 ){
					return new EmptyCursor();
				}
				routed=true;
				abolishPath();
				MVRegion nodeReg = toMVRegion((MVSeparator)predecessor.separator);
				nodeReg.updateMaxBound(null); 
				return linkReferenceQuery(predecessor, nodeReg, (MVRegion)queryRegion);
			}
			return new EmptyCursor();
		}
		
		/**
		 * new code
		 */ 
		private Cursor recursivQuery() {
				IndexEntry currentEntry=(IndexEntry)keySortedEntries.get(index);
				MVSeparator currentEntrySeparator =((MVSeparator)currentEntry.separator); 
				Comparable maxBound =  (nodeRegion).maxBound;
				if (((MVRegion)queryRegion).maxBound.compareTo(currentEntrySeparator.sepValue) < 0 ){
					index = keySortedEntries.size();
				}
				else{
					MVRegion currentEntrySubRegion = toMVRegion(currentEntrySeparator);
					if (index < keySortedEntries.size()-1){
						IndexEntry nextEntry=(IndexEntry)keySortedEntries.get(index + 1);
						maxBound = nextEntry.separator.sepValue();
					}
					index++; 
					currentEntrySubRegion.updateMaxBound(maxBound);
					if ( currentEntrySubRegion.keyOverlaps((MVRegion)queryRegion) ){
						Stack newPath=(Stack)path.clone(); 
						routed=true; // 
//						System.out.println("chosed + " + currentEntry);
						return linkReferenceQuery(currentEntry,  currentEntrySubRegion, (MVRegion)queryRegion, newPath);
					}
				}
				return new EmptyCursor();				
		}
	} // end of class MVBTreeQueryCursor
	/**
	 * Test Cursor 
	 * 
	 */
	public class PriorityQueryCursor extends Query{
		/**Query Region */
		protected MVRegion queryRegion;
		/**stores visited nodes ids*/
		protected HashSet nodesIDs;
		/**Default key comparator needed for sorting the objects in the nodes*/
		Comparator keyComparator = new Comparator() {
			public int compare(Object o1, Object o2) {
				MVSeparator mvSep1=(MVSeparator)separator(o1);
				MVSeparator mvSep2=(MVSeparator)separator(o2);
				int x= mvSep1.compareTo(mvSep2);
				if(x==0) x=mvSep1.lifespan().compareTo(mvSep2.lifespan());
				return x;
			}
		};	
		/**
		 * 
		 * @param queue
		 * @param targetLevel
		 * @param queryRegion
		 */
		public PriorityQueryCursor(Queue queue, int targetLevel, 
				 MVRegion queryRegion) {
			super(queue, targetLevel);
			this.queryRegion = queryRegion;
			nodesIDs = new HashSet();
			if (!this.queue.isEmpty()) queue.dequeue(); // deleteRootEntry of Live version
			// put 
			initQueue();
		}
		
		/** Checks whether there exists a next result
		 * @return <tt>true</tt> if the Query contains more objects
		 */
		public boolean hasNextObject () {
			for (; !queue.isEmpty() && ((Candidate)queue.peek()).parentLevel!=targetLevel;) {
				Candidate candidate = (Candidate)queue.dequeue();
				Node node = (Node)((IndexEntry)candidate.entry()).get(true);
				if (node.level>=targetLevel)
					Queues.enqueueAll(queue, expand(candidate, node));
			}
			return !queue.isEmpty();
		}
		/**
		 * Initializes queue. Runs range query on BPlusTree storing the VersionRoots, 
		 * and puts the result root entries in the queue.  
		 *
		 */
		protected void initQueue(){
			Version minVersion = queryRegion.beginVersion();
			Version maxVersion = queryRegion.endVersion();
			Iterator rootsIterator = roots.rangeQuery(minVersion, maxVersion);
			while(rootsIterator.hasNext()){
				Root rootEntry = (Root)rootsIterator.next();
				IndexEntry indexEntry = rootEntry.toIndexEntry();//TODO
				queue.enqueue(createCandidate(null, indexEntry, indexEntry.separator(), indexEntry.parentLevel()));
			}
			Version lastRootSplitVersion= ((MVSeparator)((IndexEntry)rootEntry).separator()).insertVersion();
			if((maxVersion==null)||(maxVersion.compareTo(lastRootSplitVersion)>=0)){ 
				queue.enqueue(createCandidate(null, rootEntry, ((IndexEntry)rootEntry).separator(), height()));
			}
		}
		/** Expands Candidates stored in a node.
		 *  
		 * @param parent the parent candidate
		 * @param node the node to expand
		 * @return an iterator containing the new candidates 
		 */
		protected Iterator expand (final Candidate parent, final Node node){
			Iterator entries = null;
			if (node.level > 0 ){
				entries = expandIndexNode(queryRegion, node);
			}
			else entries =  expandLeafNode(queryRegion, node, 
					(MVSeparator)parent.descriptor()) ;
			return new Mapper(
				new AbstractFunction() {
					public Object invoke (Object entry) {
						return createCandidate(parent, entry, descriptor(entry), node.level);
					}
				}
			, entries);
		}
		/**
		 * Expands the index node. 
		 * @param queryRegion
		 * @param node
		 * @return
		 */
		protected Iterator expandIndexNode(final MVRegion queryRegion, final Node node){
			if (node.level == 0) throw new IllegalArgumentException(); 
			List listEntries = new ArrayList();
			Iterator it = node.entries();
			// Filter time overlap entries
			while(it.hasNext()){
				Object entry = it.next();
				MVSeparator separator = (MVSeparator)descriptor(entry);
				MVRegion entryRegion = toMVRegion(separator);
				entryRegion.lifespan.isRightClose = false;
				if (entryRegion.lifespan().overlaps(queryRegion.lifespan()))
					listEntries.add(entry);
			}
			// sort according the key
			Collections.sort(listEntries, keyComparator);
			//Search maxBoundIndex
			if (separator(listEntries.get(0)).sepValue.compareTo(queryRegion.maxBound) > 0 ) return new EmptyCursor();
			int maxBoundIndex = listEntries.size();
			while(  separator(listEntries.get(--maxBoundIndex)).sepValue.compareTo(queryRegion.maxBound) > 0 ){}
			List candidates = listEntries.subList(0, maxBoundIndex+1);
			//Search candidates and tries to define the maximal key value of the underlying 
			//entry candidate 
			List result = new ArrayList();
firstFor:	for (int i = 0; i < candidates.size(); i++ ){
				Object entry = candidates.get(i);
				MVSeparator separator = (MVSeparator)descriptor(entry);
				MVRegion entryRegion = toMVRegion(separator);
				entryRegion.lifespan.isRightClose = false;
				for (int j = i+1; j < candidates.size(); j++ ){
					Object nextEntry = candidates.get(j);
					MVSeparator nextSeparator = (MVSeparator)descriptor(nextEntry);
					MVRegion nextEntryRegion = toMVRegion(nextSeparator);
					nextEntryRegion.lifespan.isRightClose = false;
					if (nextEntryRegion.lifespan().overlaps(entryRegion.lifespan())){
						Comparable regionMaxBound = nextSeparator.sepValue();
						entryRegion.updateMaxBound(regionMaxBound);
						if (entryRegion.overlaps(queryRegion)){
							IndexEntry indexEntry = (IndexEntry)entry;
							Object id = indexEntry.id();
							if (!nodesIDs.contains(id)){
								nodesIDs.add(id);
								result.add(entry);
							}
						}
						continue firstFor;
					}
				}
				IndexEntry indexEntry = (IndexEntry)entry;
				Object id = indexEntry.id();
				if (!nodesIDs.contains(id)){
					nodesIDs.add(id);
					result.add(entry);
				}
			}
			return (result.isEmpty()) ?  new EmptyCursor(): result.iterator();
		}
		/**
		 * Expands LeafNode. Works similar to 
		 * @see MVBTree.Node#referenceLeafQuery(xxl.core.indexStructures.MVBTree.MVRegion, xxl.core.indexStructures.MVBTree.MVRegion)
		 * @param queryRegion
		 * @param node
		 * @param nodeSeparator
		 * @return
		 */
		protected Iterator expandLeafNode(final MVRegion queryRegion, 
				final Node node, MVSeparator nodeSeparator){
			node.sortEntries(true);
			Comparable maxBound = separator(node.getLast()).sepValue();
			final MVRegion nodeRegion = toMVRegion(nodeSeparator);
			nodeRegion.updateMaxBound(maxBound);
			Predicate test=new AbstractPredicate() { //reference Point left
				public boolean invoke(Object entry) {
					MVSeparator entrySeparator = (MVSeparator)separator(entry);
					Lifespan entryLifeSpan = entrySeparator.lifespan;
					Comparable entryKey = entrySeparator.sepValue; 
					if (queryRegion.contains(entryKey) && queryRegion.lifespan.overlaps(entryLifeSpan) ){
						Comparable referenceKey= max(queryRegion.minBound(), entryKey);
						Version referenceTime=(Version) 
						max(queryRegion.beginVersion(), entryLifeSpan.beginVersion());
						return nodeRegion.contains(referenceKey) && 
							nodeRegion.lifespan().contains(referenceTime);
					}
					return false;
				}
			};
			return new Filter(node.iterator(), test);
		}
		// End  Priority Class
	}
	
	/** This Converter is concerned with serializing of nodes to write or read them into 
	 * or from the external storage.
	 */
	public class NodeConverter extends BPlusTree.NodeConverter {
		
		public Object read (DataInput dataInput, Object object) throws IOException {
			int level=dataInput.readInt();
			int number=dataInput.readInt();
			Node node = (Node)createNode(level);
			readPredecessors(dataInput, node);
			for (int i=0; i<number; i++) {
				Object entry;
				if(node.level()==0) entry= readLeafEntry(dataInput);
				else entry=readIndexEntry(dataInput,node.level);
				node.entries.add(entry);
			}
			return node;
		}
		
		public void write (DataOutput dataOutput, Object object) throws IOException {
			Node node = (Node)object;
			IntegerConverter.DEFAULT_INSTANCE.writeInt(dataOutput, node.level);
			IntegerConverter.DEFAULT_INSTANCE.writeInt(dataOutput, node.number());
			writePredecessors(dataOutput, node);
			for(int i=0; i<node.number();i++) {
				Object entry=node.getEntry(i);
				if(node.level==0) writeLeafEntry(dataOutput,(LeafEntry)entry);
					else writeIndexEntry(dataOutput,(IndexEntry)entry);
			}
			
		}
		
		public int headerSize() {
			return 2*IntegerConverter.SIZE + predecessorsSize(); 
		}

		protected void writePredecessors(DataOutput dataOutput, Node node) throws IOException {
			IntegerConverter.DEFAULT_INSTANCE.writeInt(dataOutput, node.predecessors.size());
			for(int i=0; i<node.predecessors.size(); i++) {
				IndexEntry predecessor=(IndexEntry)node.predecessors.get(i);
				writeIndexEntry(dataOutput, predecessor);
			}
		}
		
		protected void readPredecessors(DataInput dataInput, Node node) throws IOException {
			int count=IntegerConverter.DEFAULT_INSTANCE.readInt(dataInput);			
			for(int i=0; i<count; i++) {
				IndexEntry predecessor= readIndexEntry(dataInput, 1); 
				node.predecessors.add(predecessor);
			}
		}
	
		protected void writeMVRegion(DataOutput output, MVRegion region) throws IOException {
			keyConverter.write(output, region.minBound());
			keyConverter.write(output, region.maxBound());
			versionConverter.write(output, region.beginVersion());
			BooleanConverter.DEFAULT_INSTANCE.write(output, new Boolean(region.isDead()));
			if(region.isDead()) versionConverter.write(output, region.endVersion());
		}
		
		protected MVRegion readMVRegion(DataInput input) throws IOException {
			Comparable min=(Comparable)keyConverter.read(input, null);
			Comparable max=(Comparable)keyConverter.read(input, null);
			Version begin=(Version)versionConverter.read(input, null);
			Version end=null;
			boolean isDead=BooleanConverter.DEFAULT_INSTANCE.readBoolean(input);
			if(isDead) end=(Version)versionConverter.read(input, null);
			return createMVRegion(begin, end, min, max);
		}
		
		protected int mvRegionSize() {
			return 2*keyConverter.getMaxObjectSize()+2*versionConverter.getMaxObjectSize()+BooleanConverter.SIZE;
		}
		
		protected int predecessorsSize() {
			return IntegerConverter.SIZE+2*(IntegerConverter.SIZE+mvRegionSize()+container().getIdSize());
		}		

		protected IndexEntry readIndexEntry(DataInput input, int parentLevel) throws IOException {
			IndexEntry indexEntry=(IndexEntry)createIndexEntry(parentLevel);
			Object id= container().objectIdConverter().read(input, null);
			Comparable sepValue=(Comparable)keyConverter.read(input, null);
			Lifespan life= (Lifespan)lifespanConverter.read(input, null);
			MVSeparator mvSeparator= createMVSeparator(life.beginVersion(), life.endVersion(), sepValue);
			indexEntry.initialize(id, mvSeparator);
			return indexEntry;
		}

		protected void writeIndexEntry(DataOutput output, IndexEntry entry) throws IOException {
			container().objectIdConverter().write(output, entry.id());
			keyConverter.write(output, entry.separator.sepValue());
			lifespanConverter.write(output, ((MVSeparator)entry.separator()).lifespan());
		}
	
		@Override
		protected int indexEntrySize() {
			return container().getIdSize()+keyConverter.getMaxObjectSize()+lifespanConverter.getMaxObjectSize();
		}
		
		
		public int maxIndexEntrySize(){
			return  indexEntrySize();
		}
		
		public int maxLeafEntrySize(){
			return leafEntrySize() ;
			
		}

		protected LeafEntry readLeafEntry(DataInput input) throws IOException {
			Lifespan life= (Lifespan)lifespanConverter.read(input, null);
			Object data= dataConverter.read(input, null);
			return new LeafEntry(life, data);
		}
		
		protected void writeLeafEntry(DataOutput output, LeafEntry entry) throws IOException {
			lifespanConverter.write(output, entry.lifespan);
			dataConverter.write(output, entry.data());
		}
		
		@Override
		protected int leafEntrySize() {
			return dataConverter.getMaxObjectSize()+lifespanConverter.getMaxObjectSize();
		}

		/**
         * Computes the maximal size (in bytes) of a record (IndexEntry or data
         * object). It calls the method getIdSize() of the tree container. If
         * the tree container has not been initialized a NullPointerException is
         * thrown.
         * 
         * @return the maximal size of a record (IndexEntry or data object)
         */
        public int getMaxRecordSize() {
            return Math.max(dataConverter.getMaxObjectSize() + lifespanConverter.getMaxObjectSize(), indexEntrySize());
        }	
	}
}
