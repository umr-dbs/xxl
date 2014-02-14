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
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Stack;
import java.util.Map.Entry;

import xxl.core.collections.MapEntry;
import xxl.core.collections.MappedList;
import xxl.core.collections.containers.Container;
import xxl.core.cursors.Cursor;
import xxl.core.cursors.filters.Filter;
import xxl.core.cursors.unions.Merger;
import xxl.core.cursors.unions.Sequentializer;
import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;
import xxl.core.indexStructures.BPlusTree.IndexEntry;
import xxl.core.indexStructures.BPlusTree.KeyRange;
import xxl.core.indexStructures.BPlusTree.Node.SplitInfo;
import xxl.core.indexStructures.MVBTree.LeafEntry;
import xxl.core.indexStructures.MVBTree.Lifespan;
import xxl.core.indexStructures.MVBTree.MVRegion;
import xxl.core.indexStructures.MVBTree.MVSeparator;
import xxl.core.indexStructures.MVBTree.Node;
import xxl.core.indexStructures.MVBTree.Root;
import xxl.core.indexStructures.MVBTree.Version;
import xxl.core.io.converters.BooleanConverter;
import xxl.core.io.converters.IntegerConverter;
import xxl.core.io.converters.MeasuredConverter;
import xxl.core.predicates.AbstractPredicate;
import xxl.core.predicates.Predicate;

/**
 * MVBTree wiht new node layout
 * 
 */
public class MVBT extends MVBTree {
	
	/**
	 * Comparator for live entries
	 */
	protected Comparator<Object> liveIndexEntryComparator = new Comparator<Object>() {
		
		@SuppressWarnings("unchecked")
		@Override
		public int compare(Object arg0, Object arg1) {
			MVSeparator mv1 =(MVSeparator)((IndexEntry)arg0).separator; 
			MVSeparator mv2 = (MVSeparator)((IndexEntry)arg1).separator; 
			return mv1.sepValue().compareTo(mv2.sepValue());
		}
	};
	
	/**Creates a new <tt>MVBTree</tt>.
	 * KeyDomainMinValue set to default value @see {@link MVBT#DEAFUALT_KEYDOMAIN_MINVALUE}
	 * @param blockSize the block size of the underlaying <tt>Container</tt>.
	 * @param minCapRatio the minimal capacity ratio of the tree's nodes.
	 * @param e the epsilon of the <tt>strong version condition</tt>.
	 */	
	public MVBT(int blockSize, float minCapRatio, float e) {
		super(blockSize, minCapRatio,e);
	}
	
	/**Creates a new <tt>MVBTree</tt>.  The minimal capacity ratio of the tree's nodes is set ot 50%.
	 * @param blockSize the block size of the underlaying <tt>Container</tt>.
	 * @param e the epsilon of the <tt>strong version condition</tt>.
	 */	
	public MVBT(int blockSize, float e) {
		this(blockSize, 0.5f, e);
	}
	/**Creates a new <tt>MVBTree</tt>.  The minimal capacity ratio of the tree's nodes is set ot 50%.
	 * @param blockSize the block size of the underlaying <tt>Container</tt>.
	 * @param e the epsilon of the <tt>strong version condition</tt>.
	 */	
	public MVBT(int blockSize, float e, Comparable keyDomainMinValue) {
		this(blockSize, 0.5f, e, keyDomainMinValue);
	}
	
	public MVBT(int blockSize, float minCapRatio, float e, Comparable keyDomainMinValue ) {
		super(blockSize, minCapRatio, e, keyDomainMinValue);
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
	public MVBT initialize(IndexEntry rootEntry, 
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
		super.initialize(rootEntry, 
				liveRootDescriptor, 
				rootsRootEntry,	
				rootsRootDescriptor, 
				getKey,
				rootsContainer, 
				treeContainer, 
				versionConverter, 
				keyConverter, 
				dataConverter,
				createMVSeparator,
				createMVRegion);
			return this;					
	}
	
	
	

	
	
	/** Creates a new <tt>Node</tt>.
	 * @return the new created <tt>Node</tt>.
	 */		
	public Tree.Node createNode(int level) {
		return new Node(level);
	}
	

	
	/** Creates a new <tt>NodeConverter</tt>.
	 * @return the new created <tt>NodeConverter</tt>.
	 */
	protected BPlusTree.NodeConverter createNodeConverter() {
		return new NodeConverter();
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
	@SuppressWarnings({ "unchecked", "rawtypes" })
	protected Stack pathToNodeLiveRemove(Object key, Version version, int level) {
		IndexEntry indexEntry = determineRootEntry(version);
		if (indexEntry == null) throw new IllegalStateException();

		Stack path = new Stack();
		down(path, indexEntry);
		Node node = (Node)node(path);
		MVSeparator sep = createMVSeparator(	version,version, (Comparable)key);
		while (node.level() > level) {
			indexEntry = null;
			indexEntry = (IndexEntry) node.chooseSubtree(sep); 
			if (indexEntry == null)
				throw new IllegalStateException();

			down(path, indexEntry);
			node = (Node)node(path);
		}

		return path;
	}

	/*
	 * (non-Javadoc)
	 * @see xxl.core.indexStructures.MVBTree#remove(xxl.core.indexStructures.MVBTree.Version, java.lang.Object)
	 */
	@SuppressWarnings({ "unchecked", "rawtypes", "deprecation" })
	public Object remove(Version removeVersion, Object data) {
		setCurrentVersion(removeVersion);
		Object key = getKey.invoke(data);
		Stack path = pathToNodeLiveRemove(key, currentVersion(), 0);
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
 
	/*
	 * (non-Javadoc)
	 * @see xxl.core.indexStructures.MVBTree#update(xxl.core.indexStructures.MVBTree.Version, java.lang.Object, java.lang.Object)
	 */
	public void update (Version updateVersion, Object oldData, Object newData) {
		setCurrentVersion(updateVersion);
		Object key = getKey.invoke(newData);
		Stack path = pathToNodeLiveRemove(key, currentVersion(), 0);
		Iterator it = ((MVBTree.Node)node(path)).iterator();
		LeafEntry removed = null;
		while (it.hasNext()) {
			LeafEntry obj = (LeafEntry)it.next();
			if (obj.data().equals(oldData) && obj.getLifespan().isAlive()) {
				it.remove();
				removed = obj;
				Lifespan lifespan=new Lifespan(currentVersion());
				LeafEntry newEntry= new LeafEntry(lifespan, newData);
				node(path).grow(newEntry, path); 
				break;
			}
		}
		treatUnderflow(path);
	}
	
	/**
	 * The experimantal layout: for index nodes we manage two lists 
	 * first list stores live entries 
	 * second list stores deleted entries
	 * 
	 * this allows to run chooseSubTree method in O(log B) instead of O(B)
	 * and we have only 4 bytes space overhead
	 * @author Daniar
	 */
	public class Node extends MVBTree.Node {
		/**
		 * experimetal part 
		 */
		public List liveEntries; 
		
		/***********************************************************
		 * 
		 * new code live entries 
		 * 
		***********************************************************/
		
		@Override
		public int number() {
			return super.number() + liveEntries.size();
		}
		

		
		/**
		 * 
		 * @param entry
		 */
		public void insertIntoLiveList(IndexEntry entry){
			int index = searchInLiveList(entry); 
			if(index >= 0 ){
				throw new RuntimeException("Insertion failed entry with same key in live list found!"); 
			}
			index=-index-1;
			liveEntries.add(index, entry);
		}
		
		/**
		 * 
		 * @param separator
		 * @return
		 */
		public int searchInLiveList(MVSeparator separator){
			IndexEntry idx = new IndexEntry(level); 
			idx.initialize(separator); 
			return searchInLiveList(idx);
		}
		/**
		 * 
		 * @param idx
		 * @return
		 */
		public int searchInLiveList(IndexEntry idx){
			int index = Collections.binarySearch(liveEntries, idx, liveIndexEntryComparator); 
			return index;
		}
		/**
		 * 
		 * @param entry
		 */
		public void removeFromLiveList(IndexEntry entry){
			int index = searchInLiveList(entry); 
			if(index<0){
				throw new RuntimeException("Entry not found ");
			}
			liveEntries.remove(index);
		}
		/**
		 * 
		 * @param entry
		 */
		@SuppressWarnings("rawtypes")
		public void removeFromLiveListAddToOldList(IndexEntry entry){
			int index = searchInLiveList(entry); 
			if(index<0){
				throw new RuntimeException("Entry not found ");
			}
			liveEntries.remove(index);
			this.grow(entry, new Stack());
		}
		/**
		 * 
		 * @param separator
		 * @return
		 */
		public IndexEntry chooseSubTreeFromLiveList(MVSeparator separator){
			int index = searchInLiveList(separator); 
			if(index >= 0){
				return (IndexEntry) liveEntries.get(index);
			}
			index = -index-1; 
			if(index == 0){
				return (IndexEntry) liveEntries.get(index); 
			}
			index -=1;
			return (IndexEntry) liveEntries.get(index); 
		}
		/***********************************************************
		 * 
		 * new code live entries 
		 * 
		***********************************************************/
		
	
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
			liveEntries = new ArrayList<>(); 
		}
		
		/*
		 * (non-Javadoc)
		 * @see xxl.core.indexStructures.BPlusTree.Node#initialize(java.lang.Object)
		 */
        protected Tree.Node.SplitInfo initialize(Object entry) {
            Stack path = new Stack();
            grow(entry, path);
            if(level > 0){
            	return new SplitInfo(path).initialize((Separator)separator(this.liveEntries.get(this.liveEntries.size()-1)).clone()); 
            }
            return  new SplitInfo(path).initialize((Separator)separator(this.getLast()).clone());
        }
		
        
        /*
         * (non-Javadoc)
         * @see xxl.core.indexStructures.MVBTree.Node#redressOverflow(java.util.Stack, java.util.List, boolean)
         */
		@SuppressWarnings("rawtypes")
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
					Node versionSplitNode=(Node)MVBT.this.createNode(level());
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
							Entry newRootTuple = MVBT.this.grow(indexEntryOfVersionSplitNode);
							Node newRootNode= (Node)newRootTuple.getValue();
							newRootNode.grow(indexEntryOfKeySplitNode, new Stack()); 
							MVSeparator rootSeparator= (MVSeparator)indexEntryOfVersionSplitNode.separator;
							Object newRootNodeId= MVBT.this.container().insert(newRootNode);
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
		/*
		 * (non-Javadoc)
		 * @see xxl.core.indexStructures.MVBTree.Node#chooseSubtree(xxl.core.indexStructures.Descriptor, java.util.Stack)
		 */
		@SuppressWarnings("rawtypes")
		protected Tree.IndexEntry chooseSubtree (Descriptor descriptor, Stack path) {
			final MVSeparator mvSeparator= (MVSeparator) descriptor;
			return chooseSubTreeFromLiveList(mvSeparator);
		}
		/*
		 * (non-Javadoc)
		 * @see xxl.core.indexStructures.MVBTree.Node#chooseSubtree(xxl.core.indexStructures.MVBTree.MVSeparator)
		 */
		public Tree.IndexEntry chooseSubtree(MVSeparator mvSeparator){
			return chooseSubTreeFromLiveList(mvSeparator);
		}
		
		/*
		 * (non-Javadoc)
		 * @see xxl.core.indexStructures.MVBTree.Node#referenceLeafQuery(xxl.core.indexStructures.MVBTree.MVRegion, xxl.core.indexStructures.MVBTree.MVRegion)
		 */
		@SuppressWarnings("rawtypes")
		protected Cursor referenceLeafQuery(final MVRegion queryRegion, final MVRegion nodeRegion) {
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
			return new Filter(new Sequentializer(this.iterator(), liveEntries.iterator()), test);
		}

		/*
		 * (non-Javadoc)
		 * @see xxl.core.indexStructures.MVBTree.Node#coordinatesOf(xxl.core.indexStructures.MVBTree.Version)
		 */
		protected int[] coordinatesOf(Version version) {
			int from=search(version);
			int to=from;
			if(from>=0) {
				to=from+1;
				while(to<entries.size()){
					if(((MVSeparator)separator(getEntry(to))).insertVersion().compareTo(version)>0) break;
					to++;
				}
			}
			return new int[]{from,to};
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
            if (entries.isEmpty() && liveEntries.isEmpty()) return null;
            if (index < entries.size())
            	return entries.get(index);
            int nIndex = index -  entries.size();
            return liveEntries.get(nIndex); 
        }
		
		
		/*
		 * (non-Javadoc)
		 * @see xxl.core.indexStructures.MVBTree.Node#query(xxl.core.indexStructures.MVBTree.Lifespan)
		 */
		@SuppressWarnings({ "rawtypes", "unchecked" })
		public Iterator query(final Lifespan lifespan) {
			return new Filter(	new Sequentializer(iterator(), liveEntries.iterator()),
								new AbstractPredicate() {
									public boolean invoke(Object entry) {									 	
										return ((MVSeparator)separator(entry)).lifespan().overlaps(lifespan);
									}
								});
		}
		
		/*
		 * (non-Javadoc)
		 * @see xxl.core.indexStructures.MVBTree.Node#iterator()
		 */
		@SuppressWarnings({ "rawtypes", "unchecked" })
		public Iterator iterator() {
			if(level > 0 )
				return new Sequentializer(entries.iterator(), liveEntries.iterator());
			return new Iterator() {
				private int index=0;
				private boolean removeable=false;
				public boolean hasNext() {
					
					return index<MVBT.Node.this.number();
				}
				
				public Object next() {
					if(!hasNext()) throw new NoSuchElementException();
					removeable=true;
					return MVBT.Node.this.getEntry(index++);
				}
				
				public void remove() {
					if(removeable) {
						removeable=false;
						MVBT.Node.this.remove(index-1);
					}
					else throw new NoSuchElementException();
				}
			};
		}
		
		/*
		 * (non-Javadoc)
		 * @see xxl.core.indexStructures.MVBTree.Node#getCurrentEntries()
		 */
		@SuppressWarnings({ "unchecked", "rawtypes", "serial" })
		public Iterator getCurrentEntries() {
			return new Filter( new Sequentializer(iterator(), liveEntries.iterator()),
								new AbstractPredicate() {
									public boolean invoke(Object entry) {
										return ((MVSeparator)separator(entry)).isAlive();
									}
								});
		}
		
		

	

	
		
		/*
		 * (non-Javadoc)
		 * @see xxl.core.indexStructures.MVBTree.Node#grow(java.lang.Object, java.util.Stack)
		 */
		@SuppressWarnings({ "rawtypes", "unchecked" })
		protected void grow(Object entry, Stack path) {
			// experimental code
			if(level  > 0 && ((MVSeparator)((IndexEntry)entry).separator()).isAlive()){
				insertIntoLiveList((IndexEntry)entry); 	
			}else{
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
			}
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
		/*
		 * (non-Javadoc)
		 * @see xxl.core.indexStructures.MVBTree.Node#remove(int)
		 */
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
		 * @return
		 */
		@SuppressWarnings("rawtypes")
		public List getLiveEntries(){
			return liveEntries;
		}
		
		/*
		 * (non-Javadoc)
		 * @see xxl.core.indexStructures.MVBTree.Node#versionSplit(xxl.core.indexStructures.MVBTree.Version, java.util.Stack, xxl.core.indexStructures.MVBTree.Node.SplitInfo)
		 */
		@SuppressWarnings({ "rawtypes", "unchecked" })
		protected SplitInfo versionSplit(Version splitVersion, Stack path, SplitInfo splitInfo) {
			Node node = (Node)node(path);
			IndexEntry indexEntry = (IndexEntry)indexEntry(path);
			splitInfo.initVersionSplit(splitVersion, (MVSeparator)indexEntry.separator());
			if (splitVersion.compareTo(((MVSeparator)indexEntry.separator).insertVersion()) < 0){
				throw new  IllegalArgumentException("Split of:"+ indexEntry+" at Version:"+splitVersion);
			}
			((MVSeparator)indexEntry.separator).delete(splitVersion); // delete
			// new code
			MapEntry pathEntry = (MapEntry)path.pop();
			Node parentNode = (Node)node(path);
			if(parentNode != null){
				parentNode.removeFromLiveList(indexEntry); 
				parentNode.grow(indexEntry, new Stack()); 
			}
			path.push(pathEntry);
			// new code
			Iterator currentEntries = node.query(splitVersion);
			
			if(level > 0 ){
				for(Object entry : node.liveEntries){
					liveEntries.add((IndexEntry)copyEntry(entry));
				}
			}else{
				while (currentEntries.hasNext()) {	
					Object entry = currentEntries.next();
					Object cpy = copyEntry(entry);
					entries.add(cpy);
				}
				node.removeEntriesFromVersion(splitVersion);
			}
			if (level()==0) {
				this.predecessors.clear();
				IndexEntry predEntry = (IndexEntry)createIndexEntry(indexEntry.parentLevel);
				MVSeparator predSep = (MVSeparator)separator(indexEntry).clone();
				predSep.delete(splitVersion);
				predEntry.initialize(indexEntry.id(), predSep); 
				this.predecessors.add(predEntry);
			}
			
			purgeQueue.enqueue(new BlockDelInfo(indexEntry.id(), splitVersion));
			return splitInfo;
		}
		/*
		 * (non-Javadoc)
		 * @see xxl.core.indexStructures.MVBTree.Node#keySplit(java.util.Stack, xxl.core.indexStructures.MVBTree.Node.SplitInfo)
		 */
		@SuppressWarnings({ "rawtypes", "unchecked" })
		protected SplitInfo keySplit(Stack path, SplitInfo splitInfo) {
			this.sortEntries(true);
			List cpyEntries = null;
			Node newNode = null;
			if(level > 0 ){
				cpyEntries = this.liveEntries.subList((this.liveEntries.size()+1)/2, this.liveEntries.size());
				newNode = (Node)MVBT.this.createNode(level());
				newNode.liveEntries.addAll(cpyEntries);
				cpyEntries.clear();
			}else{
				cpyEntries = this.entries.subList((number()+1)/2, number());
				newNode = (Node)MVBT.this.createNode(level());
				newNode.entries.addAll(cpyEntries);
				cpyEntries.clear();
				this.sortEntries();
			}
			MVSeparator newSeparator = null; 
			if (level > 0){
				newSeparator = (MVSeparator)separator(newNode.liveEntries.get(0)).clone();
				newSeparator.union(separator((newNode.liveEntries.get(newNode.liveEntries.size()-1))));
				newSeparator.setInsertVersion(splitInfo.splitVersion());
				splitInfo.initKeySplit(newSeparator, newNode);
			}else{
				newSeparator = (MVSeparator)separator(newNode.getFirst()).clone();
				newSeparator.union(separator(newNode.getLast()));
				newSeparator.setInsertVersion(splitInfo.splitVersion());
				newNode.sortEntries();
				splitInfo.initKeySplit(newSeparator, newNode);
			}
			if (level()==0)
				newNode.predecessors.add(this.predecessors.get(0));
			return splitInfo;
		}

		protected SplitInfo strongMerge(Stack path, SplitInfo splitInfo) {
			if (path.size() <= 1)
				throw new IllegalStateException("There is no parent on the stack.");
			Node node = (Node) splitInfo.versionSplitNode(); // created node
			IndexEntry mergeSibling = determineStrongMergeSibling(path, splitInfo);
			//test new code test
			splitInfo.setSiblingMergeSeparator((MVSeparator)mergeSibling.separator);
			MapEntry pathEntry = (MapEntry)path.pop();
			down(path, mergeSibling);
			Node tempNode = (Node)MVBT.this.createNode(level());
			tempNode.versionSplit(splitInfo.splitVersion(), path);
			if(level  > 0 ){
				//  merge in linear time
				List<IndexEntry>  newLiveEntries = new ArrayList<>(); 
				Iterator<IndexEntry> mergedEntries = new Merger<IndexEntry>(liveIndexEntryComparator, node.liveEntries.iterator(), tempNode.liveEntries.iterator()); 
				while(mergedEntries.hasNext()){
					IndexEntry idxEntry = mergedEntries.next();
					newLiveEntries.add(idxEntry);
				}
				node.liveEntries = newLiveEntries; 
			}else{
				node.entries.addAll(tempNode.entries);
				node.sortEntries();
			}
			if (node.level() == 0)
				node.predecessors.add(tempNode.predecessors.get(0));
			update(path);
			up(path);
			path.push(pathEntry);
			splitInfo.setMergePerformed();
			assert node.countCurrentEntries() >= (level() == 0 ? getLeafNodeD() : getIndexNodeD());
			return splitInfo; 
		}
				
	
		
		
		
		
		@Override
		public String toString() {
			return "Node [predecessors=" + predecessors + ", liveEntries="
					+ liveEntries + ", entries=" + entries + ", level="
					+ level + "]";
		}
		
	}
	
	
	
	
	/** This Converter is concerned with serializing of nodes to write or read them into 
	 * or from the external storage.
	 */
	@SuppressWarnings("serial")
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
			int liveNumber = dataInput.readInt(); 
			for( int i = 0; i < liveNumber; i++){
				IndexEntry entry=readIndexEntry(dataInput,node.level);
				node.liveEntries.add(entry);
			}
			return node;
		}
		
		public void write (DataOutput dataOutput, Object object) throws IOException {
			Node node = (Node)object;
			IntegerConverter.DEFAULT_INSTANCE.writeInt(dataOutput, node.level);
			IntegerConverter.DEFAULT_INSTANCE.writeInt(dataOutput, node.entries.size());
			writePredecessors(dataOutput, node);
			for(int i=0; i<node.entries.size() ;i++) {
				Object entry=node.getEntry(i);
				if(node.level==0) writeLeafEntry(dataOutput,(LeafEntry)entry);
					else writeIndexEntry(dataOutput,(IndexEntry)entry);
			}
			IntegerConverter.DEFAULT_INSTANCE.writeInt(dataOutput, node.liveEntries.size());
			for(int i=0; i<node.liveEntries.size() ;i++) {
				Object entry = node.liveEntries.get(i);
				if(node.level==0) writeLeafEntry(dataOutput,(LeafEntry)entry);
					else writeIndexEntry(dataOutput,(IndexEntry)entry);
			}
		}
		
		public int headerSize() {
			return 2*IntegerConverter.SIZE + predecessorsSize()+ IntegerConverter.SIZE; 
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
			// Daniar: new code IntegerConverter.SIZE
			return IntegerConverter.SIZE+2*(mvRegionSize()+container().getIdSize());
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
