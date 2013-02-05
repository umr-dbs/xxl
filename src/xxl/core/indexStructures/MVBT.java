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
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Stack;

import xxl.core.collections.MapEntry;
import xxl.core.collections.MappedList;
import xxl.core.collections.containers.Container;
import xxl.core.cursors.Cursor;
import xxl.core.cursors.filters.Filter;
import xxl.core.cursors.unions.Sequentializer;
import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;
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
		super(blockSize, minCapRatio,keyDomainMinValue);
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
	

	

 
 
	
	/**This class represents the <tt>Nodes</tt> of the <tt>MVBTree</tt>.
	 * @author Husain Aljazzar
	 */
	public class Node extends MVBTree.Node {
		/**
		 * experimetal part 
		 */
		public List<IndexEntry> liveEntries; 
		
		/***********************************************************
		 * 
		 * new code live entries 
		 * 
		***********************************************************/
		
		@Override
		public int number() {
			return super.number() + liveEntries.size();
		}
		
		// compares values according to their keys
		protected Comparator<IndexEntry> liveIndexEntryComparator = new Comparator<BPlusTree.IndexEntry>() {
			
			@Override
			public int compare(IndexEntry arg0, IndexEntry arg1) {
				MVSeparator mv1 =(MVSeparator)arg0.separator; 
				MVSeparator mv2 = (MVSeparator)arg1.separator; 
				return mv1.sepValue().compareTo(mv2.sepValue());
			}
		};
		
		
		public void insertIntoLiveList(IndexEntry entry){
			int index = searchInLiveList(entry); 
			if(index >= 0 ){
				throw new RuntimeException("Insertion failed entry with same key in live list found!"); 
			}
			index=-index-1;
			liveEntries.add(index, entry);
		}
		
		
		public int searchInLiveList(MVSeparator separator){
			IndexEntry idx = new IndexEntry(level); 
			idx.initialize(separator); 
			return searchInLiveList(idx);
		}
		
		public int searchInLiveList(IndexEntry idx){
			int index = Collections.binarySearch(liveEntries, idx, liveIndexEntryComparator); 
			return index;
		}
		
		public void removeFromLiveList(IndexEntry entry){
			int index = searchInLiveList(entry); 
			if(index<0){
				throw new RuntimeException("Entry not found ");
			}
			liveEntries.remove(index);
		}
		
		public void removeFromLiveListAddToOldList(IndexEntry entry){
			int index = searchInLiveList(entry); 
			if(index<0){
				throw new RuntimeException("Entry not found ");
			}
			liveEntries.remove(index);
			this.grow(entry, new Stack());
		}
		
		public IndexEntry chooseSubTreeFromLiveList(MVSeparator separator){
			int index = searchInLiveList(separator); 
			if(index >= 0){
				return liveEntries.get(index);
			}
			index = -index-1; 
			if(index == 0){
				return liveEntries.get(index); 
			}
			index -=1;
			return liveEntries.get(index); 
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
		
		
		
		/** Chooses the subtree which is followed during an insertion.
		 * @param descriptor the <tt>MVSeparator</tt> of data object
		 * @param path the path from the root to the current node 
		 * @return the index entry refering to the root of the chosen subtree
		 */
		protected Tree.IndexEntry chooseSubtree (Descriptor descriptor, Stack path) {
			final MVSeparator mvSeparator= (MVSeparator) descriptor;
			Iterator iterator = getCurrentEntries();
			return chooseSubTreeFromLiveList(mvSeparator);
		}
		
		public Tree.IndexEntry chooseSubtree(MVSeparator mvSeparator){
			Iterator iterator = getCurrentEntries();
			return chooseSubTreeFromLiveList(mvSeparator);
		}
		
		
	
		
		/**Searches the minimal key in the <tt>Node</tt>.
		 * @return the position of the entry with the smallest key.
		 */
		protected int searchMinimumKey() {
			return MVBT.this.searchMinimumKey(this.entries);
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
			return new Filter(new Sequentializer<>(this.iterator(), liveEntries.iterator()), test);
		}

	
		
		/** Searches all entries of this <tt>Node</tt> whose <tt>Lifespans</tt> overlap the given <tt>Lifespan</tt>.
		 * @param lifespan the <tt>Lifespan</tt> of the query.
		 * @return a <tt>Iterator</tt> pointing to all responses (i.e. all entries of this <tt>Node</tt> whose 
		 * <tt>Lifespans</tt> overlap the given <tt>Lifespan</tt>).
		 */
		public Iterator query(final Lifespan lifespan) {
			return new Filter(	new Sequentializer<>(iterator(), liveEntries.iterator()),
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
			if(level > 0 )
				return new Sequentializer<>(entries.iterator(), liveEntries.iterator());
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
		
		/** Gives an <tt>Iterator</tt> pointing to all entries of this <tt>Node</tt> which have not been deleted yet.
		 * @return an <tt>Iterator</tt> pointing to all entries of this <tt>Node</tt> which have not been deleted yet.
		 */
		public Iterator getCurrentEntries() {
			return new Filter( new Sequentializer<>(iterator(), liveEntries.iterator()),
								new AbstractPredicate() {
									public boolean invoke(Object entry) {
										return ((MVSeparator)separator(entry)).isAlive();
									}
								});
		}
		
		

	

	
		
		/** Inserts an entry into this <tt>Node</tt>. If level>0 <tt>data</tt> must be an 
		 * <tt>IndexEntry</tt>.
		 * @param data the entry which has to be inserted into the node
		 * @param path the path from the root to the current node
		 */
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
		
				
	
		
		public List getLiveEntries(){
			return liveEntries;
		}
		
		
		
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
			((MVSeparator)indexEntry.separator).delete(splitVersion); // delete
			// new code
			MapEntry pathEntry = (MapEntry)path.pop();
			Node parentNode = (Node)node(path);
			parentNode.removeFromLiveList(indexEntry); 
			parentNode.grow(indexEntry, new Stack()); 
			path.push(pathEntry);
			// new code
			Iterator currentEntries = node.query(splitVersion);
			
			if(level > 0 ){
				for(IndexEntry entry : node.liveEntries){
					liveEntries.add((IndexEntry)copyEntry(entry));
				}
			}else{
				while (currentEntries.hasNext()) {	
					Object entry = currentEntries.next();
					Object cpy = copyEntry(entry);
					entries.add(cpy);
				}
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
			Node node = (Node) splitInfo.versionSplitNode(); // created node
			IndexEntry mergeSibling = determineStrongMergeSibling(path, splitInfo);
			//test new code test
			splitInfo.setSiblingMergeSeparator((MVSeparator)mergeSibling.separator);
			MapEntry pathEntry = (MapEntry)path.pop();
			down(path, mergeSibling);
			Node tempNode = (Node)MVBT.this.createNode(level());
			tempNode.versionSplit(splitInfo.splitVersion(), path);
			if(level  > 0 ){
				node.liveEntries.addAll(tempNode.liveEntries);
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
