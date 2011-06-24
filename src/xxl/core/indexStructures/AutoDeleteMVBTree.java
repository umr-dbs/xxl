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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Stack;

import xxl.core.collections.queues.DynamicHeap;
import xxl.core.cursors.Cursors;
import xxl.core.cursors.filters.Filter;
import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;
import xxl.core.predicates.AbstractPredicate;

/**
 * This class extends the MVBTree with auto-delete functionality, i.e.
 * objects have to be inserted with a deletion version and will be
 * automatically deleted at that version. 
 */
public class AutoDeleteMVBTree extends MVBTree {
	/**
	 * This class extends the <tt>DynamicHeap</tt> by providing a
	 * mechanism to notify a listener whenever an object in the heap
	 * changes its position.
	 * 
	 * <p>This can be used to inform each object about its current
	 * position in the heap. Whenever an object's state changes in a way
	 * that affects the ordering in the heap, that object can then call
	 * the heap's <tt>update</tt> method, providing it's current position
	 * as the parameter, to restore the heap structure.</p>
	 *  
	 * @param <E> the type of the elements of this heap.
	 */
	protected static class NotifyHeap<E> extends DynamicHeap<E> {
		protected Function onUpdate;

		public NotifyHeap(Comparator<? super E> comparator, Function onUpdate) {
			super(comparator);
			this.onUpdate = onUpdate;
		}
		
		@Override
		protected void bubbleUp(E object, int i) {
			// prerequisite: 0 <= i <= last && array[0] <= object
			while (comparator.compare(object, (E)array[i/2]) < 0) {
				onUpdate.invoke(array[i/2], i);				
				array[i] = array[i /= 2];
				/*
				 * Prevent infinite loop when array[0] > object
				 * (this can happen if bubbleUp is called from update(int)
				 */
				if (i == 0) break;
			}
			onUpdate.invoke(object, i);
			array[i] = object;
		}

		@Override
		protected int sinkIn(int i) {
			// prerequisite: 1 <= i <= last
			onUpdate.invoke(array[i], i/2);				
			array[i/2] = array[i];
			while ((i *= 2) < size-1) {
				onUpdate.invoke(array[comparator.compare((E)array[i], (E)array[i+1]) < 0 ? i : i+1], i/2);				
				array[(comparator.compare((E)array[i], (E)array[i+1]) < 0 ? i : i++)/2] = array[i];
			}
			return i/2;
		}

		@Override
		public void enqueueObject(E object) {
			onUpdate.invoke(object, 0);
			super.enqueueObject(object);
		}

		@Override
		public E dequeueObject() {
			version++;
			E minimum = (E)array[0];
			if (size > 1) {
				int index = sinkIn(1);
				if (index < size-1)
					bubbleUp((E)array[size-1], index);
			}

			array = resizer.resize(array, size());

			onUpdate.invoke(minimum, new Integer(-1));
			return minimum;
		}

		@Override
		public E replace(E object) throws NoSuchElementException {
			throw new UnsupportedOperationException();
		}
		
		@Override
		protected void update(int index) {
			computedNext = false;
			bubbleUp((E)array[index], index);
			// sink without removal!
			int i=index;
			if (i==0) {
				if (comparator.compare((E)array[0], (E)array[1]) > 0 ) {
					onUpdate.invoke(array[0], 1);
					onUpdate.invoke(array[1], 0);
					Object tmp=array[0];
					array[0] = array[1];
					array[1] = tmp;
					i=1;
				}
			}
			if (i!=0) {
				while (2*i < size) {
					int smallerIndex=2*i;
					if (smallerIndex+1<size)
						if (comparator.compare((E)array[smallerIndex], (E)array[smallerIndex+1]) > 0 )
							smallerIndex++;
					if (comparator.compare((E)array[smallerIndex], (E)array[i]) < 0 ) {
						onUpdate.invoke(array[i], smallerIndex);
						onUpdate.invoke(array[smallerIndex], i);
						Object tmp=array[i];
						array[i] = array[smallerIndex];
						array[smallerIndex] = tmp;
						i=smallerIndex;
					}
					else
						break;
				}
			}
		}

		/**
		 * Removes the element at the given position in the heap and returns it.
		 * 
		 * @param i object position in the underlying array
		 * @return the removed element
		 */
		public E remove(int index) {
			if (index < 0 || index >= size)
				throw new IllegalArgumentException();
			E object = (E)array[index];
			if (size>1) {
				// array[size-1]>=array[0]
				array[index]=array[size-1];
				size--;
				update(index);
			}
			else
				size=0;
			version++;

			onUpdate.invoke(object, new Integer(-1));
			return object;
		}

	}

	/**
	 * Contains information about one leaf node of the MVBTree.
	 */
	protected class LeafInfo {
		protected Object id;
		protected Object referenceKey;
		protected Version underflowVersion;
		protected int heapPos;

		/**
		 * Creates a new <tt>LeafInfo</tt> object.
		 * 
		 * @param key the leaf's reference key
		 * @param underflowVersion the leaf's underflow version
		 */
		public LeafInfo(Object key, Version underflowVersion) {
			if (key == null || underflowVersion == null) throw new IllegalArgumentException();
			this.referenceKey = key;
			this.underflowVersion = underflowVersion;
		}

		/**
		 * Returns the leaf's ID in the tree's container.
		 * 
		 * @return the leaf's ID in the tree's container
		 */
		public Object getId() {
			if (id == null) throw new IllegalStateException();
			return id;
		}

		/**
		 * Sets the leaf's ID in the tree's container.
		 * 
		 * @param id the leaf's ID in the tree's container
		 */
		public void setId(Object id) {
			if (id == null) throw new IllegalArgumentException();
			if (this.id != null) throw new IllegalStateException();
			this.id = id;
		}

		/**
		 * Returns the leaf's reference key.
		 * 
		 * @return the leaf's reference key
		 */
		public Object getReferenceKey() {
			return referenceKey;
		}

		/**
		 * Sets the leaf's reference key.
		 * 
		 * @param key the leaf's reference key 
		 */
		public void setReferenceKey(Object key) {
			if (key == null) throw new IllegalArgumentException();
			this.referenceKey = key;
		}

		/**
		 * Returns the leaf's underflow version.
		 * 
		 * @return the leaf's underflow version
		 */
		public Version getUnderflowVersion() {
			return underflowVersion;
		}

		/**
		 * Sets the leaf's underflow version. The new underflow version must
		 * be greater than or equal to the old one.
		 * 
		 * @param underflowVersion the leaf's underflow version
		 */
		public void setUnderflowVersion(Version underflowVersion) {
			if (underflowVersion == null) throw new IllegalArgumentException();
			assert this.underflowVersion.compareTo(underflowVersion) <= 0;
			this.underflowVersion = underflowVersion;
			leafInfoHeap.update(heapPos);
		}

		
		/**
		 * Returns this LeafInfo object's current position in the heap.
		 * 
		 * @return this LeafInfo object's current position in the heap
		 */
		public int getHeapPos() {
			return heapPos;
		}

		/**
		 * Sets this LeafInfo object's current position in the heap.
		 * 
		 * @param heapPos this LeafInfo object's current position in the heap
		 */
		public void setHeapPos(int heapPos) {
			this.heapPos = heapPos;
		}
		
		@Override
		public String toString() {
			StringBuffer sb = new StringBuffer();
			sb.append("(id=");
			sb.append(id);
			sb.append(",key=");
			sb.append(referenceKey);
			sb.append(",underflow=");
			sb.append(underflowVersion);
			sb.append(")");
			return sb.toString();
		}
	}

	protected Map<Object,LeafInfo> leafInfoMap = new HashMap<Object,LeafInfo>();

	protected NotifyHeap<LeafInfo> leafInfoHeap =
		new NotifyHeap<LeafInfo>(
				new Comparator<LeafInfo>() {
					public int compare(LeafInfo li1, LeafInfo li2) {
						return li1.getUnderflowVersion().compareTo(li2.getUnderflowVersion());
					}
				},
				new AbstractFunction() {
					public Object invoke(Object o1, Object o2) {
						LeafInfo leafInfo = (LeafInfo)o1;
						int pos = ((Integer)o2).intValue();
						leafInfo.setHeapPos(pos);
						return null;
					}
				});
	
	/**
	 * Creates a new <tt>AutoDeleteMVBTree</tt>.
	 * 
	 * @param blockSize the block size of the underlying <tt>Container</tt>.
	 * @param minCapRatio the minimal capacity ratio of the tree's nodes.
	 * @param e the epsilon of the strong version condition.
	 */	
	public AutoDeleteMVBTree(int blockSize, float minCapRatio, float e) {
		super(blockSize, minCapRatio, e);
	}

	/**
	 * Creates a new <tt>AutoDeleteMVBTree</tt>. The minimal capacity ratio of the
	 * tree's nodes is set to 50%.
	 * 
	 * @param blockSize the block size of the underlying <tt>Container</tt>.
	 * @param e the epsilon of the strong version condition.
	 */	
	public AutoDeleteMVBTree(int blockSize, float e) {
		super(blockSize, e);
	}
		
	/**
	 * Inserts a data object into the tree.
	 * This operation is unsupported and throws an UnsupportedOperationException.
	 * Use <tt>insert(Version, Version, Object)</tt> instead.
	 * 
	 * @param insertVersion the insertion version.
	 * @param data the data object to be inserted.
	 */
	@Override
	public void insert(Version insertVersion, Object data) {
		throw new UnsupportedOperationException("Insertion without defining a deletion version is not supported. Use insert(Version, Version, Object) instead.");
	}
	
	/**
	 * Inserts a data object into the tree.
	 * 
	 * @param insertVersion the insertion version.
	 * @param deleteVersion the deletion version.
	 * @param data the data object to be inserted.
	 */
	public void insert(Version insertVersion, Version deleteVersion, Object data) {
		if (insertVersion == null || deleteVersion == null) throw new IllegalArgumentException();
		if (insertVersion.compareTo(deleteVersion) >= 0)
			throw new IllegalArgumentException("insertVersion must be smaller than deleteVersion.");

		setCurrentVersion(insertVersion);

		while (!leafInfoHeap.isEmpty() &&
				leafInfoHeap.peek().getUnderflowVersion().compareTo(currentVersion) <= 0) {
			// treat weak version underflow in leaf referenced by the the heap's root
			LeafInfo leafInfo = leafInfoHeap.peek();
			Stack path = pathToNode(leafInfo.getReferenceKey(), currentVersion, 0);
			assert indexEntry(path).id().equals(leafInfo.getId()) : "Wrong leaf found. Actual ID: " + indexEntry(path).id() + ", expected ID: " + leafInfo.getId();
			assert node(path).underflows();
			treatUnderflow(path);

			// the heap's root is not removed here - this is done during the version 
			// split when the leaf dies
		}

		LeafEntry leafEntry = createLeafEntry(new Lifespan(insertVersion, deleteVersion), data);
		insert(insertVersion, leafEntry,(MVSeparator)separator(leafEntry), 0);
	}

	/**
	 * Replaces a data object in the tree.
	 * This operation is unsupported and throws an UnsupportedOperationException.
	 * 
	 * @param updateVersion the <tt>Version</tt> of this Operation.
	 * @param oldData the data object stored in the tree which is to be replaced.
	 * @param newData the new data object.
	 */
	@Override
	public void update(Version updateVersion, Object oldData, Object newData) {
		throw new UnsupportedOperationException("Updating is not supported.");
	}
	
	/**
	 * Removes a data object from the tree.
	 * This operation is unsupported and throws an UnsupportedOperationException.
	 * Data objects are automatically deleted at their designated deletion version.
	 * 
	 * @param deleteVersion the version of this operation.
	 * @param data the data object to be removed.
	 * @return the removed data object or null if no such element is stored in the tree.
	 */
	@Override
	public Object remove(Version deleteVersion, Object data) {
		throw new UnsupportedOperationException("Explicit object removal is not supported.");
	}

	/**
	 * Creates a new <tt>Node</tt>.
	 * 
	 * @return the newly created <tt>Node</tt>.
	 */		
	@Override
	public Tree.Node createNode(int level) {
		return new Node(level);
	}
	
	/**
	 * Adds a LeafInfo object for a newly created leaf.
	 * 
	 * @param nodeId the leaf's ID in the tree's container
	 * @param leafInfo leaf info for the leaf
	 */
	protected void addLeafInfo(Object nodeId, LeafInfo leafInfo) {
		leafInfo.setId(nodeId);
		LeafInfo old = leafInfoMap.put(nodeId, leafInfo);
		leafInfoHeap.enqueue(leafInfo);
		assert old == null;
		assert leafInfoMap.size() == leafInfoHeap.size();
	}

	/**
	 * Updates a LeafInfo object for an existing leaf.
	 * 
	 * @param nodeId the leaf's ID in the tree's container
	 * @param key the leaf's new reference key
	 * @param underflowVersion the leaf's new underflow version
	 */
	protected void updateLeafInfo(Object nodeId, Object key, Version underflowVersion) {
		LeafInfo leafInfo = leafInfoMap.get(nodeId);
		leafInfo.setReferenceKey(key);
		leafInfo.setUnderflowVersion(underflowVersion);
	}

	/**
	 * Removes a LeafInfo object for a leaf that died.
	 * 
	 * @param nodeId the leaf's ID in the tree's container
	 * @return leaf info that was removed
	 */
	protected LeafInfo removeLeafInfo(Object nodeId) {
		LeafInfo old = leafInfoMap.remove(nodeId);
		if (old != null)
			leafInfoHeap.remove(old.getHeapPos());
		assert leafInfoMap.size() == leafInfoHeap.size();
		return old;
	}
	
	/**
	 * Retrieves a LeafInfo object for a leaf.
	 * 
	 * @param nodeId the leaf's ID in the tree's container
	 * @return leaf info
	 */
	protected LeafInfo getLeafInfo(Object nodeId) {
		return leafInfoMap.get(nodeId);
	}

	public class Node extends MVBTree.Node {
		/**
		 * <tt>LeafInfo</tt> for this node. This field is only used temporarily:
		 * it is only set when a new leaf node is created in the tree and only
		 * read when the new node is inserted into the container. At that point
		 * the field is set to null and is never used again.
		 */
		protected LeafInfo leafInfo;
		
		public Node(int level, Function createEntryList) {
			super(level, createEntryList);
		}

		public Node(int level) {
			super(level);
		}

		@Override
		public void onInsert(Object id) {
			if (leafInfo != null) {
//				System.out.println("adding ID "+id+", LI: "+leafInfo+"\n");
				addLeafInfo(id, leafInfo);
				leafInfo = null;
			}
		}

		/**
		 * Returns an <tt>Iterator</tt> pointing to all current entries of this
		 * node, i.e. those entries whose deletion version is empty or greater
		 * than the current version.
		 *  
		 * @return an <tt>Iterator</tt> pointing to all entries of this node.
		 */
		@Override
		public Iterator getCurrentEntries() {
			return new Filter( iterator(),
								new AbstractPredicate() {
									public boolean invoke(Object entry) {
										return ((MVSeparator)separator(entry)).isAlive() ||
												((MVSeparator)separator(entry)).deleteVersion().compareTo(currentVersion()) > 0;
									}
								});
		}

		protected int countEntriesFromVersion(final Version version) {
			return Cursors.count(
					new Filter( iterator(),
							new AbstractPredicate() {
								public boolean invoke(Object entry) {
									return ((MVSeparator)separator(entry)).lifespan().contains(version);
								}
							}));
		}
		
		/**
		 * Computes the underflow version of this leaf node by choosing the d-largest
		 * deletion version of this leaf's current entries.
		 * 
		 * @return this leaf node's underflow version
		 */
		protected Version computeUnderflowVersion() {
			if (level() != 0)
				throw new IllegalArgumentException("Cannot compute underflow version of a non-leaf node.");

			// sort entries by deletion version
			List<LeafEntry> list = new ArrayList<LeafEntry>();
			Iterator<LeafEntry> it = getCurrentEntries();
			while (it.hasNext())
				list.add(it.next());
			Collections.sort(list,
				new Comparator<LeafEntry>() {
					public int compare(LeafEntry o1, LeafEntry o2) {
						return o1.getLifespan().endVersion().compareTo(o2.getLifespan().endVersion());
					}
				});

			// get d-largest deletion version
			Version delVersion = list.get(list.size() - D_LeafNode).getLifespan().endVersion();
			assert delVersion != null;
			return delVersion;			
		}

		/**
		 * Computes a reference key for this leaf node by simply taking the first entry's key value.
		 * 
		 * @return this leaf node's reference key
		 */
		protected Object computeReferenceKey() {
			if (level() != 0)
				throw new IllegalArgumentException("Cannot compute reference key of a non-leaf node.");

			return key(((LeafEntry)getEntry(0)).data());
		}
		
		@Override
		protected void grow(Object entry, Stack path) {
			super.grow(entry, path);

			if (level() == 0 && path.size() > 1) { // is this a leaf but not a root?
				Object nodeId = indexEntry(path).id();

/*
				Version underflowVersion;
				Version oldUnderflowVersion = getLeafInfo(nodeId).getUnderflowVersion();
				Version delVersion = ((LeafEntry)entry).getLifespan().endVersion();
				if (delVersion.compareTo(oldUnderflowVersion) > 0 &&
					countEntriesFromVersion(oldUnderflowVersion) == getD() - 1) {
				} else
					underflowVersion = oldUnderflowVersion;

				assert underflowVersion == computeUnderflowVersion();
*/
				updateLeafInfo(nodeId, computeReferenceKey(), computeUnderflowVersion());
				
/*				System.out.println("\nGROW");
				System.out.println("new: "+this);
				System.out.println("entry: "+entry);
				System.out.println("leaf info: "+getLeafInfo(indexEntry(path).id())+"\n");*/
			}
		}

		@Override
		protected SplitInfo split(Version splitVersion, Stack path) {
			LeafInfo oldLeafInfo = null;
			
			if (level() == 0) {
				// remember old leaf info of this node
				oldLeafInfo = getLeafInfo(indexEntry(path).id());
			}

			SplitInfo splitInfo = super.split(splitVersion, path);
			
			if (level() == 0) {
				Node newNode = (Node)splitInfo.newNode();
				Node keySplitNode = (Node)splitInfo.keySplitNode;
/*
				System.out.println("\nSPLIT at version " + splitVersion);
				System.out.println("isRoot: "+splitInfo.isRootSplit());
				System.out.println("old: "+node(path));
				System.out.println("new: "+newNode);
				System.out.println("key: "+keySplitNode);
*/
				if (splitInfo.isKeySplit()) {
//					System.out.println("key split");
					newNode.leafInfo = new LeafInfo(newNode.computeReferenceKey(), newNode.computeUnderflowVersion());
					keySplitNode.leafInfo = new LeafInfo(keySplitNode.computeReferenceKey(), keySplitNode.computeUnderflowVersion()); 
				} else if (!splitInfo.isRootSplit()) { // old node is not a root? 
					if (splitInfo.isMergePerformed()) {
//						System.out.println("merge without key split");
						newNode.leafInfo = new LeafInfo(newNode.computeReferenceKey(), newNode.computeUnderflowVersion());
					} else {
						// just a version split
//						System.out.println("just version split");
						Version underflowVersion = oldLeafInfo.getUnderflowVersion();
						assert underflowVersion.equals(newNode.computeUnderflowVersion());
						newNode.leafInfo = new LeafInfo(newNode.computeReferenceKey(), underflowVersion);
					}
				}

//				System.out.println();
			}

			return splitInfo;
		}

		@Override
		protected SplitInfo versionSplit(Version splitVersion, Stack path, SplitInfo splitInfo) {
			SplitInfo newSplitInfo = super.versionSplit(splitVersion, path, splitInfo);

//			System.out.println("node "+indexEntry(path).id()+" died");

			// this node is now dead so its leaf info is removed
			removeLeafInfo(indexEntry(path).id());

			return newSplitInfo;
		}
	}

	public void printInfo() {
		System.out.println("current tree version: "+currentVersion);
		System.out.println("Hashtable: "+leafInfoMap);
		System.out.println("Heap: "+leafInfoHeap);
		System.out.println("number of current leafs: "+leafInfoMap.size()+"\n");

		System.out.println(leafInfoMap.entrySet().size());
		for(Map.Entry<Object,LeafInfo> entry : leafInfoMap.entrySet()) {
			LeafInfo leafInfo = entry.getValue();
			System.out.println(leafInfo);
			Stack path = pathToNode(leafInfo.getReferenceKey(), currentVersion, 0);
			if (!indexEntry(path).id().equals(leafInfo.getId())) {
				System.out.println("wrong id: leaf info: "+entry.getKey()+", stack: "+indexEntry(path).id());
			}
		}
	}

}
