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
import java.util.Iterator;

import xxl.core.collections.containers.Container;
import xxl.core.functions.Function;
import xxl.core.predicates.Predicate;

/** This class provides functionality to bulk-load different types of trees.
 * The tree is created buttom-up.  
 */
public class SortBasedBulkLoading {

	/** The tree which is bulk-loaded.
	 */
	protected ORTree tree;
	
	/** A Function determining into which container a node is saved.
	 */
	protected Function determineContainer;	// Node -> Container
	
	/** A predicate determining if a node overflows (and i.e. a new node
	 * has to be allocated).
	 */
	protected Predicate overflows;
	
	/** An <tt>ArrayList</tt> containing the path of nodes in the tree where 
	 * insertions take place, i.e. the right edge of the tree.
	 */
	protected ArrayList path = new ArrayList();
	
	/** A flag indicating if <tt>tree</tt> is an <tt>MTree</tt>.
	 */
	protected boolean isMTree = false;


	/** Creates a new <tt>SortBasedBulkLoading</tt> and bulk loads the given <tt>tree</tt>
	 * with the given <tt>objects</tt>. 
	 * 
	 * @param tree the empty <tt>ORtree</tt> to bulk-load
	 * @param objects sorted iterator of the objects to be loaded into the tree
	 * @param determineContainer a function determining into which container nodes are saved
	 * @param overflows a predicate determining when a node overflows
	 */
	public SortBasedBulkLoading (ORTree tree, Iterator objects, Function determineContainer, Predicate overflows) {
		this.tree = tree;
		this.determineContainer = determineContainer;
		this.overflows = overflows;
		this.isMTree = tree instanceof MTree;
		
		// insert all objects
		if (isMTree) {
			while (objects.hasNext()) {
				insert(((MTree)tree).new LeafEntry(objects.next()), 0);
			}
		}
		else
			while (objects.hasNext())
				insert(objects.next(), 0);

		// write unsaved nodes
		for (int level = 0; level<path.size();) {
			ORTree.IndexEntry indexEntry = save((ORTree.Node)path.get(level++));

			if (level<path.size())
				insert(indexEntry, level);
			else {
				tree.rootEntry = indexEntry;
				tree.rootDescriptor = indexEntry.descriptor();
			}
		}
	}

	/** Creates a new <tt>SortBasedBulkLoading</tt> and bulk loads the given <tt>tree</tt>
	 * with the given <tt>objects</tt>. The predicate overflows of the tree is used to 
	 * determine when a node overflows. 
	 * 
	 * @param tree the empty <tt>ORtree</tt> to bulk-load
	 * @param objects sorted iterator of the objects to be loaded into the tree
	 * @param determineContainer a function determining into which container nodes are saved
	 */
	public SortBasedBulkLoading (ORTree tree, Iterator objects, Function determineContainer) {
		this(tree, objects, determineContainer, tree.overflows);
	}

	/** Inserts an entry into the given level.
	 * 
	 * @param entry the entry to add
	 * @param level the level into which <tt>entry</tt> is inserted
	 */
	protected void insert (Object entry, int level) {
		ORTree.Node node;

		if (path.size()<=level)
			path.add(tree.createNode(level));
		node = (ORTree.Node)path.get(level);
		node.entries.add(entry);
		if (overflows.invoke(node)) {
			ORTree.Node newNode = (ORTree.Node)tree.createNode(node.level);
			Iterator entries;

			newNode.entries.add(entry);
			path.set(level, newNode);
			for (entries = node.entries(); entries.next()!=entry;);
			entries.remove();
			insert(save(node), level+1);
		}
	}
	
	/** Saves a node of the tree to external memory.
	 * 
	 * @param node the node to save
	 * @return an <tt>IndexEntry</tt> pointing to the saved node  
	 */
	protected ORTree.IndexEntry save (ORTree.Node node) {
		Container container = (Container)determineContainer.invoke(node);
		Object id = container.insert(node);
		Descriptor descriptor = tree.computeDescriptor(node.entries);

		return (ORTree.IndexEntry)((ORTree.IndexEntry)tree.createIndexEntry(node.level+1)).initialize(descriptor).initialize(container, id);
	}
}
