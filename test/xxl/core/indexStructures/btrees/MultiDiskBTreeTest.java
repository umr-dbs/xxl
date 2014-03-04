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

package xxl.core.indexStructures.btrees;

import java.util.Iterator;

import xxl.core.collections.containers.Container;
import xxl.core.collections.containers.MapContainer;
import xxl.core.cursors.Cursors;
import xxl.core.cursors.sources.Permutator;
import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;
import xxl.core.indexStructures.MultiDiskBTree;
import xxl.core.util.Interval1D;

/**
 * Class for testing the {@link xxl.core.indexStructures.MultiDiskBTree}. 
 */
public class MultiDiskBTreeTest {

	/**
	 * Creates a main-memory BTree with minimum (maximum) capacity given
	 * by the command line argument 0 (1) by inserting a permutation of
	 * the numbers ranging fom 0 (inclusive) to the value of command line
	 * argument 2. Argument 3 determines the number of disks to be used
	 * for the B-tree.
	 * After that, all elements of the tree are read.
	 * This version works on main-memory-Containers.
	 * 
	 * @param args command line parameters
	 * @throws Exception
	 */
	public static void main (String [] args) throws Exception {
		int minCap=10, maxCap=25, maxNumber=10000, disks=7;
		
		if (args.length>0) {
			minCap = Integer.parseInt(args[0]);
			maxCap = Integer.parseInt(args[1]);
			maxNumber = Integer.parseInt(args[2]);
			disks = Integer.parseInt(args[3]);
		}
		
		// a function that creates a descriptor for a given object
		Function getDescriptor = new AbstractFunction() {
			public Object invoke (Object object) {
				return new Interval1D(object);
			}
		};
		// the container that stores the inner nodes of the MultiDiskBTree
		Container container = new MapContainer();
		// the containers that store the leafs of the MultiDiskBTree
		Container [] containers = new Container [disks];
		for (int i=0; i<containers.length; i++)
			containers[i] = new MapContainer();
		// initialize the MultiDiskBTree
		MultiDiskBTree btree = new MultiDiskBTree().initialize(getDescriptor, container, containers, minCap, maxCap);
		Iterator it = new Permutator(maxNumber);

		long time = System.currentTimeMillis();
		// insert an iterator of objects by inserting every single object
		while (it.hasNext())
			btree.insert(it.next());

		System.out.println("Insertion: "+(-time+(time = System.currentTimeMillis()))+" ms");
		System.out.println("Insertion complete, height: "+btree.height()+", universe: "+btree.rootDescriptor());

		time = System.currentTimeMillis();
		// perform a range-query
		System.out.println(Cursors.count(btree.query(new Interval1D(new Integer(0), new Integer(maxNumber)), 0)));
		System.out.println("Query: "+(-time+(time = System.currentTimeMillis()))+" ms");
	}
}
