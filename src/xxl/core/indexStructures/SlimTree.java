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
import java.util.Stack;

import xxl.core.collections.queues.Heap;
import xxl.core.comparators.ComparableComparator;
import xxl.core.comparators.FeatureComparator;
import xxl.core.functions.AbstractFunction;
import xxl.core.util.Distance;

/** A dynamic tree for organizing metric datasets. 
 * 
 * For a detailed discussion see 
 * Caetano Traina Jr., Agma Traina, Bernhard Seeger, Christos Faloutsos: 
 * "Slim-trees: High Performance Metric Trees Minimizing Overlap Between Nodes", 
 * EDBT 2000, pp. 51--65, Konstanz, Germany, Mar. 2000
 * 
 * @see Tree
 * @see ORTree
 * @see MTree
 */
public class SlimTree extends MTree {

	/* (non-Javadoc)
	 * @see xxl.core.indexStructures.Tree#createNode(int)
	 */
	public Tree.Node createNode (int level) {
		return new Node().initialize(level, new ArrayList(20));
	}

	/** <tt>Node</tt> is the class used to represent leaf- and non-leaf 
	 * nodes of <tt>SlimTree</tt>. Nodes are stored in containers.
	 * 
	 * @see Tree.Node
	 * @see ORTree.Node
	 * @see MTree.Node
	 */
	public class Node extends MTree.Node {

		/** Returns the partition for <tt>i</tt>.
		 * 
		 * @param i index
		 * @param partitions partitions to search in
		 * @return partition for i
		 */
		protected int getPartition (int i, int [] partitions) {
			while (partitions[i]!=i)
				i = partitions[i];
			return i;
		}

		/* (non-Javadoc)
		 * @see xxl.core.indexStructures.Tree.Node#split(java.util.Stack)
		 */
		protected Tree.Node.SplitInfo split (Stack path) {
			SlimTree.Node node = (SlimTree.Node)node(path);
			ArrayList nodeEntries = (ArrayList)node.entries;
			int minCapacity = node.splitMinNumber();
			Object [] edges = new Object[nodeEntries.size()*(nodeEntries.size()-1)/2];
			int [] partitions = new int[nodeEntries.size()];
			int [] sizes = new int [nodeEntries.size()];
			int partitionNumber = nodeEntries.size();
			
			for (int i = 0, index = 0; i<nodeEntries.size(); i++) {
				Sphere sphere = sphere(nodeEntries.get(i));

				partitions[i] = i;
				sizes[i] = 1;
				for (int j = 0; j<i; j++)
					edges[index++] = new Object[]{new Double(sphere.centerDistance(sphere(nodeEntries.get(j)))), new Integer(i), new Integer(j)};
			}
			Heap heap = new Heap(edges, new FeatureComparator(new ComparableComparator(),
				new AbstractFunction() {
					public Object invoke (Object object) {
						return ((Object[])object)[0];
					}
				}
			));
			heap.open();
			while (partitionNumber>2) {
				Object [] edge = (Object[])heap.dequeue();
				int i = getPartition(((Integer)edge[1]).intValue(), partitions);
				int j = getPartition(((Integer)edge[2]).intValue(), partitions);

				if (i!=j) {
					if (sizes[i]>sizes[j]) {
						sizes[i] += sizes[j];
						partitions[j] = i;
					}
					else {
						sizes[j] += sizes[i];
						partitions[i] = j;
					}
					partitionNumber--;
				}
			}
			heap.close();
			int firstPartition = getPartition(0, partitions), i = 0;
			for (Iterator entries = node.entries(); entries.hasNext(); i++) {
				Object entry = entries.next();

				if (getPartition(i, partitions)!=firstPartition) {
					this.entries.add(entry);
					entries.remove();
				}
			}
			// ensure that each node contains enough elements
			if (node.entries.size() < minCapacity || entries.size() < minCapacity) {
				node.entries.addAll(entries);
				entries.clear();
				return super.split(path);
			}
			else {
				Sphere newSphere = (Sphere)computeDescriptor(node.entries);
				if (!path.isEmpty()) {
					Object top = path.pop();
					newSphere.distanceToParent = newSphere.centerDistance(sphere(indexEntry(path)));
					path.push(top);
				}
				((IndexEntry)indexEntry(path)).descriptor = newSphere;
				split = true;
				return new SplitInfo(path).initialize(computeDescriptor(entries));
			}
		}
	}

	/** Creates a new <tt>SlimTree</tt> using the given distance functions.
	 *  
	 * @param pointDistance distance function for points
	 * @param sphereDistance distance function for spheres
	 */
	public SlimTree (Distance pointDistance, Distance sphereDistance) {
		super(pointDistance, sphereDistance);
	}

	/** Creates a new <tt>SlimTree</tt> using the given distance function for points.
	 *  
	 * @param pointDistance distance function for points
	 */
	public SlimTree (Distance pointDistance) {
		super(pointDistance);
	}

	/** Creates a new <tt>SlimTree</tt> using default distance functions.
	 */
	public SlimTree () {
		super();
	}
}
