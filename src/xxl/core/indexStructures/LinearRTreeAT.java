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
import java.util.LinkedList;
import java.util.List;
import java.util.Stack;

import xxl.core.cursors.Cursors;
import xxl.core.functions.AbstractFunction;
import xxl.core.spatial.rectangles.Rectangle;
/**
 * An <tt>RTree</tt> implementing the split-strategy proposed by C.H. Ang and T.C.Tan
 * 
 * For a detailed discussion see C.H. Ang and T.C.Tan: 
 * "New Linear Node Splitting Algorithm for R-trees", 
 * Proceedings of the 5th International Symposium on Advances in Spatial Databases 1997 
 *  Pages: 339 - 349  
 *
 */
public class LinearRTreeAT extends RTree{
	/**
	 * 
	 */
	public Tree.Node createNode (int level) {
		return new Node().initialize(level, new LinkedList());
	}
	
	/** A modification of {@link RTree.Node node} implementing the linear split-algorithm.
	 *
	 * @see RTree.Node 
	 */
	public class Node extends RTree.Node{
		/** 
		 *  When the deadlock occures the R*Tree split algorithm will be used 
		 *  instead of the original algorithm  
		 *
		 * @param path the nodes already visited during this insert
		 * @return a <tt>SplitInfo</tt> containig all information needed about the split
		 */
		// TODO:  outliers reinsert, falls stark geclustert
		protected Tree.Node.SplitInfo split(final Stack path) {
			final Node node = (Node)node(path);
			final int dimensions = ((Rectangle)rootDescriptor()).dimensions();
			// overflowed nodes  MBR
			final Rectangle nodesMBR = rectangle(((IndexEntry)indexEntry(path)).descriptor());
			Distribution distribution = null;
			// compute Distributions
			List distributionList = new ArrayList();
			for (int dim = 0; dim < dimensions; dim++ ){
				final double left = nodesMBR.getCorner(false).getValue(dim);
				final double right = nodesMBR.getCorner(true).getValue(dim);
				List leftEntries = new ArrayList(); //L1
				List rightEntries = new ArrayList();//L2
				Iterator entriesIterator = node.entries();
				Rectangle actuallEntry;
				double leftActuall =  0d;
				double rightActuall = 0d;
				// compute first descriptors 
				Rectangle firstDescriptor = null;
				Rectangle secondDescriptor = null;
				int secondStart = 0;
				while( entriesIterator.hasNext()){
					Object entry = entriesIterator.next();
					actuallEntry = rectangle(descriptor(entry));
					leftActuall = actuallEntry.getCorner(false).getValue(dim);
					rightActuall = actuallEntry.getCorner(true).getValue(dim);
					if ( leftActuall - left <  right - rightActuall){
						//Insert in L1
						leftEntries.add(entry);
						firstDescriptor = (firstDescriptor == null) ? actuallEntry :
							Descriptors.union(firstDescriptor, actuallEntry);
					}else{
						//Insert in L2
						rightEntries.add(entry);
						secondDescriptor = (secondDescriptor == null) ? actuallEntry :
							Descriptors.union(secondDescriptor, actuallEntry);
					}
				}
				secondStart = leftEntries.size();
				leftEntries.addAll(rightEntries);
				Object[] array = leftEntries.toArray();
				Distribution distr = new Distribution(array , secondStart, 
						firstDescriptor, secondDescriptor, dimensions);
				distributionList.add(distr);
			}
			Iterator distributions = distributionList.iterator();
			//TODO
			List evenNumberDistributions = (List)Cursors.minima(distributions, 
					new AbstractFunction(){
						public Object invoke(Object distr){
							Distribution actuallDistr = (Distribution)distr;
							int secondStart = actuallDistr.secondStart;
							int maxNumber = Math.max(secondStart, actuallDistr.entries.length - secondStart);
							return new Integer(maxNumber);
						}
					}
			);
			// if one of descriptors == null start nativ R*Tree Split algorithmus
			for (int i = 0; i < evenNumberDistributions.size(); i++ ){
				Distribution distr =  (Distribution)evenNumberDistributions.get(i);
				if (distr.firstDescriptor == null
						|| distr.secondDescriptor == null){
					return super.split(path);
				}
			}
			if (evenNumberDistributions.size() == 1){
				distribution = (Distribution) evenNumberDistributions.get(0);
			}
			else{
				List minOverlapDistributions = (List)Cursors.minima(evenNumberDistributions.iterator(), 
						new AbstractFunction(){
						public Object invoke(Object distr){
							Distribution actuallDistr = (Distribution)distr;
							double groupOverlap = actuallDistr.overlapValue();
							return new Double(groupOverlap);
						}
					}
				);
				if (minOverlapDistributions.size() == 1){
					distribution = (Distribution) minOverlapDistributions.get(0);
				}else{ // 
					distribution = (Distribution)Cursors.minima( minOverlapDistributions.iterator(), 
							new AbstractFunction(){
							public Object invoke(Object distr){
								Distribution actuallDistr = (Distribution)distr;
								double groupArea = actuallDistr.marginValue();
//								double groupArea = actuallDistr.areaValue();
								return new Double(groupArea);
							}
						}
					).getFirst();
				}
				
			}
			node.entries.clear();
			node.entries.addAll(distribution.entries(false));
			entries.addAll(distribution.entries(true));
			((IndexEntry)indexEntry(path)).descriptor = distribution.descriptor(false);
			return new SplitInfo(path).initialize(distribution);
		}
	}
}
