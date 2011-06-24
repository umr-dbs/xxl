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
package xxl.core.spatial.geometries.cursors;

import java.util.Comparator;
import java.util.Iterator;

import xxl.core.collections.queues.DynamicHeap;
import xxl.core.collections.queues.Heap;
import xxl.core.comparators.ComparableComparator;
import xxl.core.cursors.AbstractCursor;
import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;
import xxl.core.indexStructures.ORTree;
import xxl.core.indexStructures.RTree;
import xxl.core.indexStructures.Tree.Query.Candidate;
import xxl.core.spatial.KPE;
import xxl.core.spatial.geometries.Geometry2D;
import xxl.core.spatial.rectangles.Rectangle;

/** The NearestNeighborQuery is a priority based query which returns all neighbors of a given 
 *  query object sorted by their distance to the query object.
 *  There is already a NearestNeighborQuery implemented in {@link ORTree} but this operator just
 *  considers the distance of the index-entries' MBRs, but does not take into consideratoin that there
 *  may be a difference between this distance and the distance of the indexed geometries. 
 *  Therefor in this implementation the geometries extracted from the leafs are organised
 *  in a priority queue ordered by the distance, too. If the distance of the first element of
 *  this queue is less then the distance of the head of the candidate-queue this geometry 
 *  is returned, otherwise the geommetry of the candidate is extracted and inserted into 
 *  the geometry-queue.
 * 
 */
public class NearestNeighborQuery extends AbstractCursor<DistanceWeightedKPE>{
	
	/**	an iterator over the candidate queue */
	private Iterator query;
	
	/**	the heap which sorts the geometries based on their distance to the query object */
	private Heap<DistanceWeightedKPE> resultQueue;
	
	/**	the heap which sorts the candidateQueue based on their distance to the query object */
	private Heap<Candidate> candidateQueue;
	
	/** a function to extract the geometries from a candidate */
	private Function<KPE, Geometry2D> getGeometry;
			
	/**	the query object */
	private Geometry2D queryObject;
	
	/** this function returns the distance of a candidate-descriptor to the query-descriptor */
	private Function<Candidate, Double> getDistanceToQueryObject;
	

	/** the next Geometry2D to return */
	private DistanceWeightedKPE nextObject;			
	
	/** Returns a distance based comparator, which compares two objects according to their distance
	 *  to the query object
	 * @return a distance based comparator
	 */
	public Comparator<Candidate> getDistanceBasedCandidateComparator(){
		return new Comparator<Candidate>(){
			
			public int compare(Candidate c1, Candidate c2) {
				return Double.compare(
							getDistanceToQueryObject.invoke(c1),
							getDistanceToQueryObject.invoke(c2)
							);
			}				
		};
	}
	
	/** Intitializes a new NearestNeighborQuery with the given parameters.
	 * @param index the {@link RTree} which indexes the spatial data
	 * @param queryObject the query object
	 * @param getGeometry a Function which determines how to extract the geometries from the trees' leafs
	 */
	public NearestNeighborQuery( RTree index, final Geometry2D queryObject, Function<KPE, Geometry2D> getGeometry){
		this.queryObject = queryObject;
		this.getGeometry = getGeometry;
		getDistanceToQueryObject = new AbstractFunction<Candidate, Double>(){
										Rectangle queryDescriptor = queryObject.getMBR();
										public Double invoke(Candidate c){
											return ((Rectangle)c.descriptor()).distance(queryDescriptor, queryDescriptor.dimensions());
										}
									};
		candidateQueue = new DynamicHeap<Candidate>(getDistanceBasedCandidateComparator());
		resultQueue = new DynamicHeap<DistanceWeightedKPE>(new ComparableComparator());
		query = index.query(candidateQueue); 		
	}	
	
	/** Computes the next result of this query, if there is one.
	 *  Candidates are expanded to geometries as long as the head of the
	 *  candidate is less distant to the query- object than the head of
	 *  the geometry queue. 
	 */
	@Override
	protected boolean hasNextObject() {		
		while( !candidateQueue.isEmpty() && 
				( resultQueue.isEmpty() 
				  || ( getDistanceToQueryObject.invoke(candidateQueue.peek()) < resultQueue.peek().getDistance() )		
			    )
			){
				KPE k =  (KPE)((Candidate) query.next()).entry();				
				resultQueue.enqueue( new DistanceWeightedKPE(k, getGeometry.invoke(k).distance(queryObject) ));
			}				 						
		nextObject =  (!resultQueue.isEmpty() ? resultQueue.dequeue() : null); 
		return nextObject != null;
	}

	/** Returns the next geometry of this query */
	@Override
	protected DistanceWeightedKPE nextObject() {
		return nextObject;			
	}

	/** This method is not supported */
	public void remove() {
		throw new UnsupportedOperationException();
	}
}
