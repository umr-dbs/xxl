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

package xxl.core.spatial.cursors;

import java.util.Iterator;
import java.util.List;

import xxl.core.functions.Function;
import xxl.core.predicates.AbstractPredicate;
import xxl.core.predicates.Predicate;
import xxl.core.spatial.KPEzCode;
import xxl.core.spatial.SpaceFillingCurves;
import xxl.core.spatial.points.Point;
import xxl.core.util.BitSet;

/**
 *	This class provides the similarity-join algorithm "GESS: Generic External Space Sweep"
 *	(see "[DS 01] GESS: a Scalable Algorithm for Mining Large Datasets in High Dimensional Spaces
 *	by Jens-Peter Dittrich and Bernhard Seeger, ACM SIGKDD 2001." for a
 *	detailed description of this method).
 *	<br><br>
 *	The most important component of this algorithm is the Replicator-Engine
 *	({@link xxl.core.spatial.cursors.Replicator Replicator})
 *	which determines the partition(s) for incoming points and maps
 *	Points to KPEzCodes.
 *	<br><br>
 *	The use-case implemented in the main-method of this class reads 1 or
 *	2 inputs and computes the similarity-join using GESS. The use-case
 *	provided with this class reads files containing FloatPoints.
 *	<br><br>
 *	Note that GESS works on arbitrary data as long as the user provides
 *	a mapping to the internally used FixedPointRectangle-type (see
 *	parameter "inputMapping").
 *
 *	@see xxl.core.spatial.cursors.Replicator
 *	@see xxl.core.spatial.cursors.Orenstein
 *	@see xxl.core.spatial.cursors.MSJ
 *	@see xxl.core.spatial.points.FixedPoint
 *	@see xxl.core.spatial.rectangles.FixedPointRectangle
 *	@see xxl.core.spatial.KPEzCode
 *	@see xxl.core.cursors.joins.SortMergeJoin
 *
 */
public class GESS extends Orenstein{

	/** 
	 *  This class provides the Reference Point Method of GESS.
	 *	Since GESS allows hypercubes to get replicated, we have to provide
	 *	a method to eliminate possible duplicates from the result set.
	 *   <br><br>
	 *	There are two principal approaches for eliminating duplicate
	 *	results. The first is to use a hash-table that stores the entire
	 *	set of result tuples. The memory requirements of this approach
	 *	however are O(n). A second approach is to apply external sorting
	 *	to the result set. This causes additional I/O cost. In
	 *	addition, the sorting operation could not report any result until
	 *	all results had been reported by the merging algorithm.
	 *   <br><br>
	 *	Instead of using these standard techniques
	 *	we propose an inexpensive on-line method termed
	 *	Reference Point Method (RPM). This method neither allocates
	 *	additional memory nor does it cause any additional I/O operations.
	 *   <br><br>
	 *	The basic idea of RPM is to define a reference point
	 *	which is contained in the section of two hypercubes.
	 *   <br><br>
	 *	See [DS 01] GESS: a Scalable Algorithm for Mining Large Datasets in High Dimensional Spaces
	 *	by Jens-Peter Dittrich and Bernhard Seeger, ACM SIGKDD 2001. San Francisco. pages: 47-56" for a
	 *	detailed description of this algorithm.
	 *   <br><br>
	 *	Usage:
	 *	The main-method of this class contains an elaborate similarity-join use-case.
	 *   <br><br>
	 *	To make GESS work using RPM simply modify the joinPredicate using the And-predicate:
	 *	<code><pre>
	 *	Predicate joinPredicate =
	 *		new And(
	 *			new FeaturePredicate(
	 *				new DistanceWithinMaximum(epsilon),
	 *				new AbstractFunction(){
	 *					public Object invoke(Object object){
	 *						return ((KPEzCode)object).getData();
	 *					}
	 *				}
	 *			),
	 *			new GESS.ReferencePointMethod(epsilonDiv2)	//duplicate removal (modify GESS to work with reference point method)
	 *		);
	 *
	 *
	 *	@see xxl.core.spatial.cursors.Replicator
	 *	@see xxl.core.predicates.Predicate
	 *	@see xxl.core.predicates.And
	 * 	@see xxl.core.spatial.predicates.DistanceWithinMaximum
	 *	@see xxl.core.cursors.joins.SortMergeJoin
	 */
	public static class ReferencePointMethod extends AbstractPredicate<Object> {

		/** The epsilon (query) distance of the Similarity Join divided by 2.
		 */
		protected double epsilonDiv2;

		/** Constructs a new ReferencePointMethod instance.
		 *
		 *  @param epsilonDiv2 the epsilon distance of the Similarity Join divided by 2
		 */
		public ReferencePointMethod(double epsilonDiv2){
			this.epsilonDiv2 = epsilonDiv2;
		}

		/** Takes a tuple containing two KPEzCodes as its input and
		 *  checks whether a certain reference point is contained
		 *  in the partition currently processed.
		 *
		 *  Contains an optimization that applies RPM only in those cases
		 *  when at least one of the inputs is a replicate.
		 *  @param object is a two-dimensional array whith the two KPEzCodes
		 * @return return true if a certain reference point is contained in the 
		 * cell of the two points given by the KPEzCodes (see Paper)
		 */
		public boolean invoke(List<? extends Object> object){
			KPEzCode k0 = (KPEzCode)object.get(0);
			KPEzCode k1 = (KPEzCode)object.get(1);

			if( k0.getIsReplicate() || k1.getIsReplicate()){		//optimization: apply RPM only in case one of the inputs is a replicate
				final BitSet currentZCode = k0.getzCode();
				float[] p1 = (float[]) ((Point)k0.getData()).getPoint();
				float[] p2 = (float[]) ((Point)k1.getData()).getPoint();
				long[] rp = new long[p1.length];
				for(int i=0; i<p1.length; i++){
					rp[i] = xxl.core.math.Maths.doubleToNormalizedLongBits( Math.max( p1[i], p2[i] ) - epsilonDiv2 );
				}
				return SpaceFillingCurves.zCode2( rp, currentZCode.precision() ).compare(currentZCode) == 0;
			}
			else
				return true;
		}
	}

	/** Creates a new GESS-operator (Constructor for two inputs).
	 *
	 *	@param input0 first (unsorted) input
	 *	@param input1 second (unsorted) input
	 *	@param inputMapping a Function used to map incoming objects of arbitrary type to a FixedPointRectangle (internally used by the replication engine)
	 *	@param joinPredicate the join predicate to be used by this join	(e.g. DistanceWithin-predicate)
	 *	@param splitAllowed the replication strategy to be applied by GESS (see inner class Replicator.Split)
	 *	@param minBitIndex the minimal bit-index to be considered for the replication-process ( 0 <= bitIndex <= 62 )
	 *	@param newSorter a factory-Function that returns a sorting-operator (e.g. {@link xxl.core.cursors.sorters.MergeSorter})
	 *	@param newResult a factory-Function that is used to create the result-tuples that are returned by this operator (e.g. {@link xxl.core.functions.Tuplify})
	 *	@param dimensions the dimensionality of the data
	 *	@param initialCapacity the maximum number of elements that can be stored inside main-memory (i.e. by the SweepArea)
	 */
	public GESS(Iterator input0, Iterator input1, Function inputMapping, Predicate joinPredicate, Predicate splitAllowed, int minBitIndex, Function newSorter, Function newResult, int dimensions, int initialCapacity){
		super(
				new Replicator(inputMapping, input0, dimensions, splitAllowed, minBitIndex),
				new Replicator(inputMapping, input1, dimensions, splitAllowed, minBitIndex),
				joinPredicate,
				newSorter,
				newResult,
				initialCapacity
		);
	}

	/** Creates a new GESS-operator (Constructor for a self-join).
	 *
	 *	@param input (unsorted) input
	 *	@param inputMapping a Function used to map incoming objects of arbitrary type to a FixedPointRectangle (internally used by the replication engine)
	 *	@param joinPredicate the join predicate to be used by this join	(e.g. DistanceWithin-predicate)
	 *	@param splitAllowed the replication strategy to be applied by GESS (see inner class Replicator.Split)
	 *	@param minBitIndex the minimal bit-index to be considered for the replication-process ( 0 <= bitIndex <= 62 )
	 *	@param newSorter a factory-Function that returns a sorting-operator (e.g. {@link xxl.core.cursors.sorters.MergeSorter})
	 *	@param newResult a factory-Function that is used to create the result-tuples that are returned by this operator (e.g. {@link xxl.core.functions.Tuplify})
	 *	@param dimensions the dimensionality of the data
	 *	@param initialCapacity the maximum number of elements that can be stored inside main-memory (i.e. by the SweepArea)
	 */
//	public GESS(Iterator input, Function inputMapping, Predicate joinPredicate, Predicate splitAllowed, int minBitIndex, Function newSorter, Function newResult, int dimensions, int initialCapacity){
//		super(
//			new Replicator(inputMapping, input, dimensions, splitAllowed, minBitIndex),
//			joinPredicate,
//			newSorter,
//			newResult,
//			initialCapacity
//		);
//	}
	
}
