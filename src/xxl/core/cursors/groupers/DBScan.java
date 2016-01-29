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

package xxl.core.cursors.groupers;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.NoSuchElementException;

import xxl.core.collections.queues.FIFOQueue;
import xxl.core.collections.queues.ListQueue;
import xxl.core.collections.queues.Queue;
import xxl.core.collections.queues.Queues;
import xxl.core.collections.queues.StackQueue;
import xxl.core.cursors.AbstractCursor;
import xxl.core.cursors.Cursor;
import xxl.core.cursors.Cursors;
import xxl.core.cursors.filters.Filter;
import xxl.core.cursors.mappers.Mapper;
import xxl.core.cursors.unions.Sequentializer;
import xxl.core.cursors.wrappers.QueueCursor;
import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;
import xxl.core.predicates.AbstractPredicate;
import xxl.core.predicates.And;
import xxl.core.predicates.EqualReference;
import xxl.core.predicates.LeftBind;
import xxl.core.predicates.Not;
import xxl.core.predicates.Predicate;
import xxl.core.util.Classifiable;

/**
 * A grouper that generates clusters from a given input iteration. So, each call
 * to the <tt>next</tt> method returns a new cursor representing a new cluster.
 * This cursor is based on the DBScan algorithm published in <i>"A Density-Based
 * Algorithm for Discovering Clusters in Large Spatial Databases with Noise"</i>
 * ([EKS+96]) and <i>"Incremental Clustering for Mining in a Data Warehousing
 * Environment"</i> ([EKS+98]). But the algorithm is modified in a way, that the
 * computation of the clusters and their elements is implemented <i>absolutely
 * lazy</i>.
 * 
 * <p>"The key idea is that for each point of a cluster the neighborhood
 * concerning a given radius has to contain at least a minimum number of points,
 * i.e. the density in the neighborhood has to exceed some threshold."
 * ([EKS+96])</p>
 * 
 * <p><b>IMPORTANT:</b> All objects of the given input iterator have to implement
 * the {@link xxl.core.util.Classifiable classifiable} interface.</p>
 * 
 * <p><b>Note:</b> If the input iteration is given by an object of the class
 * {@link java.util.Iterator Iterator}, i.e., it does not support the
 * <tt>peek</tt> operation, it is internally wrapped to a cursor.</p>
 * 
 * <p><b>Example usage:</b>
 * <pre>
 *     // get the data; here the data is contained in a list-bag
 *     
 *     Iterator input = data.cursor();
 * 
 *     // using an <i>Euclidean</i> metric as distance function
 *     // eps = 1.6
 *     // minPts = 3
 * 
 * 		DBScan clusterCursor = new DBScan(
 *			input,
 * 			1.6,
 *			3,
 *			new AbstractFunction() {
 *				public Object invoke(Object descriptor) {
 *					final Sphere sphere = (Sphere)descriptor;
 *					return data.query(  // define a simple range query: iterating over all elements
 *						new AbstractPredicate() { // and checking if the given point is contained in the search sphere
 *							public boolean invoke(Object o) {
 *								return sphere.contains(
 *								        new Sphere(((ClassifiableObject)o).getObject(), 0d, null)
 *								);
 *							}
 * 						}
 *					);
 *				}
 *			},
 *			new AbstractFunction() {
 *				public Object invoke(Object object, Object eps) {
 *					return new Sphere(((ClassifiableObject)object).getObject(), ((Double)eps).doubleValue(), null);
 *				}
 *			}
 *		);
 *		
 *		clusterCursor.open();
 *
 *		for (int i = 0; clusterCursor.hasNext(); i++) {
 *			Cursor next = (Cursor)clusterCursor.next(); // each element of the clusterCursor is a new cursor, namely a new cluster
 * 			System.out.println("cluster " + i + ": ");
 * 			Cursors.println(next);
 * 		}
 *		
 *		clusterCursor.close();
 * </pre>
 * To perform a more efficient clustering, the range queries have to use an
 * index-structure, e.g. an {@link xxl.core.indexStructures.MTree M-tree}, where
 * the data, i.e. the classifiable objects, are located in.
 *
 * @see java.util.Iterator
 * @see xxl.core.cursors.Cursor
 * @see xxl.core.util.Classifiable
 * @see xxl.core.util.Distance
 */
public class DBScan extends AbstractCursor {

	/**
	 * The search 'radius' used for the range queries.
	 */
	protected double eps;

	/**
	 * The minimum number of elements that have to be positioned in the
	 * Eps-neighborhood of an element.
	 */
	protected int minPts;

	/**
	 * The input iteration providing the data to be grouped.
	 */
	protected Cursor input;

	/**
	 * This queue contains all elements that are already belonging to a cluster,
	 * but a range query has to be performed with them.
	 */
	protected Queue queue;

	/**
	 * This queue contains all elements that have been marked as noise. Due to
	 * performance reasons the implementation of this DBScan algorithm does not
	 * delete elements, that were inserted into the noise-cluster, but later are
	 * assigned to a special cluster. Therefore this queue may contain elements
	 * that have already been returned to the user with regard to another
	 * cluster. When returning the noise, i.e. the last cluster, all elements
	 * that have a different clusterID will be filtered out.
	 */
	protected Queue noiseQueue;

	/**
	 * This queue contains all elements that belonged to the noise-cluster, but
	 * which were assigned later to a special cluster. So, the clusterID of these
	 * elements changed.
	 */
	protected Queue changedCIDQueue;

	/**
	 * A unary function that internally holds a data structure, e.g. an index,
	 * to perform range queries efficiently. The argument of this function should
	 * be a kind of an arbitrary descriptor and its result should be a cursor of
	 * objects implementing the {@link xxl.core.util.Classifiable classifiable}
	 * interface.
	 * <pre>
	 *     f : Object (Descriptor) &rarr; Cursor of ClassifiableObjects
	 * </pre>
	 */
	protected Function rangeQuery;

	/**
	 * A binary function returning a kind of descriptor used for the range query.
	 * To determine this descriptor the function gets two arguments, namely the
	 * search object and a user-defined radius <tt>eps</tt>.
	 * <pre>
	 *     f : Object x eps &rarr; Object (Descriptor)
	 * </pre>
	 */
	protected Function getDescriptor;

	/**
	 * An internal used cursor representing the next cluster to be returned to
	 * the user by a call to the <tt>next</tt> or <tt>peek</tt> method.
	 */
	protected Cursor nextCluster;

	/**
	 * The cluster number with which the elements will be marked during the
	 * algorithms execution.
	 */
	protected long CLUSTER_NO = UNDEFINED;

	/**
	 * A constant cluster number for noise.
	 */
	public static final int NOISE = -1;

	/**
	 * A constant cluster number for a cluster that is undefined, e.g., at the
	 * start of the algorithm.
	 */
	public static final int UNDEFINED = -2;

	/**
	 * Returns an unary predicate that verifies if a given classifiable object
	 * is already classified.
	 */
	public static Predicate isUnclassified = new AbstractPredicate() {
		public boolean invoke(Object o) {
			return !((Classifiable)o).isClassified();
		}
	};

	/**
	 * Returns an unary predicate that verifies if a given classifiable object
	 * is marked as noise.
	 */
	public static Predicate isNoise = new AbstractPredicate() {
		public boolean invoke(Object o) {
			return ((Classifiable)o).getClusterID() == NOISE;
		}
	};

	/**
	 * Returns an unary function that can be used to mark classifiable objects
	 * with regard to assign them to a user-defined cluster.
	 *
	 * @param clusterID the clusterID determining to which cluster the objects
	 *        should be assigned to.
	 * @return an unary function that can be used to mark classifiable objects
	 *         with regard to assign them to a user-defined cluster.
	 */
	public static Function ClusterID_FACTORY(final long clusterID) {
		return new AbstractFunction() {
			public Object invoke(Object o) {
				((Classifiable)o).setClusterID(clusterID);
				return o;
			}
		};
	}

	/**
	 * A helper class for classifiable objects that implements the
	 * {@link xxl.core.util.Classifiable classifiable} interface. Its a standard
	 * implementation for classifiable objects and decorates each object given to
	 * a constructor with classification criteria.
	 */
	public static class ClassifiableObject implements Classifiable {

		/**
		 * The object to be classified.
		 */
		protected Object object;

		/**
		 * The clusterID assigned to this object.
		 */
		protected long CLUSTER_ID = UNDEFINED;

		/**
		 * A flag that signals if this object has already been classified.
		 */
		protected boolean isClassified = false;

		/**
		 * Creates a new classifiable object.
		 *
		 * @param object the object to be classified.
		 */
		public ClassifiableObject(Object object) {
			this.object = object;
		}

		/**
		 * Creates a new classifiable object.
		 *
		 * @param object the object to be classified.
		 * @param CLUSTER_ID the clusterID for this object.
		 */
		public ClassifiableObject(Object object, int CLUSTER_ID) {
			this.object = object;
			this.CLUSTER_ID = CLUSTER_ID;
			this.isClassified = true;
		}

		/**
		 * Returns <tt>true</tt>, if the object has already been classified.
		 *
		 * @return <tt>true</tt> if the object has already been classified,
		 *         <tt>false</tt> otherwise.
		 */
		public boolean isClassified() {
			return isClassified;
		}

		/**
		 * Returns the cluster ID of this object.
		 *
		 * @return the cluster ID of this object.
		 */
		public long getClusterID() {
			return CLUSTER_ID;
		}

		/**
		 *	Sets the cluster ID of this object.
		 *
		 * @param CLUSTER_ID the new cluster ID of this object.
		 */
		public void setClusterID(long CLUSTER_ID) {
			this.CLUSTER_ID = CLUSTER_ID;
			this.isClassified = true;
		}

		/**
		 * Returns the object specified in the constructor.
		 *
		 * @return the object.
		 */
		public Object getObject() {
			return object;
		}

		/**
		 * Returns <tt>true</tt> if two classifiable objects are equal,
		 * <tt>false</tt> otherwise.
		 *
		 * @param o the reference object with which to compare. 
		 * @return <tt>true</tt> if two classifiable objects are equal,
		 *         <tt>false</tt> otherwise.
		 */
		public boolean equals(Object o) {
			ClassifiableObject co = (ClassifiableObject)o;
			return object == co.object && isClassified == co.isClassified && CLUSTER_ID == co.CLUSTER_ID;
		}

		/**
		 * Returns a hash code value for the object. This method is supported for
		 * the benefit of hashtables such as those provided by
		 * <tt>java.util.Hashtable</tt>.
		 * 
		 * <p>The general contract of <tt>hashCode</tt> is:
		 * <ul>
		 *     <li>
		 *         Whenever it is invoked on the same object more than once
		 *         during an execution of a Java application, the
		 *         <tt>hashCode</tt> method must consistently return the same
		 *         integer, provided no information used in equals comparisons on
		 *         the object is modified. This integer need not remain
		 *         consistent from one execution of an application to another
		 *         execution of the same application.
		 *     </li>
		 *     <li>
		 *         If two objects are equal according to the
		 *         {@link java.lang.Object#equals(Object)} method, then calling
		 *         the <tt>hashCode</tt> method on each of the two objects must
		 *         produce the same integer result.
		 *     </li>
		 *     <li>
		 *         It is <i>not</i> required that if two objects are unequal
		 *         according to the {@link java.lang.Object#equals(Object)}
		 *         method, then calling the <tt>hashCode</tt> method on each of
		 *         the two objects must produce distinct integer results.
		 *         However, the programmer should be aware that producing
		 *         distinct integer results for unequal objects may improve the
		 *         performance of hashtables.
		 *     </li>
		 * </ul></p>
		 * 
		 * @return a hash code value for this object.
		 */
		public int hashCode() {
			return object.hashCode() + (int)CLUSTER_ID;
		}

		/**
		 * The string representation of a classifiable object.
		 *
		 * @return the string representation of a classifiable object.
		 */
		public String toString() {
			return "object: " + object.toString() + "; classified: " + isClassified + "; clusterID: " + CLUSTER_ID;
		}
	}

	/**
	 * Creates a new lazy DBScan cluster operator.
	 *
	 * @param input the input iteration providing the data to be grouped.
	 * @param eps the eps-radius used for range queries.
	 * @param minPts the minimum number of elements that have to be located in
	 *        the eps-neighborhood of a core point.
	 * @param rangeQuery a function performing a range query based on a given
	 *        descriptor.
	 * @param getDescriptor a function delivering a kind of descriptor for a
	 *        range query.
	 * @param newQueue a parameterless function returning a new queue used for
	 *        storing elements with which a range query will be performed.
	 * @param newNoiseQueue a parameterless function delivering a new queue
	 *        holding the noise.
	 * @param newChangedCIDQueue a parameterless function returning a queue that
	 *        gets the elements that belonged to noise, but later are assigned to
	 *        a special cluster.
	 * @throws IllegalArgumentException if a negative value for <tt>eps</tt> or
	 *         <tt>minPts</tt> has been specified.
	 */
	public DBScan(Iterator input, double eps, int minPts, Function rangeQuery, Function getDescriptor, Function newQueue, Function newNoiseQueue, Function newChangedCIDQueue) throws IllegalArgumentException {
		if (eps < 0)
			throw new IllegalArgumentException("cannot compute Eps-Neighborhood with negative eps-distance!");
		if (minPts <= 0)
			throw new IllegalArgumentException("a cluster must contain at least one element.");
		this.input = new Filter(input, isUnclassified);
		this.eps = eps;
		this.minPts = minPts;
		this.queue = (Queue)newQueue.invoke();
		this.noiseQueue = (Queue)newNoiseQueue.invoke();
		this.changedCIDQueue = (Queue)newChangedCIDQueue.invoke();
		this.rangeQuery = rangeQuery;
		this.getDescriptor = getDescriptor;
	}

	/**
	 * Creates a new lazy DBScan cluster operator. Uses a
	 * {@link xxl.core.collections.queues.StackQueue stack-queue} for storing the
	 * elements range queries will be performed with, a
	 * {@link xxl.core.collections.queues.ListQueue list-queue} for noise and a
	 * default {@link xxl.core.collections.queues.FIFOQueue FIFO-queue} for the
	 * elements that changed from the noise cluster to an other cluster.
	 *
	 * @param input the input iteration providing the data to be grouped.
	 * @param eps the eps-radius used for range queries.
	 * @param minPts the minimum number of elements that have to be located in
	 *        the eps-neighborhood of a core point.
	 * @param rangeQuery a function performing a range query based on a given
	 *        descriptor.
	 * @param getDescriptor a function delivering a kind of descriptor for a
	 *        range query.
	 * @throws IllegalArgumentException if a negative value for <tt>eps</tt> or
	 *         <tt>minPts</tt> has been specified.
	 */
	public DBScan(Iterator input, double eps, int minPts, Function rangeQuery, Function getDescriptor) throws IllegalArgumentException {
		this(input, eps, minPts, rangeQuery, getDescriptor, StackQueue.FACTORY_METHOD, ListQueue.FACTORY_METHOD, FIFOQueue.FACTORY_METHOD);
	}

	/**
	 * Opens the cursor, i.e., signals the cursor to reserve resources, open the
	 * input iteration, etc. Before a cursor has been opened calls to methods
	 * like <tt>next</tt> or <tt>peek</tt> are not guaranteed to yield proper
	 * results. Therefore <tt>open</tt> must be called before a cursor's data
	 * can be processed. Multiple calls to <tt>open</tt> do not have any effect,
	 * i.e., if <tt>open</tt> was called the cursor remains in the state
	 * <i>opened</i> until its <tt>close</tt> method is called.
	 * 
	 * <p>Note, that a call to the <tt>open</tt> method of a closed cursor
	 * usually does not open it again because of the fact that its state
	 * generally cannot be restored when resources are released respectively
	 * files are closed.</p>
	 */
	public void open() {
		if (isOpened) return;
		super.open();
		input.open();
		queue.open();
		noiseQueue.open();
		changedCIDQueue.open();
	}
	
	/**
	 * Closes the cursor, i.e., signals the cursor to clean up resources, close
	 * queues, etc. When a cursor has been closed calls to methods like
	 * <tt>next</tt> or <tt>peek</tt> are not guaranteed to yield proper
	 * results. Multiple calls to <tt>close</tt> do not have any effect, i.e.,
	 * if <tt>close</tt> was called the cursor remains in the state
	 * <i>closed</i>.
	 * 
	 * <p>Note, that a closed cursor usually cannot be opened again because of
	 * the fact that its state generally cannot be restored when resources are
	 * released respectively files are closed.</p>
	 */
	public void close () {
		if (isClosed) return;
		super.close();
		input.close();
		queue.close();
		noiseQueue.close();
		changedCIDQueue.close();
	}

	/**
	 * Returns <tt>true</tt> if the iteration has more elements. (In other
	 * words, returns <tt>true</tt> if <tt>next</tt> or <tt>peek</tt> would
	 * return an element rather than throwing an exception.)
	 * 
	 * @return <tt>true</tt> if the cursor has more elements.
	 */
	protected boolean hasNextObject() {
		if (nextCluster != null) // consume last processed cursor completely
			Cursors.consume(nextCluster);
		if (input.hasNext()) {
			CLUSTER_NO = CLUSTER_NO == UNDEFINED ?
				CLUSTER_NO = 0 :
				++CLUSTER_NO; // select new clusterID
			nextCluster = new AbstractCursor() {
				protected Classifiable x;
				protected boolean inputMode = input.hasNext();

				public boolean hasNextObject() {
					while (!queue.isEmpty() || inputMode) {
						x = !queue.isEmpty() ?
							(Classifiable)queue.dequeue() :
							(Classifiable)input.next();
						// range query
						Cursor results = (Cursor)rangeQuery.invoke(getDescriptor.invoke(x, new Double(eps)));
						// check number of elements
						LinkedList list = new LinkedList();
						while (results.hasNext() && list.size() < minPts-1)
							list.add(results.next());
						// x is a core point
						if (results.hasNext()) {
							if (inputMode) {
								x.setClusterID(CLUSTER_NO);
								inputMode = false;
								// insert all objects\{x} into the queue
								Queues.enqueueAll(
									queue,
									// mark all objects in the eps-neighborhood of x with current clusterID
									new Mapper(
										ClusterID_FACTORY(CLUSTER_NO),
										new Filter(
											new Sequentializer(
												list.iterator(),
												results
											),
											new And(
												new LeftBind(
													new Not(
														EqualReference.DEFAULT_INSTANCE
													),
													x
												),
												isUnclassified
											)
										)
									)
								);
							}
							else {
								Cursors.consume(
									new Mapper(
										// mark all objects with current clusterID
										ClusterID_FACTORY(CLUSTER_NO),
										// select all objects in the eps-neighborhood of x that
										new Filter(
											// are not yet classified or
											new Sequentializer(
												list.iterator(),
												results
											),
											// marked as noise;
											new AbstractPredicate() {
												public boolean invoke(Object o) {
													Classifiable c = (Classifiable)o;
													boolean unClassified = !c.isClassified();
													boolean isNoise = c.getClusterID() == NOISE;
													// insert the unclassified objects into the queue
													if (unClassified)
														queue.enqueue(o);
													else
														// changed clusterID
														if (isNoise)
															changedCIDQueue.enqueue(o);
													return unClassified || isNoise;
												}
											}
										)
									)
								);
							}
							next = x;
							results.close();
							return true;
						}
						// x is a border point
						if (inputMode) {
							// mark x as noise and process with input
							x.setClusterID(NOISE);
							noiseQueue.enqueue(x);
							inputMode = input.hasNext();
						}
						else {
							// set x as next result, x has been located in queue
							next = x;
							results.close();
							return true;
						}
					}
					return false;
				}

				public Object nextObject() throws NoSuchElementException {
					return next;
				}
			};
			nextCluster = new Sequentializer(
				nextCluster,
				new QueueCursor(changedCIDQueue)
			);
			if (nextCluster.hasNext())
				return true;
		}
		nextCluster = new Filter(new QueueCursor(noiseQueue), isNoise);
		if (nextCluster.hasNext())  // return all elements of the noiseQueue that are noise
			return true;
		return false;
	}

	/**
	 * Returns the next element in the iteration. This element will be
	 * accessible by some of the cursor's methods, e.g., <tt>update</tt> or
	 * <tt>remove</tt>, until a call to <tt>next</tt> or <tt>peek</tt> occurs.
	 * This is calling <tt>next</tt> or <tt>peek</tt> proceeds the iteration and
	 * therefore its previous element will not be accessible any more.
	 * 
	 * @return the next element in the iteration.
	 */
	protected Object nextObject() {
		return nextCluster;
	}

	/**
	 * Resets the DBScan-cursor to its initial state such that the caller is
	 * able to traverse the underlying data structure again without constructing
	 * a new cursor (optional operation). The modifications, removes and updates
	 * concerning the underlying data structure, are still persistent.
	 * 
	 * <p>Note, that this operation is optional and might not work for all
	 * cursors.</p>
	 *
	 * @throws UnsupportedOperationException if the <tt>reset</tt> operation is
	 *         not supported by the cursor.
	 */
	public void reset () throws UnsupportedOperationException{
		input.reset();
		queue.clear();
		noiseQueue.clear();
		changedCIDQueue.clear();
		CLUSTER_NO = UNDEFINED;
	}

	/**
	 * Returns <tt>true</tt> if the <tt>reset</tt> operation is supported by
	 * the DBScan-cursor. Otherwise it returns <tt>false</tt>.
	 *
	 * @return <tt>true</tt> if the <tt>reset</tt> operation is supported by
	 *         the DBScan-cursor, otherwise <tt>false</tt>.
	 */
	public boolean supportsReset() {
		return input.supportsReset();
	}
}
