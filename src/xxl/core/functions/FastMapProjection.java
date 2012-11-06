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

package xxl.core.functions;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import xxl.core.cursors.identities.TeeCursor;
import xxl.core.math.Maths;
import xxl.core.util.Distance;

/**
 * This class provides a projection from any object-space with a given distance
 * function to a k-dimensional real valued metric space (using the euklidean
 * distance function, also known as L2-Metric) using the FastMap algorithm.
 * The implementation is based upon [FL95 ]: Christos Faloutsos, King-Ip Lin.
 * FastMap: A Fast Algorithm for Indexing, Data-Mining and Visualization
 * of Traditional and Multimedia Datasets. SIGMOD Conference 1995. 163-174.
 * 
 * <p>Due to the lack of an efficient memory management in java the original
 * algorithm is modified. Instead of computing and storing a distance matrix in
 * advance only the pivot objects of each dimension are computed in advance and
 * the modified distance function for each dimension will be recursivly
 * evaluated.</p>
 * 
 * @param <T> the type of the objects to be projected, i.e., the object-space.
 */
@SuppressWarnings("serial")
public class FastMapProjection<T> extends AbstractFunction<T, double[]> {

	/**
	 * The chosen dimension.
	 */
	protected int dim;

	/**
	 * Stores the computed pivot elements (left) for the dimension denoted by
	 * the index.
	 */
	protected List<T> a;

	/**
	 * Stores the computed pivot elements (right) for the dimension denoted by
	 * the index.
	 */
	protected List<T> b;

	/**
	 * The distance function used for the object space.
	 */
	protected Distance<? super T> dist;

	/**
	 * Stores the distances between the pivot-objects. d[k] is the distance
	 * between a[k] and b[k]. Used for effectiveness.
	 */
	protected double[] d;

	/**
	 * Given objects of the object space. A
	 * {@link xxl.core.cursors.identities.TeeCursor tee cursor}* is used
	 * because in order to compute the pivot elements in advance one must scan
	 * the data <code>2*k</code> times, i.e. the complexity of locating the
	 * pivot elements is <code>O(2kn)</code> whereas <code>n</code> denotes the
	 * number of objects in the {@link xxl.core.cursors.Cursor cursor}.
	 *
	 * @see xxl.core.cursors.Cursor
	 * @see xxl.core.cursors.identities.TeeCursor
	 */
	protected TeeCursor<? extends T> data;

	/**
	 * Indicates whether the tee cursor has been used at least one time.
	 */
	protected boolean teecursorInit = false;

	/**
	 * Constructs a new FastMap Projection based upon the data given.
	 * 
	 * @param data init data.
	 * @param dimension dimension to project data to.
	 * @param dist distance function for the kind of data given in data.
	 * @throws IllegalArgumentException if the given dimension isn't at least 1
	 *         or the given tee cursor doesn't support multiple access.
	 *
	 * @see xxl.core.cursors.identities.TeeCursor
	 * @see xxl.core.util.Distance
	 */
	public FastMapProjection(TeeCursor<? extends T> data, int dimension, Distance<? super T> dist) throws IllegalArgumentException {
		dim = dimension ;
		if (dim <= 0)
			throw new IllegalArgumentException("Dimension to project to must be one or greater! (given=" + dim + ")");
		a = new ArrayList<T>(dim);
		b = new ArrayList<T>(dim);
		d = new double[dim];
		this.dist = dist;
		this.data = data;
		try {
			init();
		}
		catch (IllegalStateException e) {
			throw new IllegalArgumentException("Used teecursor must support multiple access!");
		}
	}

	/**
	 * Constructs a new FastMap Projection based upon the data given. The given
	 * data will be wrapped by a tee cursor in order to support multiple access
	 * to the data. For default a TeeCursor is used.
	 * 
	 * @param data init data. Will be wrapped by a tee cursor to support
	 *        multiple access.
	 * @param dimension dimension to project data to.
	 * @param dist distance function for the kind of data given.
	 * @throws IllegalArgumentException if the given dimension isn't at least
	 *         1.
	 *
	 * @see xxl.core.cursors.identities.TeeCursor
	 * @see xxl.core.cursors.identities.TeeCursor
	 * @see xxl.core.util.Distance
	 */
	public FastMapProjection(Iterator<? extends T> data, int dimension, Distance<? super T> dist) throws IllegalArgumentException {
		this(new TeeCursor<T>(data), dimension, dist);
	}

	/**
	 * Returns a projection to a k-dimensional real valued space of the given
	 * Object as an object of type <code>double[k]</code>.
	 * 
	 * @param x object to project to a subspace.
	 * @return projected object (represented by a <code>double[]</code> of
	 *         dimension <code>k</code>.)
	 */
	@Override
	public double[] invoke(T x) {
		double[] re = new double[dim];
		
		for (int i = 0; i < dim ; i++) {
			re[i] = p(i, x);
			
			if (Double.isInfinite(re[i]) || Double.isNaN(re[i])) {
				re[i] = 0;
			}
		}
		
		return re;
	}

	/**
	 * Determines the pivot objects for each dimension needed. The complexity
	 * of the initialization is <code>O(2kn)</code> where <code>n</code>
	 * denotes the number of objects in the given
	 * {@link xxl.core.cursors.identities.TeeCursor tee cursor} and
	 * <code>k</code> the given dimensions.
	 * 
	 * @throws IllegalStateException if the
	 *         {@link xxl.core.cursors.identities.TeeCursor tee cursor} doesn't
	 *         support adequate multiple access.
	 */
	protected void init() throws IllegalStateException {
		T p = null;
		if (data.hasNext())
			p = data.next();
		for (int k = 0; k < dim; k++) {
			b.add(getFurthermost(k, p));
 			a.add(getFurthermost(k, b.get(k)));
 			d[k] = d(k, a.get(k), b.get(k));
 		}
	}

	/**
	 * Returns the furthermost object wrt to p.
	 * 
	 * @param k specifies the aggregation level of the distance function used
	 *        to compute the furthermost object.
	 * @param p reference object.
	 * @throws IllegalStateException if the tee cursor doesn't support adequate
	 *         multiple access.
	 * @return the furthermost object wrt to p.
	 */
 	protected T getFurthermost(int k, T p) throws IllegalStateException {
 		Iterator<? extends T> it = null;
 		if (!teecursorInit){
 			it = data;
 			teecursorInit = true;
		}
 		else
 			it = data.cursor();
 		T fm  = p;
 		T tmp = null;
		while (it.hasNext()) {
 			tmp = it.next();
 			if (d(k, p, fm) < d(k, p, tmp))
 				fm = tmp;
 		}
 		return fm;	
 	}

	/**
	 * Returns the projected distance between the objects l and r.
	 * 
	 * @param k Specifies the aggregation level of the distance function.
	 * @param l left object for distance computing.
	 * @param r right object for distance computing.
	 * @return the modified distance between two objects of given aggregation
	 *         level.
	 */
	protected double d(int k, T l, T r) {
		double re = 0.0;
		if (k > 0)
			re = Math.sqrt(Math.pow(d(k-1, l, r), 2.0) - Math.pow(Maths.pDistance(p(k-1, l), p(k-1, r), 2), 2.0));
		else
			re = dist.distance(l, r);
		return re;
	}

	/**
	 * Returns the k-th component of the FastMap-Projection of o.
	 * 
	 * @param o the object to project.
	 * @param k dimension of the subspace the object is projected to.
	 * @return the k-th component of the projection of object o.
	 */
	protected double p(int k, T o) {
		double dab = d[k];
		double dai = d(k,  a.get(k), o);
		double dbi = d(k,  b.get(k), o);
		return (Math.pow(dai, 2.0) + Math.pow(dab, 2.0) - Math.pow(dbi, 2.0)) / (2.0 * dab);
	}
}
