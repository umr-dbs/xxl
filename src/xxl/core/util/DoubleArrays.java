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

package xxl.core.util;

import java.util.Arrays;
import java.util.Iterator;

import xxl.core.cursors.sources.Inductors;

/**
 * Static methods for dealing with <tt>double[]</tt> as numerical vectors.
 */

public class DoubleArrays {

	/** not instances and no inheritage will be allowed */
	private DoubleArrays () {}

	/**
	* Computes the minumum value of a given double array
	* @param d double array wich will be scanned to find the min value
	* @return the smallest double value in the given array
	*/
	public static double min ( double[] d){
		double r = d[0];
		for (int i=1 ; i< d.length; i++){
			if (d[i] < r) r = d[i];
		}
		return r;
	}

	/**
	* Computes the maximun value of a given double array
	* @param d double array wich will be scanned to find the max value
	* @return the biggest double value in the given array
	*/
	public static double max ( double[] d){
		double r = d[0];
		for (int i=1 ; i< d.length; i++){
			if (d[i] > r) r = d[i];
		}
		return r;
	}

	/**
	* Computes the minumum values of two given double arrays
	* @param d1 double array wich will be scanned to find the min value
	* @param d2 double array wich will be scanned to find the min value
	* @throws IllegalArgumentException if the length of the <TT>double[]</TT> don't match
	* @return the smallest double values in the given arrays
	*/
	public static double[] min ( double[] d1, double[] d2) throws IllegalArgumentException{
		if ( d1.length != d2.length) throw new IllegalArgumentException("dimensions must match");
		double [] r = ( double [] ) d1.clone();
		for (int i=0 ; i< d1.length; i++){
			if ( r[i] > d2[i]) r[i] = d2[i];
		}
		return r;
	}

	/**
	* Computes the maximum values of two given double arrays
	* @param d1 double array wich will be scanned to find the min value
	* @param d2 double array wich will be scanned to find the min value
	* @throws IllegalArgumentException if the length of the double[] don't match
	* @return the biggest double values in the given arrays
	*/
	public static double[] max ( double[] d1, double[] d2) throws IllegalArgumentException{
		if ( d1.length != d2.length) throw new IllegalArgumentException("dimensions must match");
		double [] r = ( double [] ) d1.clone();
		for (int i=0 ; i< d1.length; i++){
			if ( r[i] < d2[i]) r[i] = d2[i];
		}
		return r;
	}

/* ------------------------------------------------------------------- */
/* --- Methods for handling with double[] as numerical vectors! ------ */
/* ------------------------------------------------------------------- */

	/**
	* Adds two <TT>double[]</TT> componentwise.
	* Both arguments must be of the same length!
	* @param a First <TT>double[]</TT> to add.
	* @param b Second <TT>double[]</TT> to add.
	* @return The componentwise addition of the parameters.
	*/
	public static double[] add ( double[] a, double[] b){
		double[] r = new double[a.length];
		for (int i=0; i < r.length; i++) r[i] = a[i] + b[i];
		return r;
	}


	/**
	* Calculates the difference of two <TT>double[]</TT> componentwise.
	* Both arguments must be of the same length!
	* @param a First <TT>double[]</TT> subtract from.
	* @param b Second <TT>double[]</TT> subtract of the first <TT>double[]</TT>.
	* @return The componentwise subtraction of the parameters.
	*/
	public static double[] substract ( double[] a, double[] b){
		double[] r = new double[a.length];
		for (int i=0; i < r.length; i++) r[i] = a[i] -b[i];
		return r;
	}


	/**
	* Squares a <TT>double[]</TT> componentwise.
	* @param data <TT>double[]</TT> to square
	* @return The componentwise squared <TT>double[]</TT>.
	*/
	public static double[] square ( double[] data){
		double[] r = new double[data.length];
		for (int i=0; i < r.length; i++) r[i] = data[i] * data[i];
		return r;
	}

	/**
	* Computes the square root of a <TT>double[]</TT> componentwise.
	* @param data <TT>double[]</TT> to compute the square root from.
	* @return the componentwise computed square root of the <TT>double[]</TT>.
	*/
	public static double[] sqrt ( double[] data){
		double[] r = new double[data.length];
		for (int i=0; i < r.length; i++) r[i] = Math.sqrt (data[i]);
		return r;
	}


	/**
	* Computes the product of the given double with the given <TT>double[]</TT> componentwise
	* ,i.e., a scalar vector multiplication. (3 * ( 1, 2 ,3)' = ( 3, 6, 9)' )
	* @param scalar Scalar to multiplicate with data
	* @param data <TT>double[]</TT> to multiplicate with scalar
	* @return The scalar vector product
	*/
	public static double[] mult( double scalar, double[] data){
		double[] r = new double[data.length];
		for (int i=0; i < r.length; i++) r[i] = scalar * data[i];
		return r;
	}


	/**
	* Computes the product of the given double with the given <TT>double[]</TT> componentwise
	* ,i.e., a scalar vector multiplication. (3 * ( 1, 2 ,3)' = ( 3, 6, 9)' )
	* @param scalar Scalar to multiplicate with data
	* @param data <TT>double[]</TT> to multiplicate with scalar
	* @return The scalar vector product
	*/
	public static double[] mult( double[] data,  double scalar){
		return mult( scalar, data);
	}


	/**
	* Computes the product of the two given <TT>double[]</TT> componentwise. 
	* Example: (( 3, 2, 1)'  * ( 1, 2 ,3)' = ( 3, 4, 3)' )
	* Both <TT>double[]</TT> need to be of the same length
	* @param d1 <TT>double[]</TT> to multiplicate componentwise
	* @param d2 <TT>double[]</TT> to multiplicate componentwise
	* @return The componentwise multiplicated <TT>double[]</TT>
	*/
	public static double[] mult( double[] d1, double[] d2){
		double[] r = new double[d1.length];
		for (int i=0; i < r.length; i++) r[i] = d1[i] * d2[i];
		return r;
	}

/* ------------------------------------------------------------------- */
/* ----------------- Mixed operators : int[] + double[] -------------- */
/* ------------------------------------------------------------------- */

	/**
	* Computes the product of a given <TT>int[]</TT> and a given <TT>double[]</TT> componentwise. 
	* Example: (( 3, 2, 1)'  * ( 1.1, 2.1 ,3.1)' = ( 3.3, 4.2, 3.1)' )
	* Both arrays had to be of the same length.
	* @param i <TT>int[]</TT> to multiplicate componentwise
	* @param d <TT>double[]</TT> to multiplicate componentwise
	* @return The componentwise multiplicated <TT>double[]</TT>
	*/
	public static double[] mult( int[] i, double[] d){
		double[] r = new double[d.length];
		for (int j=0; j < r.length; j++) r[j] = i[j] * d[j];
		return r;
	}


	/**
	* Computes the product of a given <TT>int[]</TT> and a given <TT>double[]</TT> componentwise. 
	* Example: (( 3, 2, 1)'  * ( 1.1, 2.1 ,3.1)' = ( 3.3, 4.2, 3.1)' )
	* Both arrays had to be of the same length.
	* @param i <TT>int[]</TT> to multiplicate componentwise
	* @param d <TT>double[]</TT> to multiplicate componentwise
	* @return The componentwise multiplicated <TT>double[]</TT>
	*/
	public static double[] mult( double[] d, int[] i){
		return mult ( i, d);
	}


	/**
	* Adds a given <TT>int[]</TT> and a given <TT>double[]</TT> componentwise.
	* Both arguments must be of the same length!
	* @param a <TT>int[]</TT> to add.
	* @param b <TT>double[]</TT> to add.
	* @return The componentwise addition of the parameters.
	*/
	public static double[] add ( int[] a, double[] b){
		double[] r = new double[a.length];
		for (int i=0; i < r.length; i++) r[i] = a[i] + b[i];
		return r;
	}


	/**
	* Adds a given <TT>int[]</TT> and a given <TT>double[]</TT> componentwise.
	* Both arguments have to be of the same length!
	* @param a <TT>int[]</TT> to add.
	* @param b <TT>double[]</TT> to add.
	* @return The componentwise addition of the parameters.
	*/
	public static double[] add( double[] b, int[] a){
		return add ( a, b);
	}

	/**
	* Computes the inverse product of a given <TT>double[]</TT> to a given <TT>int[]</TT> componentwise. 
	* Example: (( 3.3, 2.5, 5.5)'  / ( 3, 1 , 2)' = ( 1.0, 2.5, 2.25)' )
	* Both arrays have to be of the same length.
	* @param dividend <TT>double[]</TT> 
	* @param divisor <TT>int[]</TT> 
	* @return The componentwise divided <TT>double[]</TT>
	*/
	public static double[] divide ( double[] dividend, int [] divisor){
		double[] r = new double [ dividend.length ];
		for (int j = 0; j < r.length ; j++ ) r[j] =  dividend[j] / divisor [j];
		return r;
	}


	/** Returns a grid of equidistant points between two given borders
	 * containing n values (start and end values inclusivly)
	 * @param start left-handed border of the grid
	 * @param end right-handed border of the grid
	 * @param n number of grid points 
	 * @return a <TT>double[]</TT> containing grid points as a = x_0 < x_1 < ... < x_(n-1) = b with |x_i - x_(i-1)| = c for i=1, ... , n-1
	 * @throws IllegalArgumentException if lower border is not bigger as an upper one or the number of grid points is to small
	 */
	public static double [] equiGrid ( double start, double end, int n) throws IllegalArgumentException {
		if ( end <= start) throw new IllegalArgumentException ("lower border must be bigger than upper border (" + end +" !> " + start +")");
		if ( n < 2) throw new IllegalArgumentException ("the number of grid points must be at least 2 or bigger");
		double [] r = new double [n];
		double stepWidth = (end - start) / (n-1);
		for ( int i = 0 ; i < n-1 ; i++){
			r [i] = start + i * stepWidth;
		}
		r [n-1] = end;
		return r;
	}

	/** Returns a sequence of n-dim data points.
	 * @param start starting vector
	 * @param end ending vector (exclusivly)
	 * @param resolution number of computed values for each dimension
	 * @return a iterator containing Objects of type <tt>double[]</tt>
	 * representing the counter objects.
	 */
	public static Iterator realGrid ( final double [] start, final double [] end, final int [] resolution  ){
		final double [] steps = DoubleArrays.divide ( DoubleArrays.substract ( end, start), resolution);
		final Iterator counter = Inductors.nDimCounter ( resolution);

		return new Iterator(){
			protected double [] next = start;
			public boolean hasNext() {
				return counter.hasNext();
			}
			public Object next() {
				int [] point = ( int [] ) counter.next();
				next = DoubleArrays.add ( start, DoubleArrays.mult ( point, steps));
				return next;
			}
			public void remove() throws UnsupportedOperationException{
				throw new UnsupportedOperationException("remove not supported!");
			}
		};
	}

	/** Returns a sequence of n-dim data points.
	 * @param start starting vector
	 * @param end ending vector (exclusivly)
	 * @param resolution number of computed values for each dimension (using the same for each dimension)
	 * @return a iterator containing Objects of type <tt>double[]</tt>
	 * representing the counter objects.
	 */
	public static Iterator realGrid ( final double [] start, final double [] end, int resolution  ){
		int [] res = new int [start.length];
		Arrays.fill ( res, resolution);
		return realGrid ( start, end, res);
	}
}
