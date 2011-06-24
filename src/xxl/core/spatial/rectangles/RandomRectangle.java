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

package xxl.core.spatial.rectangles;

import java.util.Random;

import xxl.core.cursors.AbstractCursor;
import xxl.core.cursors.Cursors;
import xxl.core.cursors.mappers.Mapper;
import xxl.core.cursors.sources.ArrayCursor;
import xxl.core.functions.AbstractFunction;
import xxl.core.functions.ArrayFactory;
import xxl.core.spatial.points.DoublePoint;
import xxl.core.util.random.ContinuousRandomWrapper;
import xxl.core.util.random.JavaContinuousRandomWrapper;

/**
 *	A Cursor that returns randomly created Rectangles (of type Rectangle).
 *
 */
public class RandomRectangle extends AbstractCursor{

	/** The extension of a rectangle in each dimension.
	 */
	protected double d;

	/** The dimensionality of the rectangle
	 */
	protected int dim;

	/** The number of rectangles to create
	 */
	protected int number;

	/** 1 minus the attribute d (used for internal computations)
	 */
	protected double _1minusd;

	/** The ContinuousRandomWrappers to use.
	 */
	protected ContinuousRandomWrapper[] randomWrapper;
	
	/** Creates a new RandomRectangle returning <code>number</code> rectangles of dimension
	 *	<code>dim</code> and extension <code>d</code> in each dimension.
	 *
	 * @param d the extension of a rectangle in each dimension
	 * @param dim the dimensionality of the rectangle
	 * @param number the number of rectangles to create
	 * @param randomWrapper the ContinuousRandomWrappers to use
	 */
	public RandomRectangle(double d, int dim, int number, ContinuousRandomWrapper[] randomWrapper){
		this.d = d;
		this.dim = dim;
		this.number = number;
		this.randomWrapper = randomWrapper;
		this._1minusd = (1-d);
	}

	/** Creates a new RandomRectangle returning <code>number</code> rectangles of dimension
	 *	<code>dim</code> and extension <code>d</code> in each dimension.
	 *
	 * @param d the extension of a rectangle in each dimension
	 * @param dim the dimensionality of the rectangle
	 * @param number the number of rectangles to create
	 * @param random the Random enginge to use to use
	 */
	public RandomRectangle(double d, int dim, int number, Random[] random){
		this(d,dim, number, 
			(JavaContinuousRandomWrapper[]) Cursors.toArray(
				new Mapper(
					new AbstractFunction(){
						public Object invoke(Object object){
							return new JavaContinuousRandomWrapper((Random)object);
						}
					}
					, new ArrayCursor<Random>(random)
				),
				new JavaContinuousRandomWrapper[random.length]
			)
		);
	}

	/** Creates a new RandomRectangle returning <code>number</code> rectangles of dimension
	 *	<code>dim</code> and extension <code>d</code> in each dimension.
	 *
	 * @param d the extension of a rectangle in each dimension
	 * @param dim the dimensionality of the rectangle
	 * @param number the number of rectangles to create
	 */
	public RandomRectangle(double d, final int dim, int number){
		this(d, dim, number, 
			(Random[])new ArrayFactory(
				new AbstractFunction(){
					public Object invoke(Object object){
						return new Random[dim];
					}
				},
				new AbstractFunction(){
					int t = 4;
					
					public Object invoke(Object object){
						return new Random(System.currentTimeMillis()+(t+=1234)); 
					}
				}
			).invoke()
		);
	}
	
	/** 
	 * If the needed number of rectangles was not reached yet 
	 * computes the next rectangle and returns <tt>true</tt> 
	 * otherwise returns <tt>false</tt>
	 * 
	 * @return <tt>true</tt> if next rectangle were created and <tt>false</tt> 
	 *          if needed number of rectangles was reached already. 
	 */
	public boolean hasNextObject(){
		if (number-->0){
			double[] leftBorders = new double[dim];
			double[] rightBorders = new double[dim];
			for(int i=0; i<dim; i++){
				leftBorders[i] = randomWrapper[i].nextDouble()*_1minusd;
				rightBorders[i] = leftBorders[i]+d;
			}
			DoublePoint leftPoint = new DoublePoint(leftBorders);
			DoublePoint rightPoint = new DoublePoint(rightBorders);
			next =  new DoublePointRectangle(leftPoint, rightPoint);
			return true;
		}
		return false;
	}
	
	/**
	 * Returns the next element in the iteration. This element will be
	 * accessible by some of the cursor's methods, e.g., <tt>update</tt> or
	 * <tt>remove</tt>, until a call to <tt>next</tt> or <tt>peek</tt> occurs.
	 * This is calling <tt>next</tt> or <tt>peek</tt> proceeds the iteration and
	 * therefore its previous element will not be accessible any more.
	 * 
	 * <p>This abstract operation should implement the core functionality of
	 * the <tt>next</tt> method which secures that the cursor is in a proper
	 * state when this method is called. Due to this the <tt>nextObject</tt>
	 * method need not to deal with exception handling.</p>
	 *
	 * @return the next element in the iteration.
	 */
	public Object nextObject() {
		return next;
	}
}
