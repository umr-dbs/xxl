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

package xxl.core.spatial.points;

import java.io.File;
import java.util.Iterator;

import xxl.core.cursors.Cursor;
import xxl.core.cursors.Cursors;
import xxl.core.cursors.mappers.Mapper;
import xxl.core.functions.AbstractFunction;
import xxl.core.spatial.cursors.PointInputCursor;
import xxl.core.spatial.rectangles.DoublePointRectangle;
import xxl.core.spatial.rectangles.FloatPointRectangle;
import xxl.core.spatial.rectangles.Rectangle;


/**
 *	A utility-class that provides useful static methods for Points like distance methods
 *  and universe computations.
 *
 *  @see xxl.core.spatial.points.Point
 *  @see xxl.core.spatial.points.FloatPoint
 *  @see xxl.core.spatial.points.DoublePoint
 *  @see xxl.core.spatial.points.FixedPoint
 *
 */
public abstract class Points{

	/** Computes the L_p-distance of two Points.
	 *
	 *	@param p1 Point 1
	 *	@param p2 Point 2
	 *	@param p the value of p
	 * 
	 *  @return returns the L_p-distance
	 */
	public static double lpDistance(Point p1, Point p2, int p){
		return Math.pow( lpDistanceWithoutRoot(p1, p2, p), 1.0/p);
	}

	/** Computes the L_p-distance of two Points WITHOUT performing the final 1/p power-operation.
	 *
	 *	This makes sense in situations where (|p1,p2|)^p is available which is often the case
	 *	in ranking algorithms. By using this method one can save calls to the expensive Math.pow-operation.
	 *
	 *	@param p1 Point 1
	 *	@param p2 Point 2
	 *	@param p value of p
	 * 
	 *  @return returns the L_p-distance of two points without performing 1/p power-operation
	 */
	public static double lpDistanceWithoutRoot(Point p1, Point p2, int p){
		double ret = 0.0;
		for(int d=0; d<p1.dimensions(); d++){
			ret += Math.pow( Math.abs( p1.getValue(d) - p2.getValue(d) ), p );
		}
		return ret;
	}

	/** Computes the maximum-distance (=L_\infty-metric) between two given Points.
	 *  Calculates the difference between coordinates for each dimension and returns maximum 
	 *
	 *	@param p1 first Point 
	 *	@param p2 second Point 
	 *
	 *  @return returns the maximum coordinate difference for two points given
	 */
	public static double maxDistance(Point p1, Point p2){
		double ret = 0.0;
		for(int d=0; d<p1.dimensions(); d++){
			ret = Math.max(ret, Math.abs( p1.getValue(d) - p2.getValue(d) ));
		}
		return ret;
	}

	/** Checks whether the the maximum-distance (=L_\infty-metric) of two Points is less equal than the given distance.
	 *	This method will return true if the coordinate difference between the points in
	 *  any dimension is not bigger than the given distance 
	 *
	 *	@param p1 Point 1
	 *	@param p2 Point 2
	 *	@param distance the query distance
	 *
	 *	@see xxl.core.predicates.Predicate 
	 *	@see xxl.core.predicates.DistanceWithin
	 *  
	 *  @return returns true if the coordinate difference between the points in
	 *  any dimension is not bigger than the given distance
	 */
	public static boolean withinMaximumDistance(Point p1, Point p2, double distance){
		for(int d=0; d<p1.dimensions(); d++){
			if( Math.abs( p1.getValue(d) - p2.getValue(d) ) > distance )
				return false;
		}
		return true;
	}

	/** A factory-method that returns an input-Iterator for FixedPoints
	 *	of the given dimensionality.
	 *
	 * @param file the input flat file containing the FixedPoints
	 * @param dim the dimensionality of the FixedPoints
	 * 
	 * @return returns an input-Iterator for FixedPoints of the given dimensionality.
	 */
	public static Cursor newFixedPointInputCursor(final File file, final int dim){
		return new PointInputCursor(file, 2, dim);
	}

	/** A factory-method that returns an input-Iterator for FloatPoints
	 *	of the given dimensionality.
	 *
	 * @param file the input flat file containing the FloatPoints
	 * @param dim the dimensionality of the FloatPoints
	 * 
	 * @return returns an input-Iterator for FloatPoints of the given dimensionality.
	 */
	public static Cursor newFloatPointInputCursor(final File file, final int dim){
		return new PointInputCursor(file, 1, dim);
	}
	
	/** A factory-method that returns an input-Iterator for DoublePoints
	 *	of the given dimensionality 
	 *
	 * @param file the input flat file containing the DoublePoints
	 * @param dim the dimensionality of the DoublePoints
	 * 
 	 * @return returns an input-Iterator for DoublePoints of the given dimensionality.
	 */
	public static Cursor newDoublePointInputCursor(final File file, final int dim){
		return new PointInputCursor(file, 0, dim);
	}

	/** Transforms a given Iterator of DoublePoints to the unit-cube [0;1)^dim assuming that
	 *	all Points are inside the given universe-DoublePointRectangle and that
	 *	the Points are of type DoublePoint.
	 *
	 *	@param iterator an Iterator containing DoublePoints
	 *	@param universe the minimal bounding rectangle of the Points
	 *
	 *  @return cursor returning input DoublePoints converted to to the DoublePoints points in unit-cube [0;1)^dim
	 */	
	public static Cursor mapToUnitCubeDouble(final Iterator iterator, final Rectangle universe){
		return new Mapper( new AbstractFunction(){
				protected double[] ll = (double[]) universe.getCorner(false).getPoint();	//lower-left corner
				protected double[] ur = (double[]) universe.getCorner(true).getPoint();		//upper-right corner

				public Object invoke(Object object){
					double[] point = (double[]) ((Point) ((DoublePoint) object).getPoint()).clone();//clone the input in order to avoid strange side-effects

					for(int i=0; i < point.length; i++){
						//scale to unit-cube:
						point[i] = (point[i]-ll[i]) / (ur[i]-ll[i]);	//new coordinate = point-leftBorder of MBR /(extension of MBR in actual dimension)

						//ensure that value is in [0;1):
						point[i] = Math.min(Math.max(0, point[i]), 0.9999999999999999);
					}

					return new DoublePoint(point);	//create a new DoublePoint (to avoid side-effects);
				}		
			}
			, iterator
		);		
	}

	/** Transforms a given Iterator of FloatPoints to the unit-cube [0;1)^dim assuming that
	 *	all Points are inside the given universe-FloatPointRectangle and that
	 *	the Points are of type FloatPoint.
	 *
	 *	@param iterator an Iterator containing FloatPoints
	 *	@param universe the minimal bounding rectangle of the Points
	 *  
	 *  @return cursor returning input FloatPoints converted to to the FloatPoints points in unit-cube [0;1)^dim
	 */	
	public static Cursor mapToUnitCubeFloat(final Iterator iterator, final Rectangle universe){
		return new Mapper( new AbstractFunction(){
				protected float[] ll = (float[]) universe.getCorner(false).getPoint();		//lower-left corner
				protected float[] ur = (float[]) universe.getCorner(true).getPoint();		//upper-right corner

				public Object invoke(Object object){
					float[] point = (float[]) ((Point) ((FloatPoint) object).getPoint()).clone();//clone the input in order to avoid strange side-effects

					for(int i=0; i < point.length; i++){
						//scale to unit-cube:
						point[i] = (point[i]-ll[i]) / (ur[i]-ll[i]);	//new coordinate = point-leftBorder of MBR /(extension of MBR in actual dimension)

						//ensure that value is in [0;1):
						point[i] = Math.min(Math.max(0, point[i]), 0.999999f);
					}

					return new FloatPoint(point);	//create a new FloatPoint (to avoid side-effects);
				}		
			}
			, iterator
		);		
	}

	/** Computes the minimal bounding rectangle (i.e. the "universe") of an Iterator of DoublePoint-objects.
	 *	Note that this method is blocking since it has to consume the entire input-Iterator
	 *	in order to compute the minimal bounding rectangle of the Points.
	 *
	 *	@param input an Iterator containing DoublePoints
	 *  @return returns DoublePointRectangle which is the MBR of the points given
	 */
	public static Rectangle universeDouble(final Iterator input){
		if(!input.hasNext())
			throw new IllegalArgumentException("input-Iterator is empty!");

		Cursor cursor = Cursors.wrap(input);
		final int dim = ((DoublePoint)cursor.peek()).dimensions();
		double[] ll = (double[]) ((Point) ((DoublePoint)cursor.peek()).getPoint()).clone();
		double[] ur = (double[]) ((Point) ((DoublePoint)cursor.peek()).getPoint()).clone();
	
		while(input.hasNext()){
			double[] point = (double[]) ((DoublePoint)input.next()).getPoint();	
			for(int i=0; i<dim; i++){
				ll[i] = Math.min(ll[i], point[i]);	
				ur[i] = Math.max(ur[i], point[i]);	
			}
		}
		DoublePoint llp = new DoublePoint(ll);
		DoublePoint urp = new DoublePoint(ur);
		return new DoublePointRectangle(llp, urp);	//return the computed universe
	}

	/** Computes the minimal bounding rectangle (i.e. the "universe") of an Iterator of FloatPoint-objects.
	 *	Note that this method is blocking since it has to consume the entire input-Iterator
	 *	in order to compute the minimal bounding rectangle of the Points.
	 *
	 *	@param input an Iterator containing FloatPoints
	 *  @return returns FloatPointRectangle which is the MBR of the points given
	 */
	public static Rectangle universeFloat(final Iterator input){
		if(!input.hasNext())
			throw new IllegalArgumentException("input-Iterator is empty!");

		Cursor cursor = Cursors.wrap(input);
		final int dim = ((FloatPoint)cursor.peek()).dimensions();
		float[] ll = (float[]) ((Point) ((FloatPoint)cursor.peek()).getPoint()).clone();
		float[] ur = (float[]) ((Point) ((FloatPoint)cursor.peek()).getPoint()).clone();
	
		while(input.hasNext()){
			float[] point = (float[]) ((FloatPoint)input.next()).getPoint();	
			for(int i=0; i<dim; i++){
				ll[i] = Math.min(ll[i], point[i]);	
				ur[i] = Math.max(ur[i], point[i]);	
			}
		}
		FloatPoint llp = new FloatPoint(ll);
		FloatPoint urp = new FloatPoint(ur);
		return new FloatPointRectangle(llp, urp);	//return the computed universe
	}
}
