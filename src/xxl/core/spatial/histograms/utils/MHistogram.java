/* XXL: The eXtensible and fleXible Library for data processing

Copyright (C) 2000-2013 Prof. Dr. Bernhard Seeger
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
package xxl.core.spatial.histograms.utils;

import java.io.IOException;
import java.util.List;
import java.util.Properties;



import xxl.core.cursors.Cursor;
import xxl.core.spatial.rectangles.DoublePointRectangle;


/**
 *  
 * This is a simple interface for spatial histograms. 
 * Histogram buckets are represented using WeightedDoublePointRectangle.  
 *
 */
public interface MHistogram {
	/**
	 * computes spatial selectivity
	 */
	public double getSelectivity(DoublePointRectangle queryRec);
	/**
	 * builds histogram
	 * @param rectangles
	 * @param numberOfBuckets
	 * @param props
	 */
	public void buildHistogram(Cursor<DoublePointRectangle> rectangles, int numberOfBuckets, Properties props) throws IOException;
	/**
	 * 
	 * @return number of buckets in  this histogram
	 */
	public int numberOfBuckets();
	/**
	 * 
	 * @return a list of {@link SpatialHistogramBucket}
	 */
	public List<SpatialHistogramBucket> getBuckets();
}
