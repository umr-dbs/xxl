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
package xxl.core.spatial;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;

import xxl.core.cursors.Cursor;
import xxl.core.cursors.Cursors;
import xxl.core.cursors.sources.io.FileInputCursor;
import xxl.core.io.converters.ConvertableConverter;
import xxl.core.spatial.rectangles.DoublePointRectangle;


public class TestM4 {
	
	
	
	public static PrintStream getPrintStream(String output) throws IOException{
		return new PrintStream(new File(output)); 
	}

	/**
	 * 
	 * Assumption data is a set of 2 Dim doublePointRectangles 
	 * 
	 * Test data can be obtained from:
	 * 
	 * www.mathematik.uni-marburg.de/~achakeye/data/data
	 * 
	 * Query rectangles from:
	 * 
	 * www.mathematik.uni-marburg.de/~achakeye/data/query_100
	 * 
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		String dataPath = "f:/rtree/data/"; // data path change for your 
		String queryPath ="f:/rtree/query_100/"; // change 
		String path = "f:/hist/"; // change
		String[] prefix  = {
			"rea", // rea data set 
		};
		int[] buckets = {1000};
		for(String p :prefix){
			System.out.println("++++++++++++++++++++++++++++++++++++\n");
			System.out.println("Data: " + p);
			FileInputCursor<DoublePointRectangle> data = new FileInputCursor<DoublePointRectangle>(
					new ConvertableConverter<DoublePointRectangle>(SpatialUtils.factoryFunction(2)), 
					new File(dataPath + "/" + p +"02.rec"));
			HistogramEval eval = new HistogramEval(data, path); 
			for(int numberOfBuckets : buckets){
				System.out.println("Buckets " + numberOfBuckets);
				eval.buildRTreeHist(numberOfBuckets, true); // rtree loaded bulk loaded using hilbert curve equi sized partitioning 
//				eval.buildRKHist(numberOfBuckets, 0.1, HistogramEval.BLOCKSIZE , true); // rkHist Method
				eval.buildRHistogramV(numberOfBuckets, 0.4, true); // RV histogram
//				eval.buildMinSkewHist(numberOfBuckets*2, 8, true); // standard min skew 2^7 x 2^7 grid
//				eval.buildMinSkewProgressiveHist(numberOfBuckets*2, 8, 3, true); // standard min skew 2^7 x 2^7 grid and three refinerment steps 
				//
				String query = queryPath + "/" + p  + "02.rec"; 
				// 
				Cursor<DoublePointRectangle> queryCursor = null;
				System.out.println("RTree");
				queryCursor = new FileInputCursor<DoublePointRectangle>(
						new ConvertableConverter<DoublePointRectangle>(SpatialUtils.factoryFunction(2)), new File(query));
				eval.testHistogram(queryCursor, eval.getRTreeHist());
//				System.out.println("RK-Hist");
//				queryCursor = new FileInputCursor<DoublePointRectangle>(
//						new ConvertableConverter<DoublePointRectangle>(RGOhist.factoryFunction(2)), new File(query));
//				eval.testHistogram(queryCursor, eval.getRkHist());
				System.out.println("RTree Volume");
				queryCursor = new FileInputCursor<DoublePointRectangle>(
						new ConvertableConverter<DoublePointRectangle>(SpatialUtils.factoryFunction(2)), new File(query));
				eval.testHistogram(queryCursor, eval.getRhistogram_V());
				eval.showHist("Normal", eval.getRTreeHist()); 
				eval.showHist("Volume", eval.getRhistogram_V()); 
//				System.out.println("MinSkew");
//				queryCursor = new FileInputCursor<DoublePointRectangle>(
//						new ConvertableConverter<DoublePointRectangle>(RGOhist.factoryFunction(2)), new File(query));
//				eval.testHistogram(queryCursor, eval.getMinSkewHist());
//				System.out.println("MinSkew Progressive");
//				queryCursor = new FileInputCursor<DoublePointRectangle>(
//						new ConvertableConverter<DoublePointRectangle>(RGOhist.factoryFunction(2)), new File(query));
//				eval.testHistogram(queryCursor, eval.getMinSkewProgressiveRefinementHistogram());
				System.out.println("******" +
						"****************\n*******************\n**************");
			}
		}
	}

}