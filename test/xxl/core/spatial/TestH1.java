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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import xxl.core.cursors.Cursor;
import xxl.core.cursors.sources.CollectionCursor;
import xxl.core.cursors.sources.io.FileInputCursor;
import xxl.core.io.converters.ConvertableConverter;
import xxl.core.spatial.histograms.utils.MHistogram;
import xxl.core.spatial.histograms.utils.SpatialHistogramBucket;
import xxl.core.spatial.points.Point;
import xxl.core.spatial.rectangles.DoublePointRectangle;


public class TestH1 {



	public static PrintStream getPrintStream(String output) throws IOException{
		return new PrintStream(new File(output)); 
	}
	
	
	public static Cursor<DoublePointRectangle> getData(String path) throws IOException {
		Cursor<DoublePointRectangle> data ;
		List<DoublePointRectangle> rectangles ;
		
		if (path.endsWith("rec"))
		{
			data = new FileInputCursor<DoublePointRectangle>(
					new ConvertableConverter<DoublePointRectangle>(SpatialUtils.factoryFunction(2)), 
					new File(path));
		}
		else {
			BufferedReader br = new BufferedReader(new FileReader (path));
			String line ;
			rectangles = new ArrayList<>();
//			double [] leftCorner = new double [2];
//			double [] rightCorner = new double [2];
			while (null != (line = br.readLine()))
			{
				String [] sp = line.split("\t");
				double [] leftCorner = new double [2];
				double [] rightCorner = new double [2];
				leftCorner[0] = Double.parseDouble(sp[1]);
				leftCorner[1] = Double.parseDouble(sp[2]);
				rightCorner[0] = Double.parseDouble(sp[3]);
				rightCorner[1] = Double.parseDouble(sp[4]);
				
				rectangles.add(new DoublePointRectangle(leftCorner, rightCorner));
			}
			
			br.close();
			System.err.println("Collection size: " + rectangles.size());
			data = new CollectionCursor<DoublePointRectangle>(rectangles);
		}
		
		return data ;
	}
	

	public static void main(String[] args) throws IOException {
//		if (args.length <3 )
//		{
//			System.err.println("Usage: "+ TestH1.class.getSimpleName()+ 
//					" [number of buckets] [input Data] [output File]");
//			System.exit(0);
//		}

		int numberOfBuckets = 100;//Integer.parseInt(args[0]) ;
		String inPath = "F:/data/osm.norm.dat" ; //args[1]; // data path
		String outPath = "" ;//args[2]; // data path
		String tempPath =  "F:/data/"; 

		System.err.println("++++++++++++++++++++++++++++++++++++\n");
		System.err.println("Data: " + inPath);
		
		

		HistogramEval eval = new HistogramEval(getData(inPath),tempPath);

		System.err.println("Buckets " + numberOfBuckets);

		eval.buildRTreeHist(numberOfBuckets, true); // rtree loaded bulk loaded using hilbert curve equi sized partitioning 

		//eval.buildRKHist(numberOfBuckets, 0.1, HistogramEval.BLOCKSIZE , true); // rkHist Method
		eval.buildRHistogramV(numberOfBuckets, 0.4, true); // RV histogram
		eval.buildMinSkewHist(numberOfBuckets*2, 8, true); // standard min skew 2^7 x 2^7 grid
		//eval.buildMinSkewProgressiveHist(numberOfBuckets*2, 8, 3, true); // standard min skew 2^7 x 2^7 grid and three refinerment steps 

//		eval.dumpHistogram(eval.getRTreeHist(),getPrintStream(outPath));
		
		eval.showHist("R Tree Simple", eval.getRTreeHist());
		
		eval.showHist("RV", eval.getRhistogram_V());
		
		eval.showHist("minskew", eval.getMinSkewHist());
		
		System.err.println("Done.");
		System.err.println("++++++++++++++++++++++++++++++++++++\n");
	}
	
	/*
	 * public void dumpHistogram(MHistogram mhistogram, PrintStream out){
		List<WeightedDoublePointRectangle> buckets = mhistogram.getBuckets();
		String TAB = "\t";
		int i = 0;  
		for (WeightedDoublePointRectangle bucket :buckets)
		{
			Point left = bucket.getCorner(false);
			Point right = bucket.getCorner(true);
			out.println(i+ TAB+ left.getValue(0)+TAB + left.getValue(1) + TAB + right.getValue(0)+TAB+right.getValue(1) );	
			i++;
		}
		
		}
	 * 
	 */

}