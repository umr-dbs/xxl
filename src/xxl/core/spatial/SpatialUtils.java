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



import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import xxl.core.collections.MapEntry;
import xxl.core.collections.containers.CounterContainer;
import xxl.core.cursors.Cursor;
import xxl.core.cursors.Cursors;
import xxl.core.cursors.mappers.Mapper;
import xxl.core.cursors.sources.io.FileInputCursor;
import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;
import xxl.core.functions.Functional.UnaryFunction;
import xxl.core.indexStructures.ORTree;
import xxl.core.indexStructures.ORTree.IndexEntry;
import xxl.core.indexStructures.RTree;
import xxl.core.indexStructures.RTree.Node;
import xxl.core.io.converters.ConvertableConverter;
import xxl.core.spatial.histograms.utils.STHist;
import xxl.core.spatial.histograms.utils.SpatialHistogramBucket;
import xxl.core.spatial.histograms.utils.STHist.STHistBucket;
import xxl.core.spatial.points.DoublePoint;
import xxl.core.spatial.rectangles.DoublePointRectangle;

/**
 * 
 *
 * 
 * helper class for spatial histograms 
 *
 */
public class SpatialUtils {
	
	
	
	/**
	 * factory funtcion for convertable converter
	 */
	public static final Function<Object, DoublePointRectangle> factoryFunction(final int dimension){ 
		return new AbstractFunction<Object, DoublePointRectangle>() {
		public DoublePointRectangle invoke() {
			return new DoublePointRectangle(dimension);
		}
	};
	}
	/**
	 * 
	 */
	public static SpatialHistogramBucket universeUnit(final int dimension) { 
		double[] lft = new double[dimension];
		double[] rgt = new double[dimension];
		for(int i = 0; i < dimension; i++){
			lft[i] = 0;
			rgt[i] = 1;
		}
		return new SpatialHistogramBucket(
			lft, rgt);
	}
	
	
	public static List<SpatialHistogramBucket> readHistogram(String path, final int dimension){
		FileInputCursor<SpatialHistogramBucket> rectangles = new FileInputCursor<>(new ConvertableConverter<>(new AbstractFunction<Object, SpatialHistogramBucket>() {
		@Override
		public SpatialHistogramBucket invoke() {

			return new SpatialHistogramBucket(dimension);
		}
		}), new File(path));
		List<SpatialHistogramBucket> histogram = new LinkedList<>(); 
		while(rectangles.hasNext()){
			histogram.add(rectangles.next());
		}
		return histogram;
	}
	
	public static void writeHistogram(List<SpatialHistogramBucket> histogram, String path) throws IOException{
		DataOutputStream out = new DataOutputStream(new FileOutputStream(new File(path)));
		for(SpatialHistogramBucket rec: histogram)
			rec.write(out);
		out.close();
	}
	
	
	/**
	 * computes simple histogram
	 * @param tree
	 * @param numberOfBuckets
	 * @return
	 */
	public static List<SpatialHistogramBucket> computeSimpleRTreeHistogram(RTree tree, int numberOfBuckets){
		List<SpatialHistogramBucket> histogram = new ArrayList<SpatialHistogramBucket>(numberOfBuckets);
		int numberOfNodes = Cursors.count(tree.query(1));
		int hyperBucketSize = numberOfNodes/(numberOfBuckets)  + 1; // remain part will be assigned to the last bucket
		Cursor<MapEntry<DoublePointRectangle, RTree.Node>> nodes = getNodesAndMBRs(tree, 1);
		for(;nodes.hasNext();){
			int sum = 0;
			SpatialHistogramBucket uni = null;
			for(int i = 0; i < hyperBucketSize && nodes.hasNext(); i++){
				MapEntry<DoublePointRectangle, RTree.Node> entry = nodes.next();
				sum += entry.getValue().number();
				if(uni == null){
					uni = new SpatialHistogramBucket(entry.getKey());
				}else{
					uni.union(entry.getKey());
				}
				// compute avg
				Iterator it = entry.getValue().entries();
				while(it.hasNext()){
					DoublePointRectangle rec = (DoublePointRectangle) tree.descriptor(it.next());
					uni.updateAverage(rec);
				}
			}
			uni.setWeight(sum);
			histogram.add(uni);
		}
		if (histogram.size() > numberOfBuckets){ // one bucket more
			SpatialHistogramBucket recF = histogram.get(histogram.size()-2);
			recF.union(histogram.get(histogram.size()-1));
			recF.setWeight(recF.getWeight()+histogram.get(histogram.size()-1).getWeight() );
			histogram.remove(histogram.size()-1);
		}
		return histogram;
	}
	
	
	/**
	 * Note: RTree manages weighted rectangles @see {@link SpatialHistogramBucket}
	 * @param tree
	 * @param queryWindow
	 * @return
	 */
	@SuppressWarnings("rawtypes")
	public static double computeEstimation(RTree tree, DoublePointRectangle queryWindow, int level){
		if (tree.height() < level)
			throw new RuntimeException("Check level");
		double costs = 0d;
		Cursor cursor = tree.query(queryWindow, level);
		while(cursor.hasNext()){
			Object result = cursor.next();
			SpatialHistogramBucket rectangle = (SpatialHistogramBucket)( (level > 0) ? 
				 ((ORTree.IndexEntry) result).descriptor() : 
					 result);
			double overlap = rectangle.overlap(queryWindow)/ rectangle.area();
			double localSel = overlap*rectangle.getWeight();
			costs += localSel;
		}
		return costs;
	}
	
	/**
	 * Note: RTree manages weighted rectangles @see {@link SpatialHistogramBucket}
	 * @param tree
	 * @param queryWindow
	 * @return
	 */
	public static double computeEstimation(Iterator<SpatialHistogramBucket> histogram, DoublePointRectangle queryWindow){
		double costs = 0d;
		while(histogram.hasNext()){
			SpatialHistogramBucket rectangle = histogram.next();
			// extend rectnagles 
		
			if (rectangle.overlaps(queryWindow)){
				DoublePointRectangle deltass = new DoublePointRectangle(rectangle);
				deltass.intersect(queryWindow);
				// compute left point
				double[] leftR=  (double[]) rectangle.getCorner(false).getPoint();
				double[] rightR=  (double[]) rectangle.getCorner(true).getPoint();
				double[] left =  (double[]) deltass.getCorner(false).getPoint();
				double[] right = (double[]) deltass.getCorner(true).getPoint();
				for(int i = 0; i <left.length; i++){
					// low point
					left[i] = ((left[i]-rectangle.getExtent()[i]) < leftR[i])? leftR[i] : (left[i]-rectangle.getExtent()[i]);
					right[i] = ((right[i]+rectangle.getExtent()[i]) > rightR[i])? rightR[i] : (right[i]+rectangle.getExtent()[i]);
				}
				
				deltass = new DoublePointRectangle(left, right);
				
//				for(int i = 0; i < rectangle.dimensions(); i++){
//					area *=  deltass.deltas()[i]; // + rectangle.getExtent()[i]; 
//				}
				
				double overlap = deltass.area()/rectangle.area();
				double localSel = overlap*rectangle.getWeight();
				costs += localSel;
			}
			
//			double overlap = rectangle.overlap(queryWindow)/ rectangle.area();
//			double localSel = overlap*rectangle.getWeight();
//			costs += localSel;
			
		}
		return costs;
	}
	
	
	public static int numberOfResults(RTree rtree, DoublePointRectangle queryRec){
		return Cursors.count(rtree.query(queryRec));
	}
	
	/**
	 * Computes SSE distance based 
	 * 
	 * @param recs
	 * @param lp
	 * @return
	 */
	public static double computeSSEDist(DoublePointRectangle[] recs, int lp){
		double[] dists = new double[recs.length];
		double cost = 0;
		double sum = 0;
		
		for(int i = 0; i < recs.length; i++ ){
			DoublePointRectangle rec = recs[i];
			double dist = Double.MAX_VALUE;
			for(int j = 0; j < recs.length; j++ ){
				if (j != i){
					double computedDist = recs[j].distance(rec, lp);
					if(computedDist < dist )
						dist = computedDist;
				}
			}
			sum +=dist;
			dists[i] = dist;
		}
		double avg = sum/recs.length; 
		for(double d : dists){
			cost += Math.pow((d-avg), 2);
		}
		return cost;
	}
	  
	
	/*
	 * extractes nodes from rtree
	 */
	@SuppressWarnings({ "serial", "unchecked" })
	public static Cursor<RTree.Node> getNodes(RTree tree, int level){
		if(level > tree.height() || level == 0)
			throw new RuntimeException("check the level");
		return new Mapper<Object, RTree.Node>(new AbstractFunction<Object,  RTree.Node>() {
			
			public RTree.Node invoke(Object argument){
				//cast to ORTree.indexEntry
				ORTree.IndexEntry entry = (ORTree.IndexEntry) argument;
				RTree.Node node = (RTree.Node)entry.get();
				return node;
			}
			
		},  tree.query(level));
	}
	
	
	
	
	
	
	/*
	 * extractes nodes from rtree
	 */
	@SuppressWarnings({ "serial", "unchecked" })
	public static Cursor<SpatialHistogramBucket> getRectangles(final RTree tree, final  int level){
		if(level > tree.height())
			throw new RuntimeException("check the level");
		return new Mapper<Object, SpatialHistogramBucket>(new AbstractFunction<Object,  SpatialHistogramBucket>() {
			
			public SpatialHistogramBucket invoke(Object argument){
				//cast to ORTree.indexEntry
				// get entry
				return new SpatialHistogramBucket((DoublePointRectangle)tree.descriptor(argument));
			}
			
		},  tree.query(level));
	}
	
	
	/*
	 * extractes nodes from rtree
	 */
	@SuppressWarnings({ "serial", "unchecked" })
	public static Cursor<SpatialHistogramBucket> getRectanglesLevel1(final RTree tree){
		return new Mapper<Object, SpatialHistogramBucket>(new AbstractFunction<Object,  SpatialHistogramBucket>() {
			
			public SpatialHistogramBucket invoke(Object argument){
				//cast to ORTree.indexEntry
				// get entry
				RTree.Node node = (Node) ((IndexEntry)argument).get();
				SpatialHistogramBucket rectangle = new SpatialHistogramBucket((DoublePointRectangle)tree.descriptor(argument), node.number());
				Iterator it = node.entries();
				while(it.hasNext()){
					DoublePointRectangle rec = (DoublePointRectangle) tree.descriptor(it.next());
					rectangle.updateAverage(rec);
				}
				return rectangle;	
			}
			
		},  tree.query(1));
	}
	
	
	/*
	 * extractes nodes from rtree
	 */
	@SuppressWarnings({ "serial", "unchecked" })
	public static Cursor<MapEntry<DoublePointRectangle, RTree.Node>> getNodesAndMBRs(RTree tree, int level){
		if(level > tree.height() || level == 0)
			throw new RuntimeException("check the level");
		return new Mapper<Object, MapEntry<DoublePointRectangle, RTree.Node>>(new AbstractFunction<Object,  MapEntry<DoublePointRectangle, RTree.Node>>() {
			
			public MapEntry<DoublePointRectangle, RTree.Node> invoke(Object argument){
				//cast to ORTree.indexEntry
				ORTree.IndexEntry entry = (ORTree.IndexEntry) argument;
				RTree.Node node = (RTree.Node)entry.get();
				return new MapEntry<DoublePointRectangle, RTree.Node>((DoublePointRectangle)entry.descriptor(), node);
			}
			
		},  tree.query(level));
	}
	
	
	/*
	 * extractes nodes from rtree
	 */
	@SuppressWarnings({ "serial", "unchecked" })
	public static Cursor<MapEntry<DoublePointRectangle, ORTree.IndexEntry>> getIndexEntriesAndMBRs(RTree tree, int level){
		if(level > tree.height() || level == 0)
			throw new RuntimeException("check the level");
		return new Mapper<Object, MapEntry<DoublePointRectangle, ORTree.IndexEntry>>(new AbstractFunction<Object,  MapEntry<DoublePointRectangle,ORTree.IndexEntry>>() {
			
			public MapEntry<DoublePointRectangle,ORTree.IndexEntry> invoke(Object argument){
				//cast to ORTree.indexEntry
				ORTree.IndexEntry entry = (ORTree.IndexEntry) argument;
				RTree.Node node = (RTree.Node)entry.get();
				return new MapEntry<DoublePointRectangle,ORTree.IndexEntry>((DoublePointRectangle)entry.descriptor(), entry);
			}
			
		},  tree.query(level));
	}
	
	
	
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static Comparator<DoublePointRectangle> getHilbert2DComparator(final DoublePointRectangle universe, 
			final int FILLING_CURVE_PRECISION ){
		return (Comparator<DoublePointRectangle>) new Comparator() {
			protected double uni[];
			protected double uniDeltas[];

			public int compare(Object o1, Object o2) {
				if (uni == null) {
					uni = (double[]) universe.getCorner(false).getPoint();
					uniDeltas = universe.deltas();
				}
				double leftBorders1[] = (double[]) 
						((DoublePointRectangle) o1).getCenter().getPoint();
				double leftBorders2[] = (double[]) 
						((DoublePointRectangle) o2).getCenter().getPoint();

				double x1 = (leftBorders1[0] - uni[0]) / uniDeltas[0];
				double y1 = (leftBorders1[1] - uni[1]) / uniDeltas[1];
				double x2 = (leftBorders2[0] - uni[0]) / uniDeltas[0];
				double y2 = (leftBorders2[1] - uni[1]) / uniDeltas[1];

				long h1 = SpaceFillingCurves.hilbert2d(
						(int) (x1 * FILLING_CURVE_PRECISION),
						(int) (y1 * FILLING_CURVE_PRECISION));
				long h2 = SpaceFillingCurves.hilbert2d(
						(int) (x2 * FILLING_CURVE_PRECISION),
						(int) (y2 * FILLING_CURVE_PRECISION));
				return (h1 < h2) ? -1 : ((h1 == h2) ? 0 : +1);
			}
	};
	}
	
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static Comparator<DoublePointRectangle> getZCurve2DComparator(final DoublePointRectangle universe, 
			final int FILLING_CURVE_PRECISION){
		return new Comparator() {
			protected double uni[];
			protected double uniDeltas[];

			public int compare(Object o1, Object o2) {
				if (uni == null) {
					uni = (double[]) universe.getCorner(false).getPoint();
					uniDeltas = universe.deltas();
				}
				double leftBorders1[] = (double[]) 
						((DoublePointRectangle) o1).getCenter().getPoint();
				double leftBorders2[] = (double[]) 
						((DoublePointRectangle) o2).getCenter().getPoint();

				double x1 = (leftBorders1[0] - uni[0]) / uniDeltas[0];
				double y1 = (leftBorders1[1] - uni[1]) / uniDeltas[1];
				double x2 = (leftBorders2[0] - uni[0]) / uniDeltas[0];
				double y2 = (leftBorders2[1] - uni[1]) / uniDeltas[1];

				long h1 = SpaceFillingCurves.peano2d(
						(int) (x1 * FILLING_CURVE_PRECISION),
						(int) (y1 * FILLING_CURVE_PRECISION));
				long h2 = SpaceFillingCurves.peano2d(
						(int) (x2 * FILLING_CURVE_PRECISION),
						(int) (y2 * FILLING_CURVE_PRECISION));
				return (h1 < h2) ? -1 : ((h1 == h2) ? 0 : 1);
			}
		};
	}
	
	/**
	 * Mapping function for Hilbert 
	 */
	public static UnaryFunction<DoublePointRectangle, Long> hilbert2dSFC(final DoublePointRectangle universe, 
			final int FILLING_CURVE_PRECISION){ 
		return new UnaryFunction<DoublePointRectangle, Long>() {
		
			protected double uni[];
			protected double uniDeltas[];
			
			@Override
			public Long invoke(DoublePointRectangle arg) {
				if (uni == null) {
					uni = (double[]) universe.getCorner(false).getPoint();
					uniDeltas = universe.deltas();
				}
				double leftBorders[] = (double[]) 
						((DoublePointRectangle) arg).getCenter().getPoint();
				double x1 = (leftBorders[0] - uni[0]) / uniDeltas[0];
				double y1 = (leftBorders[1] - uni[1]) / uniDeltas[1];
				long h1 = SpaceFillingCurves.hilbert2d(
						(int) (x1 * FILLING_CURVE_PRECISION),
						(int) (y1 * FILLING_CURVE_PRECISION));
				
				return h1;
			}
		
		}; 
	
	}
	
	
	/**
	 * Mapping function for Hilbert 
	 */
	public static UnaryFunction<DoublePointRectangle, Long> zCruveSFC(final DoublePointRectangle universe, 
			 final int BITS_PRO_DIM){ 
		final int FILLING_CURVE_PRECISION = 1 << (BITS_PRO_DIM - 1);
		return new UnaryFunction<DoublePointRectangle, Long>() {
		
			protected double uni[];
			protected double uniDeltas[];
			
			@Override
			public Long invoke(DoublePointRectangle arg) {
				if (uni == null) {
					uni = (double[]) universe.getCorner(false).getPoint();
					uniDeltas = universe.deltas();
				}
				double[] center1 = (double[]) ((DoublePointRectangle) arg).getCenter().getPoint();
				double[] normCenter1 = normalize(center1, uni, uniDeltas);
				int[] coord1 = new int[normCenter1.length];
				for(int i = 0; i < coord1.length; i++){
					coord1[i] = (int) (normCenter1[i] * (FILLING_CURVE_PRECISION));
				}
				long h1 = SpaceFillingCurves.computeZCode(coord1, BITS_PRO_DIM);
				return h1;
			}
		
		}; 
	
	}
	
	
	
	
	
	@SuppressWarnings("unchecked")
	public static Comparator<DoublePointRectangle> getZCurveComparator(final DoublePointRectangle universe,  final int BITS_PRO_DIM){
		final int FILLING_CURVE_PRECISION = 1 << (BITS_PRO_DIM - 1);
		return new  Comparator() {
			protected double uni[];
			protected double uniDeltas[];	
			
			public int compare(Object o1, Object o2) {
				if (uni == null) {
					uni = (double[])universe.getCorner(false).getPoint();
					uniDeltas = universe.deltas();	
				}
				double center1[] = (double[])((DoublePointRectangle)o1).getCenter().getPoint();
				double center2[] =(double[])((DoublePointRectangle)o2).getCenter().getPoint();
				double[] normCenter1 = normalize(center1, uni, uniDeltas);
				double[] normCenter2 = normalize(center2, uni, uniDeltas);
				int[] coord1 = new int[normCenter1.length];
				int[] coord2 = new int[normCenter1.length];
				for(int i = 0; i < coord1.length; i++){
					coord1[i] = (int) (normCenter1[i] * (FILLING_CURVE_PRECISION));
					coord2[i] = (int) (normCenter2[i] * (FILLING_CURVE_PRECISION));
				}
				long h1 = SpaceFillingCurves.computeZCode(coord1, BITS_PRO_DIM);
				long h2 =  SpaceFillingCurves.computeZCode(coord2, BITS_PRO_DIM);
				return (h1<h2)?-1: ((h1==h2)?0:+1);
				
			}
		};
	}
	
	
	
	@SuppressWarnings("unchecked")
	public static Comparator<DoublePoint> getZCurvePointComparator(final DoublePointRectangle universe,  final int BITS_PRO_DIM){
		final int FILLING_CURVE_PRECISION = 1 << (BITS_PRO_DIM - 1);
		return new  Comparator() {
			protected double uni[];
			protected double uniDeltas[];	
			
			public int compare(Object o1, Object o2) {
				if (uni == null) {
					uni = (double[])universe.getCorner(false).getPoint();
					uniDeltas = universe.deltas();	
				}
				double center1[] =(double[])((DoublePointRectangle)o1).getCenter().getPoint();
				double center2[] = (double[])((DoublePointRectangle)o2).getCenter().getPoint();
				double[] normCenter1 = normalize(center1, uni, uniDeltas);
				double[] normCenter2 = normalize(center2, uni, uniDeltas);
				int[] coord1 = new int[normCenter1.length];
				int[] coord2 = new int[normCenter1.length];
				for(int i = 0; i < coord1.length; i++){
					coord1[i] = (int) (normCenter1[i] * (FILLING_CURVE_PRECISION));
					coord2[i] = (int) (normCenter2[i] * (FILLING_CURVE_PRECISION));
				}
				long h1 = SpaceFillingCurves.computeZCode(coord1, BITS_PRO_DIM);
				long h2 =  SpaceFillingCurves.computeZCode(coord2, BITS_PRO_DIM);
				return (h1<h2)?-1: ((h1==h2)?0:+1);
				
			}
		};
	}
	
	
	public static UnaryFunction<DoublePointRectangle, Long> sfcZCurve2DFunction(final DoublePointRectangle universe, 
			final int FILLING_CURVE_PRECISION){
		return new UnaryFunction<DoublePointRectangle, Long>() {
			protected double uni[];
			protected double uniDeltas[];
			
			@Override
			public Long invoke(DoublePointRectangle arg) {
				if (uni == null) {
					uni = (double[]) universe.getCorner(false).getPoint();
					uniDeltas = universe.deltas();
				}
				double leftBorders[] = (double[]) 
						((DoublePointRectangle) arg).getCenter().getPoint();
				double x1 = (leftBorders[0] - uni[0]) / uniDeltas[0];
				double y1 = (leftBorders[1] - uni[1]) / uniDeltas[1];
				long h1 = SpaceFillingCurves.peano2d(
						(int) (x1 * FILLING_CURVE_PRECISION),
						(int) (y1 * FILLING_CURVE_PRECISION));
				
				return h1;
			}
		};
	}
	
	
	
	
	
	
	
	/**
	 * 
	 * @param point
	 * @param uni
	 * @param unideltas
	 * @return
	 */
	public static double[] normalize(double[] point, double[] uni, double[] unideltas){
		double[] normPoint = new double[point.length];
		for(int i = 0; i < normPoint.length; i++){
			normPoint[i] = (point[i] - uni[i])/unideltas[i];
		}
		return normPoint;
	}
	
	/**
	 * 
	 * @param recs
	 * @param dimension
	 * @param universe
	 * @return
	 */
	public static double[] computeQuerySides(Iterator<DoublePointRectangle> recs, int dimension, 
	    		DoublePointRectangle universe){
	    	double[] querySides = new double[dimension];
	    	int counter = 0;
	    	
	    	while(recs.hasNext()){
	    		DoublePointRectangle rec = recs.next();
	    		double[] deltas = rec.deltas();
	    		for (int i = 0; i < dimension; i++){
	    			querySides[i] += deltas[i];
	    		}
	    		counter++;
	    	}
	    	for(int i = 0; i < dimension; i++  ){
	    		querySides[i] /= (double)counter;
	    	}
	    	
	    	return querySides;
	    }
	
	
	public static DoublePointRectangle computeUniverse(Iterator<DoublePointRectangle> rectangles){
		DoublePointRectangle universe = null;
		while(rectangles.hasNext()){
			DoublePointRectangle item = rectangles.next(); 
			if (universe == null)
				universe = new DoublePointRectangle(item);
			else 
				universe.union(item);
		}
		return universe;
	}
	
	
	
	/**
	 * computes average absolut error
	 * @param rtree
	 * @param queryIn
	 * @param histogram
	 */
	public static void estimationTest(RTree rtree, Iterator<DoublePointRectangle> queryIn,List<SpatialHistogramBucket> histogram){
		
		double sumDiff =0;
		int count = 0;
		double error = 0;
		double avgerror = 0;
		double max = Double.MIN_VALUE;
		double min = Double.MAX_VALUE;
		while(queryIn.hasNext()){
			DoublePointRectangle queryRec = queryIn.next();
			double actualCount = Cursors.count(rtree.query(queryRec)); 
			double estimation = 0;
			double diff = 0;
			if(histogram != null){
				estimation =  SpatialUtils.computeEstimation(histogram.iterator(), queryRec);
			}else{
				estimation = SpatialUtils.computeEstimation(rtree, queryRec, 1);
			}
			diff = Math.abs(actualCount-estimation);
			avgerror += diff/(Math.max(1, actualCount)); 
			count++;
			sumDiff +=diff;
			if (diff> max)
				max = diff;
			if (diff < min)
				min = diff;
		}
		error = sumDiff/count;
		System.out.println("Estimation Average Relative Error: " + avgerror/count);
		System.out.println("Estimation Average Absolut Error: " + error);
		System.out.println("Buckets Considered: " + histogram.size());
		System.out.println("Max: " + max);
		System.out.println("Min: " + min);
		System.out.println("Queries issued:  " + count);	
	}
	
	public static void estimationTestSTHist(RTree rtree, Iterator<DoublePointRectangle> queryIn, List<STHist.STHistBucket> histogram ){

		double sumDiff =0;
		int count = 0;
		double error = 0;
		double avgerror = 0;
		double max = Double.MIN_VALUE;
		double min = Double.MAX_VALUE;
		while(queryIn.hasNext()){
			DoublePointRectangle queryRec = queryIn.next();
			double actualCount = Cursors.count(rtree.query(queryRec)); 
			double estimation = 0;
			double diff = 0;
			estimation = STHist.getSelectivity(histogram, queryRec);
			diff = Math.abs(actualCount-estimation);
			avgerror += diff/(Math.max(1, actualCount)); 
			count++;
			sumDiff +=diff;
			if (diff> max)
				max = diff;
			if (diff < min)
				min = diff;
		}
		error = sumDiff/count;
		System.out.println("Estimation Average Relative Error: " + avgerror/count);
		System.out.println("Estimation Average Absolut Error: " + error);
		System.out.println("Buckets Considered: " + histogram.size());
		System.out.println("Max: " + max);
		System.out.println("Min: " + min);
		System.out.println("Queries issued:  " + count);	
		
		
		
	}
	
	
	
	
	
	public static void queryCheck(Iterator<DoublePointRectangle> queries, RTree tree, CounterContainer counterC, double[] querySideLength){
		int overallIO = 0;
		int counter = 0;
		counterC.reset();
		int overallCandidates =0;
		int leafsOverall = 0;
		while(queries.hasNext() ){
			DoublePointRectangle queryRec = queries.next();
			Iterator<DoublePointRectangle> result =  tree.query(queryRec);
			int resultCounter = Cursors.count(result);
			int leafs = tree.leafsTouched;
			leafsOverall += leafs;
			overallCandidates += resultCounter;
			counter++;
			overallIO += counterC.gets;
			counterC.reset();
		}
		// gather statistic abpout first level
		if(tree.height() > 0){
			double volumeSum = 0;
			double volumeExtended = 0;
			Cursor levelCursor = tree.query(1);
			while(levelCursor.hasNext() ){
				DoublePointRectangle descriptor =
					(DoublePointRectangle) (((IndexEntry) levelCursor.next()).descriptor());
				volumeSum +=descriptor.area();
				double[] sides = descriptor.deltas();
				double extV = 1;
				for(int i = 0; i < sides.length; i++){
					extV *=(sides[i]+querySideLength[i]);
				}
				volumeExtended+=extV;
			}
			System.out.println("Sum. Volume Costs: " + volumeSum);
			System.out.println("Sum. Ext Volume Costs: " + volumeExtended);
		}
		// gather costs per 
		System.out.println("Tree height: " + tree.height());
		System.out.println("Overall IOs " + overallIO);
		System.out.println("Overall Leafs " + leafsOverall);
		System.out.println("Queries issued  " + counter );
		double ov = (double)overallIO;
		double cn = (double)counter;
		System.out.println("Avg IO pro Query " + (ov/cn));
		System.out.println("Sum Results  " + (overallCandidates));
		System.out.println("Avg Results pro Query  " + (overallCandidates/counter));
	}
	

}
