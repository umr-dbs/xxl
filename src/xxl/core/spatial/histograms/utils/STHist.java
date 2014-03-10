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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import xxl.core.collections.queues.io.BlockBasedQueue;
import xxl.core.cursors.Cursor;
import xxl.core.cursors.Cursors;
import xxl.core.cursors.filters.Filter;
import xxl.core.functions.Functional.UnaryFunction;
import xxl.core.predicates.AbstractPredicate;
import xxl.core.spatial.SpaceFillingCurves;
import xxl.core.spatial.SpatialUtils;
import xxl.core.spatial.points.DoublePoint;
import xxl.core.spatial.rectangles.DoublePointRectangle;
import xxl.core.util.Pair;
/**
 * 
 * @see Y. J. Roh, J. H. Kim, Y. D. Chung, J. H. Son, and
* M. H. Kim. Hierarchically organized skew-tolerant
* histograms for geographic data objects. In SIGMOD'10
 * 
 *
 */
public class STHist {

	
	public boolean verbose = false; 
	
	public static final String tempSortX = "x_sort";
	public static final String tempSortY = "y_sort";
	public static final String sortTemp = "xy_sort";
	public static final int BLOCK_SIZE = 4096;
	public static final int BITS_PRO_DIM = 7;
	
	protected Cursor<DoublePointRectangle> cursorX; 
	protected Cursor<DoublePointRectangle> cursorY;
	
	protected BlockBasedQueue queueX; 
	protected BlockBasedQueue queueY;
	
	
	protected List<DoublePointRectangle> data;
	
	
	// Buck
	public STHistBucket rootBucket; 
	UnaryFunction<DoublePointRectangle, DoublePoint> toPoint;
	// we need a grid to estimate skew 
	Map<Long, Integer> grid;
	UnaryFunction<double[], int[]> getSfcKey;
	int bitsPerDim; 
	int dimension; 
	
	public List<STHistBucket> forest;
	
	
	
	
	// we build pseudo rtree in main memory we can build an rtree
	public void buildHistogram(Iterator<DoublePointRectangle> rectangles, DoublePointRectangle universe,  int buckets) throws IOException{ 
		//
		toPoint = new UnaryFunction<DoublePointRectangle, DoublePoint>() {

			@Override
			public DoublePoint invoke(DoublePointRectangle arg) {
				return arg.getCenter();
			}
		};
		bitsPerDim = BITS_PRO_DIM;
		getSfcKey = MinSkewHist.getSFCFunction( (1 << (bitsPerDim-1)));
		dimension = 2;
		
		data = new ArrayList<DoublePointRectangle>();
		while(rectangles.hasNext() ){
			data.add(rectangles.next());
		}
		buildGrid(Cursors.wrap(data.iterator()), bitsPerDim, dimension);
		SpatialHistogramBucket recUniverse = new SpatialHistogramBucket(universe);
		this.rootBucket = new STHistBucket();
		recUniverse.setWeight(data.size());
		this.rootBucket.setInfoRectangle(recUniverse);
		buildNode(this.rootBucket, data.iterator(), buckets);
	}
	
	
	protected void buildGrid(Cursor<DoublePointRectangle> rectangles, int bitsPerDim,
			int dimensions){
		grid = MinSkewHist.computeGridForForest( rectangles,  bitsPerDim, dimensions);
	}
	
	
	
	protected double computeSkew(SpatialHistogramBucket rectangle ){
		//
		double skew = 0;
		double average = 0;
		double cells = 0;
		// compute 
		DoublePointRectangle actRectangle =  rectangle;
		double[] lowleft = (double[]) actRectangle.getCorner(false)
				.getPoint();
		double[] upright = (double[]) actRectangle.getCorner(true)
				.getPoint();
		int[] intLowLeft = getSfcKey.invoke(lowleft);
		int[] intUpRight = getSfcKey.invoke(upright);
		List<long[]> zcodesList = SpaceFillingCurves.computeZBoxRanges(
				intLowLeft, intUpRight, bitsPerDim, dimension);
		
		for(long[] zcodes : zcodesList){
			cells +=( zcodes[1] - zcodes[0] + 1); 
		}
		average = rectangle.getWeight() / cells; // x^		
				
		for (long[] zcodes : zcodesList) {
			for (long zcode = zcodes[0]; zcode <= zcodes[1]; zcode++) {
				if (grid.containsKey(zcode)) {
					double value = grid.get(zcode);
					skew += Math.pow((value - average), 2); 
				}
			}
		}
		return skew;
	}
	
	
	
	
	protected void buildNode(STHistBucket subroot, Iterator<DoublePointRectangle> dataIt, int bucketsNumber) throws IOException{
		//
		if(verbose)
			System.out.println("Run build node with nbi : "  + bucketsNumber);
		double universeSizeFraction = Math.max(bucketsNumber, 2); 
		double fractionFrequency = Math.max(bucketsNumber, 2);
		List<Pair<Double,SpatialHistogramBucket>> hotSpots = detectHotSpotsMainMemory(
				subroot.getInfoRectangle(), subroot.getInfoRectangle().getWeight(), Cursors.wrap(dataIt), 
				toPoint, universeSizeFraction, fractionFrequency);
		// 
		double allSkew = 0;
		for(Pair<Double,SpatialHistogramBucket> r : hotSpots){
			subroot.getChildList().add(new STHistBucket(r.getElement2(), r.getElement1()));
			allSkew += r.getElement1();
		}
		if (hotSpots.size() < bucketsNumber){
			
			for (STHistBucket r : subroot.childList){
				double nbi = hotSpots.size() * (r.getSkew()/ allSkew); 
				if (nbi >= 1){
					buildNode(r,getDataFilter(data.iterator(), r.infoRectangle), (int) nbi);
				}
			
			}
			
		}
		
	}
	
	
	protected Iterator<DoublePointRectangle> getDataFilter(Iterator<DoublePointRectangle> iterator, final  DoublePointRectangle rect){
		return new Filter<DoublePointRectangle>(iterator, new AbstractPredicate<DoublePointRectangle>() {
			@Override
			public boolean invoke(DoublePointRectangle argument) {
				DoublePoint p = toPoint.invoke(argument);
				return rect.contains(p);
			}
		});
	}
	
	
	
	
	public void buildHotSpotForest(Iterator<DoublePointRectangle> rectangles, DoublePointRectangle universe,  int buckets) throws IOException{
		//
		toPoint = new UnaryFunction<DoublePointRectangle, DoublePoint>() {

			@Override
			public DoublePoint invoke(DoublePointRectangle arg) {
				return arg.getCenter();
			}
		};
		bitsPerDim = BITS_PRO_DIM;
		getSfcKey = MinSkewHist.getSFCFunction( (1 << (bitsPerDim-1)));
		dimension = 2;
		
		data = new ArrayList<DoublePointRectangle>();
		while(rectangles.hasNext() ){
			data.add(rectangles.next());
		}
		buildGrid(Cursors.wrap(data.iterator()), bitsPerDim, dimension);
		
		forest = partition(universe, buckets);
		double allSkew = 0;
		for(STHistBucket bucket : forest){
			allSkew += bucket.skew;
		}
		for(STHistBucket bucket : forest) {
			double nbi = forest.size() * (bucket.getSkew()/ allSkew); 
			if (nbi >= 1){
				buildNode(bucket, getDataFilter(data.iterator(), bucket.infoRectangle), (int) nbi);
			}
		}
		
	}
	
	protected List<STHist.STHistBucket> partition(final DoublePointRectangle universe,  int buckets){
		// create partitions
		List<STHist.STHistBucket> candidates = new ArrayList<STHist.STHistBucket>();
		double xStep = Math.sqrt(universe.area()/buckets);
		double yStep = xStep;
		double xmin = universe.getCorner(false).getValue(0);
		double ymin = universe.getCorner(false).getValue(1);
		double xmax = universe.getCorner(true).getValue(0);
		double ymax = universe.getCorner(true).getValue(1);
		// sorted on x and then on y 
		for(double x = xmin; x <= xmax -  xStep; x+=xStep){
			for(double y = ymin; y <= ymax - yStep; y+=yStep){
				double[] left = {x,y};
				double[] right =  { (x+ xStep <  xmax) ? x + xStep : xmax ,
						 (y+ yStep <  ymax) ? y + yStep : ymax};
				SpatialHistogramBucket rectangle = new SpatialHistogramBucket(left, right);
				// compute weight
				int count = 0; 
				for(DoublePointRectangle r: data){
					if (r.overlaps(rectangle)){
						count++;
					}
				}
				if (count != 0){
					rectangle.setWeight(count);
					double skew = computeSkew(rectangle);
					candidates.add(new STHistBucket(rectangle, skew));
				}
			}
		}
		// merge candidates if skew(s1 +s2 ) <= skew(s1) +skew(s2)
		boolean change = true;  
		List<STHist.STHistBucket> tempList = new ArrayList<STHist.STHistBucket>(candidates.size());
		// sort data according predifined SFC
		Comparator<STHist.STHistBucket> bucketComparator = new Comparator<STHist.STHistBucket>() {
			
			Comparator<DoublePointRectangle> recComparator = SpatialUtils.getHilbert2DComparator(universe, 1 << 30);
			
			@Override
			public int compare(STHistBucket o1, STHistBucket o2) {
				return recComparator.compare(o1.getInfoRectangle(), o2.getInfoRectangle());
			}
		};
		
		
		Collections.sort(candidates, bucketComparator);
		
		while(change){
			change = false;
			Iterator<STHist.STHistBucket> itr = candidates.iterator();
			STHistBucket b1 = (itr.hasNext()) ? itr.next(): null;
			while(itr.hasNext()){
				STHistBucket b2 = itr.next();
				// compute union 
				SpatialHistogramBucket rectangle = new SpatialHistogramBucket(b1.getInfoRectangle());
				rectangle.union(b2.getInfoRectangle());
				rectangle.setWeight(b1.getInfoRectangle().getWeight() + b2.getInfoRectangle().getWeight());
				double skewUnion = computeSkew(rectangle);
				if (skewUnion <= b1.getSkew() + b2.getSkew()){
					tempList.add(new STHistBucket(rectangle, skewUnion));
					change = true;
					if (itr.hasNext())
						b1 = itr.next();
					else
						b1 = null;
				}else{
					tempList.add(new STHistBucket(b1.getInfoRectangle(), b1.skew));
					b1 = b2;
				}
			}
			if (b1 != null){
				tempList.add(new STHistBucket(b1.getInfoRectangle(), b1.skew));
			}
			candidates = tempList;
			tempList = new ArrayList<STHist.STHistBucket>(candidates.size());
		}
		return candidates;
	}
	
	
	
	
	
	
	
	
	public  List<Pair<Double,SpatialHistogramBucket>> detectHotSpotsMainMemory(
			DoublePointRectangle universe,  double universeFrequency, Cursor<DoublePointRectangle> recs, 
			UnaryFunction<DoublePointRectangle, DoublePoint> toPoint, 
			double universeSizeFraction, double fractionFrequency) throws IOException{
		
		
		
		
		List<Pair<Double,SpatialHistogramBucket>> hotspots = new ArrayList<Pair<Double,SpatialHistogramBucket>>();
		// create to cursors one sorted on x and y 
		List<DoublePointRectangle> dataX = new ArrayList<DoublePointRectangle>(10000);
		List<DoublePointRectangle> dataY = new ArrayList<DoublePointRectangle>(10000);
		while(recs.hasNext()){
			DoublePointRectangle rectangle = recs.next();
			dataX.add(new DoublePointRectangle(rectangle));
			dataY.add(new DoublePointRectangle(rectangle));
		}
		//sort
		Collections.sort(dataX, getComparator( toPoint, 0));
		Collections.sort(dataY, getComparator( toPoint, 1));
		//
		double windowSizeX = (1/Math.sqrt(universeSizeFraction)) * universe.deltas()[0];
		double windowSizeY = (1/Math.sqrt(universeSizeFraction)) * universe.deltas()[1];
		double minFreq = universeFrequency/fractionFrequency;
		
		
		if(verbose){
			System.out.println("Detect Hot Spots : "  +minFreq + ", " +  windowSizeX + ",  " + windowSizeY);
		}
		
		DoublePointRectangle r = null;
		DoublePoint p = null;
		int index = 0;
		int count = dataX.size();
		while(!dataX.isEmpty() && dataX.size() >= minFreq){ 
			r = (DoublePointRectangle) dataX.get(index); 
			p  = toPoint.invoke(r);
			double x = p.getValue(0);
			double range = x+windowSizeX;
			boolean currentFreq  = computeFreq(dataX, index, minFreq, range, 0,  toPoint);
			if(currentFreq){ // at least F/f elements in MPF x
				// start to search in y dimension
				List<DoublePointRectangle> inRegionListY = computeList(dataY, toPoint, x, range, 0);
				int indexY = 0;
				DoublePointRectangle ry = null;
				DoublePoint py 	= null;
				while(!inRegionListY.isEmpty() && inRegionListY.size() >= minFreq){
					ry = inRegionListY.get(indexY);
					py = toPoint.invoke(ry);
					double y = py.getValue(1);
					double rangeY = y+windowSizeY;
					boolean currenFreqY = computeFreq(inRegionListY, indexY, minFreq, rangeY, 1,  toPoint);
					if(currenFreqY){
						// build hot spot rectangle
						double[] minPoint = {x, y};
						double[] maxPoint = {x+windowSizeX, y+windowSizeY};
						
						boolean quickOverlap = false;
						DoublePoint pp = new DoublePoint(minPoint);
						for(Pair<Double,SpatialHistogramBucket> bst: hotspots){
							if (bst.getElement2().contains(pp) ){
								quickOverlap = true;
								break;
							}
						}
						if (!quickOverlap){
							SpatialHistogramBucket hotspot   = adjustDoublePointRectangle(new DoublePointRectangle(minPoint, maxPoint),
									inRegionListY,  toPoint);
							// 
							boolean overlap = overlapCheck(hotspots, hotspot);
							if (!overlap){
								
								double skew = computeSkew(hotspot); 
								hotspots.add(new Pair<Double, SpatialHistogramBucket>(skew, hotspot));
								if (verbose)
									System.out.println("Nr: " +hotspots.size() + " HotSpot Found: " + new Pair<Double, SpatialHistogramBucket>(skew, hotspot));
								dataX = removeObjects(hotspot, dataX , toPoint);//remove objects from dataX via copy
								dataY = removeObjects(hotspot, dataY , toPoint);//remove objects from dataY via copy
								inRegionListY = removeObjects(hotspot, inRegionListY , toPoint);//remove objects from inRegionListY via copy
							}
						}	
					}
					if (!inRegionListY.isEmpty())
						inRegionListY.remove(indexY);	
					
				}
			}
			if(!dataX.isEmpty() )
				dataX.remove(index);
			
			if (verbose && (count - dataX.size()) >= 10000 ){
				System.out.print(".");
				count = dataX.size();
			}
		}
		
		if (verbose)
			System.out.println("\n HotSpots: " +  hotspots.size());
		return hotspots;
	}
	
	
	protected List<DoublePointRectangle> removeObjects(DoublePointRectangle queryRectangle, List<DoublePointRectangle> inputList , UnaryFunction<DoublePointRectangle, DoublePoint> toPoint){
		List<DoublePointRectangle> outputList = new ArrayList<DoublePointRectangle>(inputList.size()); 
		for(DoublePointRectangle rectangle: inputList){
			if (!queryRectangle.contains(toPoint.invoke(rectangle))){
				outputList.add(rectangle);
			}
		}
		return outputList;
	}
	
	
	protected boolean overlapCheck(List<Pair<Double,SpatialHistogramBucket>> hotspots, DoublePointRectangle hotspot){
		for(Pair<Double,SpatialHistogramBucket>  r: hotspots){
				if(r.getElement2().overlaps(hotspot))	
					return true;
		}
		return false;
	}
	
	
	protected SpatialHistogramBucket adjustDoublePointRectangle(DoublePointRectangle rectangle, List<DoublePointRectangle> inputList , UnaryFunction<DoublePointRectangle, DoublePoint> toPoint){
		 SpatialHistogramBucket rec = null;
		for(DoublePointRectangle r : inputList ) {
			DoublePoint p = toPoint.invoke(r);
			if (rectangle.contains(p)){
				if(rec == null){
					rec = new SpatialHistogramBucket(new DoublePointRectangle(p, p));
					rec.setWeight(1);
				}
				else
					rec.union(new DoublePointRectangle(p, p));
				rec.setWeight(rec.getWeight() + 1);
				rec.updateAverage(r);
			}
		}
		return rec;
	}
	
	//
	protected List<DoublePointRectangle> computeList( List<DoublePointRectangle> inputList , 
			UnaryFunction<DoublePointRectangle, DoublePoint> toPoint,  double min, double max, int dimension){
		 List<DoublePointRectangle> subList = new ArrayList<DoublePointRectangle>(20000);
		 DoublePoint p = null; 
		 for(DoublePointRectangle dp : inputList){
			 p = toPoint.invoke(dp);
			 if ( min <= p.getValue(dimension)  && p.getValue(dimension)  <= max ){
				 subList.add(new DoublePointRectangle(dp));
			 }	
			 
		 }
		 return subList;
	}
	

	// jump to element 
	protected boolean computeFreq(List<DoublePointRectangle> rec, int position,  
			double minFreq, 
			double max, 
			int dimension, UnaryFunction<DoublePointRectangle, DoublePoint> toPoint){
		int jump = (int) Math.ceil(minFreq);
		if (position+jump-1  > rec.size() -1)
			return false;
		DoublePointRectangle r = rec.get(position+jump-1);
		DoublePoint p  = toPoint.invoke(r);
		return p.getValue(dimension) <= max;
	}
	
	private Comparator<DoublePointRectangle> getComparator(final UnaryFunction<DoublePointRectangle, DoublePoint> toPoint, final  int sortDimension ){
		return  new Comparator<DoublePointRectangle>() {
			@Override
			public int compare(DoublePointRectangle arg0,
					DoublePointRectangle arg1) {
				DoublePoint p1 = toPoint.invoke(arg0);
				DoublePoint p2 = toPoint.invoke(arg1);
				double dimVal1 = p1.getValue(sortDimension);
				double dimVal2 = p2.getValue(sortDimension);
				return (dimVal1 == dimVal2)  ?  0 : (dimVal1 < dimVal2 ) ? -1: 1;
			}
		};
	}
	
	public static double getSelectivity(List<STHistBucket> buckets, DoublePointRectangle query){
		double selectivity = 0;
		for(STHistBucket bucket: buckets){
			selectivity += getSelectivity(bucket, query);
		}
		return selectivity;
	}
	
	
	
	public static double getSelectivity(STHistBucket bucket, DoublePointRectangle query) {
		DoublePointRectangle intersect = new DoublePointRectangle(bucket.infoRectangle);
		if (intersect.overlaps(query))
			intersect.intersect(query);
		else 
			intersect = new DoublePointRectangle(new double[]{0,0}, new double[]{0,0});
		if (bucket.isLeaf()){
			return (intersect.area()/bucket.infoRectangle.area() )*(double)bucket.infoRectangle.getWeight(); 
		}
		double leafSum = 0; 
		double volSumChild = 0; 
		for(STHistBucket buck : bucket.childList){
			DoublePointRectangle in = new DoublePointRectangle(buck.infoRectangle);
			if (in.overlaps(query))
				in.intersect(query);
			else 
				in = new DoublePointRectangle(new double[]{0,0}, new double[]{0,0});
			volSumChild +=in.area();
			leafSum += getSelectivity(buck, query);
		}
		volSumChild = intersect.area() - volSumChild;
		volSumChild /= bucket.infoRectangle.area();
		volSumChild *= (double)bucket.infoRectangle.getWeight();
		return leafSum + volSumChild;
	}
	
	
	public static void  forest(List<STHistBucket> hist, List<SpatialHistogramBucket> forest){
		for(STHistBucket bucket : hist){
			STHist.getRectangles(bucket, forest);
		}
	}
	
	public static void getRectangles(STHistBucket bucket, List<SpatialHistogramBucket> list){
		list.add(bucket.infoRectangle);
		for(STHistBucket b : bucket.childList){
			getRectangles(b, list);
		}
	}

	public static class STHistBucket{
		
		
		private SpatialHistogramBucket infoRectangle;
		
		private double skew;
		
		private List<STHistBucket> childList;
		
		
		
		public STHistBucket() {
			childList = new ArrayList<STHist.STHistBucket>();
		}
		
		public STHistBucket( SpatialHistogramBucket infoRectangle, double skew) {
			this();
			this.infoRectangle = infoRectangle;
			this.skew = skew; 
		}
		
		
		public boolean isLeaf(){
			return childList.isEmpty();
		}


		public List<STHistBucket> getChildList() {
			return childList;
		}


		public SpatialHistogramBucket getInfoRectangle() {
			return infoRectangle;
		}


		public void setInfoRectangle(SpatialHistogramBucket infoRectangle) {
			this.infoRectangle = infoRectangle;
		}

		public double getSkew() {
			return skew;
		}

		public void setSkew(double skew) {
			this.skew = skew;
		}
		
		
		
		
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
	}

}
