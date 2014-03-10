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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import xxl.core.collections.MapEntry;
import xxl.core.cursors.Cursor;
import xxl.core.indexStructures.ORTree;
import xxl.core.indexStructures.RTree;
import xxl.core.indexStructures.RTree.Node;
import xxl.core.spatial.SpatialUtils;
import xxl.core.spatial.points.DoublePoint;
import xxl.core.spatial.rectangles.DoublePointRectangle;
/**
 * 
 * @see T. Eavis and A. Lopez. Rk-hist: an r-tree based
 * histogram for multi-dimensional selectivity estimation. CIKM 2007
 * 
 * 
 *
 */
public class RKhist {


	
	/**
	 * 
	 * @param bulkLoadedRTree
	 * @param numberOfHyperBlocks
	 * @param ratio
	 * @return
	 */
	public static List<SpatialHistogramBucket> buildRKHist(RTree bulkLoadedRTree,  int numberOfNodes,   
			int numberOfHyperBlocks, double ratio, int dimension){
		// partition the 

		double overestimation =  (numberOfHyperBlocks-( numberOfHyperBlocks*ratio)); 
		int hyperBlockSize = (int) Math.ceil(numberOfNodes/overestimation);
		int floor = (int) Math.floor(numberOfNodes/overestimation);
		int maxHC = numberOfNodes /hyperBlockSize;
		int maxHF = numberOfNodes /floor;
		int penaltyNumber = (int) ((numberOfHyperBlocks*ratio)/2);
		System.out.println("Overestimetion: " + overestimation);
		System.out.println("Ceil: "  +hyperBlockSize + " floor: " + floor);
		if ((penaltyNumber/2) + maxHF <= numberOfHyperBlocks){
			hyperBlockSize = floor;
		}
		System.out.println("hyper block size:  " + hyperBlockSize);
		List<MapEntry< Double,List<MapEntry<DoublePointRectangle, RTree.Node>>>> nodeList = new ArrayList<MapEntry< Double,List<MapEntry<DoublePointRectangle, RTree.Node>>>>();
		List<MapEntry< Double,List<MapEntry<DoublePointRectangle, RTree.Node>>>> penaltyList = new ArrayList<MapEntry< Double,List<MapEntry<DoublePointRectangle, RTree.Node>>>>();
		// cursor of level 0 leaf nodes buckets
		Cursor<MapEntry<DoublePointRectangle, RTree.Node>> nodes = SpatialUtils.getNodesAndMBRs(bulkLoadedRTree, 1);
		// create hyperblocks and create penalty list;
		while(nodes.hasNext()){
			List<MapEntry<DoublePointRectangle, RTree.Node>> hyperNode = new ArrayList<MapEntry<DoublePointRectangle, RTree.Node>>();
			for(int i = 0; nodes.hasNext() && i < hyperBlockSize; i++ ){
				MapEntry<DoublePointRectangle, RTree.Node> h = nodes.next();
				hyperNode.add(h);
			}
			double costs =  kMetricCosts(hyperNode, dimension);
			nodeList.add(new MapEntry<Double, List<MapEntry<DoublePointRectangle,Node>>>(costs, hyperNode));
		}
		// sort list and take penaltyNumber
		Collections.sort(nodeList, new Comparator<MapEntry< Double,List<MapEntry<DoublePointRectangle, RTree.Node>>>>() {

			@Override
			public int compare(
					MapEntry<Double, List<MapEntry<DoublePointRectangle, Node>>> arg0,
					MapEntry<Double, List<MapEntry<DoublePointRectangle, Node>>> arg1) {
				return arg0.getKey().compareTo(arg1.getKey());
			}
		});
		penaltyList.addAll(nodeList.subList(0, penaltyNumber));
		nodeList.subList(0, penaltyNumber).clear();
		// step 2.
		boolean enhanceable = true;
		int splits = nodeList.size()+penaltyList.size(); 
mark:	while(enhanceable){
		//	if(penaltyList.size()  + overestimation >= numberOfHyperBlocks)
			if(penaltyList.size()  + nodeList.size() >= numberOfHyperBlocks)
				break mark;
			List<MapEntry< Double,List<MapEntry<DoublePointRectangle, RTree.Node>>>> penaltyTempList = new 
			ArrayList<MapEntry< Double,List<MapEntry<DoublePointRectangle, RTree.Node>>>>();
			boolean noChange = true;
formark:	for(int i = 0; i < penaltyList.size(); i++){
				MapEntry< Double,List<MapEntry<DoublePointRectangle, RTree.Node>>> hyperNode = 
					penaltyList.get(i);
				List<MapEntry<DoublePointRectangle, RTree.Node>> list = hyperNode.getValue();
				int argMin = 0;
				double min = Double.MAX_VALUE;
				double argCostLeft = 0;
				double argCostRight = 0;
				for(int j = 0; j < list.size()-1; j++){
					double costsLeft = kMetricCosts(list.subList(0, j+1),  dimension);
					double costRight = kMetricCosts(list.subList(j+1, list.size()),  dimension);
					if (costsLeft + costRight < min){
						min = costsLeft+ costRight;
						argCostLeft = costsLeft;
						argCostRight = costRight;
						argMin = j;
					}
				}
				if(min < hyperNode.getKey()){
					penaltyTempList.add(new MapEntry< Double,List<MapEntry<DoublePointRectangle, RTree.Node>>> (argCostLeft,
							list.subList(0, argMin+1) ));
					penaltyTempList.add(new MapEntry< Double,List<MapEntry<DoublePointRectangle, RTree.Node>>> (argCostRight,
							list.subList(argMin+1, list.size()) ));
					penaltyList.remove(i);
					i--;
					splits++;
					noChange = false;
					if (splits  >= numberOfHyperBlocks){
						break formark;
					}
				}
			}
			penaltyList.addAll(penaltyTempList);
			enhanceable = !noChange; 
		}
		// step 3. merge to histogram
		nodeList.addAll(penaltyList);
		List<SpatialHistogramBucket> histogram = new ArrayList<SpatialHistogramBucket>();
		for(MapEntry< Double,List<MapEntry<DoublePointRectangle, RTree.Node>>> entry : nodeList){
			SpatialHistogramBucket mbr = null;
			int weight = 0;
			for (MapEntry<DoublePointRectangle, RTree.Node> value :  entry.getValue()){
				if(mbr == null){
					mbr = new SpatialHistogramBucket(value.getKey());
				}else
					mbr.union(value.getKey());
				weight +=value.getValue().number();
				// compute avg
				Iterator it = value.getValue().entries();
				while(it.hasNext()){
					DoublePointRectangle rec = (DoublePointRectangle) bulkLoadedRTree.descriptor(it.next());
					mbr.updateAverage(rec);
				}
				
			}
			mbr.setWeight(weight);
			histogram.add(mbr);
		}
		// todo compute
		return histogram;
	}
	
	/**
	 * 
	 * @param bulkLoadedRTree
	 * @param numberOfHyperBlocks
	 * @param ratio
	 * @return
	 */
	public static List<SpatialHistogramBucket> buildRKHist2(RTree bulkLoadedRTree,  int numberOfNodes,   
			int numberOfHyperBlocks, double ratio, int dimension){
		// partition the 

		double overestimation =  (numberOfHyperBlocks-( numberOfHyperBlocks*ratio)); 
		int hyperBlockSize = (int) Math.ceil(numberOfNodes/overestimation);
		int floor = (int) Math.floor(numberOfNodes/overestimation);
		int maxHC = numberOfNodes /hyperBlockSize;
		int maxHF = numberOfNodes /floor;
		int penaltyNumber = (int) ((numberOfHyperBlocks*ratio)/2);
		System.out.println("Overestimetion: " + overestimation);
		System.out.println("Ceil: "  +hyperBlockSize + " floor: " + floor);
		if ((penaltyNumber/2) + maxHF <= numberOfHyperBlocks){
			hyperBlockSize = floor;
		}
		System.out.println("hyper block size:  " + hyperBlockSize);
		List<MapEntry< Double,List<MapEntry<DoublePointRectangle, ORTree.IndexEntry>>>> nodeList = new ArrayList<MapEntry< Double,List<MapEntry<DoublePointRectangle, ORTree.IndexEntry>>>>();
		List<MapEntry< Double,List<MapEntry<DoublePointRectangle, ORTree.IndexEntry>>>> penaltyList = new ArrayList<MapEntry< Double,List<MapEntry<DoublePointRectangle, ORTree.IndexEntry>>>>();
		// cursor of level 0 leaf nodes buckets
//		Cursor<MapEntry<DoublePointRectangle, RTree.Node>> nodes = RGOhist.getNodesAndMBRs(bulkLoadedRTree, 1);
		
		
		Cursor<MapEntry<DoublePointRectangle, ORTree.IndexEntry>> nodes =  SpatialUtils.getIndexEntriesAndMBRs(bulkLoadedRTree, 1); 
		
		// create hyperblocks and create penalty list;
		while(nodes.hasNext()){
			List<MapEntry<DoublePointRectangle, ORTree.IndexEntry>> hyperNode = new ArrayList<MapEntry<DoublePointRectangle, ORTree.IndexEntry>>();
			for(int i = 0; nodes.hasNext() && i < hyperBlockSize; i++ ){
				MapEntry<DoublePointRectangle, ORTree.IndexEntry> h = nodes.next();
				hyperNode.add(h);
			}
			double costs =  kMetricCostsIndex(hyperNode, dimension);
			nodeList.add(new MapEntry<Double, List<MapEntry<DoublePointRectangle,ORTree.IndexEntry>>>(costs, hyperNode));
		}
		// sort list and take penaltyNumber
		Collections.sort(nodeList, new Comparator<MapEntry< Double,List<MapEntry<DoublePointRectangle, ORTree.IndexEntry>>>>() {

			@Override
			public int compare(
					MapEntry<Double, List<MapEntry<DoublePointRectangle, ORTree.IndexEntry>>> arg0,
					MapEntry<Double, List<MapEntry<DoublePointRectangle,ORTree.IndexEntry>>> arg1) {
				return arg0.getKey().compareTo(arg1.getKey());
			}
		});
		penaltyList.addAll(nodeList.subList(0, penaltyNumber));
		nodeList.subList(0, penaltyNumber).clear();
		// step 2.
		boolean enhanceable = true;
		int splits = nodeList.size()+penaltyList.size(); 
mark:	while(enhanceable){
		//	if(penaltyList.size()  + overestimation >= numberOfHyperBlocks)
			if(penaltyList.size()  + nodeList.size() >= numberOfHyperBlocks)
				break mark;
			List<MapEntry< Double,List<MapEntry<DoublePointRectangle,ORTree.IndexEntry>>>> penaltyTempList = new 
			ArrayList<MapEntry< Double,List<MapEntry<DoublePointRectangle,ORTree.IndexEntry>>>>();
			boolean noChange = true;
formark:	for(int i = 0; i < penaltyList.size(); i++){
				MapEntry< Double,List<MapEntry<DoublePointRectangle,ORTree.IndexEntry>>> hyperNode = 
					penaltyList.get(i);
				List<MapEntry<DoublePointRectangle, ORTree.IndexEntry>> list = hyperNode.getValue();
				int argMin = 0;
				double min = Double.MAX_VALUE;
				double argCostLeft = 0;
				double argCostRight = 0;
				for(int j = 0; j < list.size()-1; j++){
					double costsLeft = kMetricCostsIndex(list.subList(0, j+1),  dimension);
					double costRight = kMetricCostsIndex(list.subList(j+1, list.size()),  dimension);
					if (costsLeft + costRight < min){
						min = costsLeft+ costRight;
						argCostLeft = costsLeft;
						argCostRight = costRight;
						argMin = j;
					}
				}
				if(min < hyperNode.getKey()){
					penaltyTempList.add(new MapEntry< Double,List<MapEntry<DoublePointRectangle,ORTree.IndexEntry>>> (argCostLeft,
							list.subList(0, argMin+1) ));
					penaltyTempList.add(new MapEntry< Double,List<MapEntry<DoublePointRectangle, ORTree.IndexEntry>>> (argCostRight,
							list.subList(argMin+1, list.size()) ));
					penaltyList.remove(i);
					i--;
					splits++;
					noChange = false;
					if (splits  >= numberOfHyperBlocks){
						break formark;
					}
				}
			}
			penaltyList.addAll(penaltyTempList);
			enhanceable = !noChange; 
		}
		// step 3. merge to histogram
		nodeList.addAll(penaltyList);
		List<SpatialHistogramBucket> histogram = new ArrayList<SpatialHistogramBucket>();
		for(MapEntry< Double,List<MapEntry<DoublePointRectangle, ORTree.IndexEntry>>> entry : nodeList){
			SpatialHistogramBucket mbr = null;
			int weight = 0;
			for (MapEntry<DoublePointRectangle,ORTree.IndexEntry> value :  entry.getValue()){
				 Node node =  getNode(value.getValue());
				
				if(mbr == null){
					mbr = new SpatialHistogramBucket(value.getKey());
				}else
					mbr.union(value.getKey());
				weight +=node.number();
				// compute avg
				Iterator it = node.entries();
				while(it.hasNext()){
					DoublePointRectangle rec = (DoublePointRectangle) bulkLoadedRTree.descriptor(it.next());
					mbr.updateAverage(rec);
				}
				
			}
			mbr.setWeight(weight);
			histogram.add(mbr);
		}
		// todo compute
		return histogram;
	}
	
	
	private static  Node getNode(ORTree.IndexEntry indeEntry){
		RTree.Node node = (RTree.Node)indeEntry.get();
		return node;
	} 
	
	
	/**
	 * 
	 * @param rectangles
	 * @param dimension
	 * @return
	 */
	@SuppressWarnings("rawtypes")
	public static double kMetricCosts(List<MapEntry<DoublePointRectangle, RTree.Node>> rectangles, int dimension){
		List<DoublePoint> recs = new ArrayList<DoublePoint>();		
		DoublePointRectangle uni = null;
		for(MapEntry<DoublePointRectangle, RTree.Node> entry: rectangles){
			Iterator it = entry.getValue().entries();
			while(it.hasNext()){
				DoublePointRectangle rec = (DoublePointRectangle)it.next();
				recs.add(rec.getCenter());
				// FIXME
				if(uni ==  null){
					uni = new DoublePointRectangle(rec);
				}else{
					uni.union(rec);
				}
			}
		}
		return kMetricCost( recs,   uni);
	}
	
	
	/**
	 * 
	 * @param rectangles
	 * @param dimension
	 * @return
	 */
	@SuppressWarnings("rawtypes")
	public static double kMetricCostsIndex(List<MapEntry<DoublePointRectangle,ORTree.IndexEntry>> rectangles, int dimension){
		List<DoublePoint> recs = new ArrayList<DoublePoint>();		
		DoublePointRectangle uni = null;
		for(MapEntry<DoublePointRectangle, ORTree.IndexEntry> entry: rectangles){
			Node node = getNode(entry.getValue());
			Iterator it = node.entries();
			while(it.hasNext()){
				DoublePointRectangle rec = (DoublePointRectangle)it.next();
				recs.add(rec.getCenter());
				// FIXME
				if(uni ==  null){
					uni = new DoublePointRectangle(rec);
				}else{
					uni.union(rec);
				}
			}
		}
		return kMetricCost( recs,   uni);
	}
	
	/**
	 * 
	 * @param recs
	 * @param dimension
	 * @return
	 */
	public static double kMetricCost(List<DoublePoint> recs, DoublePointRectangle uni){
		List<Double> volumes = new ArrayList<Double>();
		kMetricCosts(recs, 0, uni.dimensions(), volumes, uni);
		double sum = 0;
		for(Double vol : volumes){
			sum +=vol;
		}
		double avg = sum / volumes.size();
		sum = 0;
		for(Double vol : volumes){
			sum +=Math.pow((vol - avg), 2);
		}
		return sum;
	}
	/**
	 * 
	 * @param recs
	 * @param currentDim
	 * @param dimensions
	 * @param volumes
	 * @param vol
	 */
	public static void kMetricCosts(List<DoublePoint> recs, final int currentDim, int dimensions, List<Double> volumes, DoublePointRectangle vol){
		if (recs.size() == 1){
			//compute 
			volumes.add(vol.area());
			return;
		}
		// find median
		int nextDim = (currentDim+1) % dimensions;
		// FIXME  change to 5 median problem in linear time
		// now simple solution sort and take n/2 
		Collections.sort(recs, new Comparator<DoublePoint>() {

			@Override
			public int compare(DoublePoint o1, DoublePoint o2) {
				double d1 = o1.getValue(currentDim);
				double d2 = o2.getValue(currentDim);
				return (d1==d2)? 0 : (d1>d2)? 1:-1 ;
				}
			}
		);
		DoublePoint p = recs.get(recs.size()/2);
		double val = p.getValue(currentDim);
		// compute mmbr left mbr right
		DoublePointRectangle[] leftRight = cut(vol, val, currentDim);
//		System.out.println("cut  " + Arrays.toString(leftRight));
//		System.out.println("#######################################");
		// process left 
		kMetricCosts(recs.subList(0, recs.size()/2), nextDim, dimensions, volumes, leftRight[0]);
		kMetricCosts(recs.subList(recs.size()/2, recs.size()), nextDim, dimensions, volumes, leftRight[1]);
	}
	/**
	 * 
	 * @param rec
	 * @param val
	 * @param dim
	 * @return
	 */
	public static DoublePointRectangle[] cut(DoublePointRectangle rec, double val, int dim){
		DoublePointRectangle[] recs = new DoublePointRectangle[2];
		DoublePoint pl1 = (DoublePoint) rec.getCorner(false);
		DoublePoint ph2 = (DoublePoint) rec.getCorner(true);
		double[] p1 = new double[rec.dimensions()];
		double[] p2 = new double[rec.dimensions()];
		for(int i = 0 ; i < ph2.dimensions(); i++){
			p1[i] = ph2.getValue(i);
			p2[i] = pl1.getValue(i);
		}
		p1[dim] = val;
		p2[dim] = val;
		DoublePoint ph1 = new DoublePoint(p1);
		DoublePoint pl2 = new DoublePoint(p2);
		recs[0] = new DoublePointRectangle(pl1, ph1);
		recs[1] = new DoublePointRectangle(pl2, ph2);
		return recs;
	}
	
}
