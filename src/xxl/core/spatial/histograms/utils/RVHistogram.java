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
import java.util.Iterator;
import java.util.List;

import xxl.core.functions.Functional.UnaryFunction;
import xxl.core.indexStructures.RTree;
import xxl.core.indexStructures.rtrees.GenericPartitioner;
import xxl.core.indexStructures.rtrees.RtreeIterativeBulkloader;
import xxl.core.indexStructures.rtrees.GenericPartitioner.Bucket;
import xxl.core.indexStructures.rtrees.GenericPartitioner.CostFunctionArrayProcessor;
import xxl.core.indexStructures.rtrees.GenericPartitioner.DefaultArrayProcessor;
import xxl.core.spatial.rectangles.DoublePointRectangle;



/**
 * 
 * This class provides Spatial Histogram construction methods ( 
 * We refer for details to  D. Achakeev and B. Seeger A class of R-tree histograms for spatial databases GIS 2012 ). 
 * These methods are used after, generating e.g. leaf node MBRs of an R-tree.  
 *  The input of the method is an iterator of {@link SpatialHistogramBucket} objects (micro clusters). 
 *  To map the leaf nodes of R-tree {@link RTree} to a an {@link SpatialHistogramBucket} object the following methods are provided   
 *  {@see SpatialHistogramUtils.getRectanglesLevel1}.
 * 
 * 
 *  
 * 
 *
 */
public class RVHistogram {
	/**
	 * These types definitions for processing the optimal partitioning. 
	 * The default method is SOPT
	 * 
	 */
	public static enum HistType{
		SOPT,
		GOPT,
		NOPT
	}
	/**
	 * Default min capacity ratio for histogram buckets. Let N be the number of input micro-clusters (e.g. leaf nodes), then N/m defines average number of micro clusters 
	 * per histogram bucket. We set the minimal bucket capacity as follows  (Math.max(Math.floor(  N/m * DEFAULT_MIN_CAPACITY_RATIO), 1))
	 * 
	 */
	public static final double DEFAULT_MIN_CAPACITY_RATIO = 0.5; 
	/**
	 * Default partition size. This parameter is used if the number of input micro clusters is too large to be process in main memory. 
	 * To this end, simple heuristic is applied: we apply dynamic programming of sufficient large partitons of an input data.  
	 */
	public static final int DEFAULT_CHUNK_SIZE = 10_0000; 
	/**
	 * creates and stores an array of {@link SpatialHistogramBucket} objects from iterator.
	 * @param iterator
	 * @param size
	 * @return
	 */
	public static SpatialHistogramBucket[] toWeightedArray(Iterator<SpatialHistogramBucket> iterator, int size){
		SpatialHistogramBucket[] recs = new SpatialHistogramBucket[size];
		int i = 0; 
		while(iterator.hasNext()){
			recs[i] = iterator.next();
			i++;
		}
		return recs;
	}
	
	

	
	/**
	 * This is a basic generic method for spatial histogram generation.  
	 * 
	 * @param levelEntries iterator containing {@link SpatialHistogramBucket} objects
	 * @param b minimal number of objects per histogram bucket
	 * @param B maximal number of objects per histogram bucket
	 * @param inputSize number of {@link SpatialHistogramBucket} buckets
	 * @param numberOfBuckets target number of histogram buckets
	 * @param spaceUtil {@link #DEFAULT_MIN_CAPACITY_RATIO}
	 * @param processor different cost function can be provided 
	 * @param type processing type of dynamic programmin scheme
	 * @param chunkSize chunk size
	 * @return a list of {@link SpatialHistogramBucket} 
	 */
	public static List<SpatialHistogramBucket> computeHistogramOPT(
			Iterator<SpatialHistogramBucket> levelEntries,
			int b, int B, 
			int inputSize, int numberOfBuckets, double spaceUtil,
			CostFunctionArrayProcessor<DoublePointRectangle> processor,
			HistType type, 
			int chunkSize){
		List<SpatialHistogramBucket> histogram = new ArrayList<SpatialHistogramBucket>();
		List<SpatialHistogramBucket> buffer = new ArrayList<SpatialHistogramBucket>(chunkSize);
		for(; levelEntries.hasNext() ; ){
			for(int i= 0 ; i < chunkSize && levelEntries.hasNext(); i++){
				SpatialHistogramBucket rec = levelEntries.next();
				buffer.add(rec); 
			}
			// compute distribution 
			int[] distribution = null;
			SpatialHistogramBucket[] processingList =  toWeightedArray(buffer.iterator(), buffer.size());
			int n = (int) (Math.ceil(processingList.length/(spaceUtil * B )));
			if (buffer.size() > B){
				processor.reset();
				switch(type){
					case GOPT : {
						Bucket[] buckets = GenericPartitioner.computeGOPT(processingList, b, B, processor);
						distribution = GenericPartitioner.getDistribution(buckets[processingList.length-1]);
						
					}break;
					case NOPT : {
						Bucket[][] buckets = GenericPartitioner.computeNOPT(processingList, n, processor);	
						distribution =  GenericPartitioner.getDistribution(buckets[n-1][processingList.length-1]);
					}break;
					default :{
						Bucket[][] buckets = GenericPartitioner.computeOPTF(processingList, b, B, n, processor);	
						distribution =  GenericPartitioner.getDistribution(buckets[n-1][processingList.length-1]);
					}
				}
				processor.reset();
			}
			else{
				distribution = new int [] {buffer.size()};
			}
			int k = 0; 
			for(int i: distribution){
				SpatialHistogramBucket sumRec = null;
				int weight= 0;
				for(int j = 0; j < i ; j++, k++){
					if (sumRec == null){
						sumRec = new SpatialHistogramBucket(processingList[k]);
					}else{
						sumRec.union(processingList[k]);
					}
					weight+=processingList[k].getWeight();
					sumRec.updateAverage(processingList[k].avgExtent);
				}
				sumRec.setWeight(weight);
				histogram.add(sumRec);
			}
			// clear buffer 
			buffer.clear();
		}
		return histogram;
	}
	
	
	/**
	 * Default method for computing a spatial histogram.  The cost function is a sum of MBR volumes. OPT-Partitioning is used. 
	 * Use this method if the input size cannot be processed in memory.  
	 *  
	 * @param levelEntries
	 * @param inputSize
	 * @param numberOfBuckets
	 * @param chunkSize
	 * @return
	 */
	public static List<SpatialHistogramBucket> computeRVHistogramChunkHeuristic(Iterator<SpatialHistogramBucket> levelEntries, int inputSize, int numberOfBuckets,
			int chunkSize){
		UnaryFunction<DoublePointRectangle, Double> function =  new UnaryFunction<DoublePointRectangle, Double>() {

			@Override
			public Double invoke(DoublePointRectangle arg) {
				DoublePointRectangle rec =   new DoublePointRectangle(arg);
				return  rec.area();
			}
			
		};
		CostFunctionArrayProcessor<DoublePointRectangle> arrayProcessor = new DefaultArrayProcessor(function); 
		double f = inputSize/ (double)numberOfBuckets; // 1-avgLoad 
		int d = (int) Math.ceil((inputSize/ ((double)numberOfBuckets)));
		int b = (int)(Math.max(Math.floor(f * DEFAULT_MIN_CAPACITY_RATIO), 1));
		b = Math.max(b, 2);
		int B = b+d;//
		double rat = ((double)f)/ (double)B; // ~2/3 if ratio = 0.5
		return computeHistogramOPT(	levelEntries,	b, 	B, 	inputSize, numberOfBuckets, rat, 	arrayProcessor,
				HistType.SOPT, 
				chunkSize); 
	}
	
	
	/**
	 * Default method for computing a spatial histogram.  The cost function is a sum of MBR volumes. OPT-Partitioning is used. 
	 * Use this method if the input size cannot be processed in memory.  
	 *  #DEFAULT_CHUNK_SIZE is used.
	 *  
	 * @param levelEntries
	 * @param inputSize
	 * @param numberOfBuckets
	 * @return
	 */
	public static List<SpatialHistogramBucket> computeRVHistogramChunkHeuristic(Iterator<SpatialHistogramBucket> levelEntries,
			int inputSize, int numberOfBuckets){
		return computeRVHistogramChunkHeuristic(
				levelEntries,
				inputSize,  numberOfBuckets,
				DEFAULT_CHUNK_SIZE); 
	}
	
	/**
	 * Default method for computing a spatial histogram.  The cost function is a sum of MBR volumes. OPT-Partitioning is used. 
	 * Use this method if the input size cannot be processed in memory.  
	 *  
	 * @param levelEntries
	 * @param inputSize
	 * @param numberOfBuckets
	 * @param chunkSize
	 * @return
	 */
	public static List<SpatialHistogramBucket> computeRVHistogram(Iterator<SpatialHistogramBucket> levelEntries, int inputSize, int numberOfBuckets){
		UnaryFunction<DoublePointRectangle, Double> function =  new UnaryFunction<DoublePointRectangle, Double>() {

			@Override
			public Double invoke(DoublePointRectangle arg) {
				DoublePointRectangle rec =   new DoublePointRectangle(arg);
				return  rec.area();
			}
			
		};
		CostFunctionArrayProcessor<DoublePointRectangle> arrayProcessor = new DefaultArrayProcessor(function); 
		double f = inputSize/ (double)numberOfBuckets; // 1-avgLoad 
		int d = (int) Math.ceil((inputSize/ ((double)numberOfBuckets)));
		int b = (int)(Math.max(Math.floor(f * DEFAULT_MIN_CAPACITY_RATIO), 1));
		b = Math.max(b, 2);
		int B = b+d;//
		return computeHistogramOPT(	levelEntries,	b, 	B, 	inputSize, numberOfBuckets,	arrayProcessor,
				HistType.SOPT); 
	}
	
	
	/**
	 * This is a generic spatial histogram computation method.
	 * Use this method if input set can be processed in main memory, otherwise consider to use a heuristic method {@link #computeHistogramOPT(Iterator, int, int, int, int, double, CostFunctionArrayProcessor, HistType, int)}
	 * 
	 * 
	 * @param levelEntries
	 * @param b
	 * @param B
	 * @param inputSize
	 * @param numberOfBuckets
	 * @param processor
	 * @param type
	 * @return
	 */
	public static List<SpatialHistogramBucket> computeHistogramOPT(Iterator<SpatialHistogramBucket> levelEntries,
			int b, int B,  
			int inputSize, 
			int numberOfBuckets, 
			CostFunctionArrayProcessor<DoublePointRectangle> processor, 
			HistType type){
		SpatialHistogramBucket[] processingList = toWeightedArray(levelEntries, inputSize);
		List<SpatialHistogramBucket> histogram = new ArrayList<SpatialHistogramBucket>();
		// compute distribution 
		int[] distribution = null;
		processor.reset();
		switch(type){
			case GOPT : {
				Bucket[] buckets = GenericPartitioner.computeGOPT(processingList, b, B, processor);
				distribution = GenericPartitioner.getDistribution(buckets[processingList.length-1]);
				
			}break;
			case NOPT : {
				Bucket[][] buckets = GenericPartitioner.computeNOPT(processingList,  numberOfBuckets, processor);	
				distribution =  GenericPartitioner.getDistribution(buckets[numberOfBuckets-1][inputSize-1]);
			}break;
			default :{
				Bucket[][] buckets = GenericPartitioner.computeOPTF(processingList, b, B, numberOfBuckets, processor);	
				distribution =  GenericPartitioner.getDistribution(buckets[numberOfBuckets-1][inputSize-1]);
			}
		}
		// update statistical information
		int k = 0; 
		for(int i: distribution){
			SpatialHistogramBucket sumRec = null;
			int weight= 0;
			for(int j = 0; j < i ; j++, k++){
				if (sumRec == null){
					sumRec = new SpatialHistogramBucket(processingList[k]);
				}else{
					sumRec.union(processingList[k]);
				}
				weight+=processingList[k].getWeight();
				sumRec.updateAverage(processingList[k].avgExtent);
			}
			sumRec.setWeight(weight);
			histogram.add(sumRec);
		}
		processor.reset();
		return histogram;
	}
}
