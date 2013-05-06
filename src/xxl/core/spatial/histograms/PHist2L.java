package xxl.core.spatial.histograms;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import xxl.core.indexStructures.rtrees.GenericPartitioner;
import xxl.core.indexStructures.rtrees.GenericPartitioner.Bucket;
import xxl.core.indexStructures.rtrees.GenericPartitioner.CostFunctionArrayProcessor;
import xxl.core.spatial.rectangles.DoublePointRectangle;



/**
 * 
 *  
 * histogram build approach first level leaf level with normal sopt/gopt/nopt second level with sopt (i, numberOfBuckets)
 *
 */
public class PHist2L {
	
	public static enum HistType{
		SOPT,
		GOPT,
		NOPT
	}
	

	
	
	/**
	 * Assumption the input size can be hold in main memory
	 * 
	 * 
	 * @param levelEntries
	 * @param minWeight
	 * @param maxWeight
	 * @param partitionSize
	 * @param numberOfBuckets
	 * @param pType
	 * @return
	 */
	public static List<WeightedDoublePointRectangle> computeHistogramOPT(Iterator<WeightedDoublePointRectangle> levelEntries,
			int minWeight, int maxWeight,  int inputSize, int numberOfBuckets, CostFunctionArrayProcessor<DoublePointRectangle> processor, HistType type){
		WeightedDoublePointRectangle[] processingList = toWeightedArray(levelEntries, inputSize);
		List<WeightedDoublePointRectangle> histogram = new ArrayList<WeightedDoublePointRectangle>();
		// compute distribution 
		int[] distribution = null;
		processor.reset();
		switch(type){
			case GOPT : {
				Bucket[] buckets = GenericPartitioner.computeGOPT(processingList, minWeight, maxWeight, processor);
				distribution = GenericPartitioner.getDistribution(buckets[processingList.length-1]);
				
			}break;
			case NOPT : {
				Bucket[][] buckets = GenericPartitioner.computeNOPT(processingList,  numberOfBuckets, processor);	
				distribution =  GenericPartitioner.getDistribution(buckets[numberOfBuckets-1][inputSize-1]);
			}break;
			default :{
				Bucket[][] buckets = GenericPartitioner.computeOPTF(processingList, minWeight, maxWeight, numberOfBuckets, processor);	
				distribution =  GenericPartitioner.getDistribution(buckets[numberOfBuckets-1][inputSize-1]);
			}
		}
		
		int k = 0; 
		for(int i: distribution){
			WeightedDoublePointRectangle sumRec = null;
			int weight= 0;
			for(int j = 0; j < i ; j++, k++){
				if (sumRec == null){
					sumRec = new WeightedDoublePointRectangle(processingList[k]);
				}else{
					// XXX overwrite union idea
					sumRec.union(processingList[k]);
				}
				weight+=processingList[k].getWeight();
				sumRec.updateAverage(processingList[k].avgExtent);
				//XXX update average
			}
			sumRec.setWeight(weight);
			histogram.add(sumRec);
		}
		processor.reset();
		return histogram;
	}
	
	
	/**
	 * Assumption the input size can be hold in main memory
	 * 
	 * 
	 * @param levelEntries
	 * @param minWeight
	 * @param maxWeight
	 * @param partitionSize
	 * @param numberOfBuckets
	 * @param pType
	 * @return
	 */
	public static List<WeightedDoublePointRectangle> computeHistogramOPT(
			Iterator<WeightedDoublePointRectangle> levelEntries,
			int b, int B, 
			int inputSize, int numberOfBuckets, double spaceUtil,
			CostFunctionArrayProcessor<DoublePointRectangle> processor,
			HistType type, 
			int chunkSize){
		List<WeightedDoublePointRectangle> histogram = new ArrayList<WeightedDoublePointRectangle>();
		List<WeightedDoublePointRectangle> buffer = new ArrayList<WeightedDoublePointRectangle>(chunkSize);
		for(; levelEntries.hasNext() ; ){
			
			for(int i= 0 ; i < chunkSize && levelEntries.hasNext(); i++){
				WeightedDoublePointRectangle rec = levelEntries.next();
				buffer.add(rec); 
			}
			
			
			// compute distribution 
			int[] distribution = null;
			WeightedDoublePointRectangle[] processingList =  toWeightedArray(buffer.iterator(), buffer.size());
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
				WeightedDoublePointRectangle sumRec = null;
				int weight= 0;
				for(int j = 0; j < i ; j++, k++){
					if (sumRec == null){
						sumRec = new WeightedDoublePointRectangle(processingList[k]);
					}else{
						// XXX overwrite union idea
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
	 * Assumption the input size can be hold in main memory
	 * 
	 * 
	 * @param levelEntries
	 * @param minWeight
	 * @param maxWeight
	 * @param partitionSize
	 * @param numberOfBuckets
	 * @param pType
	 * @return
	 */
	public static List<WeightedDoublePointRectangle> computeHistogramOPT(Iterator<WeightedDoublePointRectangle> levelEntries,
			int minWeight, int maxWeight,  int inputSize, int numberOfBuckets, CostFunctionArrayProcessor<DoublePointRectangle> processor, boolean gopt){
		WeightedDoublePointRectangle[] processingList = toWeightedArray(levelEntries, inputSize);
		List<WeightedDoublePointRectangle> histogram = new ArrayList<WeightedDoublePointRectangle>();
		// compute distribution 
		int[] distribution = null;
		processor.reset();
		if(gopt){
			Bucket[] buckets = GenericPartitioner.computeGOPT(processingList, minWeight, maxWeight, processor);
			distribution = GenericPartitioner.getDistribution(buckets[processingList.length-1]);
		}else{
			Bucket[][] buckets = GenericPartitioner.computeOPTF(processingList, minWeight, maxWeight, numberOfBuckets, processor);	
			distribution =  GenericPartitioner.getDistribution(buckets[numberOfBuckets-1][inputSize-1]);
		}
		int k = 0; 
		for(int i: distribution){
			WeightedDoublePointRectangle sumRec = null;
			int weight= 0;
			for(int j = 0; j < i ; j++, k++){
				if (sumRec == null){
					sumRec = new WeightedDoublePointRectangle(processingList[k]);
				}else{
					// XXX overwrite union idea
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
	
	


	
	/*
	 * 
	 */
	public static WeightedDoublePointRectangle[] toWeightedArray(Iterator<WeightedDoublePointRectangle> iterator, int size){
		WeightedDoublePointRectangle[] recs = new WeightedDoublePointRectangle[size];
		int i = 0; 
		while(iterator.hasNext()){
			recs[i] = iterator.next();
			i++;
		}
		return recs;
	}
	
	
}
