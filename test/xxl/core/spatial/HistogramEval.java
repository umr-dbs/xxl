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

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

import xxl.core.collections.containers.Container;
import xxl.core.collections.containers.io.BlockFileContainer;
import xxl.core.collections.containers.io.ConverterContainer;
import xxl.core.collections.queues.Queue;
import xxl.core.collections.queues.io.BlockBasedQueue;
import xxl.core.cursors.Cursor;
import xxl.core.cursors.Cursors;
import xxl.core.cursors.mappers.Mapper;
import xxl.core.cursors.mappers.ReservoirSampler;
import xxl.core.cursors.sorters.MergeSorter;
import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Constant;
import xxl.core.functions.Function;
import xxl.core.functions.Functional.UnaryFunction;
import xxl.core.indexStructures.RTree;
import xxl.core.indexStructures.rtrees.GenericPartitioner.DefaultArrayProcessor;
import xxl.core.indexStructures.rtrees.RtreeIterativeBulkloader;
import xxl.core.io.converters.ConvertableConverter;
import xxl.core.io.converters.Converter;
import xxl.core.math.Statistics;
import xxl.core.spatial.histograms.utils.MHistogram;
import xxl.core.spatial.histograms.utils.MHistograms;
import xxl.core.spatial.histograms.utils.MHistograms.MinSkewHistogram;
import xxl.core.spatial.histograms.utils.MHistograms.MinSkewProgressiveRefinementHistogram;
import xxl.core.spatial.histograms.utils.MHistograms.RHistogram;
import xxl.core.spatial.histograms.utils.MHistograms.RKHistHistogram;
import xxl.core.spatial.histograms.utils.MHistograms.RTreeBasicHistogram;
import xxl.core.spatial.histograms.utils.MHistograms.STHistForest;
import xxl.core.spatial.histograms.utils.RVHistogram.HistType;
import xxl.core.spatial.histograms.utils.PartitionerUtils.ProcessorType;
import xxl.core.spatial.rectangles.DoublePointRectangle;
import xxl.core.spatial.rectangles.Rectangles;


/**
 * 
 * This class provides classes for testing histogram tests
 *
 * 1. Object of this class creates an RTree which holds the data for testing
 * 2. The it build histogram for the data defined in constructor and runs queries against histogram  
 */
public class HistogramEval {
	//class vars
	public static final int BITS_PRO_DIM = 31;
	public static final int FILLING_CURVE_PRECISION = 1 << (BITS_PRO_DIM-1);
	public static final int DIMENSION = 2; 
	public static final int BLOCKSIZE = 100*32+6;//4096; //100*32+6; //  
	public static final boolean VERBOSE = true; 
	
	// object vars
	private int dimension = DIMENSION; 
	private Comparator<DoublePointRectangle>	comparator;
	private Converter<DoublePointRectangle> converter; 
	private String tempPath = "/";
	private RTree rtree;
	// histograms
	private RKHistHistogram rkHist;
	private RTreeBasicHistogram rTreeHist;
	private MinSkewHistogram minSkewHist;
	private MinSkewProgressiveRefinementHistogram minSkewProgressive; 
	private STHistForest stHistForest;
	//rhistograms
	private RHistogram rhistogram_RK;
	private RHistogram rhistogram_SKEW;
	private RHistogram rhistogram_QA;
	private RHistogram rhistogram_V;
	private RHistogram rhistogram_NOPT;
	
	public PrintStream stream; 
	
	/**
	 * 
	 * @param inputData
	 * @param tempPath
	 * @throws IOException
	 */
	public HistogramEval(Cursor<DoublePointRectangle> inputData, String tempPath, int dimension) throws IOException{
		// build RTree over the data set
		comparator  = (dimension == 2 ) ?  SpatialUtils.getHilbert2DComparator(SpatialUtils.universeUnit(dimension), FILLING_CURVE_PRECISION)
					: SpatialUtils.getZCurveComparator(SpatialUtils.universeUnit(dimension), BITS_PRO_DIM); // default 2D comparator
		converter = new ConvertableConverter<DoublePointRectangle>(SpatialUtils.factoryFunction(dimension)); 
		this.tempPath = tempPath;
		rtree =  buildExtRtree(sortData(inputData));
	}
	
	/**
	 * 
	 * @param inputData
	 * @param tempPath
	 * @throws IOException
	 */
	public HistogramEval(Cursor<DoublePointRectangle> inputData, String tempPath) throws IOException{
		this(inputData, tempPath, DIMENSION);
	}
	/* ************************************************************************** 
	 * Util Part
	 * ***************************************************************************
	 */
	/**
	 * 
	 * @return
	 */
	private Cursor<DoublePointRectangle> getData(){
		return new Mapper<Object, DoublePointRectangle>( new AbstractFunction<Object, DoublePointRectangle>() {
			@Override
			public DoublePointRectangle invoke(Object argument) {
				return (DoublePointRectangle)rtree.descriptor(argument);
			}
		}, rtree.query(0)){
			
			@Override
			public boolean supportsReset() {
				// TODO Auto-generated method stub
				return true;
			}
			
			
			@Override
			public void reset() throws UnsupportedOperationException {
				
//				super.reset();
				inputs.clear();
				inputs.add(rtree.query(0));
				
				hasNext = false;
				computedHasNext = false;
				isValid = false;
				assignedNext = false;
				
			}
		};
	}
	
	/**
	 * sorts data 
	 * @param rectangles
	 * @return
	 */
	private Cursor<DoublePointRectangle> sortData(Cursor<DoublePointRectangle> rectangles){
		
		final Container queueContainer = new BlockFileContainer(tempPath + "tmpsortqueue.tmp" ,BLOCKSIZE);
		final Function<Function<?, Integer>, Queue<?>> queueFunction =
				new AbstractFunction<Function<?, Integer>, Queue<?>>() {
				public Queue<?> invoke(Function<?, Integer> function1, Function<?, Integer> function2) {
					return new BlockBasedQueue(queueContainer, BLOCKSIZE, converter,
							function1, function2);
				}
			};
		
		return 	new MergeSorter(rectangles, comparator, 8*2*2, BLOCKSIZE * 2000*10, BLOCKSIZE * 2000*10, queueFunction, false);
	}
	/**
	 * 
	 * @param rectCursor
	 * @param pType
	 * @return
	 * @throws IOException
	 */
	private RTree buildExtRtree(Cursor<DoublePointRectangle> rectCursor) throws IOException{
		if(VERBOSE)
			System.out.println("Start build RTree for data");
		
		RTree sortBasedRTree = new RTree();
		Container treeContainer =  new ConverterContainer( new BlockFileContainer(tempPath, BLOCKSIZE), 
				sortBasedRTree.nodeConverter(Rectangles.getDoublePointRectangleConverter(dimension), dimension));
		sortBasedRTree.determineContainer = new Constant<Object>(treeContainer);
		sortBasedRTree.getContainer =new Constant<Object>(treeContainer);
		boolean processList =  true;
		double[] sideLength = new double[dimension];
		for(int i = 0; i < sideLength.length; i++){
			sideLength[i] = 0d;
		}
		UnaryFunction<DoublePointRectangle, Double> costFunction = RtreeIterativeBulkloader.generateDefaultFunction(sideLength);
		DefaultArrayProcessor arrayProcessor = new DefaultArrayProcessor(costFunction, processList);
		RtreeIterativeBulkloader<DoublePointRectangle> bulkLoader = new RtreeIterativeBulkloader<DoublePointRectangle>(sortBasedRTree, 
				tempPath,  dimension, BLOCKSIZE, 0.33, 0.9, 20_000);
		bulkLoader.init(arrayProcessor, RtreeIterativeBulkloader.ProcessingType.GOPT, dimension*8*2, Rectangles.getDoublePointRectangleConverter(dimension),  new UnaryFunction<DoublePointRectangle, DoublePointRectangle>() {
			@Override
			public DoublePointRectangle invoke(DoublePointRectangle arg) {
				return new DoublePointRectangle(arg);
			}
		}); 
		bulkLoader.buildRTree(rectCursor); 
		if(VERBOSE)
			System.out.println("Evaluator is initialized!");
		return bulkLoader.getRTree();
	}
	
	/* ************************************************************************** 
	 * End Util Part
	 * ***************************************************************************
	 */
	
	/* ************************************************************************** 
	 * Histogram Part
	 * ***************************************************************************
	 */
	/**
	 * 
	 * @param blockSize
	 * @param alpha
	 * @param verbose
	 * @throws IOException
	 */
	public void buildRKHist(int numberOfBuckets, double alpha, boolean verbose) throws IOException{
		if(verbose)
			System.out.println("build RKHist");
		long time = System.currentTimeMillis();
		Properties props = new Properties(); 
		props.setProperty(MHistograms.RTREE_BLOCK_SIZE, new Integer(BLOCKSIZE).toString());
		props.setProperty(MHistograms.RTREE_PATH, tempPath + "rkHist");
		props.setProperty(MHistograms.RKHIST_U_RATIO, new Double(alpha).toString());
		props.setProperty(MHistograms.RTREE_UTIL, new Double(0.99).toString());
		rkHist = new MHistograms.RKHistHistogram();
		rkHist.buildHistogram(getData(), numberOfBuckets , props);
		if (verbose){
			System.out.println("Build Time: " + (System.currentTimeMillis() - time)+ "  " + rkHist.numberOfBuckets());
		}
	}
	
	/**
	 * 
	 * @param blockSize
	 * @param alpha
	 * @param verbose
	 * @throws IOException
	 */
	public void buildRKHist(int numberOfBuckets, double alpha, int blockSize,  boolean verbose) throws IOException{
		if(verbose)
			System.out.println("build RKHist");
		long time = System.currentTimeMillis();
		Properties props = new Properties(); 
		props.setProperty(MHistograms.RTREE_BLOCK_SIZE, new Integer(blockSize).toString());
		props.setProperty(MHistograms.RTREE_PATH, tempPath + "rkHist");
		props.setProperty(MHistograms.RKHIST_U_RATIO, new Double(alpha).toString());
		props.setProperty(MHistograms.RTREE_UTIL, new Double(0.8).toString());
		if (rkHist == null){
			rkHist = new MHistograms.RKHistHistogram();
		}
		rkHist.buildHistogram(getData(), numberOfBuckets , props);
		Cursor<DoublePointRectangle> data = getData();
		if (verbose){
			System.out.println("Build Time: " + (System.currentTimeMillis() - time)+ "  " + rkHist.numberOfBuckets());
		}
	}
	/**
	 * 
	 * @param blockSize
	 * @param verbose
	 * @throws IOException
	 */
	public void buildRTreeHist(int numberOfBuckets, boolean verbose) throws IOException{
		if(verbose)
			System.out.println("build RTreeHist");
		long time = System.currentTimeMillis();
		Properties props = new Properties(); 
		props.setProperty(MHistograms.RTREE_BLOCK_SIZE, new Integer(BLOCKSIZE).toString());
		props.setProperty(MHistograms.RTREE_PATH, tempPath + "RTreeSimple");
		props.setProperty(MHistograms.RTREE_UTIL, new Double(0.5).toString());
		rTreeHist = new RTreeBasicHistogram();
		rTreeHist.buildHistogram(getData(), numberOfBuckets, props);
		if (verbose){
			System.out.println("Build Time: " + (System.currentTimeMillis() - time));
		}
	}
	/**
	 * 
	 * @param gridSize
	 * @param verbose
	 * @throws IOException 
	 */
	public void buildMinSkewHist(int numberOfBuckets, int gridSize, boolean verbose) throws IOException{
		if(verbose)
			System.out.println("build MinSkew");
		long time = System.currentTimeMillis();
		Properties props = new Properties(); 
		props.setProperty(MHistograms.MINSKEW_PATH, tempPath + ".minskewTemp");
		props.setProperty(MHistograms.MINSKEW_GRID_SIZE, new Integer(gridSize).toString());
		minSkewHist = new MinSkewHistogram();
		minSkewHist.buildHistogram(getData(), numberOfBuckets, props);
		if (verbose){
			System.out.println("Build Time: " + (System.currentTimeMillis() - time) + "  " + minSkewHist.numberOfBuckets());
		}
	}
	
	/**
	 * 
	 * @param gridSize
	 * @param verbose
	 * @throws IOException 
	 */
	public void buildMinSkewProgressiveHist(int numberOfBuckets,
			int gridSize,
			int refinementSteps,
			boolean verbose) throws IOException{
		if(verbose)
			System.out.println("build MinSkew");
		long time = System.currentTimeMillis();
		Properties props = new Properties(); 
		props.setProperty(MHistograms.MINSKEW_PATH, tempPath + ".minskewTempRef");
		props.setProperty(MHistograms.MINSKEW_GRID_SIZE, new Integer(gridSize).toString());
		props.setProperty(MHistograms.MINSKEW_REF, new Integer(refinementSteps).toString());
		minSkewProgressive = new MinSkewProgressiveRefinementHistogram();
		minSkewProgressive.buildHistogram(getData(), numberOfBuckets, props);
		if (verbose){
			System.out.println("Build Time: " + (System.currentTimeMillis() - time) +  "  " + minSkewProgressive.numberOfBuckets() );
		}
	}
	/**
	 * 
	 * @param verbose
	 * @throws IOException 
	 */
	public void buildSTForestHist(int numberOfBuckets, double samplingRate, boolean verbose) throws IOException{
		if(verbose)
			System.out.println("build STHistForest");
		Properties props = new Properties(); 
		stHistForest = new STHistForest(samplingRate);
		int count = Cursors.count(getData());
		
		int samplingSize =  (int) ((double)count*samplingRate);
		if(verbose)
			System.out.println("Sampling size for stforest: "  
				 + samplingSize);
		Iterator<DoublePointRectangle> dataIterator = (samplingRate >= 1.0 )  ? getData(): HistogramUtils.createRandomSFCBasedSample
				(getData(), samplingSize, ReservoirSampler.RTYPE);
		long time = System.currentTimeMillis();
		stHistForest.buildHistogram(Cursors.wrap(dataIterator), numberOfBuckets, props);
		if (verbose){
			System.out.println("Build Time: " + (System.currentTimeMillis() - time));
		}
	}
//	/**
//	 * 
//	 * @param blockSize
//	 * @param verbose
//	 * @throws IOException 
//	 */
//	public void buildSOPTRtreeHist(int numberOfBuckets, boolean verbose) throws IOException{
//		if(verbose)
//			System.out.println("build soptHist");
//		long time = System.currentTimeMillis();
//		Properties props = new Properties(); 
//		props.setProperty(MHistograms.RTREE_BLOCK_SIZE, new Integer(BLOCKSIZE).toString());
//		props.setProperty(MHistograms.RTREE_RATIO, new Double(0.5).toString());
//		props.setProperty(MHistograms.RTREE_PATH, tempPath + "soptTree");
//		soptHist = new SOPTHistogram();
//		soptHist.buildHistogram(getData(), numberOfBuckets, props);
//		if (verbose){
//			System.out.println("Build Time: " + (System.currentTimeMillis() - time));
//		}
//	}
	
	/**
	 * 
	 * @param blockSize
	 * @param verbose
	 * @throws IOException 
	 */
	public void buildRHistogramRK(int numberOfBuckets, double rtreeRatio, double hRatio, double avgRatio, boolean verbose) throws IOException{
		 buildRHistogramRK(numberOfBuckets, rtreeRatio,hRatio, avgRatio, HistType.SOPT, verbose); 
	}
	
	public void buildRHistogramSKEW(int numberOfBuckets, double rtreeRatio, double hRatio, double avgRatio, boolean verbose) throws IOException{
		buildRHistogramSKEW(numberOfBuckets, rtreeRatio, hRatio,  avgRatio, HistType.SOPT,  verbose);
	}
	
	public void buildRHistogramQA(int numberOfBuckets, Cursor<DoublePointRectangle> queries, 
			double rtreeRatio, double hRatio, double avgRatio,   boolean verbose) throws IOException{
		buildRHistogramQA(numberOfBuckets, queries, 
				rtreeRatio, hRatio, avgRatio, HistType.SOPT,   verbose); 
	}
	
	
	public void buildRHistogramV(int numberOfBuckets, double rtreeRatio, boolean verbose) throws IOException{
		 buildRHistogramV(numberOfBuckets, rtreeRatio, rtreeRatio, 0.8, HistType.SOPT,  verbose);
	}
	
	public void buildRHistogramV(int numberOfBuckets, double rtreeRatio, double hRatio, double avgRatio, boolean verbose) throws IOException{
		 buildRHistogramV(numberOfBuckets, rtreeRatio, hRatio, avgRatio, HistType.SOPT,  verbose);
	}
	
	
	public void buildRHistogramNOPT(int numberOfBuckets, double rtreeRatio, double hRatio, double avgRatio, boolean verbose) throws IOException{
		 buildRHistogramNOPT(numberOfBuckets, rtreeRatio, hRatio, avgRatio, HistType.NOPT,  verbose);
	}
	
	/**
	 * 
	 * @param blockSize
	 * @param verbose
	 * @throws IOException 
	 */
	public void buildRHistogramRK(int numberOfBuckets, double rtreeRatio, double hRatio, double avgRatio, HistType histType,
			boolean verbose) throws IOException{
		if(verbose)
			System.out.println("build soptHist");
		long time = System.currentTimeMillis();
		Properties props = new Properties(); 
		props.setProperty(MHistograms.RTREE_BLOCK_SIZE, new Integer(BLOCKSIZE).toString());
		props.setProperty(MHistograms.RTREE_RATIO, new Double(0.5).toString());
		props.setProperty(MHistograms.RTREE_PATH, tempPath + "soptTreeRK");
		rhistogram_RK = new RHistogram(DIMENSION, BLOCKSIZE, rtreeRatio, hRatio, avgRatio,histType, ProcessorType.RK_HIST);
		rhistogram_RK.buildHistogram(getData(), numberOfBuckets, props);
		if (verbose){
			System.out.println("Build Time: " + (System.currentTimeMillis() - time));
		}
	}
	
	/**
	 * 
	 * @param blockSize
	 * @param verbose
	 * @throws IOException 
	 */
	public void buildRHistogramSKEW(int numberOfBuckets, double rtreeRatio, double hRatio, double avgRatio, HistType histType, boolean verbose) throws IOException{
		if(verbose)
			System.out.println("build soptHist");
		long time = System.currentTimeMillis();
		Properties props = new Properties(); 
		props.setProperty(MHistograms.RTREE_BLOCK_SIZE, new Integer(BLOCKSIZE).toString());
		props.setProperty(MHistograms.RTREE_RATIO, new Double(0.5).toString());
		props.setProperty(MHistograms.RTREE_PATH, tempPath + "soptTreeSKEW");
		if(rhistogram_SKEW == null)
			rhistogram_SKEW = new RHistogram(DIMENSION, BLOCKSIZE, rtreeRatio, hRatio, avgRatio, histType, ProcessorType.GRID_SSE);
		rhistogram_SKEW.buildHistogram(getData(), numberOfBuckets, props);
		if (verbose){
			System.out.println("Build Time: " + (System.currentTimeMillis() - time));
		}
	}
	
	
	/**
	 * 
	 * @param blockSize
	 * @param verbose
	 * @throws IOException 
	 */
	public void buildRHistogramQA(int numberOfBuckets, Cursor<DoublePointRectangle> queries, 
			double rtreeRatio, double hRatio, double avgRatio,  HistType histType,  boolean verbose) throws IOException{
		if(verbose)
			System.out.println("build soptHist");
		long time = System.currentTimeMillis();
		Properties props = new Properties(); 
		props.setProperty(MHistograms.RTREE_BLOCK_SIZE, new Integer(BLOCKSIZE).toString());
		props.setProperty(MHistograms.RTREE_RATIO, new Double(0.5).toString());
		props.setProperty(MHistograms.RTREE_PATH, tempPath + "soptTreeQA");
		rhistogram_QA = new RHistogram(DIMENSION, BLOCKSIZE, rtreeRatio, hRatio, avgRatio,  histType, ProcessorType.VOLUME, queries, 
				SpatialUtils.universeUnit(DIMENSION));
		rhistogram_QA.buildHistogram(getData(), numberOfBuckets, props);
		Cursor<DoublePointRectangle> data = getData();
		if (verbose){
			System.out.println("Build Time: " + (System.currentTimeMillis() - time));
		}
	}
	
	

	/**
	 * 
	 * @param blockSize
	 * @param verbose
	 * @throws IOException 
	 */
	public void buildRHistogramV(int numberOfBuckets, double rtreeRatio, double hRatio, double avgRatio,  HistType histType,  boolean verbose) throws IOException{
		if(verbose)
			System.out.println("build soptHist");
		long time = System.currentTimeMillis();
		Properties props = new Properties(); 
		props.setProperty(MHistograms.RTREE_BLOCK_SIZE, new Integer(BLOCKSIZE).toString());
		props.setProperty(MHistograms.RTREE_RATIO, new Double(hRatio).toString());
		props.setProperty(MHistograms.RTREE_PATH, tempPath + "soptTree");
		if (rhistogram_V == null)
			rhistogram_V = new RHistogram(DIMENSION, BLOCKSIZE, rtreeRatio, hRatio, avgRatio,  histType, ProcessorType.VOLUME);
		rhistogram_V.buildHistogram(getData(), numberOfBuckets, props);
		if (verbose){
			System.out.println("Build Time: " + (System.currentTimeMillis() - time)+ "  " + rhistogram_V.numberOfBuckets());
		}
	}
	
	/**
	 * 
	 * @param blockSize
	 * @param verbose
	 * @throws IOException 
	 */
	public void buildRHistogramNOPT(int numberOfBuckets, double rtreeRatio, double hRatio, double avgRatio,  HistType histType,  boolean verbose) throws IOException{
		if(verbose)
			System.out.println("build soptHist");
		long time = System.currentTimeMillis();
		Properties props = new Properties(); 
		props.setProperty(MHistograms.RTREE_BLOCK_SIZE, new Integer(BLOCKSIZE).toString());
		props.setProperty(MHistograms.RTREE_RATIO, new Double(0.4).toString());
		props.setProperty(MHistograms.RTREE_PATH, tempPath + "soptTree");
		rhistogram_NOPT = new RHistogram(DIMENSION, BLOCKSIZE, rtreeRatio, hRatio, avgRatio,  histType, ProcessorType.VOLUME);
		rhistogram_NOPT.buildHistogram(getData(), numberOfBuckets, props);
		if (verbose){
			System.out.println("Build Time: " + (System.currentTimeMillis() - time)+ "  " + rhistogram_NOPT.numberOfBuckets());
		}
	}
	
	/**
	 * 
	 * @return
	 */
	public RTree getRtree() {
		return rtree;
	}
	/**
	 * 
	 * @return
	 */
	public RKHistHistogram getRkHist() {
		return rkHist;
	}
	/**
	 * 
	 * @return
	 */
	public RTreeBasicHistogram getRTreeHist() {
		return rTreeHist;
	}
	/**
	 * 
	 * @return
	 */
	public STHistForest getSTHistForest() {
		return stHistForest;
	}
	/**
	 * 
	 * @return
	 */
	public MinSkewHistogram getMinSkewHist() {
		return this.minSkewHist;
	}
	
	public MinSkewProgressiveRefinementHistogram getMinSkewProgressiveRefinementHistogram(){
		return this.minSkewProgressive;
	}
	
	public RHistogram getRhistogram_RK() {
		return rhistogram_RK;
	}

	public RHistogram getRhistogram_SKEW() {
		return rhistogram_SKEW;
	}

	public RHistogram getRhistogram_QA() {
		return rhistogram_QA;
	}

	public RHistogram getRhistogram_V() {
		return rhistogram_V;
	}
	
	public RHistogram getRhistogramNOPT() {
		return rhistogram_NOPT;
	}
	/**
	 * 
	 */
	public TestPlot showHist(String name, MHistogram hist){
		return new TestPlot( name , hist.getBuckets().iterator(), 500,  SpatialUtils.universeUnit(DIMENSION));
	}
	/* ************************************************************************** 
	 * End Histogram Part
	 * ***************************************************************************
	 */
	/* ************************************************************************** 
	 *  Histogram Test Part
	 * ***************************************************************************
	 */
	public void testHistogram(Iterator<DoublePointRectangle> queryIn, MHistogram mhistogram, 
			PrintStream out, boolean verbose){
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
			estimation = mhistogram.getSelectivity(queryRec);
			diff = Math.abs(actualCount-estimation); // abs error
			double relError = diff/(Math.max(1, actualCount));
			avgerror += diff/(Math.max(1, actualCount)); 
			// create csv
			out.println( count + ", " + actualCount+ ", "  + estimation + " ,"  + diff +  ", " +  relError );
			count++;
			sumDiff +=diff;
			if (diff> max)
				max = diff;
			if (diff < min)
				min = diff;
		}
		error = sumDiff/count;
		// create comments in csv
		out.println("# Estimation Average Relative Error: " + avgerror/count);
		out.println("# Estimation Average Absolut Error: " + error);
		out.println("# Buckets Considered: " + mhistogram.numberOfBuckets());
		out.println("# Max: " + max);
		out.println("# Min: " + min);
		out.println("# Queries issued:  " + count);	
		
		if(verbose){
			System.out.println("Estimation Average Relative Error: " + avgerror/count);
			System.out.println("Estimation Average Absolut Error: " + error);
			System.out.println("Buckets Considered: " + mhistogram.numberOfBuckets());
			System.out.println("Max: " + max);
			System.out.println("Min: " + min);
			System.out.println("Queries issued:  " + count);	
		}
	}
	boolean var = false;
	/**
	 * 
	 * @param queryIn
	 * @param mhistogram
	 */
	public void testHistogram(Iterator<DoublePointRectangle> queryIn, MHistogram mhistogram){
		double sumDiff =0;
		int count = 0;
		double error = 0;
		double avgerror = 0;
		double max = Double.MIN_VALUE;
		double min = Double.MAX_VALUE;
		double overallSum = 0; 
		double absSum = 0; // x
		double absSumQ = 0;
		List<Double> abss = new ArrayList<>();
		
		while(queryIn.hasNext()){
			DoublePointRectangle queryRec = queryIn.next();
			double actualCount = Cursors.count(rtree.query(queryRec)); 
			double estimation = 0;
			double diff = 0;
			estimation = mhistogram.getSelectivity(queryRec);
			
			
			diff = Math.abs(actualCount-estimation); // abs error
			avgerror += diff/(Math.max(1, actualCount)); 
			count++;
			sumDiff +=diff;
			
			overallSum += actualCount;
			absSum += diff; 
			
			absSumQ += diff*diff;
			
			if(var)
				abss.add(diff);
			
			if (diff> max)
				max = diff;
			if (diff < min)
				min = diff;
		}
		error = sumDiff/count;
		double errorMinSkew = absSum/overallSum;
		double varianz = (var) ?  Statistics.variance(abss.toArray()) : 1;
		double std = Math.sqrt(varianz);
		// create comments in csv
		if (stream!=null)
		{
			stream.printf(Locale.GERMANY, "%f; %f; %f; %f; %d; %f; %f; %f \n", (error), (avgerror/count), max, min, count, absSum ,overallSum, errorMinSkew );
		}
		System.out.println("avg abs; avg rel; max; min; count");
		System.out.printf(Locale.GERMANY, "%f; %f; %f; %f; %d; %f; %f; %f \n", (error), (avgerror/count), max, min, count, absSum ,overallSum, errorMinSkew );
		if (var)
		System.out.printf(Locale.GERMANY, "%f; %f; \n", varianz,  std);
		
		
//		System.out.println("Estimation Average Relative Error: " + avgerror/count);
//		System.out.println("Estimation Average Absolut Error: " + error);
//		System.out.println("Buckets Considered: " + mhistogram.numberOfBuckets());
//		System.out.println("Max: " + max);
//		System.out.println("Min: " + min);
//		System.out.println("Queries issued:  " + count);	
	}
	
	/* ************************************************************************** 
	 *  End Histogram Test Part
	 * ***************************************************************************
	 */
}
