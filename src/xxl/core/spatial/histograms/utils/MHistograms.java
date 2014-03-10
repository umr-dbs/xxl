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

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;

import xxl.core.collections.containers.Container;
import xxl.core.collections.containers.CounterContainer;
import xxl.core.collections.containers.io.BlockFileContainer;
import xxl.core.collections.containers.io.ConverterContainer;
import xxl.core.collections.queues.Queue;
import xxl.core.collections.queues.io.BlockBasedQueue;
import xxl.core.cursors.Cursor;
import xxl.core.cursors.Cursors;
import xxl.core.cursors.sorters.MergeSorter;
import xxl.core.cursors.sources.io.FileInputCursor;
import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Constant;
import xxl.core.functions.Function;
import xxl.core.functions.Functional.UnaryFunction;
import xxl.core.indexStructures.ORTree;
import xxl.core.indexStructures.RTree;
import xxl.core.indexStructures.SortBasedBulkLoading;
import xxl.core.indexStructures.rtrees.AbstractIterativeRtreeBulkloader.ProcessingType;
import xxl.core.indexStructures.rtrees.GenericPartitioner.CostFunctionArrayProcessor;
import xxl.core.indexStructures.rtrees.GenericPartitioner.DefaultArrayProcessor;
import xxl.core.indexStructures.rtrees.RtreeIterativeBulkloader;
import xxl.core.io.converters.ConvertableConverter;
import xxl.core.io.converters.Converter;
import xxl.core.predicates.AbstractPredicate;
import xxl.core.predicates.Predicate;
import xxl.core.spatial.SpatialUtils;
import xxl.core.spatial.histograms.utils.RVHistogram.HistType;
import xxl.core.spatial.histograms.utils.PartitionerUtils.ProcessorType;
import xxl.core.spatial.histograms.utils.STHist.STHistBucket;
import xxl.core.spatial.rectangles.DoublePointRectangle;
import xxl.core.spatial.rectangles.Rectangles;
import xxl.core.util.WrappingRuntimeException;




/**
 * This class implements spatial histograms for selectivity estimation 
 * Assumption: input data is a collection of doublePointRectangles (  {@link DoublePointRectangle} ) in unit space!
 * 
 * @see D. Achakeev and B. Seeger A class of R-tree histograms for spatial databases GIS 2012
 *
 */
public class MHistograms {
	
	/**
	 * Property names of R-tree based histograms
	 */
	public static final String RTREE_SORT_PATH = "rtree.sortpath";
	public static final String RTREE_PATH = "rtree.path";
	public static final String RTREE_BLOCK_SIZE = "rtree.blockSize"; 
	public static final String RTREE_SORT_COMP = "rtree.sort"; // Hilbert 0; ZCurve 1; 
	public static final String RTREE_BULKLOAD = "rtree.bulkload"; // simple 0, gopt 1,  sopt 2 
	public static final String RTREE_DIMENSION = "rtree.dimension"; // simple 0, gopt 1,  sopt 2 
	public static final String RTREE_RATIO = "rtree.ratio";
	public static final String RTREE_UTIL = "rtree.storageUtil";
	public static final String RTREE_PARTSIZE = "rtree.partSize";
	public static final String RTREE_BITS = "rtree.bits";
	public static final int NAIVE_BULKLOAD = 0;
	public static final int GOPT_BULKLOAD = 1;
	public static final int SOPT_BULKLOAD = 2;
	public static final int BITS_PRO_DIM = 31;
	public static final int FILLING_CURVE_PRECISION = 1 << (BITS_PRO_DIM-1);
	public static final String RKHIST_U_RATIO = "rkhist.ratio";
	/**
	 * Property names of MinSkew based histograms		
	 */
	public static final String MINSKEW_DIM = "minskew.dim";
	public static final String MINSKEW_PATH = "minskew.path";
	public static final String MINSKEW_GRID_SIZE = "minskew.gridsize"; // in bits pro dim
	public static final String MINSKEW_REF = "minskew.ref";
	/**
	 * simple descriptor function 
	 */
	@SuppressWarnings("deprecation")
	public static Function dataDescriptor = new AbstractFunction(){
		public Object invoke(Object o){
			 return  (DoublePointRectangle)o;
		}
	};
	/**
	 * Groundwork class for spatial histogram generation. 
	 * 
	 */
	public static abstract class AbstractSelHistogram implements MHistogram{
		
		/**
		 * intern representation of a histogram
		 * a list that contains a buckets of type {@link SpatialHistogramBucket}
		 */
		protected List<SpatialHistogramBucket> histogram; 
		/**
		 * sets properties for histogram method
		 * @param props
		 */
		protected abstract void setProperties(Properties props); 
		/*
		 * (non-Javadoc)
		 * @see xxl.core.spatial.histograms.utils.MHistogram#getSelectivity(xxl.core.spatial.rectangles.DoublePointRectangle)
		 */
		@Override
		public double getSelectivity(DoublePointRectangle queryRec) {
			return SpatialUtils.computeEstimation(histogram.iterator(), queryRec);
		}
		/*
		 * (non-Javadoc)
		 * @see xxl.core.spatial.histograms.utils.MHistogram#getBuckets()
		 */
		public List<SpatialHistogramBucket> getBuckets(){
			return histogram;
		}
		/*
		 * (non-Javadoc)
		 * @see xxl.core.spatial.histograms.utils.MHistogram#numberOfBuckets()
		 */
		@Override
		public int numberOfBuckets() {
			return histogram.size();
		}
	}
	/**
	 * 
	 * This is a groundwork class for R-tree based histograms. 
	 * 
	 * This class provides method for R-tree bulk loading. To bulk load an R-tree a sort based algorithm is used.  
	 * In case of 2 dimensions a hilbert comparator is used, otherwise we use a Z-Curve.
	 * 
	 *
	 */
	public static class RTreeBasicHistogram extends AbstractSelHistogram {
		/**
		 * default parameters
		 */
		/**
		 * Blocks size of R-tree
		 */
		protected int blockSize = 4096; // default value
		/**
		 * number of dimensions, default 2
		 * 
		 */
		protected int dimension = 2; // default value
		/**
		 * input rectangle  (DoublePointRectangle) converter
		 *   
		 */
		protected Converter<DoublePointRectangle> converter = new ConvertableConverter<DoublePointRectangle>(SpatialUtils.factoryFunction(dimension));// default 2D Converter
		/**
		 * comparator, based on SFC mapping; For two-dimensional space we use Hilbert, otherwise Z-Curve.
		 */
		protected Comparator<DoublePointRectangle> comparator = SpatialUtils.getHilbert2DComparator(SpatialUtils.universeUnit(dimension), FILLING_CURVE_PRECISION); // default 2D comparator
		/**
		 * Path were temporal sort files are stored
		 */
		protected String sortpath = "./"; // default
		/**
		 * Path were temporal R-tree is stored
		 */
		protected String rtreePath ="./"; //default
		/**
		 * bulk load type
		 */
		protected int bulkLoad = 0; // DEAFULT
		/**
		 * min R-tree page capacity, defined by the fraction of page capacity default value 0.33
		 */
		protected double rtreeRatio = 0.33; // default 
		/**
		 * average target R-tree page capacity, defined by the fraction of page capacity default value 0.8 
		 */
		protected double loadRatio = 0.8; // default
		/**
		 * maximal number of entries for optimal partitioning computation during the loading of R-tree, default value 50.000 rectangles 
		 */
		protected int partitionSize = 50000; // default 
		/**
		 * number of bits used in a SFC key computation. SFC key is a long value. 
		 */
		protected int bitProDim = 63/dimension;
		/**
		 * mask for SFC computation
		 */
		protected int precision = 1 << (bitProDim-1);
		/**
		 * function for optimal partitioning e.g. volume based
		 */
		DefaultArrayProcessor arrayProcessor = null;
		
		/**
		 * sets properties
		 */
		protected void setProperties(Properties props){
			try{
				bitProDim = new Integer(props.getProperty(RTREE_BITS, "31")); //
				precision = 1 << (bitProDim-1);
				blockSize = new Integer(props.getProperty(RTREE_BLOCK_SIZE, "4096")); // default value
				dimension = new Integer(props.getProperty(RTREE_DIMENSION, "2")); // default value
				converter = new ConvertableConverter<DoublePointRectangle>(SpatialUtils.factoryFunction(dimension));// default 2D Converter
				comparator = (dimension == 2 ) ? 
						SpatialUtils.getHilbert2DComparator(SpatialUtils.universeUnit(dimension),precision) :
							SpatialUtils.getZCurveComparator(SpatialUtils.universeUnit(dimension), bitProDim);
//							: RGOhist.getHilbertComparator(RGOhist.universeUnit(dimension), bitProDim); // default 2D comparator
//				comparator = 
//								RGOhist.getZCurveComparator(RGOhist.universeUnit(dimension), 31); // default 2D comparator
				sortpath = props.getProperty(RTREE_PATH, "./") + "sortTmp"; // default 
				rtreePath =props.getProperty(RTREE_PATH, "./"); //default
				bulkLoad =  new Integer(props.getProperty(RTREE_BULKLOAD, "0")); // DEAFULT
				if (bulkLoad <  0  && bulkLoad  > 3)
					throw new RuntimeException("simple: 0, gopt: 1,  sopt: 2 ");
				rtreeRatio = new Double(props.getProperty(RTREE_RATIO, "0.33")); // default 
				loadRatio = new Double(props.getProperty(RTREE_UTIL, "0.8")); // default
				partitionSize = new Integer(props.getProperty(RTREE_PARTSIZE, "50000")); // default 
				
			}catch(NumberFormatException ex){
				throw new RuntimeException("Check property! ", ex);
			}
		}
		
		/*
		 * (non-Javadoc)
		 * @see xxl.core.spatial.histograms.utils.MHistogram#buildHistogram(xxl.core.cursors.Cursor, int, java.util.Properties)
		 */
		@Override
		public void buildHistogram(Cursor<DoublePointRectangle> rectangles,
				int numberOfBuckets, Properties props) throws IOException  {
			setProperties(props);
			//sortdata 
			Cursor<DoublePointRectangle> sortedRectangles = sortData(rectangles);
			RTree tree = null;
			if(bulkLoad == NAIVE_BULKLOAD ){
				 tree = buildSimpleRtree(sortedRectangles);
			}else{
				ProcessingType pType = (bulkLoad == GOPT_BULKLOAD) ? ProcessingType.GOPT : ProcessingType.SOPT_F;
				tree = buildExtRtree(sortedRectangles, pType);
			}
			histogram = SpatialUtils.computeSimpleRTreeHistogram(tree, numberOfBuckets);
		}
		
		/**
		 * this method sorts data according hilbert or z-curve
		 * @param rectangles
		 * @return
		 */
		@SuppressWarnings({ "deprecation", "serial", "unchecked" })
		protected Cursor<DoublePointRectangle> sortData(Cursor<DoublePointRectangle> rectangles){
			final Container queueContainer = new BlockFileContainer(sortpath + "tmpsortqueue.tmp" , 4096);
			final Function<Function<?, Integer>, Queue<?>> queueFunction =
					new AbstractFunction<Function<?, Integer>, Queue<?>>() {
					public Queue<?> invoke(Function<?, Integer> function1, Function<?, Integer> function2) {
						return new BlockBasedQueue(queueContainer, 4096, converter,
								function1, function2);
					}
				};
			
			return 	new MergeSorter(rectangles, comparator, 8*2*dimension, 4096 * 2000, 4096 * 2000, queueFunction, false);
		}
		
		/**
		 * Builds R-tree using {@link SortBasedBulkLoading} method. 
		 * 
		 * 
		 * 
		 * @param rectCursor
		 * @return
		 */
		protected RTree buildSimpleRtree(Cursor<DoublePointRectangle> rectCursor){
			RTree sortBasedRTree = new RTree();
			CounterContainer treeCounter = new CounterContainer(new BlockFileContainer(rtreePath, blockSize));
			Container treeContainer =  new ConverterContainer(treeCounter,	 sortBasedRTree.nodeConverter(converter, dimension));
			sortBasedRTree.initialize(dataDescriptor, treeContainer,
					blockSize, (8*2*dimension) + 4, (8*2*dimension) + 8, 0.33);
			final Predicate<ORTree.Node> overflows = new AbstractPredicate<ORTree.Node>() {
				public boolean invoke(ORTree.Node node) {
					if (node.level() == 0)
						return node.number() > ((int) (((blockSize-6) / (8*2*dimension)) * loadRatio));
					return node.number() > ((int) (((blockSize-6) / (8*2*dimension + 8)) * loadRatio));
				}
			};
			new SortBasedBulkLoading(sortBasedRTree,rectCursor,
					sortBasedRTree.determineContainer, overflows);
			return sortBasedRTree;
		}
		
		/**
		 * 
		 * @param rectCursor
		 * @param container
		 * @return
		 */
		public  RTree buildSimpleRtree(Cursor<DoublePointRectangle> rectCursor, Container container){
			RTree sortBasedRTree = new RTree();
			CounterContainer treeCounter = new CounterContainer(container);
			Container treeContainer =  new ConverterContainer(treeCounter,	 sortBasedRTree.nodeConverter(converter, dimension));
			sortBasedRTree.initialize(dataDescriptor, treeContainer,
					blockSize, (8*2*dimension) + 4, (8*2*dimension) + 8, 0.33);
			final Predicate<ORTree.Node> overflows = new AbstractPredicate<ORTree.Node>() {
				public boolean invoke(ORTree.Node node) {
					if (node.level() == 0)
						return node.number() > ((int) (((blockSize-6) / (8*2*dimension)) * loadRatio));
					return node.number() > ((int) (((blockSize-6) / (8*2*dimension + 8)) * loadRatio));
				}
			};
			new SortBasedBulkLoading(sortBasedRTree,rectCursor,
					sortBasedRTree.determineContainer, overflows);
			return sortBasedRTree;
		}
		
		/**
		 * 
		 * Builds an R-tree using {@link RtreeIterativeBulkloader}
		 * 
		 * @param rectCursor
		 * @param pType
		 * @return
		 * @throws IOException
		 */
		protected RTree buildExtRtree(Cursor<DoublePointRectangle> rectCursor, xxl.core.indexStructures.rtrees.RtreeIterativeBulkloader.ProcessingType pType) throws IOException{
			RTree sortBasedRTree = new RTree();
			Container treeContainer =  new ConverterContainer( new BlockFileContainer(rtreePath, blockSize), 
					sortBasedRTree.nodeConverter(Rectangles.getDoublePointRectangleConverter(dimension), dimension));
			sortBasedRTree.determineContainer = new Constant<Object>(treeContainer);
			sortBasedRTree.getContainer =new Constant<Object>(treeContainer);
			boolean processList = (pType == ProcessingType.GOPT) ? true: false;
			double[] sideLength = new double[dimension];
			for(int i = 0; i < sideLength.length; i++){
				sideLength[i] = 0d;
			}
			UnaryFunction<DoublePointRectangle, Double> costFunction = RtreeIterativeBulkloader.generateDefaultFunction(sideLength);
			DefaultArrayProcessor arrayProcessor = new DefaultArrayProcessor(costFunction, processList);
			RtreeIterativeBulkloader<DoublePointRectangle> bulkLoader = new RtreeIterativeBulkloader<DoublePointRectangle>(sortBasedRTree, 
					rtreePath,  dimension, blockSize, rtreeRatio, loadRatio, partitionSize);
			bulkLoader.init(arrayProcessor, pType, dimension*8*2, Rectangles.getDoublePointRectangleConverter(dimension),  new UnaryFunction<DoublePointRectangle, DoublePointRectangle>() {
				@Override
				public DoublePointRectangle invoke(DoublePointRectangle arg) {
					return new DoublePointRectangle(arg);
				}
			}); 
			bulkLoader.buildRTree(rectCursor); 
			return bulkLoader.getRTree();
		}
		
	}
	
	

	
	/**
	 * Two step R-tree based histogram. 
	 * 
	 * In the first step R-tree leaf nodes are build using gopt strategy. 
	 * In the second step histogram is constructed using opt strategy.  
	 * In the basic variant a sum of volumes of bucket MBR are used for optimization. 
	 * 
	 * We refer for details to  D. Achakeev and B. Seeger A class of R-tree histograms for spatial databases GIS 2012
	 * 
	 * p := number of buckets 
	 * B = N/(p*avgLoad) 
	 * b = B*minBound
	 *
	 * As histogram is build using opt partitioning strategy, in the second step dynamic programming table occupies quadratic space in the number of input leafs. 
	 * Therefore, we use a simple heuristic. We partition the input set of leaf nodes in chunks of 20 000 pages according to the sorting order 
	 * and apply opt partitioning on each.
	 *  
	 *
	 *
	 */
	public static class RHistogram extends RTreeBasicHistogram{
		
		public static final int BITS_PRO_DIM = 8;  
		public static final int B_FACTOR = 2;
		
		
		double[] sideLength = new double[dimension];
		DoublePointRectangle universe; 
		public HistType type;
		ProcessorType pType;
		double rtreeratio, avgratio, hratio;
		public RTree tree = null; 

		
		public RHistogram(int dimension, int blockSize, double rtreeRatio, 
				double hratio, double avgratio,  HistType type, ProcessorType pType){
			this.blockSize = blockSize;
			this.rtreeratio = rtreeRatio;
			this.avgratio = avgratio;
			this.hratio = hratio;
			this.type = type;
			this.dimension = dimension;
			this.pType = pType;
			sideLength = new double[dimension];
			for(int i = 0; i < sideLength.length; i++){
				sideLength[i] = 0d;
			}
		}
		
		public RHistogram(int dimension, int blockSize, double rtreeRatio, 
				double hratio, double avgratio,  HistType type, 
				ProcessorType pType, 
				Cursor<DoublePointRectangle> queryPoints, 
				DoublePointRectangle universe) {
			this(dimension, blockSize, rtreeRatio, hratio,avgratio, type, pType);
			sideLength = SpatialUtils.computeQuerySides(queryPoints, dimension, universe); 
		}
		
		
		
		@Override
		public void buildHistogram(Cursor<DoublePointRectangle> rectangles,
				int numberOfBuckets, Properties props) throws IOException {
			// set properties 
			setProperties(props);
			// build rtree
			Cursor<DoublePointRectangle> sortedRectangles = null;
			if(tree == null){
				sortedRectangles = sortData(rectangles);
				tree = buildExtRtree(sortedRectangles, ProcessingType.GOPT);
			}
			int count = Cursors.count(SpatialUtils.getRectanglesLevel1(tree));
			if(count <= numberOfBuckets){
				this.histogram = SpatialUtils.computeSimpleRTreeHistogram(tree, numberOfBuckets);
				return;
			}
			// after computing the leaf node level of R-tree
			// we set the parameter for Hisotgram generation
			// they set in dependency of desired number of buckets 
			double f =  count/ (double)numberOfBuckets; // 1-avgLoad 
			int d = (int) Math.ceil((count/ ((double)numberOfBuckets)));
			int b = (int)(Math.max(Math.floor(f * hratio), 1));
			b = Math.max(b, 2);
			int B = b+d;//
			if(type==HistType.GOPT ){
				b = count/numberOfBuckets;
				B = B_FACTOR *b;
			}
			UnaryFunction<DoublePointRectangle, Double> function = RtreeIterativeBulkloader.generateDefaultFunction(sideLength);
			CostFunctionArrayProcessor<DoublePointRectangle> arrayProcessor = null;
			switch(pType){
				case RK_HIST : 
					arrayProcessor = new PartitionerUtils.RKHistMetrikProcessor(B, count, true);
					break;
				case GRID_SSE:
					rectangles.reset();
					arrayProcessor = new PartitionerUtils.SpatialSkewProcessor(
							rectangles, BITS_PRO_DIM
							, dimension, B, count, true); 
					break;
				default : 
					arrayProcessor  = new DefaultArrayProcessor(function); 
			}
			double rat = ((double)f)/ (double)B;
			if (count > 20000) // 128*128
				this.histogram =  RVHistogram.computeHistogramOPT(
						SpatialUtils.getRectanglesLevel1(tree), b , B, count, numberOfBuckets, rat, arrayProcessor, type, 10000);
			else
				this.histogram =  RVHistogram.computeHistogramOPT(
						SpatialUtils.getRectanglesLevel1(tree), b , B, count, numberOfBuckets,  arrayProcessor, type);
		}
		
		
		public void buildRtree(Cursor<DoublePointRectangle> rectangles) throws IOException{
			Cursor<DoublePointRectangle> sortedRectangles = null;
			if(tree == null){
				sortedRectangles = sortData(rectangles);
				tree = buildExtRtree(sortedRectangles, ProcessingType.GOPT);
			}
		} 
		
		public void buildHistogram(	int numberOfBuckets, Properties props, int b, int B) throws IOException {
			// set properties 
			setProperties(props);
			int count = Cursors.count(SpatialUtils.getRectanglesLevel1(tree));
			if(count <= numberOfBuckets){
				this.histogram = SpatialUtils.computeSimpleRTreeHistogram(tree, numberOfBuckets);
				return;
			}
			//.out.println("B for tree "  + B +" min b " +b  + " entries to consider " + count);
			if(type==HistType.GOPT ){
				b = count/numberOfBuckets;
				B = B_FACTOR *b;
			}
			UnaryFunction<DoublePointRectangle, Double> function = RtreeIterativeBulkloader.generateDefaultFunction(sideLength);
			CostFunctionArrayProcessor arrayProcessor = null;
			arrayProcessor  = new DefaultArrayProcessor(function); 
			double rat = 1.0/ avgratio;
			if (count > 40000)
//				this.histogram =  PHist2L.computeHistogramOPT(
//						RGOhist.getRectanglesLevel1(tree), b , B, count, numberOfBuckets, avgratio, arrayProcessor, type, 128*128);
				this.histogram =  RVHistogram.computeHistogramOPT(
						SpatialUtils.getRectanglesLevel1(tree), b , B, count, numberOfBuckets, rat, arrayProcessor, type, 128*128);
			else
				this.histogram =  RVHistogram.computeHistogramOPT(
						SpatialUtils.getRectanglesLevel1(tree), b , B, count, numberOfBuckets,  arrayProcessor, type);
		}
		
		/**
		 * 
		 * @param numberOfBuckets
		 */
		public void buildSimpleHist(int numberOfBuckets){
			histogram = SpatialUtils.computeSimpleRTreeHistogram(tree, numberOfBuckets);
		}
		
		
		
		
		
	
	}
	
	/**
	 * Histogram which implements 
	 * RK-Hist
	 *
	 */
	public static class RKHistHistogram extends  RTreeBasicHistogram{
		
		
		
		protected double undersampligRatio = 0.1; // default value 
		
		RTree tree = null;
	
		@Override
		public void buildHistogram(Cursor<DoublePointRectangle> rectangles,
				int numberOfBuckets, Properties props) throws IOException {
			setProperties(props);
			//sortdata 
			if(tree == null){
//				System.out.println("null");
				Cursor<DoublePointRectangle> sortedRectangles = sortData(rectangles);
				if(bulkLoad == NAIVE_BULKLOAD ){
					 tree = buildSimpleRtree(sortedRectangles);
				}else{
					ProcessingType pType = (bulkLoad == GOPT_BULKLOAD) ? ProcessingType.GOPT : ProcessingType.SOPT_F;
//					System.out.println("ext");
					tree = buildExtRtree(sortedRectangles, pType);
				}
			}
			int numberOfNodes = Cursors.count(SpatialUtils.getNodes(tree, 1));
			histogram = RKhist.buildRKHist(tree, numberOfNodes, numberOfBuckets, undersampligRatio, dimension);
		}
		
		@Override
		protected void setProperties(Properties props) {
			super.setProperties(props);
			try{
				undersampligRatio = new Double(props.getProperty(RKHIST_U_RATIO, "0.1"));
			}catch(NumberFormatException ex){
				throw new RuntimeException("check property");
			}
		}
		
		@Override
		public RTree buildSimpleRtree(Cursor<DoublePointRectangle> rectCursor,
				Container container) {
//			long time = System.currentTimeMillis();
			Cursor<DoublePointRectangle> sortedRectangles = sortData(rectCursor);
			tree = super.buildSimpleRtree(sortedRectangles, container);
//			System.out.println((System.currentTimeMillis()-time) + " time");
			return tree;
		}
		
	}
	/**
	 * MinSkew Histogram basic variant
	 * 
	 *
	 */
	public static class MinSkewHistogram extends AbstractSelHistogram{
		
	
		
		protected String tempPath = "./cursor.tmp"; // default value for the path
		protected int dimension = 2; // default
		protected int bitsPerDim = 7; // default; 1024 * dimesnions
		
		
		@Override
		public void buildHistogram(Cursor<DoublePointRectangle> rectangles,
				int numberOfBuckets, Properties props) throws IOException {
			setProperties(props);
			createTmpFile(rectangles);
			Cursor<DoublePointRectangle> recCursor = new FileInputCursor<DoublePointRectangle>(
					new ConvertableConverter<DoublePointRectangle>(SpatialUtils.factoryFunction(dimension)), new File(tempPath)){	
						@Override
						public boolean supportsReset() {
							return true;
						}
						@Override
						public void reset() throws UnsupportedOperationException {
							super.reset();
							try {
								input = new DataInputStream(
									new BufferedInputStream(
										new FileInputStream(new File(tempPath)), 4096
									)
								);
							}
							catch (IOException ie) {
								throw new WrappingRuntimeException(ie);
							}
						}
			};
			this.histogram = MinSkewHist.buildHistogram(recCursor, SpatialUtils.universeUnit(dimension), bitsPerDim, dimension, numberOfBuckets); 
		}

		@Override
		protected void setProperties(Properties props) {
			try{
				dimension = new Integer(props.getProperty(MINSKEW_DIM, "2")); // default value
				tempPath = props.getProperty(MINSKEW_PATH, "./cursor.tmp");
				bitsPerDim = new Integer(props.getProperty(MINSKEW_GRID_SIZE, "7"));	
			}catch(NumberFormatException ex){
				throw new RuntimeException("Check property! ", ex);
			}
		}
		
		private void createTmpFile(Cursor<DoublePointRectangle> rectangles) throws IOException{
			DataOutputStream stream = new DataOutputStream(new FileOutputStream(new File(tempPath)));
			try{
				while(rectangles.hasNext()){
					DoublePointRectangle dpr = rectangles.next();
					dpr.write(stream);
					stream.flush();
				}
			}finally{
				if (stream != null)
					stream.close();
			}
		}
	}
	
	/**
	 * MinSkew Histogram basic variant
	 * 
	 *
	 */
	public static class MinSkewProgressiveRefinementHistogram extends MinSkewHistogram{
		
		
	
		protected int refSteps = 2; // default; 1024 * dimesnions
		
		
		@Override
		public void buildHistogram(Cursor<DoublePointRectangle> rectangles,
				int numberOfBuckets, Properties props) throws IOException {
			setProperties(props);
			createTmpFile(rectangles);
			Cursor<DoublePointRectangle> recCursor = new FileInputCursor<DoublePointRectangle>(
					new ConvertableConverter<DoublePointRectangle>(SpatialUtils.factoryFunction(dimension)), new File(tempPath)){	
						@Override
						public boolean supportsReset() {
							return true;
						}
						@Override
						public void reset() throws UnsupportedOperationException {
							super.reset();
							try {
								input = new DataInputStream(
									new BufferedInputStream(
										new FileInputStream(new File(tempPath)), 4096
									)
								);
							}
							catch (IOException ie) {
								throw new WrappingRuntimeException(ie);
							}
						}
			};
			this.histogram = MinSkewHist.buildProgressiveRefinement(recCursor, SpatialUtils.universeUnit(dimension), bitsPerDim, dimension, numberOfBuckets, refSteps); 
		}

		@Override
		protected void setProperties(Properties props) {
			try{
				dimension = new Integer(props.getProperty(MINSKEW_DIM, "2")); // default value
				tempPath = props.getProperty(MINSKEW_PATH, "./cursor.tmp");
				bitsPerDim = new Integer(props.getProperty(MINSKEW_GRID_SIZE, "7"));
				refSteps = new Integer(props.getProperty(MINSKEW_REF, "2"));
			}catch(NumberFormatException ex){
				throw new RuntimeException("Check property! ", ex);
			}
		}
		
		private void createTmpFile(Cursor<DoublePointRectangle> rectangles) throws IOException{
			DataOutputStream stream = new DataOutputStream(new FileOutputStream(new File(tempPath)));
			try{
				while(rectangles.hasNext()){
					DoublePointRectangle dpr = rectangles.next();
					dpr.write(stream);
					stream.flush();
				}
			}finally{
				if (stream != null)
					stream.close();
			}
		}
	}
	
	
	/**
	 * 
	 * 
	 *
	 */
	public static class STHistForest implements MHistogram{
		
		public static final String SKEW_GRID_SIZE = "skew.gridsize"; // in bits pro dim
		
		protected int gridSize; 
		
		protected List<STHistBucket> forest;
		
		protected List<SpatialHistogramBucket> buckets; 
		
		protected double samplingRate = 1.0; 
		
		public STHistForest() {
			super();
			this.samplingRate = 1.0;
		}
		
		
		public STHistForest(double samplingRate) {
			super();
			this.samplingRate = samplingRate;
			System.out.println("StForest with samplig rate = "  + samplingRate);
		}

		protected void setProperties(Properties props) {
			try{
				gridSize = new Integer(props.getProperty(SKEW_GRID_SIZE, "7"));	
			}catch(NumberFormatException ex){
				throw new RuntimeException("Check property! ", ex);
			}
		}
		
		@Override
		public void buildHistogram(Cursor<DoublePointRectangle> rectangles,
				int numberOfBuckets, Properties props) throws IOException {
			STHist histogram = new STHist();
			histogram.buildHotSpotForest(rectangles, SpatialUtils.universeUnit(2), numberOfBuckets);
			forest = histogram.forest;
			buckets = new ArrayList<SpatialHistogramBucket>();
			STHist.forest(forest, buckets);
		}
		
		@Override
		public double getSelectivity(DoublePointRectangle queryRec) {
			return STHist.getSelectivity(forest, queryRec) * ( 1/samplingRate);
		}
		
		/**
		 * 
		 * @return
		 */
		public List<SpatialHistogramBucket> getBuckets(){
			return buckets; 
		}
		
		@Override
		public int numberOfBuckets() {
			return buckets.size();
		}
		
	}
	
}
