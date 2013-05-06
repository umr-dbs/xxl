package xxl.core.spatial.histograms;

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
import xxl.core.indexStructures.rtrees.GenericPartitioner.CostFunctionArrayProcessor;
import xxl.core.indexStructures.rtrees.GenericPartitioner.DefaultArrayProcessor;
import xxl.core.indexStructures.rtrees.RtreeIterativeBulkloader;
import xxl.core.indexStructures.rtrees.RtreeIterativeBulkloader.ProcessingType;
import xxl.core.io.converters.ConvertableConverter;
import xxl.core.io.converters.Converter;
import xxl.core.predicates.AbstractPredicate;
import xxl.core.predicates.Predicate;
import xxl.core.spatial.histograms.PHist2L.HistType;
import xxl.core.spatial.histograms.PartitionerUtils.ProcessorType;
import xxl.core.spatial.histograms.STHist.STHistBucket;
import xxl.core.spatial.rectangles.DoublePointRectangle;
import xxl.core.spatial.rectangles.Rectangles;
import xxl.core.util.WrappingRuntimeException;




/**
 * This class implements  spatial histograms for selectivity estimation 
 * Assumption: input data is a collection of doublePointRectangles (  {@link DoublePointRectangle} ) in unit space!
 * 
 * @see D. Achakeev and B. Seeger A class of R-tree histograms for spatial databases GIS 2012
 *
 */
public class MHistograms {
	
	/**
	 * 
	 */
	public  static Function dataDescriptor = new AbstractFunction(){
		public Object invoke(Object o){
			 return  (DoublePointRectangle)o;
		}
	};
	
	
	
	
	
	public static abstract class AbstractSelHistogram implements MHistogram{
		
	
		public List<WeightedDoublePointRectangle> histogram; 
		/**
		 * sets properties for histogram method
		 * @param props
		 */
		protected abstract void setProperties(Properties props); 
		
		@Override
		public double getSelectivity(DoublePointRectangle queryRec) {
			return RGOhist.computeEstimation(histogram.iterator(), queryRec);
		}
		/**
		 * 
		 * @return
		 */
		public List<WeightedDoublePointRectangle> getBuckets(){
			return histogram;
		}
		
		@Override
		public int numberOfBuckets() {
			return histogram.size();
		}
	}
	
	
	
	/**
	 * 
	 * 
	 *
	 */
	public static class RTreeNaiveHistogram extends AbstractSelHistogram {
		
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
		
		protected int blockSize = 4096; // default value
		protected int dimension = 2; // default value
		protected Converter<DoublePointRectangle> converter = new ConvertableConverter<DoublePointRectangle>(RGOhist.factoryFunction(dimension));// default 2D Converter
		protected Comparator<DoublePointRectangle> comparator = RGOhist.getHilbert2DComparator(RGOhist.universeUnit(dimension), FILLING_CURVE_PRECISION); // default 2D comparator
		protected String sortpath = "./"; // default 
		protected String rtreePath ="./"; //default
		protected int bulkLoad = 0; // DEAFULT
		protected double rtreeRatio = 0.33; // default 
		protected double loadRatio = 0.8; // default
		protected int partitionSize = 50000; // default 
		protected int bitProDim = 63/dimension;
		protected int precision = 1 << (bitProDim-1);
		
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
				converter = new ConvertableConverter<DoublePointRectangle>(RGOhist.factoryFunction(dimension));// default 2D Converter
				comparator = (dimension == 2 ) ? 
						RGOhist.getHilbert2DComparator(RGOhist.universeUnit(dimension),precision) :
							RGOhist.getZCurveComparator(RGOhist.universeUnit(dimension), bitProDim);
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
			histogram = RGOhist.computeSimpleRTreeHistogram(tree, numberOfBuckets);
		}
		
		/**
		 * sorts data 
		 * @param rectangles
		 * @return
		 */
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
		 * @param rectCursor
		 * @param pType
		 * @return
		 * @throws IOException
		 */
		protected RTree buildExtRtree(Cursor<DoublePointRectangle> rectCursor, xxl.core.indexStructures.rtrees.RtreeIterativeBulkloader.ProcessingType pType) throws IOException{
			
			RTree sortBasedRTree = new RTree();
			Container treeContainer =  new ConverterContainer( new BlockFileContainer(rtreePath, blockSize), 
					sortBasedRTree.nodeConverter(Rectangles.getDoublePointRectangleConverter(dimension), dimension));
//			if (buffer != null){
//				treeContainer = new BufferedContainer(treeContainer, buffer); 
//			}
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
			bulkLoader.initTreeBulkloader(arrayProcessor, pType, dimension*8*2, Rectangles.getDoublePointRectangleConverter(dimension),  new UnaryFunction<DoublePointRectangle, DoublePointRectangle>() {
				@Override
				public DoublePointRectangle invoke(DoublePointRectangle arg) {
					return new DoublePointRectangle(arg);
				}
			}); 
			bulkLoader.buildRTree(rectCursor); 
//			treeContainer.flush();
			
			
//			RtreeIterativeBulkloader<DoublePointRectangle> bulkLoader = new RtreeIterativeBulkloader(
//					rtreePath, 
//					dimension, 
//					blockSize, 
//					rtreeRatio,
//					loadRatio, 
//					partitionSize,
//					RGOhist.universeUnit(dimension));
//			
//			arrayProcessor = new DefaultArrayProcessor(function);
//			bulkLoader.initTreeBulkloader(arrayProcessor, pType, new BlockFileContainer(rtreePath, blockSize));
//			bulkLoader.buildRTree(rectCursor);
			return bulkLoader.rtree;
		}
		
	}
	
	
	/**
	 * two level histogram computes for predefined pages a with gopt algorithm and 
	 * then computes under assumption that opt* algortihm with following settings
	 * Algoirthm has follwing parameter:
	 * Initial BlockSize, ratio, minBound, avgLoad
	 * p := number of buckets 
	 * B = N/(p*avgLoad) 
	 * b = B*minBound
	 *
	 */
	public static class SOPTHistogram extends RTreeNaiveHistogram{
		
		@Override
		public void buildHistogram(Cursor<DoublePointRectangle> rectangles,
				int numberOfBuckets, Properties props) throws IOException {
			// set properties 
			setProperties(props);
			// build rtree
			Cursor<DoublePointRectangle> sortedRectangles = sortData(rectangles);
			RTree tree = buildExtRtree(sortedRectangles, ProcessingType.GOPT);
			// get entries from level 1
			double[] sideLength = new double[dimension];
			for(int i = 0; i < sideLength.length; i++){
				sideLength[i] = 0d;
			}
			int count = Cursors.count(RGOhist.getRectanglesLevel1(tree));
			loadRatio = 0.8;
			rtreeRatio = 0.4;
			int B = count/ (int)(loadRatio*numberOfBuckets); // 1-avgLoad 
			int b = (int)(((double)B) * rtreeRatio);
			System.out.println("B for tree "  + B +" min b " +b  + " entries to consider " + count);
			UnaryFunction<DoublePointRectangle, Double> function = RtreeIterativeBulkloader.generateDefaultFunction(sideLength);
			DefaultArrayProcessor arrayProcessor = new DefaultArrayProcessor(function);
			this.histogram =  PHist2L.computeHistogramOPT(RGOhist.getRectanglesLevel1(tree),b , B, count, numberOfBuckets, arrayProcessor, false);
		}
		
	
	}
	
	/**
	 * two level histogram computes for predefined pages a with gopt algorithm and 
	 * then computes under assumption that opt* algortihm with following settings
	 * Algoirthm has follwing parameter:
	 * Initial BlockSize, ratio, minBound, avgLoad
	 * p := number of buckets 
	 * B = N/(p*avgLoad) 
	 * b = B*minBound
	 *
	 */
	public static class RHistogram extends SOPTHistogram{
		
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
			sideLength = RGOhist.computeQuerySides(queryPoints, dimension, universe); 
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
			int count = Cursors.count(RGOhist.getRectanglesLevel1(tree));
			if(count <= numberOfBuckets){
				this.histogram = RGOhist.computeSimpleRTreeHistogram(tree, numberOfBuckets);
				return;
			}
			
			double f =  count/ (double)numberOfBuckets; // 1-avgLoad 
//			System.out.println("avg load " + f);
			int d = (int) Math.ceil((count/ ((double)numberOfBuckets)));
			int b = (int)(Math.max(Math.floor(f * hratio), 1));
			int bM =  (int)(Math.max(Math.ceil(f * hratio), 1));
			System.out.println("bM "  + bM);
			int window = Math.max(2, bM);
			b = Math.max(b, 2);
			int B = b+d;//(b*3-1 <= (d+2)) ? window + d : b*3-1;
			System.out.println("B for tree "  + B +" min b " +b  + " entries to consider " + count);
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
			System.out.println("ratio: " + rat);
			if (count > 20000) // 128*128
				this.histogram =  PHist2L.computeHistogramOPT(
						RGOhist.getRectanglesLevel1(tree), b , B, count, numberOfBuckets, rat, arrayProcessor, type, 10000);
			else
				this.histogram =  PHist2L.computeHistogramOPT(
						RGOhist.getRectanglesLevel1(tree), b , B, count, numberOfBuckets,  arrayProcessor, type);
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
			int count = Cursors.count(RGOhist.getRectanglesLevel1(tree));
			if(count <= numberOfBuckets){
				this.histogram = RGOhist.computeSimpleRTreeHistogram(tree, numberOfBuckets);
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
				this.histogram =  PHist2L.computeHistogramOPT(
						RGOhist.getRectanglesLevel1(tree), b , B, count, numberOfBuckets, rat, arrayProcessor, type, 128*128);
			else
				this.histogram =  PHist2L.computeHistogramOPT(
						RGOhist.getRectanglesLevel1(tree), b , B, count, numberOfBuckets,  arrayProcessor, type);
		}
		
		
		public void buildSimpleHist(int numberOfBuckets){
		
			histogram = RGOhist.computeSimpleRTreeHistogram(tree, numberOfBuckets);
			
		}
		
		
		
		
		
	
	}
	
	/**
	 * Histogram which implements 
	 * RK-Hist
	 *
	 */
	public static class RKHistHistogram extends  RTreeNaiveHistogram{
		
		public static final String RKHIST_U_RATIO = "rkhist.ratio";
		
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
			int numberOfNodes = Cursors.count(RGOhist.getNodes(tree, 1));
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
		
		public static final String MINSKEW_DIM = "minskew.dim";
		public static final String MINSKEW_PATH = "minskew.path";
		public static final String MINSKEW_GRID_SIZE = "minskew.gridsize"; // in bits pro dim
		
		protected String tempPath = "./cursor.tmp"; // default value for the path
		protected int dimension = 2; // default
		protected int bitsPerDim = 7; // default; 1024 * dimesnions
		
		
		@Override
		public void buildHistogram(Cursor<DoublePointRectangle> rectangles,
				int numberOfBuckets, Properties props) throws IOException {
			setProperties(props);
			createTmpFile(rectangles);
			Cursor<DoublePointRectangle> recCursor = new FileInputCursor<DoublePointRectangle>(
					new ConvertableConverter<DoublePointRectangle>(RGOhist.factoryFunction(dimension)), new File(tempPath)){	
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
			this.histogram = MinSkewHist.buildHistogram(recCursor, RGOhist.universeUnit(dimension), bitsPerDim, dimension, numberOfBuckets); 
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
		
		public static final String MINSKEW_REF = "minskew.ref";
	
		protected int refSteps = 2; // default; 1024 * dimesnions
		
		
		@Override
		public void buildHistogram(Cursor<DoublePointRectangle> rectangles,
				int numberOfBuckets, Properties props) throws IOException {
			setProperties(props);
			createTmpFile(rectangles);
			Cursor<DoublePointRectangle> recCursor = new FileInputCursor<DoublePointRectangle>(
					new ConvertableConverter<DoublePointRectangle>(RGOhist.factoryFunction(dimension)), new File(tempPath)){	
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
			this.histogram = MinSkewHist.buildProgressiveRefinement(recCursor, RGOhist.universeUnit(dimension), bitsPerDim, dimension, numberOfBuckets, refSteps); 
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
		
		protected List<WeightedDoublePointRectangle> buckets; 
		
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
			histogram.buildHotSpotForest(rectangles, RGOhist.universeUnit(2), numberOfBuckets);
			forest = histogram.forest;
			buckets = new ArrayList<WeightedDoublePointRectangle>();
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
		public List<WeightedDoublePointRectangle> getBuckets(){
			return buckets; 
		}
		
		@Override
		public int numberOfBuckets() {
			return buckets.size();
		}
		
	}
	
}
