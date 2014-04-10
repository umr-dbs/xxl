package xxl.core.spatial.spatialBPlusTree;




import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;






import xxl.core.collections.containers.Container;
import xxl.core.collections.containers.io.BufferedContainer;
import xxl.core.collections.containers.io.ConverterContainer;
import xxl.core.cursors.Cursor;
import xxl.core.cursors.filters.Filter;
import xxl.core.cursors.unions.Sequentializer;
import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;
import xxl.core.functions.Functional.BinaryFunction;
import xxl.core.functions.Functional.UnaryFunction;
import xxl.core.indexStructures.BPlusTree;
import xxl.core.indexStructures.BPlusTreeBulkLoading;
import xxl.core.indexStructures.BPlusTree.Node;
import xxl.core.io.Buffer;
import xxl.core.io.converters.Converters;
import xxl.core.io.converters.LongConverter;
import xxl.core.io.converters.MeasuredConverter;
import xxl.core.predicates.AbstractPredicate;
import xxl.core.predicates.Predicate;
import xxl.core.spatial.SpaceFillingCurves;
import xxl.core.spatial.SpatialUtils;
import xxl.core.spatial.rectangles.DoublePointRectangle;
import xxl.core.spatial.spatialBPlusTree.AdaptiveZCurveMapper.SpatialZQueryRange;
import xxl.core.spatial.spatialBPlusTree.SingleLevelwiseOptimizedBulkloader.DistributionType;
import xxl.core.spatial.spatialBPlusTree.cursors.SFCFunctionSpatialCursor;
import xxl.core.spatial.spatialBPlusTree.cursors.SpatialMultiRangeCursor;
import xxl.core.spatial.spatialBPlusTree.cursors.SpatialRangeQueryBPlusCursor;
import xxl.core.spatial.spatialBPlusTree.separators.LongKeyRange;
import xxl.core.spatial.spatialBPlusTree.separators.LongSeparator;

/**
 * 
 * provides methods for creating and initializing Z-Curve Spatial Index based on B+Tree, 
 * Keys are long values. The double values of rectangles or points should be mapped to integer values. 
 * 
 *
 */
@SuppressWarnings("serial")
public class ZBPlusTreeIndexFactrory {
	
	/**
	 * 
	 */
	public static final int DEFAULT_PARTITION_SIZE = 20_000; 
	
	/**
	 * 
	 */
	public static final double DEFAULT_MIN_CAPACITY = 0.4; 
	
	/**
	 * 
	 */
	public static final double  DEFAULT_AVG_LOAD = 0.8; 
	
	/**
	 * 
	 * @param bitsProDim
	 * @param dimension
	 * @return
	 */
	public static  SFCFunctionSpatialCursor<Long> createSFCFunction(final int bitsProDim, final int dimension){
		
		return new SFCFunctionSpatialCursor<Long>(){

			@Override
			public Long getMaxKeyInBox(int[] lPoint, int[] rPoint, boolean max) {
				return (max) ? SpaceFillingCurves.computeZCode(rPoint, bitsProDim) : 
					SpaceFillingCurves.computeZCode(lPoint, bitsProDim);
			}

			@Override
			public Long getNextPointInBox(int[] lPoint, int[] rPoint, Long key,
					boolean next) {
				return  SpaceFillingCurves.nextInBoxZValue(key, lPoint, rPoint, bitsProDim, dimension);
			}

			@Override
			public Long getSuccessor(Long key) {
				return key+1L;
			}

			
		};
	}
	
	/**
	 * Key Converter, Z-strings are represented as long values
	 */
	public static MeasuredConverter<Long> longKeyMeasuredConverter = Converters.createMeasuredConverter(LongConverter.DEFAULT_INSTANCE);
	
	/**
	 *  This method extracts low left or upper right  point of the rectangle 
	 *  and maps it to integer value according to provided resolution and universe 
	 * @param universe
	 * @param rectangle
	 * @param bitsProDim
	 * @param right
	 * @return
	 */
	public static int[] getPoint(DoublePointRectangle universe, DoublePointRectangle rectangle, int bitsProDim, boolean right){
		double[] uni = (double[])universe.getCorner(false).getPoint();
		double[] uniDeltas = universe.deltas();	
		double[] point = (double[]) rectangle.getCorner(right).getPoint();
		int[] pointInt = new int[point.length];
		for(int i = 0; i < point.length; i ++ ){
			point[i] = (point[i] - uni[i])/ uniDeltas[i];
			pointInt[i] = (int) (point[i] *  (1 <<(bitsProDim-1))); 
		}
		return pointInt;
	}
	

	/**
	 * 
	 * @param dimension
	 * @param bitsProDim
	 * @param universe
	 * @return
	 */
	@SuppressWarnings({ "deprecation", "serial" })
	public static Function<DoublePointRectangle, Long> createGetKey2DFunction(
			final int dimension, 
			final int bitsProDim, 
			final DoublePointRectangle universe, 
			final int FILLING_CURVE_PRECISION){
		return new AbstractFunction<DoublePointRectangle, Long>() {
			private double uni[];
			private double uniDeltas[];	
			@Override
			public Long invoke(DoublePointRectangle argument) {
				if (dimension!=2) throw new IllegalArgumentException();
				if (uni == null) {
					uni = (double[])universe.getCorner(false).getPoint();
					uniDeltas = universe.deltas();	
				}
				double center[] = (double[])argument.getCenter().getPoint();
				double x1 = (center[0]-uni[0])/uniDeltas[0];
				double y1 = (center[1]-uni[1])/uniDeltas[1];
				long z = SpaceFillingCurves.peano2d((int) (x1*FILLING_CURVE_PRECISION),(int) (y1*FILLING_CURVE_PRECISION));
				return z;
			}
		};
	}
	
	/**
	 * 
	 * @param DIMENSION
	 * @param bitsProDim
	 * @param universe
	 * @return
	 */
	@SuppressWarnings({ "deprecation", "serial" })
	public static Function<DoublePointRectangle, Long> createGetKeyFunction(
			final DoublePointRectangle universe,  final int[] mappingFunction, final int[] masks){
		final int[] resolutions = new int[masks.length];
		for(int i = 0; i < masks.length; i++){
			resolutions[i] = 1<< masks[i];   
		}
		return new AbstractFunction<DoublePointRectangle, Long>() {
			private double uni[];
			private double uniDeltas[];	
			@Override
			public Long invoke(DoublePointRectangle argument) {
				if (uni == null) {
					uni = (double[])universe.getCorner(false).getPoint();
					uniDeltas = universe.deltas();	
				}
				double center[] = (double[])argument.getCenter().getPoint();
				double[] normCenter = SpatialUtils.normalize(center, uni, uniDeltas);
				int[] coord = new int[normCenter.length];
				for(int i = 0; i < coord.length; i++){
					coord[i] = (int) (normCenter[i] * (resolutions[i]));
				}
				long z = AdaptiveZCurveMapper.computeZKey(coord, mappingFunction, masks);
				return z;
			}
		};
	}
	
	/**
	 * 
	 * @param inputData
	 * @param dataConverter
	 * @param getKeyFunction
	 * @param container
	 * @param blockSize
	 * @param buffer
	 * @return
	 */
	public static <T> BPlusTree loadZBPlusTreeTupleByTuple(Iterator<T> inputData,
			MeasuredConverter<T> dataConverter, Function<T,Long> getKeyFunction, Container container, int blockSize, Buffer buffer){
		BPlusTree tree = new BPlusTree(blockSize, true);
		Container treeContainer =  new ConverterContainer(container, tree.nodeConverter());
		if (buffer != null){
			treeContainer = new BufferedContainer(treeContainer, buffer); 
		}
		tree.initialize(null, null, 
				getKeyFunction, 
				treeContainer, 
				ZBPlusTreeIndexFactrory.longKeyMeasuredConverter, 
				dataConverter,
				LongSeparator.FACTORY_FUNCTION,  
				LongKeyRange.FACTORY_FUNCTION);
		for(;inputData.hasNext();){
			T data = inputData.next(); 
			tree.insert(data); 
		}
		treeContainer.flush();
		return tree;
	}
	
	/**
	 * 
	 * @param inputData
	 * @param dataConverter
	 * @param getKeyFunction
	 * @param container
	 * @param blockSize
	 * @param buffer
	 * @return
	 */
	public static <T> BPlusTree loadZBPlusTreeNonOptimized(Iterator<T> sortedData,
			MeasuredConverter<T> dataConverter, Function<T,Long> getKeyFunction, Container container, final  int blockSize, Buffer buffer, final double spaceUtil){
		final int dataSize = dataConverter.getMaxObjectSize(); 
		BPlusTree tree = new BPlusTree(blockSize, true);
		Container treeContainer =  new ConverterContainer(container, tree.nodeConverter());
		if (buffer != null){
			treeContainer = new BufferedContainer(treeContainer, buffer); 
		}
		tree.initialize(null, null, 
				getKeyFunction, 
				treeContainer, 
				ZBPlusTreeIndexFactrory.longKeyMeasuredConverter, 
				dataConverter,
				LongSeparator.FACTORY_FUNCTION,  
				LongKeyRange.FACTORY_FUNCTION);
		new BPlusTreeBulkLoading(tree, sortedData, tree.determineContainer, new AbstractPredicate() {
			public boolean invoke(Object arg){
				Node node = (Node)arg;
				int payLoad = blockSize - 2 - 4 - 8;
				int number = node.number();
				if(node.level() == 0)
					return number >= (int)((payLoad/(dataSize)) * spaceUtil);
				return number >= (int)((payLoad/(8*2)) * spaceUtil) ;
			}
		});
		treeContainer.flush();
		return tree;
	}
	
	
	
	
	/**
	 * 
	 * @param inputData
	 * @param dataConverter
	 * @param getKeyFunction
	 * @param container
	 * @param blockSize
	 * @param buffer
	 * @return
	 */
	public static <T> BPlusTree loadZBPlusTreeOptimizedGOP(Iterator<T> sortedData, MeasuredConverter<T> dataConverter, 
			UnaryFunction<T,Long> getKeyFunction, 
			Container container, String treePrefix,  final  int blockSize, Buffer buffer, final double spaceUtil, int dimension){
		SingleLevelwiseOptimizedBulkloader<T> bulkloader = new SingleLevelwiseOptimizedBulkloader<T>(dataConverter, 
						DEFAULT_PARTITION_SIZE, 
						dimension, 
						blockSize,
						DEFAULT_MIN_CAPACITY, 
						spaceUtil, 
						spaceUtil, 
						container, 
						DistributionType.DISTRIBUTION_GOPT,
						treePrefix + "levelData.met", 	
						getKeyFunction,
						buffer);
		return bulkloader.tree;
	}
	
	
	
	
	/**
	 * 
	 * @param tree
	 * @param queryRec
	 * @param universe
	 * @param numberOfBitsPerDimension
	 * @param dimension
	 * @return
	 */
	public static Cursor queryNextPointInBoxCursor(BPlusTree tree, final DoublePointRectangle queryRec, DoublePointRectangle universe, int numberOfBitsPerDimension, int dimension){
		// create cursor
		Predicate<DoublePointRectangle> leafNodePredicate = new AbstractPredicate<DoublePointRectangle>() {
			@Override
			public boolean invoke(DoublePointRectangle argument) {
				return queryRec.overlaps(argument);
			}
		};
		int[] lPoint = getPoint(universe, queryRec, numberOfBitsPerDimension, false);
		int[] rPoint = getPoint(universe, queryRec, numberOfBitsPerDimension, true);
		SpatialRangeQueryBPlusCursor cursor = 
			new SpatialRangeQueryBPlusCursor(tree, lPoint, rPoint, createSFCFunction(numberOfBitsPerDimension, dimension), LongKeyRange.FACTORY_FUNCTION,
					leafNodePredicate);
		return cursor; 
		
	}
	

	/**
	 * Only for point data
	 * @param tree
	 * @param ranges
	 * @return
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static Cursor queryMultiRangeJumper(BPlusTree tree, List<SpatialZQueryRange> ranges, DoublePointRectangle query){
		SpatialMultiRangeCursor multiRangeCursor = new SpatialMultiRangeCursor(ranges, tree, query, new UnaryFunction<Object, DoublePointRectangle>() {
			@Override
			public DoublePointRectangle invoke(Object arg) {
				DoublePointRectangle dpr = (DoublePointRectangle)arg; 
				return new DoublePointRectangle(dpr);
			}
		}); 
		return  multiRangeCursor; 
	}
	
	
	
	/**
	 * Only for point data
	 * @param tree
	 * @param ranges
	 * @return
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static Cursor queryMultiRange(BPlusTree tree, List<SpatialZQueryRange> ranges, DoublePointRectangle query){
		List<Cursor> cursors = new LinkedList<>(); 
		Iterator[] iterators = new Iterator[ranges.size()];
		int i = 0; 
		for(SpatialZQueryRange range: ranges){
			iterators[i] = tree.rangeQuery(range.getFirst(), range.getSecond()); 
			i++;
		}
		return new Sequentializer<>(iterators); 
	}
	
	/**
	 * Only for point data
	 * @param tree
	 * @param ranges
	 * @return
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static Cursor queryMultiRangeZCurve(BPlusTree tree, DoublePointRectangle query, 
			final DoublePointRectangle universe,  final int[] mappingFunction, final int[] masks, int lastDimensionFirstIndex, int hyperPlaneIndex){
		final int[] resolutions = new int[masks.length];
		for(int i = 0; i < masks.length; i++){
			resolutions[i] = 1<< masks[i];   
		}
		double[] uni = (double[])universe.getCorner(false).getPoint();
		double[] uniDeltas = universe.deltas();	
		double low[] = (double[])query.getCorner(false).getPoint(); 
		double high[] = (double[])query.getCorner(true).getPoint(); 
		double[] normLow = SpatialUtils.normalize(low, uni, uniDeltas);
		double[] normHigh = SpatialUtils.normalize(high, uni, uniDeltas);
		int[] coordLow = new int[normLow.length];
		int[] coordHigh = new int[normLow.length];
		for(int i = 0; i < coordLow.length; i++){
			coordLow[i] = (int) (normLow[i] * (resolutions[i]));
			coordHigh[i] = (int) (normHigh[i] * (resolutions[i]));
		}
		int[] resolutionAcc = new int[masks.length];
		System.arraycopy(masks, 0, resolutionAcc, 0, resolutionAcc.length); 
		List<SpatialZQueryRange> ranges = AdaptiveZCurveMapper.computesRanges(coordLow, coordHigh, mappingFunction, 
				masks, resolutionAcc, lastDimensionFirstIndex, hyperPlaneIndex); 
		return queryMultiRange(tree, ranges, query); 
	}
	
	
	/**
	 * Only for point data
	 * @param tree
	 * @param ranges
	 * @return
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static Cursor queryMultiRangeZCurveJumper(BPlusTree tree, DoublePointRectangle query, 
			final DoublePointRectangle universe,  final int[] mappingFunction, final int[] masks, int lastDimensionFirstIndex, int hyperPlaneIndex){
		final int[] resolutions = new int[masks.length];
		for(int i = 0; i < masks.length; i++){
			resolutions[i] = 1<< masks[i];   
		}
		double[] uni = (double[])universe.getCorner(false).getPoint();
		double[] uniDeltas = universe.deltas();	
		double low[] = (double[])query.getCorner(false).getPoint(); 
		double high[] = (double[])query.getCorner(true).getPoint(); 
		double[] normLow = SpatialUtils.normalize(low, uni, uniDeltas);
		double[] normHigh = SpatialUtils.normalize(high, uni, uniDeltas);
		int[] coordLow = new int[normLow.length];
		int[] coordHigh = new int[normLow.length];
		for(int i = 0; i < coordLow.length; i++){
			coordLow[i] = (int) (normLow[i] * (resolutions[i]));
			coordHigh[i] = (int) (normHigh[i] * (resolutions[i]));
		}
		int[] resolutionAcc = new int[masks.length];
		System.arraycopy(masks, 0, resolutionAcc, 0, resolutionAcc.length); 
		List<SpatialZQueryRange> ranges = AdaptiveZCurveMapper.computesRanges(coordLow, coordHigh, mappingFunction, 
				masks, resolutionAcc, lastDimensionFirstIndex, hyperPlaneIndex); 
		return queryMultiRangeJumper(tree, ranges, query); 
	}
	
	/**
	 * 
	 * @param tree
	 * @param query
	 * @param universe
	 * @param mappingFunction
	 * @param masks
	 * @return
	 */
	public static Cursor queryMultiRange(BPlusTree tree,final DoublePointRectangle query, 
			final DoublePointRectangle universe,  final int[] mappingFunction,  final int[] resolutions, 
			final int firstPrefixL, final int bitsPerLastDim){
		int lastDimensionFirstIndex = AdaptiveZCurveMapper.getLastDimensionPrefixIndex(firstPrefixL, bitsPerLastDim);
		int hyperPlaneIndex = AdaptiveZCurveMapper.getIndexOfHighestBit(resolutions);
		return new Filter<>(queryMultiRangeZCurve(tree,  query,  universe,   mappingFunction,  resolutions, lastDimensionFirstIndex, hyperPlaneIndex), new AbstractPredicate() {
		
		@Override
		public boolean invoke(Object argument) {
			DoublePointRectangle argR = (DoublePointRectangle)argument; 
			return query.overlaps(argR);
		}
		
		}) ;
	}
	
	/**
	 * 
	 * @param tree
	 * @param query
	 * @param universe
	 * @param mappingFunction
	 * @param masks
	 * @return
	 */
	public static Cursor queryMultiRangeJumper(BPlusTree tree,final DoublePointRectangle query, 
			final DoublePointRectangle universe,  final int[] mappingFunction,  final int[] resolutions, 
			final int firstPrefixL, final int bitsPerLastDim){
		int lastDimensionFirstIndex = AdaptiveZCurveMapper.getLastDimensionPrefixIndex(firstPrefixL, bitsPerLastDim);
		int hyperPlaneIndex = AdaptiveZCurveMapper.getIndexOfHighestBit(resolutions);
		return queryMultiRangeZCurveJumper(tree,  query,  universe,   mappingFunction,  resolutions, lastDimensionFirstIndex, hyperPlaneIndex); 
	}
	
}
