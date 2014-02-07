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

package xxl.core.indexStructures.rtrees;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import xxl.core.collections.MapEntry;
import xxl.core.collections.containers.Container;
import xxl.core.cursors.Cursor;
import xxl.core.cursors.mappers.Mapper;
import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Constant;
import xxl.core.functions.Function;
import xxl.core.functions.Functional.UnaryFunction;
import xxl.core.indexStructures.ORTree;
import xxl.core.indexStructures.ORTree.IndexEntry;
import xxl.core.indexStructures.ORTree.Node;
import xxl.core.indexStructures.RTree;
import xxl.core.indexStructures.rtrees.GenericPartitioner.Bucket;
import xxl.core.indexStructures.rtrees.GenericPartitioner.CostFunctionArrayProcessor;
import xxl.core.io.converters.ConvertableConverter;
import xxl.core.io.converters.Converter;
import xxl.core.io.converters.LongConverter;
import xxl.core.spatial.rectangles.DoublePointRectangle;
import xxl.core.spatial.rectangles.Rectangle;
import xxl.core.spatial.rectangles.Rectangles;

/**
 * This class provides basic functionality for level-by-level loading of R-trees. 
 * First the leaf level is build; Produced index entries are stored in auxiliary data structure, this  could be file, list or container; 
 * 
 *
 * @param <T>
 */
public abstract class AbstractIterativeRtreeBulkloader<T> implements IterativeBulkLoaderInterface<T>{
	/**
	 * 
	 * 
	 *
	 */
	public static enum ProcessingType{
		GOPT, // partitioning type cost based optimal with linear run time
		SOPT_F, // quadratic run time, is used if desired storage utilization should be achieved
		SIMPLE, // naive, equal size partitioning 
	}
	/**
	 * stores tree meta data 
	 * @param path
	 * @param rootNodeId
	 * @param rootEntry
	 * @param rootDescriptor
	 * @throws IOException
	 */
	public static void storeMetaData(String path, Long rootNodeId, IndexEntry rootEntry, Rectangle rootDescriptor) throws IOException{
		DataOutputStream dOut = new DataOutputStream(new FileOutputStream(path + "treeTemp.dat"));
		dOut.writeInt(rootEntry.parentLevel());
		dOut.writeLong(rootNodeId);
		rootDescriptor.write(dOut);
		dOut.close();	
	}
	/**
	 * 
	 * @param iterator
	 * @param size
	 * @return
	 */
	protected static DoublePointRectangle[] toArray(Iterator<DoublePointRectangle> iterator, int size){
		DoublePointRectangle[] recs = new DoublePointRectangle[size];
		int i = 0; 
		while(iterator.hasNext()){
			recs[i] = iterator.next();
			i++;
		}
		return recs;
	}
	
	
	/**
	 * Computes optimal one-dimensional distribution 
	 * 
	 * @param iterator
	 * @param level
	 * @param size
	 * @param arrayProcessor
	 * @param B
	 * @param b
	 * @param storageUtil
	 * @return
	 */
	public static int[] computeDistribution(Iterator<DoublePointRectangle> iterator,  int level, int size, CostFunctionArrayProcessor<? extends Rectangle> arrayProcessor, 
			int B, int b, double storageUtil, ProcessingType pType){
		arrayProcessor.reset();
		switch(pType){
			case SOPT_F :  {
				int n = (int) (Math.ceil(size/(storageUtil * B)));
				Bucket[][] buckets = GenericPartitioner.computeOPTF(toArray(iterator, size), b, B, n, arrayProcessor);	
				// take last bucket
				return GenericPartitioner.getDistribution(buckets[n-1][size-1]);
			}
			case SIMPLE:{
				B = (int) (storageUtil * B);
				List<Integer> list = new ArrayList<>();
				int number = size;
				while(number > 0){
					int n = (number - B ) >= 0 ? B : number;  
					list.add(n);
					number -= B;
				}
				int[] distribution = new int[list.size()];
				for(int i = 0; i < distribution.length; i++){
					distribution[i] = list.get(i);
				}
				return distribution;
			}
			default : {
				// GOPT is default
				Bucket[] buckets = GenericPartitioner.computeGOPT(toArray(iterator, size), b, B, arrayProcessor);	
				return  GenericPartitioner.getDistribution(buckets[buckets.length-1]);
			}
		}
	}
	
	/**
	 * Returns cost function as extended volume (area in 2d). 
	 *  
	 * @param normalizedQuerySideLength  is an array of avg query side length; note that we assume unit cube.
	 * @return cost function as volume of rectangle extended with a average query side length
	 */
	public static UnaryFunction<DoublePointRectangle, Double>  generateDefaultFunction(
			final double[] normalizedQuerySideLength){
		return new UnaryFunction<DoublePointRectangle, Double>() {

			@Override
			public Double invoke(DoublePointRectangle arg) {
				DoublePointRectangle rec =   new DoublePointRectangle(arg);
				double[] deltas = rec.deltas();
				double cost = 1d;
				for(int i = 0; i < deltas.length; i++ ){
					cost *= (deltas[i] + normalizedQuerySideLength[i]);
				}
				return  cost;
			}
			
		};
		
	}; 
	
	
	/**
	 * Returns cost function as extended volume (area in 2d). 
	 *  
	 * @param normalizedQuerySideLength  is an array of avg query side length; note that we assume unit cube.
	 * @return cost function as volume of rectangle extended with a average query side length
	 */
	public static UnaryFunction<DoublePointRectangle, Double>  generateDefaultFunctionVolume(){
		return new UnaryFunction<DoublePointRectangle, Double>() {

			@Override
			public Double invoke(DoublePointRectangle arg) {
				return  arg.area();
			}
			
		};
	}; 
	
	/**
	 * Data converter, used to serialize objects in leaf nodes
	 */
	protected  Converter<T> dataConverter;
	/**
	 * Data size in bytes 
	 */
	protected int dataSize; 
	/**
	 * intern representation of index entries computed in one iteration step; 
	 * First value stores nodes address, second value stores doublePointRectangle (descriptor of the computed node)
	 *  
	 */
	protected  Converter<MapEntry<Long, DoublePointRectangle>> mapEntryConverter; 
	/**
	 * number of dimensions
	 */
	protected int dimension; 
	/**
	 * R-tree to bulk load; 
	 * R-tree is initialized and ready to use after bulk loading is finished. 
	 * 
	 */
	protected RTree rtree;
	/**
	 * Block size in bytes; Used for computation of the minimal and maximal number of object per node  
	 */
	protected int blockSize;
	/**
	 * Is used to compute the minimal number of objects per node;
	 */
	protected double ratio; 
	/**
	 *  
	 */
	protected int partitionSize;
	/**
	 * number of elements per leaf node
	 * computed from block size and data size
	 */
	protected int B_Leaf;
	/**
	 * number of elements per index node
	 * computed from block size, dimension and double point rectangle is a descriptor additionally long is stored for node address 
	 * 
	 */
	protected int B_Index;
	/**
	 * minimal number of elements per index node
	 *  computed from block size, dimension and double point rectangle is a descriptor additionally long is stored for node address 
	 */
	protected int b_Index;
	/**
	 * minimal number of elements per leaf node
	 * computed from block size and data size
	 */
	protected int b_Leaf;
	/**
	 * target container of the Rtree
	 */
	protected Container treeContainer; 
	/**
	 * this is used for optimal partitioning computation
	 */
	protected CostFunctionArrayProcessor<? extends Rectangle> arrayProcessor; 
	/**
	 * optional value for the overall number of objects to load
	 */
	protected int numberOfDataObjects;
	/**
	 * partitioning types
	 */
	protected ProcessingType pType;
	/**
	 * avg. storage utilization per node e.g. value 0.5 repersents avg space utilization of 50% per node.
	 */
	protected double storageUtil;
	/**
	 * function for mapping the data to doublepoint rectngles
	 */
	UnaryFunction<T, DoublePointRectangle> toRectangle;
	/**
	 * internal state 
	 */
	protected int level = 0;
	/**
	 * is used for initializing the auxiliary storage for level entries
	 * 
	 * @throws IOException
	 */
	protected abstract void reinitTempLevelStorage() throws IOException; 
	/**
	 * stores the auxiliary entry, for index levels 
	 * 
	 * @param entry
	 * @throws IOException 
	 */
	protected abstract void storeTempIndexEntry(MapEntry<Long,DoublePointRectangle> entry) throws IOException; 
	/**
	 * gets cursor for next level computation
	 *  
	 * @return
	 */
	protected abstract Cursor getLevelIterator();
	
	/**
	 * 
	 */
	public AbstractIterativeRtreeBulkloader(RTree tree, 
			int dimension, 
			int blockSize, 
			double ratio,
			double storageUtil, 
			int partitionSize) {
		this.rtree = tree;
		this.dimension = dimension;
		this.blockSize = blockSize;
		this.ratio = ratio;
		this.partitionSize = partitionSize;
		this.storageUtil = storageUtil;
		// redirect container
		this.treeContainer = (Container) rtree.determineContainer.invoke(); 
	}
	
	/**
	 * 
	 * @param arrayProcessor used for computing optimal one dimensional partitioning
	 * @param pType partitioning type
	 * @param dataSize size in
	 * @param dataConverter
	 * @param toRectangle
	 * @return
	 */
	public AbstractIterativeRtreeBulkloader<T> init(
			CostFunctionArrayProcessor<? extends DoublePointRectangle>	arrayProcessor,
			ProcessingType pType,  
			int dataSize, 	
			final Converter<T> dataConverter, 
			UnaryFunction<T, DoublePointRectangle> toRectangle){
		this.arrayProcessor = arrayProcessor;
		int payload = blockSize-6;
		b_Leaf = (int)( (payload *  ratio)/ (dataSize ));
		B_Leaf = payload / (dataSize);
		b_Index =  (int)( (payload *  ratio)/ (dimension * 16 + 8 )); 
		B_Index = payload / (dimension * 16 + 8 );
		this.pType = pType;
		//
		this.dataSize = dataSize; 
		this.dataConverter = dataConverter; 
		//
		final Converter<DoublePointRectangle> rectangleConverter = new ConvertableConverter<DoublePointRectangle>(
				Rectangles.factoryFunctionDoublePointRectangle(dimension));
		//
		this.toRectangle = toRectangle;
		//
		mapEntryConverter = new Converter<MapEntry<Long, DoublePointRectangle>>(){

			@Override
			public MapEntry<Long, DoublePointRectangle> read(DataInput arg0,
					MapEntry<Long,DoublePointRectangle> arg1) throws IOException {
				long key = LongConverter.DEFAULT_INSTANCE.readLong(arg0);
				DoublePointRectangle value = rectangleConverter.read(arg0);
				return new MapEntry<Long, DoublePointRectangle>(key, value) ;
			}

			@Override
			public void write(DataOutput arg0,
					MapEntry<Long, DoublePointRectangle> arg1) throws IOException {
				LongConverter.DEFAULT_INSTANCE.write(arg0, arg1.getKey());
				arg1.getValue().write(arg0);
			}};
		return this;
	}
	
	
	
	
	/* (non-Javadoc)
	 * @see xxl.core.indexStructures.rtrees.IterativeBulkLoaderInterface#buildRTree(java.util.Iterator)
	 */
	@Override
	public void buildRTree(Iterator<T> rectangles) throws IOException {
		Iterator tempIterator = rectangles;
		while(tempIterator.hasNext()){
			reinitTempLevelStorage();
			int B = (level > 0) ? B_Index : B_Leaf;
			int written = writeLevel(tempIterator, level, partitionSize,  rtree, treeContainer,   B);
			level++;
			Cursor levelIterator = getLevelIterator();
			tempIterator = levelIterator;
			 numberOfDataObjects = written;
			if (written <= 1){
				break;
			}
		}
		// create rtree
		MapEntry<Long, DoublePointRectangle> entry = (MapEntry<Long, DoublePointRectangle>)(tempIterator.next());
		DoublePointRectangle rootDescriptor = entry.getValue();
		IndexEntry indexEntry = (IndexEntry) rtree.createIndexEntry(level);
		IndexEntry rootEntry = (IndexEntry) ((ORTree.IndexEntry)indexEntry.initialize(entry.getKey())).initialize(rootDescriptor);
		// init tree set rootEntry and root descriptor
		rtree.initialize(rootEntry, rootDescriptor, new AbstractFunction() {
			@Override
			public Object invoke(Object argument) {
				return toRectangle.invoke((T)argument);
			}
		},
		blockSize, 
		treeContainer, dataSize, dimension * 8 *2, ratio);
	}
	
	/**
	 * Default processing; Reads the level data, partitions is in chunks of size equal to partitionSize computes optimal partitioning for each chunk and 
	 * writes into auxiliary storage for a next level generation 
	 * 	
	 * @param data
	 * @param level
	 * @param partitionSize
	 * @param rtree
	 * @param treeContainer
	 * @param B
	 * @return
	 * @throws IOException
	 */
	@SuppressWarnings({ "unchecked", "serial" })
	public int writeLevel(Iterator data, 
			final  int level, 
			int partitionSize,
			RTree rtree, 
			Container treeContainer, 
			int B) throws IOException{
		// read  partitions size to a list
		int counter = 0;
		List partition = new LinkedList();
		if (pType ==ProcessingType.SIMPLE){
			int P = (level > 0 ) ?  B_Index : B_Leaf; 
			P = (int) (storageUtil * P);
			partitionSize = P*P;
		}
		while(data.hasNext()){
			for(int  i = 0; data.hasNext() && i < partitionSize; i++ ){
				if (level > 0 ){
					MapEntry<Long, DoublePointRectangle> mapEntry = (MapEntry<Long, DoublePointRectangle>) data.next();
					DoublePointRectangle rec = mapEntry.getValue();
					// create index entry 
					IndexEntry indexEntry = (IndexEntry) rtree.createIndexEntry(level);
					((ORTree.IndexEntry)indexEntry.initialize(mapEntry.getKey())).initialize(rec);
					partition.add(indexEntry);
				}else{ 
					Object rec = data.next();
					partition.add(rec);
				}
			}
			
			if (partition.size() >  B ){
				Function mapping = new AbstractFunction() {
					
					public Object invoke(Object obj ){
						return (level ==  0 )? 
							 (toRectangle.invoke((T)obj) )	: (DoublePointRectangle)((IndexEntry)obj).descriptor(); 
					}
				}; 
				
				final int[] distribution = computeDistribution((Iterator<DoublePointRectangle>)new Mapper(mapping,  partition.iterator() ),  level, partition.size());
				counter += 	writePartition(distribution, partition.iterator(), level, B,  rtree,  treeContainer);
			}else{   
				// just allocate one node
				MapEntry<Long, DoublePointRectangle> entry = writeNode(partition, level,  rtree,  treeContainer) ;
				storeTempIndexEntry(entry);
				//mapEntryConverter.write(out, entry );
				counter++;
			}
			partition = new LinkedList();
		}
		return counter;
	}
	/**
	 * Default method for processing a chunk a of level data after applying the optimal partitioning
	 * 
	 * @param distribution
	 * @param data
	 * @param level
	 * @param B
	 * @param rtree
	 * @param treeContainer
	 * @return
	 * @throws IOException
	 */
	public int writePartition(int[] distribution, 
			Iterator data, 
			int level, 
			int B,  RTree rtree, Container treeContainer) throws IOException{
		for(int i : distribution){
			List entries = new ArrayList(i);
			for(int k = 0;  data.hasNext() && k < i ; k++){
				DoublePointRectangle rec = null;
				if(level != 0 ){
					IndexEntry indexEntry  = (IndexEntry) data.next();
					entries.add(indexEntry);
				}else{
					Object obj =  data.next();
					entries.add(obj);
				}
			}
			if (i > B ){
				throw new RuntimeException("too many entries per block");
			}		
			MapEntry<Long,DoublePointRectangle> entry = writeNode(entries, level,  rtree,  treeContainer);
			storeTempIndexEntry(entry);
		}
		return distribution.length;
	}
	
	/**
	 * Writes node to a R-tree container
	 * @param entries
	 * @param level
	 * @param rtree
	 * @param treeContainer
	 * @return
	 */
	public MapEntry<Long,DoublePointRectangle> writeNode(List<?> entries, int level, RTree rtree, Container treeContainer) {
		DoublePointRectangle descriptor = null;
		for (Object o : entries ){
			DoublePointRectangle rec = (level == 0) ? (DoublePointRectangle)(toRectangle.invoke((T)o)): (DoublePointRectangle)((IndexEntry)o).descriptor();
			
			if (descriptor == null)
				descriptor = new DoublePointRectangle(rec);
			else 
				descriptor.union(rec);
		}
		final Node node = (Node) rtree.createNode(level);
		Long nodeId = (Long) treeContainer.reserve(new Constant<Node>(node));
		node.initialize(level, entries);
		treeContainer.update(nodeId, node);// I/O 
		return new MapEntry<Long, DoublePointRectangle>(nodeId, descriptor);
	}
	
	/**
	 * 
	 * @param iterator
	 * @param level
	 * @param size
	 * @return
	 */
	public int[] computeDistribution(Iterator<DoublePointRectangle> iterator,  int level, int size){
		int b = (level > 0 ) ?  b_Index : b_Leaf;
		int B = (level > 0 ) ?  B_Index : B_Leaf; 
		return AbstractIterativeRtreeBulkloader.computeDistribution(iterator, level, size, arrayProcessor, B, b, storageUtil, pType); 
	}
	
	/**
	 * 
	 * @return
	 */
	public RTree getRTree(){
		return this.rtree;
	}
}
