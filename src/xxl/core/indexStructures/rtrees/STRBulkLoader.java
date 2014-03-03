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

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import xxl.core.collections.MapEntry;
import xxl.core.collections.containers.Container;
import xxl.core.collections.containers.io.BlockFileContainer;
import xxl.core.collections.queues.Queue;
import xxl.core.collections.queues.io.BlockBasedQueue;
import xxl.core.cursors.Cursor;
import xxl.core.cursors.sorters.MergeSorter;
import xxl.core.cursors.sources.io.FileInputCursor;
import xxl.core.cursors.wrappers.QueueCursor;
import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Constant;
import xxl.core.functions.Function;
import xxl.core.functions.Functional.UnaryFunction;
import xxl.core.indexStructures.ORTree;
import xxl.core.indexStructures.ORTree.IndexEntry;
import xxl.core.indexStructures.RTree;
import xxl.core.io.converters.Converter;
import xxl.core.spatial.points.DoublePoint;
import xxl.core.spatial.rectangles.DoublePointRectangle;


/**
 * This class implements STR loading algorithm by Leutenegger et al.:
 * 
 * 
 * See: 
 * Scott Leutenegger and Mario A. Lopez and J. Edgington
 * STR: A Simple and Efficient Algorithm for R-Tree Packing (ICDE 1997)
 * 
 * 
 * 
 * For storing temporal level data simple file is used. 
 * 
 * @author d
 *
 * @param <T>
 */
public class STRBulkLoader<T> extends AbstractIterativeRtreeBulkloader<T>{

	/**
	 * path to store auxiliary file
	 */
	protected String path = null; 
	/**
	 * auxiliary storage as file
	 */
	protected File file = null;
	/**
	 * stream to the file
	 */
	protected DataOutputStream out = null; 
	/**
	 * 
	 */
	protected int[] sortingFunction;
	/**
	 * 
	 */
	Container queueContainer; 
	/**
	 * 
	 */
	public int numberOfRectangles;
	/**
	 * 
	 */
	public int SORT_BUFFER_SIZE = 10*1024*1024;
	
	/**
	 * For initializing the STR loader we need 
	 * an Rtree, number of dimensions, blocksize, storage utilization per node in percent and so called sortign function
	 * this provides the ordering of dimensions, since str sorts and partitions data according to one dimension at one step
	 * sorting function provides which dimension should be taken as next
 	 * e.g. in two dimensional space we have 4 different sorting functions x,x or x,y or y,y, or x,x
	 * in this example we use a default one x,y
	 * @param rtree
	 * @param path
	 * @param dimension
	 * @param blockSize
	 * @param ratio is used after loading for R* split 
	 * @param nodeUtil based on this value maximal entries per node are computed
	 * @param sortingFunction
	 */
	public STRBulkLoader(RTree rtree, 
			String path, 
			int dimension,
			int blockSize, 
			double ratio, 
			double nodeUtil,
			int[] sortingFunction) {
		super(rtree, dimension, blockSize, ratio, nodeUtil, 20_000);
		this.path = path;
		this.sortingFunction = sortingFunction;
		//check if it right
		queueContainer = new BlockFileContainer(path  + "_queues.dat", blockSize);
	}

	/**
	 * 
	 * @param number
	 * @param sortMemoryBufferSize
	 * @param dataSize
	 * @param dataConverter
	 * @param toRectangle
	 * @return
	 */
	public IterativeBulkLoaderInterface<T> init(int number, int sortMemoryBufferSize, int dataSize, 	
			final Converter<T> dataConverter, 
			UnaryFunction<T, DoublePointRectangle> toRectangle){
		super.init(null, ProcessingType.SIMPLE, dataSize, dataConverter, toRectangle);
		int payload = blockSize-6;
		B_Leaf = (int)((double)(payload / (dataSize)) * storageUtil);
		B_Index = (int)((double)(payload / (dimension * 16 + 8 ))*storageUtil);
		this.numberOfDataObjects = number; 
		this.SORT_BUFFER_SIZE = sortMemoryBufferSize;
		return this; 
	}

	/*
	 * (non-Javadoc)
	 * @see xxl.core.indexStructures.rtrees.AbstractIterativeRtreeBulkloader#buildRTree(java.util.Iterator)
	 */
	public void buildRTree(Iterator rectangles) throws IOException{
		Iterator tempIterator = rectangles;
		int level = 0;
		int numberOfRecs = numberOfDataObjects;
		while(tempIterator.hasNext()){
			reinitTempLevelStorage();
			int written = 
				sortSTRData(tempIterator, numberOfRecs, level, sortingFunction[0], dimension);
			level++;
			numberOfRecs = written; 
			tempIterator  = getLevelIterator();
			if (written <= 1){
				break;
			}
			file.deleteOnExit();
		}
		// create rtree
		MapEntry<Long, DoublePointRectangle> entry = (MapEntry<Long, DoublePointRectangle>)(tempIterator.next());
		DoublePointRectangle rootDescriptor = entry.getValue();
		IndexEntry indexEntry = (IndexEntry) rtree.createIndexEntry(level);
		IndexEntry rootEntry = (IndexEntry) ((ORTree.IndexEntry)indexEntry.initialize(entry.getKey())).initialize(rootDescriptor);
		//
		storeMetaData(path, entry.getKey(),  rootEntry,  rootDescriptor);
		// init tree
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
	 * 
	 * @param data
	 * @param number
	 * @param level
	 * @param dim
	 * @return
	 * @throws IOException
	 */
	protected int writeSlab(Iterator data, int number, int level, int dim) throws IOException{
		// write rectangles to 
		Iterator sorter = sort(data, dim, level);
		int written = 0;
		int nodeSize = (level > 0) ? B_Index: B_Leaf;
		while(sorter.hasNext() ){
			List entries = new ArrayList(nodeSize);
			for (int i = 0; i <  nodeSize && sorter.hasNext(); i++ ){
				DoublePointRectangle rec = null;
				if(level != 0 ){
					MapEntry<Long, DoublePointRectangle> mapEntry = (MapEntry<Long, DoublePointRectangle>) sorter.next();
					rec = mapEntry.getValue();
					// create index entry 
					IndexEntry indexEntry = (IndexEntry) rtree.createIndexEntry(level);
					((ORTree.IndexEntry)indexEntry.initialize(mapEntry.getKey())).initialize(rec);
					entries.add(indexEntry);
				}else{
					rec = (DoublePointRectangle) sorter.next();
					entries.add(rec);
				}
			}
			MapEntry<Long, DoublePointRectangle> entry = writeNode(entries, level, this.rtree, this.treeContainer);
			written++;
			storeTempIndexEntry(entry);
		}
		return written;
	}
	/**
	 * 
	 * @param data
	 * @param number
	 * @param level
	 * @param dim
	 * @param depth
	 * @return
	 * @throws IOException
	 */
	public int sortSTRData(Iterator data, int number, int level, int dim, int depth) throws IOException{
		if (depth <= 1){
			return writeSlab(data, number, level, dim); 
		}
		// define number of splits prodim
		int numberOfBlocks = number / ((level== 0) ? (B_Leaf) : B_Index); 
		int splitsProDim = (int) Math.pow(numberOfBlocks, 1.0 / dimension);
		if(numberOfBlocks <= 1){
			return writeSlab(data, number, level, dim); 
		}
		//1. sort with comprator
		Iterator sorter = sort(data, dim, level);
		int written = 0;
		depth--;
		for(int i = 0; i < splitsProDim+1; i++){
			BlockBasedQueue queue = new BlockBasedQueue(queueContainer, 
					blockSize, 
					(level> 0)? mapEntryConverter: dataConverter, 
					new Constant(0),
					new Constant(0));
			int j = 0;
			for(j = 0 ; sorter.hasNext() && j < (number /splitsProDim); j++  ){
				Object obj = sorter.next();
				queue.enqueue(obj);
			}
			// recursive call 
			written  += sortSTRData(new QueueCursor(queue), j, level, sortingFunction[dimension-depth], depth);
		}
		return written;
	}
	
	/**
	 * 
	 * 
	 * 
	 * @param dim
	 * @param level
	 * @return dimension comparator that is used for next recursive step
	 */
	protected Comparator getDimensionComparator(final int dim, final int level){
		return  new Comparator() {
			@Override
			public int compare(Object  o1, Object o2) {
				DoublePointRectangle rec1 = (level > 0) ? ((MapEntry<Long,DoublePointRectangle>)o1).getValue(): (DoublePointRectangle)o1;
				DoublePointRectangle rec2 = (level > 0) ? ((MapEntry<Long,DoublePointRectangle>)o2).getValue(): (DoublePointRectangle)o2;
				DoublePoint first = rec1.getCenter();
				DoublePoint second = rec2.getCenter();
				return ( first.getValue(dim) == second.getValue(dim)) ? 0 :  ( first.getValue(dim) < second.getValue(dim)) ? 
						-1 : 1;
			}
		};
	}
	/**
	 * internal sorting method
	 *  External sort method used
	 * {@link MergeSorter}
	 * 
	 * @param data
	 * @param dim
	 * @param level
	 * @return
	 * @throws IOException
	 */
	protected Iterator sort(Iterator data, int dim, int level) throws IOException{
		final Converter converter = (level > 0 ) ? mapEntryConverter: dataConverter;
		int objectSize = (level > 0 ) ? dimension *  16 : dataSize; 
		Container container = new BlockFileContainer(path  + "tmpsortqueue.tmp" + dim  + level, blockSize);
		final Container queueContainer = container;
		final Function<Function<?, Integer>, Queue<?>> queueFunction =
			new AbstractFunction<Function<?, Integer>, Queue<?>>() {
			public Queue<?> invoke(Function<?, Integer> function1, Function<?, Integer> function2) {
				return new BlockBasedQueue(queueContainer, blockSize, converter,
						function1, function2);
			}
		};
		Comparator<?> comp = getDimensionComparator(dim, level);
		return  new MergeSorter(data, 
				comp, objectSize , SORT_BUFFER_SIZE,SORT_BUFFER_SIZE, queueFunction, false);
	}
	/*
	 * (non-Javadoc)
	 * @see xxl.core.indexStructures.rtrees.AbstractIterativeRtreeBulkloader#reinitTempLevelStorage()
	 */
	protected void reinitTempLevelStorage() throws IOException{
		file = File.createTempFile("levelRecs_", "dat");
		out = new DataOutputStream(new FileOutputStream(file));
	}
	/*
	 * (non-Javadoc)
	 * @see xxl.core.indexStructures.rtrees.AbstractIterativeRtreeBulkloader#storeTempIndexEntry(xxl.core.collections.MapEntry)
	 */
	protected void storeTempIndexEntry(MapEntry<Long,DoublePointRectangle> entry) throws IOException{
		mapEntryConverter.write(out, entry);
	}
	/*
	 * (non-Javadoc)
	 * @see xxl.core.indexStructures.rtrees.AbstractIterativeRtreeBulkloader#getLevelIterator()
	 */
	protected Cursor getLevelIterator(){
		return  new FileInputCursor<MapEntry<Long,DoublePointRectangle>>(mapEntryConverter, file);
	}
		
}
