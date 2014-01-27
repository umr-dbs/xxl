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
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import xxl.core.collections.MapEntry;
import xxl.core.collections.containers.Container;
import xxl.core.collections.containers.io.ConverterContainerRawAccess;
import xxl.core.cursors.Cursor;
import xxl.core.cursors.Cursors;
import xxl.core.cursors.mappers.Mapper;
import xxl.core.cursors.sources.io.FileInputCursor;
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
 * 
 * This class implements level by level loading with partitioning methods proposed in: 
 * 
 * 
 * 
 * 
 * D Achakeev, B Seeger and P Widmayer:
 * "Sort-based query-adaptive loading of R-trees" in CIKM 2012
 * 
 * 
 *
 */
public class RtreeIterativeBulkloader<T> extends AbstractIterativeRtreeBulkloader<T>  {
	/**
	 * for debugging purpose
	 */
	public static boolean verbose = false;
	
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
	 * @param path to store temporal level data
	 * @param dimension
	 * @param blockSize
	 * @param ratio for minimal block occupation e.g. 0.33 
	 * @param storageUtil mainly used for OPT algorithm, provides average storage utilization
	 * @param partitionSize size of partition to be hold in memory for partitioning algorithms
	 * @param universe
	 */
	public RtreeIterativeBulkloader(RTree tree, 
			String path, 
			int dimension, 
			int blockSize, 
			double ratio,
			double storageUtil, 
			int partitionSize) {
		super(tree, dimension, blockSize, ratio, storageUtil, partitionSize);
		this.path = path;
	}
	
	/**
	 * 
	 * @throws IOException
	 */
	protected void reinitTempLevelStorage() throws IOException{
		file = File.createTempFile("levelRecs_", "dat");
		out = new DataOutputStream(new FileOutputStream(file));
	}
	/**
	 * 
	 * @param entry
	 * @throws IOException 
	 */
	protected void storeTempIndexEntry(MapEntry<Long,DoublePointRectangle> entry) throws IOException{
		mapEntryConverter.write(out, entry);
	}
	/**
	 * 
	 * @return
	 */
	protected Cursor getLevelIterator(){
		return  new FileInputCursor<MapEntry<Long,DoublePointRectangle>>(mapEntryConverter, file);
	}
	
	/******************************************************************************************************
	 * Static Classes
	 * 
	 * 
	 * 
	 ******************************************************************************************************/

	/**
	 * 
	 * @author achakeye
	 *
	 */
	public static class MainMemoryCollectionBulkLoader<T> extends RtreeIterativeBulkloader<T> {
		
		
		
		protected List<MapEntry<Long,DoublePointRectangle>> levelList;   	
		
		public MainMemoryCollectionBulkLoader(RTree tree, 
				String path, 
				int dimension,
				int blockSize, double ratio, double storageUtil,
				int partitionSize) {
			super(tree, path, dimension, blockSize, ratio, storageUtil, partitionSize);
		}
		
		
		@Override
		public AbstractIterativeRtreeBulkloader<T> init(
				CostFunctionArrayProcessor<? extends DoublePointRectangle> arrayProcessor,
				ProcessingType pType,  int dataSize,
				Converter<T> dataConverter,
				UnaryFunction<T, DoublePointRectangle> toRectangle) {
			 super.init(arrayProcessor, pType,  dataSize,
					dataConverter, toRectangle);
			 return this;
		}
		
		/*
		 * (non-Javadoc)
		 * @see core.bulkloader.RtreeIterativeBulkloader#reinitTempLevelStorage()
		 */
		protected void reinitTempLevelStorage() throws IOException{
			levelList = new LinkedList<MapEntry<Long,DoublePointRectangle>>();
		}
		/*
		 * (non-Javadoc)
		 * @see core.bulkloader.RtreeIterativeBulkloader#storeTempIndexEntry(xxl.core.collections.MapEntry)
		 */
		protected void storeTempIndexEntry(MapEntry<Long,DoublePointRectangle> entry) throws IOException{
			levelList.add(entry);
		}
		
		/*
		 * (non-Javadoc)
		 * @see core.bulkloader.RtreeIterativeBulkloader#getLevelIterator()
		 */
		protected Cursor getLevelIterator(){
			return Cursors.wrap(levelList.iterator());
		}
		
		@Override
		public int[] computeDistribution(Iterator<DoublePointRectangle> iterator,
			int level, int size) {
		return super.computeDistribution(iterator, level, size);
		}
		
	}
	
	
	
	/**
	 * 
	 * @author achakeye
	 *
	 */
	public static class RtreeIterativeBulkloaderAsynch<T> extends RtreeIterativeBulkloader<T>{
		
		private Future<?> future = null;
		
		private ExecutorService service = null;
		
		public RtreeIterativeBulkloaderAsynch(RTree tree, String path,
				int dimension, int blockSize, double ratio, double storageUtil,
				int partitionSize) {
			super(tree, path, dimension, blockSize, ratio, storageUtil, partitionSize);
			service =  Executors.newSingleThreadExecutor();
		}
		@SuppressWarnings("rawtypes")
		@Override
		public void buildRTree(Iterator rectangles) throws IOException {
			super.buildRTree(rectangles);
			service.shutdown();
		}

		/**
		 * 
		 * @param data
		 * @param level
		 * @param partitionSize
		 * @param out
		 * @param rtree
		 * @param mapEntryConverter
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
					if (verbose)
						System.out.println("L: " + level + " " + partition.size() );
				}else{   
					// just allocate one node
					MapEntry<Long, DoublePointRectangle> entry = writeNode(partition, level,  rtree,  treeContainer) ;
					storeTempIndexEntry(entry);
					//mapEntryConverter.write(out, entry );
					counter++;
				}
				partition = new LinkedList();
			}
			if (future!=null){
				try {
					future.get();
				} catch (InterruptedException e) {
					
					e.printStackTrace();
				} catch (ExecutionException e) {
					
					e.printStackTrace();
				}
			}
			return counter;
		}
		/**
		 * writes a partition according computed distribution
		 * @param distribution
		 * @param data
		 * @param level
		 * @param out
		 * @return
		 * @throws IOException
		 */
		public int writePartition(int[] distribution, 
				Iterator data, 
				int level, 
				int B,  RTree rtree, Container treeContainer) throws IOException{
			return  writeFlushModus( distribution,  data, level, B,  rtree, treeContainer);
		}
		/**
		 * 
		 * @param distribution
		 * @param dataIter
		 * @param level
		 * @param B
		 * @param rtree
		 * @param treeContainer
		 * @return
		 * @throws IOException
		 */
		private int writeFlushModus(final int[] distribution, 
				final Iterator dataIter, 
				final int level, 
				final int B, final  RTree rtree,final  Container treeContainer) throws IOException{
		
			
			Runnable writer = new Runnable() {
				
				 List<Object> rList = consume();
				
				
				private List<Object> consume(){
					List<Object> recs = new LinkedList<>();
					while(dataIter.hasNext()){
						recs.add(dataIter.next());
					}
					return recs;
				}
				
				
				@Override
				public void run() {
					Node[] nodes = new Node[distribution.length];
					DoublePointRectangle[] recs = new DoublePointRectangle[distribution.length];
					int j = 0; 
					Iterator data = rList.iterator();
					for(int i : distribution){
						List entries = new ArrayList(i);
						DoublePointRectangle descriptor = null;
						for(int k = 0;  data.hasNext() && k < i ; k++){
							DoublePointRectangle rec = null;
							Object o = data.next();
							if(level != 0 ){
								IndexEntry indexEntry  = (IndexEntry) o;
								entries.add(indexEntry);
							}else{
								Object obj =  o;
								entries.add(obj);
							}
							rec = (level == 0) ? (DoublePointRectangle)(toRectangle.invoke((T)o)): (DoublePointRectangle)((IndexEntry)o).descriptor();
							
							if (descriptor == null)
								descriptor = new DoublePointRectangle(rec);
							else 
								descriptor.union(rec);
						}
						if (i > B ){
							throw new RuntimeException("too many entries per block");
						}		
						final Node node = (Node) rtree.createNode(level);
						node.initialize(level, entries);
						nodes[j] = node;
						recs[j] = descriptor;
						j++;
					}
					
					ConverterContainerRawAccess rawContainer = (ConverterContainerRawAccess)treeContainer; 
					Long[] ids = (Long[]) rawContainer.flushArrayOfBlocks(nodes);
					
					for(int i = 0;  i <ids.length ; i++){
						MapEntry<Long,DoublePointRectangle> entry = new MapEntry<Long, DoublePointRectangle>(ids[i], recs[i]);
						try {
							storeTempIndexEntry(entry);
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
					
				}
			};
			future = service.submit(writer);
			return distribution.length;
		}
	}
	
}
