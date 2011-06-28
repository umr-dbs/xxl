/* XXL: The eXtensible and fleXible Library for data processing

Copyright (C) 2000-2011 Prof. Dr. Bernhard Seeger
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

package xxl.core.indexStructures;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.Stack;

import xxl.core.collections.MapEntry;
import xxl.core.collections.ReversedList;
import xxl.core.collections.containers.Container;
import xxl.core.comparators.ComparableComparator;
import xxl.core.comparators.FeatureComparator;
import xxl.core.cursors.AbstractCursor;
import xxl.core.cursors.Cursor;
import xxl.core.cursors.Cursors;
import xxl.core.cursors.filters.Filter;
import xxl.core.cursors.identities.TeeCursor;
import xxl.core.cursors.mappers.Mapper;
import xxl.core.cursors.sources.EmptyCursor;
import xxl.core.cursors.sources.Enumerator;
import xxl.core.cursors.sources.SingleObjectCursor;
import xxl.core.cursors.unions.Sequentializer;
import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;
import xxl.core.io.converters.ConvertableConverter;
import xxl.core.io.converters.Converter;
import xxl.core.predicates.AbstractPredicate;
import xxl.core.spatial.points.DoublePoint;
import xxl.core.spatial.points.Point;
import xxl.core.spatial.rectangles.DoublePointRectangle;
import xxl.core.spatial.rectangles.Rectangle;

/** 
 * Revised R*-Tree
 * 
 * For a detailed discussion see 
 * Norbert Beckmann,  Bernhard Seeger:
 * "A Rivised R*-Tree in Comparsion with Related Index Structures"
 * 
 * @see RTree
 */
public class RevisedRTree extends RTree{
	/**Defalut value of parameter s*/
	public static final double PARAM_S = 0.5d;
	/** Parameter s allows to control the variance of the Weighting (Gaussian) function
	 * (default value s = 0.5 ) 
	 */
	protected final double parameterS; // param s
	/** scale paramter for weighting function */
	protected final double yOne;
	/** scale paramter for weighting function */
	protected final double yTwo;
	/**Dimension of the data */
	protected int dimension;
	/** Size of the middle point of the node */
	protected int pointSize;
	/**
	 * computes maximal margin bound value after split 
	 */
	protected Function<Rectangle, Double> perimMax;
	/**
	 * computes the middle point of the rectangle
	 */
	protected Function<Rectangle, Point> computeMiddlePoint;
	/**
	 * Creates the new RevisedR*-tree
	 * with default parameter s = 0.5
	 * @param dimension
	 */
	public RevisedRTree(int dimension){
		this(dimension, RevisedRTree.PARAM_S);
	}
	
	/**
	 * Creates the new RevisedR*-tree
	 * @param dimension
	 * @param parameter S 
	 */
	public RevisedRTree(int dimension, double paramS){
		this.dimension = dimension;
		this.pointSize = dimension*8; // doublePoint
		this.parameterS = paramS;
		this.yOne = Math.pow(Math.E, -1d/(parameterS*parameterS));
		this.yTwo = 1d/(1d-yOne);
		computeMiddlePoint = new AbstractFunction<Rectangle, Point>(){
			public Point invoke(Rectangle rectangle){
				double[] pointArray = new double[rectangle.dimensions()];
				for (int i = 0; i < pointArray.length; i++){
					pointArray[i] = (rectangle.getCorner(true).getValue(i) + 
									rectangle.getCorner(false).getValue(i))/2d ;
				}
				return new DoublePoint(pointArray);
			} 
		};
		perimMax = new AbstractFunction<Rectangle, Double>(){
				public Double  invoke(Rectangle r){
					double min = r.margin();
					for (double d: r.deltas()){
						min = (d < min) ? d : min;
					}
					return (2.0*r.margin())- min;
				} 
		};
	}
	/**
	 * @since 1.1
	 * @see ORTree#initialize(Function, Container, int, int, int, double)
	 */
	public ORTree initialize (Function getDescriptor, Container container, int blockSize, int dataSize, int descriptorSize, double minMaxFactor) {
		return (ORTree)super.initialize(getDescriptor, blockSize-pointSize, container, dataSize, descriptorSize, minMaxFactor);	
	}
	/**
	 * @see ORTree#initialize(IndexEntry, Function, Container, int, int, int, double)
	 */
	public ORTree initialize (IndexEntry rootEntry, Function getDescriptor, Container container, int blockSize, int dataSize, int descriptorSize, double minMaxFactor) {
		return (ORTree)super.initialize(rootEntry, rootEntry==null? null: rootEntry.descriptor(), getDescriptor,  blockSize-pointSize, container, dataSize, descriptorSize, minMaxFactor);	
	}
	
	/* (non-Javadoc)
	 * @see xxl.core.indexStructures.Tree#createNode(int)
	 */
	public Tree.Node createNode (int level) {
		return new Node().initialize(level, new LinkedList());
	}
	
	
	/** Gets a suitable Converter to serialize the tree's nodes.
	 * 
	 * @param objectConverter a converter to convert the data objects stored in the tree
	 * @param dimensions the dimensions of the bounding rectangles 
	 * @return a NodeConverter
	 */
	public Converter nodeConverter (Converter objectConverter) {
		return nodeConverter (objectConverter, this.dimension) ;
	}
	
	/** Gets a suitable Converter to serialize the tree's nodes.
	 * 
	 * @param objectConverter a converter to convert the data objects stored in the tree
	 * @param dimensions the dimensions of the bounding rectangles 
	 * @return a NodeConverter
	 */
	public Converter nodeConverter (Converter objectConverter, final int dimensions) {
		return nodeConverter(objectConverter, indexEntryConverter(
			new ConvertableConverter(
				new AbstractFunction() {
					public Object invoke () {
						return new DoublePointRectangle(dimensions);
					}
				}
			)
		));
	}
	
	/** Gets a suitable Converter to serialize the tree's nodes.
	 * 
	 * @param objectConverter a converter to convert the data objects stored in the tree
	 * @param indexEntryConverter a converter to convert the index entries
	 * @return a NodeConverter
	 * @see Converter
	 * @see ORTree.NodeConverter
	 */
	public Converter nodeConverter (Converter objectConverter, Converter indexEntryConverter) {
		ORTree.NodeConverter converter = (ORTree.NodeConverter)super.nodeConverter(objectConverter, indexEntryConverter);
		return new NodeConverter(converter, dimension);
	}
	/** This method is an implemtation of an efficient querying algorithm. 
	 * The result is a lazy Cursor pointing to all objects whose descriptors 
	 * overlap with the given queryDescriptor.
	 *( This is  original algorithmus of  ORTree.query, it is  
	 * only differs in remove method. 
	 * updates Middle point to the center of recomputed MBB)
	 * @param queryDescriptor describes the query in terms of a descriptor
	 * @param targetLevel the tree-level to provide the answer-objects
	 * @return a lazy cursor pointing to all response objects
	*/
	public Cursor query (final Descriptor queryDescriptor, final int targetLevel){
		// 
		final Iterator [] iterators = new Iterator[height()+1]; 
		Arrays.fill(iterators, EmptyCursor.DEFAULT_INSTANCE);
		if (height()>0 && queryDescriptor.overlaps(rootDescriptor()))
			iterators[height()] = new SingleObjectCursor(rootEntry());
		return new AbstractCursor () {
			int queryAllLevel = 0;
			Object toRemove = null;
			Stack path = new Stack();
			public boolean hasNextObject() {
				for (int parentLevel = targetLevel;;)
					if (iterators[parentLevel].hasNext())
						if (parentLevel==targetLevel)
							return true;
						else {
							IndexEntry indexEntry = (IndexEntry)iterators[parentLevel].next();

							if (indexEntry.level()>=targetLevel) {
								Tree.Node node = indexEntry.get(true);
								Iterator queryIterator;

								if (parentLevel<=queryAllLevel || queryDescriptor.contains(indexEntry.descriptor())) {
									queryIterator = node.entries();
									if (parentLevel>queryAllLevel && !iterators[node.level].hasNext())
											queryAllLevel = node.level;
								}
								else
									queryIterator = node.query(queryDescriptor);
								iterators[parentLevel = node.level] =
									iterators[parentLevel].hasNext()?
										new Sequentializer(queryIterator, iterators[parentLevel]):
										queryIterator;
								path.push(new MapEntry(indexEntry, node));
							}
						}
					else
						if (parentLevel==height())
							return false;
						else {
							if (parentLevel==queryAllLevel)
								queryAllLevel = 0;
							if (level(path)==parentLevel)
								path.pop();
							iterators[parentLevel++] = EmptyCursor.DEFAULT_INSTANCE;
						}
			}

			public Object nextObject() {
				return toRemove = iterators[targetLevel].next();
			}

			public void update (Object object) throws UnsupportedOperationException, IllegalStateException, IllegalArgumentException {
				super.update(object);
				if (targetLevel > 0)
					throw new IllegalStateException();
				else
					if (targetLevel!=0 || !descriptor(object).equals(descriptor(toRemove)))
						throw new IllegalArgumentException();
					else {
						IndexEntry indexEntry = (IndexEntry)indexEntry(path);
						Node node = (Node)node(path);

						iterators[0].remove();
						node.grow(object, path);
						indexEntry.update(node, true);
					}
			}
			
			public boolean supportsUpdate() {
				return true;
			}

			public void remove () throws UnsupportedOperationException, IllegalStateException {
				super.remove();
				if (targetLevel<height()) {
					IndexEntry indexEntry = (IndexEntry)indexEntry(path);
					Node node = (Node)node(path);

					iterators[node.level].remove();
					for (;;) {
						if (indexEntry==rootEntry() && node.level>0 && node.number()==1) {
							rootEntry = (IndexEntry)node.entries().next();
							rootDescriptor = ((IndexEntry)rootEntry()).descriptor();
							indexEntry.remove();
							break;
						}
						if (node.number()==0) {
							up(path);
							indexEntry.remove();
							if (height()==1) {
								rootEntry = null;
								rootDescriptor = null;
								break;
							}
							else {
								indexEntry = (IndexEntry)indexEntry(path);
								node = (Node)node(path);
								iterators[node.level].remove();
							}
						}
						else if (indexEntry!=rootEntry() && node.underflows()) {
							Iterator entries = node.entries();

							indexEntry.descriptor = computeDescriptor(node.entries);
							up(path);
							indexEntry.remove();
							iterators[level(path)].remove();
							indexEntry = (IndexEntry)node(path).chooseSubtree(indexEntry.descriptor(), path);
							RevisedRTree.this.update(path);
							node = (Node)down(path, indexEntry);
							while (entries.hasNext())
								node.grow(entries.next(), path);
							if (node.overflows())
								node.redressOverflow(path);
							else {
								RevisedRTree.this.update(path);
								up(path);
							}
							indexEntry = (IndexEntry)indexEntry(path);
							node = (Node)node(path);
						}
						else {
							RevisedRTree.this.update(path);
							while (up(path)!=rootEntry()) {
								if (!indexEntry.descriptor().equals
										(indexEntry.descriptor = computeDescriptor(node.entries)))
								// set middle point to recomputed MBB
								node.setMiddlePoint(indexEntry.descriptor());
								RevisedRTree.this.update(path);
								indexEntry = (IndexEntry)indexEntry(path);
								node = (Node)node(path);
							}
							((IndexEntry)rootEntry).descriptor = rootDescriptor = computeDescriptor(node.entries);
							node.setMiddlePoint(indexEntry.descriptor()); // set middle point to recomputed MBB 
							break;
						}
					}
				}
				else {
					rootEntry = null;
					rootDescriptor = null;
				}
				if (targetLevel>0) {
					IndexEntry indexEntry = (IndexEntry)toRemove;

					indexEntry.removeAll();
				}
			}
			public boolean supportsRemove() {
				return true;
			}
		};
	}
	/** <tt>Node</tt> is the class used to represent leaf- and non-leaf nodes of <tt>RTree</tt>.
	 *	Nodes are stored in containers.
	 *	@see Tree.Node
	 *  @see ORTree.Node
	 *  @see RTree.Node
	 */
	public class Node extends RTree.Node {
		/**Middle Point that keeps track about size changes of the node */
		protected Point middlePoint;
		
		/** Initializes the node by inserting a new entry. 
		 *  Computes and sets the the middle point of the node 
		 * @param entry the entry wto inserted
		 * @return SplitInfo contains information about a possible split
		 */
		public Node initialize(Descriptor oBox) {
			this.setMiddlePoint(oBox);
			return this;
		}
		
		/** Initializes the node by inserting a new entry. 
		 * 
		 * @param entry the entry wto inserted
		 * @return SplitInfo contains information about a possible split
		 */
		public Tree.Node.SplitInfo initialize (Object entry) {
			Tree.Node.SplitInfo splitInfo = super.initialize(entry);
			initialize(descriptor(entry));
			return splitInfo;
		}
		
		/**
		 * returns middle point of the node
		 * @return middlePoint of the node
		 */
		protected Point middlePoint(){
			return this.middlePoint;
		}
		/**
		 * Updates der middle Point of the Node
		 * @param nodeRegion
		 */
		protected void setMiddlePoint(Descriptor nodeRegion){
			middlePoint =  computeMiddlePoint.invoke(rectangle(nodeRegion));
		}
		
		/**
		 * Computes overlap of rectangles (recT union entryRec) and recJ 
		 * @param recT
		 * @param recJ
		 * @param entryRec
		 * @return
		 */
		private double getOverlap(Rectangle recT, Rectangle recJ, Rectangle entryRec){
			Rectangle unionRecTEntryRec = Descriptors.union(recT,entryRec);
			double overlap = unionRecTEntryRec.overlap(recJ) - recT.overlap(recJ);
			return overlap; 
		}
		/* (non-Javadoc)
		 * @see xxl.core.indexStructures.ORTree.Node#chooseSubtree(xxl.core.indexStructures.Descriptor, java.util.Iterator)
		 */
		protected ORTree.IndexEntry chooseSubtree (Descriptor descriptor, Iterator minima){
			//CSOrig
			final Rectangle dataRectangle = (Rectangle)descriptor;
			TeeCursor validEntries = new TeeCursor(minima);
			IndexEntry qulifiedEntry = null; 
			
			minima = new Filter(validEntries,
				new AbstractPredicate() {
					public boolean invoke (Object object) {
						return rectangle(object).contains(dataRectangle);
					}
				}
			);
			if (minima.hasNext()){
				minima = Cursors.minima(minima,
						new AbstractFunction() {
							public Object invoke (Object object) {
								return new Double(rectangle(object).area());
							}
						}
					).iterator();
				qulifiedEntry = (IndexEntry)minima.next();
			}
			//Optimierungs stelle
			else {
				// sort Entries nach delta Perimeter 
				List<MapEntry<IndexEntry,Object>> entriesList = new ArrayList<MapEntry<IndexEntry,Object>>(); 
				Iterator it = validEntries.cursor(); 
				while(it.hasNext()){ // copy entries 
					entriesList.add(new MapEntry(it.next(), null));
				}
				Collections.sort(entriesList, new Comparator<MapEntry<IndexEntry,Object>>(){ // sort nach delta perimeter 
					public int compare(MapEntry<IndexEntry,Object> o1, MapEntry<IndexEntry,Object> o2) {
						Rectangle rec1 = rectangle(o1.getKey());
						Rectangle rec2 = rectangle(o2.getKey());
						Rectangle unionRec1Entry =  Descriptors.union(rec1, dataRectangle);
						Double deltaPerim1 =  unionRec1Entry.margin()- rec1.margin();
						Rectangle unionRec2Entry =  Descriptors.union(rec2, dataRectangle);
						Double deltaPerim2 =  unionRec2Entry.margin()-  rec2.margin();
						return deltaPerim1.compareTo(deltaPerim2);
					}			
				} );
				// An dieser Stelle berechnung von delta ovlp 1[1, k]
				double sum = 0d;
				Rectangle recT = rectangle(entriesList.get(0).getKey());
				for(int j = 1 ; j < entriesList.size(); j++ ){
					double overlap = getOverlap(recT, rectangle( entriesList.get(j).getKey()), dataRectangle);
					entriesList.get(j).setValue(new Double(overlap));
					sum += overlap;
				}
				if (sum == 0d){
					qulifiedEntry = entriesList.get(0).getKey();
				}
				else{//bestimme index P
					Set<MapEntry<IndexEntry,Object>>  cand = new HashSet<MapEntry<IndexEntry,Object>>();
					int p = 1; // index 
					double maxOverlap = 0d; 
					for (int i = 1; i < entriesList.size(); i++){
						double overlap =  ((Double)(entriesList.get(i).getValue())).doubleValue();
						if (overlap >= maxOverlap){
							maxOverlap = overlap;
							p = i;
						}
					}
					// checkIndex 
					checkIndex(entriesList, cand, 0, p , dataRectangle);
					
					if (cand.size() == 1){
						qulifiedEntry = cand.iterator().next().getKey();
					}
					// bestimme minimum
					if (qulifiedEntry==null){
						minima =  Cursors.minima(cand.iterator(),
								        new AbstractFunction() {
									    public Object invoke (Object object) {
									    	MapEntry mapEntry = (MapEntry)object;
									    	Double deltaOverlap = (Double)mapEntry.getValue();
										return deltaOverlap;
									}
								}
							).iterator();
						qulifiedEntry = ((MapEntry<IndexEntry,Object>)minima.next()).getKey();
					}
				}
			}
			validEntries.close();
			return qulifiedEntry;
		}
		/**
		 * Subroutine CheckIndex computes and tries to find in depth-first manner an overlap free 
		 * candidate  
		 * @param entries
		 * @param cand
		 * @param t
		 * @param p
		 * @return
		 */
		private boolean checkIndex(List<MapEntry<IndexEntry,Object>> entries,
				Set<MapEntry<IndexEntry,Object>> cand, int t, int p , Rectangle dataRectangle){
			double deltaOverlap = 0d;
			MapEntry<IndexEntry,Object> candEntry = entries.get(t);
			Rectangle rec = rectangle(candEntry.getKey());
			cand.add(candEntry);
			for (int i = 0; i <= p; i++){
				if (t!=i){
					double overlap = getOverlap(rectangle(rec),
						rectangle(entries.get(i).getKey()), dataRectangle);
					deltaOverlap += overlap;
					if (overlap != 0d && !cand.contains(entries.get(i))){		
						if (checkIndex(entries, cand, i, p , dataRectangle)){ 
							return true;
						}
					}
				}
			}
			if (deltaOverlap == 0d){
				cand.clear();
				cand.add(candEntry);
				return true;
			}
			if (cand.contains(candEntry) ){
				cand.remove(candEntry);
				candEntry.setValue(new Double(deltaOverlap));
				cand.add(candEntry);
			}
			return false;
		}
		
		/* (non-Javadoc)
		 * @see xxl.core.indexStructures.Tree.Node#split(java.util.Stack)
		 */
		protected Tree.Node.SplitInfo split (Stack path) {
			final Node node = (Node) node(path);
			final IndexEntry indexEntry = (IndexEntry)indexEntry(path);
//			 chooseSubTree aktulisiert den Descriptor von IndexEntry von Uebergelaufenen Knoten	
			// zur jeder Dim 2(M-2m+2) moegliche Distr
			Iterator<List<Distribution>>  itListDistr = getAllDistributions(node);
			Iterator<Distribution> iteratorWeightedDist = null;
			if (node.level()> 0){ //inner Nodes
				iteratorWeightedDist = computeCostsDistr(itListDistr, node, indexEntry);
			}
			else{ //Leaf Nodes, find first suitable dimension 
				@SuppressWarnings("serial")
				Function<Collection<RevisedRTree.Node.Distribution>, Double> getMarginOfDistr = 
					new AbstractFunction
					<Collection<RevisedRTree.Node.Distribution>, Double>(){
					public Double invoke (Collection<RevisedRTree.Node.Distribution> list) {
						double marginValue = 0.0;
						for (Iterator distributions = list.iterator(); distributions.hasNext();)
							marginValue += ((Distribution)distributions.next()).marginValue();
						return new Double(marginValue);
					}
					
					
				};  // minium of sum of margins
				List<Distribution> listDist = Cursors.minima(itListDistr, getMarginOfDistr).getFirst();
				Distribution firstDistribution = listDist.get(0);
				int dim = firstDistribution.getDim();
				// create Function for this dimension 
				Rectangle mBBN = rectangle(indexEntry);
				double pMax = perimMax.invoke(mBBN);
				DoublePoint middlePointN = (DoublePoint)computeMiddlePoint.invoke(mBBN);
				DoublePoint middlePointOld = (DoublePoint)node.middlePoint();
				double[] deltas = mBBN.deltas();
				final Function<Distribution, Double> computeCost = createWeightFunktion(node.splitMinNumber(), node.number(), pMax, middlePointN,
						middlePointOld, deltas, dim); // map this function 
				iteratorWeightedDist = new Mapper(new AbstractFunction<Distribution, Distribution>() {	
					public Distribution invoke(Distribution distr){
						distr.setWeight(computeCost.invoke(distr).doubleValue());
						return distr;
					} 
				}, listDist.iterator());
			}
			Distribution minCostDistr = (Distribution)Cursors.minima(iteratorWeightedDist,
					new AbstractFunction<Distribution, Double>(){
						public Double invoke(Distribution d){
							return d.weight();
					}
				}).getFirst();
			// Fill the pages with the entries according to the distribution
			node.entries.clear();
			node.entries.addAll(minCostDistr.entries(false));
			entries.addAll(minCostDistr.entries(true));
			// update the descriptor of the old index entry
			((IndexEntry)indexEntry(path)).descriptor = minCostDistr.descriptor(false);
			//setMiddlePoint
			node.setMiddlePoint(minCostDistr.descriptor(false));
			this.setMiddlePoint(minCostDistr.descriptor(true));
			return new SplitInfo(path).initialize(minCostDistr);
		}
		/**
		 * Maps weighting Function for each dimension List 
		 * @param listIterator
		 * @return Iterator which elements are weighted distributions
		 */
		protected Iterator<Distribution> computeCostsDistr(final Iterator<List<Distribution>> listIterator, final Node node, final IndexEntry indexEntry){
			Rectangle mBBN = rectangle(indexEntry);
			final int m = node.splitMinNumber();
			final int maxNumber = node.number();
			final double pMax = perimMax.invoke(mBBN);
			final DoublePoint middlePointN = (DoublePoint)computeMiddlePoint.invoke(mBBN);
			final DoublePoint middlePointOld = (DoublePoint)node.middlePoint();
			final double[] deltas = mBBN.deltas();
			return new  Mapper<Distribution, Distribution>( new AbstractFunction<Distribution, Distribution>(){
				public Distribution invoke(Distribution distribution){
					int dim = distribution.getDim();
					Function<Distribution, Double> weightFunction = createWeightFunktion(m, maxNumber, pMax, 
							middlePointN, middlePointOld, deltas,  dim);
					distribution.setWeight(weightFunction.invoke(distribution).doubleValue());
					return distribution;
				}
			} , new Sequentializer<Distribution>(new Mapper<List<Distribution>, Iterator<Distribution> >(new AbstractFunction<Collection<Distribution>, Iterator<Distribution> >(){ 
						public  Iterator<Distribution> invoke(Collection<Distribution> list){
								return list.iterator();
						} 
						}, listIterator )));
		}
		/**
		 * Creates weighting function for one dimension 
		 * 
		 * @param m  min number of entries in the node
		 * @param max number max Numebr of entries in the node
		 * @param pMax max Margin value for Split Distribution 
		 * @param middlePointN old middle point of nodes mbb  
		 * @param middlePointOld new middle point of nodes mbb
		 * @param deltas array of deltas 
		 * @param dim split axis 
		 * @return weighting function 
		 */
		protected Function<Distribution, Double> createWeightFunktion(final int m, final int maxNumber, final double pMax,  
				final DoublePoint middlePointN, final DoublePoint middlePointOld, final double[] deltas, final int dim){
			final double asym = (2.0*(middlePointN.getValue(dim)-middlePointOld.getValue(dim) )) 
			/ deltas[dim]; // functin asym  2*(x(MBB(N)) - x(OBox)) / delta dim
			final double mu = (1.0-(2.0*m/( maxNumber)))* asym;  // mu
			final double sigma = parameterS*(1+ Math.abs(mu));  // sigma = s*(1+ |mu|)
			return new AbstractFunction<Distribution, Double>(){
				public Double invoke (Distribution distr){
					double index = (double)distr.secondStart; // hole den index j von distribution
					double xi = ((2d*index )/(maxNumber))-1;  // berechnung von xi 
					double wf = yTwo*(Math.pow(Math.E,-(((xi - mu )*(xi - mu ))/sigma*sigma))-yOne); // wf(i)
					double wgi = (distr.overlapValue() == 0d) ? (distr.marginValue()- pMax)  
							: distr.overlapValue();  // wg(i)
					double wi = (distr.overlapValue() == 0d) ? wgi * wf : wgi/wf; // composed Function 
					return new Double(wi);
				}
			};
		}
		/**
		 * Original RTree Algorithmus, Computes all Distributions (2*dim(M-2m+2))
		 * @param computeDistrCost
		 * @param node
		 * @return
		 */
		protected Iterator<List<Distribution>> getAllDistributions(final Node node){
			final int minEntries = node.splitMinNumber();
			final int maxEntries = node.splitMaxNumber();
			int dimensions = ((Rectangle)rootDescriptor()).dimensions();
			return 	new Mapper(
					new AbstractFunction() {
						public Object invoke (Object object) {
							final int dim = ((Integer)object).intValue();
							// list of distributions for this dimension
							ArrayList distributionList = new ArrayList(2*(maxEntries-minEntries+1));
							Rectangle [][] rectangles = new Rectangle[2][];
							// Consider the entrys sorted by left or right borders
							for (int i=0; i<2; i++) {
								Object [] entryArray = node.entries.toArray();
								final boolean right = i==1;
								// Sort the entries by left or right border in the actual dimension
								Arrays.sort(entryArray, new FeatureComparator(new ComparableComparator(),
									new AbstractFunction() {
										public Object invoke (Object entry) {
											return new Double(rectangle(entry).getCorner(right).getValue(dim));
										}
									}
								));
								// Calculation of descriptors for all distributions (linear!)
								for (int k = 0; k<2; k++) {
									List entryList = Arrays.asList(entryArray);
									Iterator entries = (k==0? entryList: new ReversedList(entryList)).iterator();
									Rectangle rectangle = new DoublePointRectangle(rectangle(entries.next()));

									for (int l = (k==0? minEntries: node.number()-maxEntries); --l>0;)
										rectangle.union(rectangle(entries.next()));
									(rectangles[k] = new Rectangle[maxEntries-minEntries+1])[0] = rectangle;
									for (int j=1; j<=maxEntries-minEntries; rectangles[k][j++] = rectangle)
										rectangle = Descriptors.union(rectangle, rectangle(entries.next()));
								}
								// Creation of the distributions for this dimension
								for (int j = minEntries; j<=maxEntries; j++){
									 Distribution distribution = new Distribution(entryArray, j, 
											 rectangles[0][j-minEntries], rectangles[1][maxEntries-j], dim);
									distributionList.add(distribution);
									}
							}
							return distributionList;
						}
					}
				,new Enumerator(dimensions));
		} 
		/** <tt>Distribution</tt> is the class used to represent the distribution of
		 * entries of a node of the <tt>RTree</tt> into two partitions used for a split.
		 */
		protected class Distribution extends RTree.Node.Distribution {		
			/**weight of the distribution*/
			protected double weight; // cost
			
			protected Distribution(Object[] entries, int secondStart, Rectangle firstDescriptor, Rectangle secondDescriptor, int dim) {
				super(entries, secondStart, firstDescriptor, secondDescriptor, dim);
			}
			/**
			 * 
			 * @param cost
			 */
			protected void setWeight(double cost){
				weight = cost;
			}
			/**
			 * 
			 * @return
			 */
			protected double weight(){
				return this.weight;
			}
			
		}
		
	}
	/** The instances of this class are converters to write nodes to the 
	 * external storage (or any other {@link DataOutput}) or read 
	 * them from it (or any other {@link DataInput}).
	 *   
	 * @see Converter
	 */
	public class NodeConverter extends Converter {
		protected int dimension;	
		/**
		 * A converter for index entries.
		 */
		protected ORTree.NodeConverter orTreeNodeConverter; 
		
		/** Creates a new NodeConverter.
		 * 
		 * @param objectConverter a converter to convert the data objects stored in the tree
		 * @param indexEntryConverter a converter to convert the index entries
		 */
		public NodeConverter (ORTree.NodeConverter orTreeNodeConverter, int dimension) {
			this.orTreeNodeConverter = orTreeNodeConverter;
			this.dimension = dimension;
		}

		/** Reads a node from the data input. 
		 * 
		 * @param dataInput the data input stream
		 * @param object is ignored
		 * @return the read node
		 * 
		 * @see Converter#read(java.io.DataInput, java.lang.Object)
		 */
		public Object read (DataInput dataInput, Object object) throws IOException {
			Node node = (Node)orTreeNodeConverter.read(dataInput, object);
			node.middlePoint = new DoublePoint(dimension);
			node.middlePoint.read(dataInput);
			return node;
		}	
		/** Writes a node into the data output.
		 * @param dataOutput the data output stream
		 * @param object the node to write
		 * @see xxl.core.io.converters.Converter#write(java.io.DataOutput, java.lang.Object)
		 */
		public void write (DataOutput dataOutput, Object object) throws IOException {
			Node node = (Node)object;
			orTreeNodeConverter.write(dataOutput, object);
			if (node.middlePoint == null ){
				Rectangle nodeMBR = rectangle(computeDescriptor(node.entries));
				node.setMiddlePoint(nodeMBR);
			}
			node.middlePoint().write(dataOutput);
		}
	}
}
