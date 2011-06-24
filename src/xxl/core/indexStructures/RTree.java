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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.Stack;

import xxl.core.collections.ReversedList;
import xxl.core.collections.queues.Queue;
import xxl.core.collections.queues.Queues;
import xxl.core.comparators.ComparableComparator;
import xxl.core.comparators.FeatureComparator;
import xxl.core.cursors.Cursors;
import xxl.core.cursors.filters.Filter;
import xxl.core.cursors.identities.TeeCursor;
import xxl.core.cursors.mappers.Mapper;
import xxl.core.cursors.sources.Enumerator;
import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;
import xxl.core.io.converters.ConvertableConverter;
import xxl.core.io.converters.Converter;
import xxl.core.predicates.AbstractPredicate;
import xxl.core.predicates.Predicate;
import xxl.core.spatial.points.DoublePoint;
import xxl.core.spatial.rectangles.DoublePointRectangle;
import xxl.core.spatial.rectangles.Rectangle;
import xxl.core.spatial.rectangles.Rectangles;

/** An <tt>ORTree</tt> for objects with bounding rectangles as regions. 
 * This implementation of a member of the R-Tree family uses the 
 * split-strategy of the R*-Tree.  
 * 
 * For a detailed discussion see 
 * Norbert Beckmann, Hans-Peter Kriegel, Ralf Schneider, Bernhard Seeger:
 * "The R*-tree: An Efficient and Robust Access Method for Points and Rectangles",
 * ACM-SIGMOD (1990)322-331.
 * 
 * @see Tree
 * @see ORTree
 * @see LinearRTree
 * @see QuadraticRTree
 * @see GreenesRTree 
 */
public class RTree extends ORTree {

	/** Returns the bounding rectangle of <tt>entry</tt>.
	 * 
	 * @param entry an entry
	 * @return the bounding rectangle of <tt>entry</tt>
	 */
	public Rectangle rectangle (Object entry) {
		return (Rectangle)descriptor(entry);
	}

	/* (non-Javadoc)
	 * @see xxl.core.indexStructures.Tree#createNode(int)
	 */
	public Tree.Node createNode (int level) {
		return new Node().initialize(level, new LinkedList());
	}

	/** <tt>Node</tt> is the class used to represent leaf- and non-leaf nodes of <tt>RTree</tt>.
	 *	Nodes are stored in containers.
	 *	@see Tree.Node
	 *  @see ORTree.Node
	 */
	public class Node extends ORTree.Node {

		/** SplitInfo contains information about a split. The enclosing
		 * Object of this SplitInfo-Object (i.e. Node.this) is the new node
		 * that was created by the split.
		 */		
		public class SplitInfo extends ORTree.Node.SplitInfo {
						
			/** The distribution of rectangles for the split.
			 */
			protected Distribution distribution;

			/** Creates a new <tt>SplitInfo</tt> with a given path.
			 * @param path the path from the root to the splitted node
			 */
			public SplitInfo (Stack path) {
				super(path);
			}

			/** Initializes the SplitInfo by setting the distribution of 
			 * the split.
			 * 
			 * @param distribution the distribution for the split
			 * @return the initialized <tt>SplitInfo</tt>
			 */
			public ORTree.Node.SplitInfo initialize (Distribution distribution) {
				this.distribution = distribution;
				return initialize(distribution.descriptor(true));
			}

			/** Returns the distribution of the <tt>SplitInfo</tt>.
			 * 
			 * @return the distribution of the <tt>SplitInfo</tt>
			 */
			public Distribution distribution(){
				return distribution;
			}
		}

		/* (non-Javadoc)
		 * @see xxl.core.indexStructures.ORTree.Node#chooseSubtree(xxl.core.indexStructures.Descriptor, java.util.Iterator)
		 */
		protected ORTree.IndexEntry chooseSubtree (Descriptor descriptor, Iterator minima) {
			final Rectangle dataRectangle = (Rectangle)descriptor;
			TeeCursor validEntries = new TeeCursor(minima);

			minima = new Filter(validEntries,
				new AbstractPredicate() {
					public boolean invoke (Object object) {
						return rectangle(object).contains(dataRectangle);
					}
				}
			);
			if (!minima.hasNext()) {
				minima = Cursors.minima(validEntries.cursor(),
					new AbstractFunction() {
						public Object invoke (Object object) {
							Rectangle oldRectangle = rectangle(object);
							Rectangle newRectangle = Descriptors.union(oldRectangle, dataRectangle);
							return new Double(newRectangle.area()-oldRectangle.area());
						}
					}
				).iterator();
			}
			minima = Cursors.minima(minima,
				new AbstractFunction() {
					public Object invoke (Object object) {
						return new Double(rectangle(object).area());
					}
				}
			).iterator();
			validEntries.close();
			return (IndexEntry)minima.next();
		}

		/* (non-Javadoc)
		 * @see xxl.core.indexStructures.Tree.Node#split(java.util.Stack)
		 */
		protected Tree.Node.SplitInfo split (Stack path) {
			final Node node = (Node) node(path);
			Distribution distribution;
			final int minEntries = node.splitMinNumber();
			final int maxEntries = node.splitMaxNumber();
      int dimensions = ((Rectangle)rootDescriptor()).dimensions();

			// For each dimension generate a list of all possible distributions
			Iterator distributionLists = new Mapper(
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
							for (int j = minEntries; j<=maxEntries; j++)
								distributionList.add(new Distribution(entryArray, j, rectangles[0][j-minEntries], rectangles[1][maxEntries-j], dim));
						}
						return distributionList;
					}
				}
			,new Enumerator(dimensions));
			
			// Return the distributionList of the dimension for which the margin-sum of all
			// of its distributions is minimal (i.e. choose the dimension)
			List distributionList = (List)Cursors.minima(distributionLists,
				new AbstractFunction() {
					public Object invoke (Object object) {
						double marginValue = 0.0;

						for (Iterator distributions = ((List)object).iterator(); distributions.hasNext();)
							marginValue += ((Distribution)distributions.next()).marginValue();
						return new Double(marginValue);
					}
				}
			).getFirst();
			
			// Choose the distributions of the chosen dimension with minimal overlap 
			distributionList = Cursors.minima(distributionList.iterator(),
				new AbstractFunction() {
					public Object invoke (Object object) {
						return new Double(((Distribution)object).overlapValue());
					}
				}
			);
			
			// If still more than one distribution has to be considered, choose one
			// with minimal area 
			distributionList = Cursors.minima(distributionList.iterator(),
				new AbstractFunction() {
					public Object invoke (Object object) {
						return new Double(((Distribution)object).areaValue());
					}
				}
			);
			
			distribution = (Distribution)distributionList.get(0);
			// Fill the pages with the entries according to the distribution
			node.entries.clear();
			node.entries.addAll(distribution.entries(false));
			entries.addAll(distribution.entries(true));
			// update the descriptor of the old index entry
			((IndexEntry)indexEntry(path)).descriptor = distribution.descriptor(false);
			return new SplitInfo(path).initialize(distribution);
		}

		/** <tt>Distribution</tt> is the class used to represent the distribution of
		 * entries of a node of the <tt>RTree</tt> into two partitions used for a split.
		 */
		protected class Distribution {
			
			/** Entries stored in this distribution.
			 */
			protected Object [] entries;
			
			/** Start index of the second part of the distribution.
			 */
			protected int secondStart;
			
			/** Bounding Rectangle of the first part of the distribution.
			 */
			protected Rectangle firstDescriptor;
			
			/** Bounding Rectangle of the first part of the distribution.
			 */
			protected Rectangle secondDescriptor;
			
			/** Number of dimensions.
			 */
			protected int dim;

			/**
			 * @param entries an array containing all entries to be distributed
			 * @param secondStart the start index of the second partition
			 * @param firstDescriptor the descriptor for the first partition 
			 * @param secondDescriptor the descriptor for the second partition
			 * @param dim the number of dimensions
			 */
			protected Distribution (Object [] entries, int secondStart, Rectangle firstDescriptor, Rectangle secondDescriptor, int dim) {
				this.entries = entries;
				this.secondStart = secondStart;
				this.firstDescriptor = firstDescriptor;
				this.secondDescriptor = secondDescriptor;
				this.dim = dim;
			}

			/** Returns one of the partitions of this distribution.
			 * 
			 * @param second a <tt>boolean</tt> value determining if the second partition
			 * should be returned
			 * @return the entries of the first (if <tt>second == false</tt>) or of the
			 * second partition (if <tt>second == true</tt>)
			 */
			protected List entries (boolean second) {
				return Arrays.asList(entries).subList(second? secondStart: 0, second? entries.length: secondStart);
			}

			/** Returns a descriptor of one of the partitions of this distribution.
			 * 
			 * @param second a <tt>boolean</tt> value determining if the descriptor of 
			 * second partition should be returned
			 * @return the descriptor of the first (if <tt>second == false</tt>) or of the 
			 * second partition (if <tt>second == true</tt>)
			 */
			protected Descriptor descriptor (boolean second) {
				return second? secondDescriptor: firstDescriptor;
			}

			/** Returns the number of dimenssions.
			 * 
			 * @return the number of dimenssions
			 */
			protected int getDim(){
				return dim;
			}

			/** Returns the sum of the margins of the two partitions. 
			 * 
			 * @return the sum of the margins of the two partitions
			 */
			protected double marginValue () {
				return firstDescriptor.margin()+secondDescriptor.margin();
			}

			/** Returns the overlap of the two partitions.
			 * 
			 * @return the overlap of the two partitions
			 */
			protected double overlapValue () {
				return firstDescriptor.overlap(secondDescriptor);
			}

			/** Returns the sum of the areas of the two partitions. 
			 * 
			 * @return the sum of the areas of the two partitions
			 */
			protected double areaValue () {
				return firstDescriptor.area()+secondDescriptor.area();
			}
		}
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
	
	public class AsymmetricTwoDimensionalSkylineQuery extends Query {

		protected DoublePoint queryPoint;
		protected Set<DoublePoint>[][] skylines;
		protected DoublePointRectangle[][] quadrants;
		protected Predicate<DoublePoint>[][] dominates;
		protected Function<DoublePointRectangle, DoublePoint> determineCorner[][];
		
		public AsymmetricTwoDimensionalSkylineQuery(Queue queue, DoublePoint queryPoint) {			
			super(queue, 0);
			this.queryPoint = queryPoint;
			if (((Rectangle)rootDescriptor()).dimensions()!=2)
				throw new IllegalStateException();
			if (queryPoint.dimensions()!=2)
				throw new IllegalArgumentException();
			this.skylines = new Set[2][2];
			this.dominates = new Predicate[2][2];
			this.determineCorner = new Function[2][2];
			for (int x=0; x<skylines.length; x++)
				for (int y=0; y<skylines[x].length; y++) {					
					final boolean xup = x==1;
					final boolean yup = y==1;
					skylines[x][y] = new HashSet<DoublePoint>();
					dominates[x][y] = new AbstractPredicate<DoublePoint>() {
						public boolean invoke(DoublePoint a, DoublePoint b) {
							if ((b.getValue(0)>=a.getValue(0)) != xup)
								return false;
							if ((b.getValue(1)>=a.getValue(1)) != yup)
								return false;							
							return true;
						}
					};
				}
			determineCorner[0][0] = new AbstractFunction<DoublePointRectangle, DoublePoint>() {
				public DoublePoint invoke(DoublePointRectangle r) {
					return (DoublePoint)r.getCorner(true);
				}
			};
			determineCorner[0][1] = new AbstractFunction<DoublePointRectangle, DoublePoint>() {
				public DoublePoint invoke(DoublePointRectangle r) {
					return Rectangles.lowerRightCorner(r);
				}
			};
			determineCorner[1][0] = new AbstractFunction<DoublePointRectangle, DoublePoint>() {
				public DoublePoint invoke(DoublePointRectangle r) {
					return Rectangles.upperLeftCorner(r);
				}
			};
			determineCorner[1][1] = new AbstractFunction<DoublePointRectangle, DoublePoint>() {
				public DoublePoint invoke(DoublePointRectangle r) {
					return (DoublePoint)r.getCorner(false);
				}
			};
			this.quadrants = new DoublePointRectangle[2][2];
			DoublePointRectangle universe = (DoublePointRectangle)rootDescriptor();
			DoublePoint universeLowerLeft = (DoublePoint)universe.getCorner(false);
			DoublePoint universeUpperRight = (DoublePoint)universe.getCorner(true);
			quadrants[0][0] = new DoublePointRectangle(universeLowerLeft, queryPoint);
			quadrants[0][1] = new DoublePointRectangle(
					new double[] { universeLowerLeft.getValue(0), queryPoint.getValue(1) }, 
					new double[] { queryPoint.getValue(0), universeUpperRight.getValue(1) }
				);
			quadrants[1][0] = new DoublePointRectangle(
					new double[] { queryPoint.getValue(0), universeLowerLeft.getValue(1) }, 
					new double[] { universeUpperRight.getValue(0), queryPoint.getValue(1) }
				);			
			quadrants[1][1] = new DoublePointRectangle(queryPoint, universeUpperRight);
		}

		public boolean hasNextObject () {
			queue : while (!queue.isEmpty()) {
				Candidate candidate = (Candidate)queue.dequeue();
				// inner node
				if (candidate.parentLevel>targetLevel) {
					DoublePointRectangle rect = (DoublePointRectangle) candidate.descriptor();
					xloop : for (int x=0; x<skylines.length; x++)
						for (int y=0; y<skylines[x].length; y++) 
							if(quadrants[x][y].contains(rect)) {
								DoublePoint corner = determineCorner[x][y].invoke(rect);
								for (DoublePoint sp : skylines[x][y]) 
									if (dominates[x][y].invoke(sp,corner)) 
										continue queue;
								break xloop;
							}
					Node node = (RTree.Node)((IndexEntry)candidate.entry()).get(true);
					Queues.enqueueAll(queue, expand(candidate, node));
				}
				// leaf node
				else {
					DoublePoint po = (DoublePoint) candidate.entry;
					xloop: for (int x=0; x<skylines.length; x++)
						for (int y=0; y<skylines[x].length; y++)
							if (quadrants[x][y].contains(po)) {
								for (DoublePoint sp : skylines[x][y]) 
									if (dominates[x][y].invoke(sp,po)) 
										continue queue;										
								skylines[x][y].add(po);
								break xloop;
							}
					nextCandidate = candidate;					
					return true;
				}
			}
			return false;
		}


	}

	static Function<double[], double[]> absNorm(final DoublePoint queryPoint) {
		return new AbstractFunction<double[], double[]>() {
			double[] qp = (double[])queryPoint.getPoint();
			public double[] invoke(double[] point) {
				double [] res = new double[point.length];				
				for (int i=0; i<res.length; i++)
					res[i] = point[i]<qp[i] ? 2*qp[i]-point[i] : point[i];
				return res;
			}
		};
	}
	
	public class SymmetricSkylineQuery extends Query {
		
		protected DoublePoint queryPoint;
		protected Set<double[]> skyline;
		protected Predicate<double[]> dominates;
		protected Function<double[], double[]> norm;
		protected final int dim;
		
		public SymmetricSkylineQuery(Queue queue, DoublePoint queryPoint, Function<double[], double[]> norm) {			
			super(queue, 0);
			this.queryPoint = queryPoint;
			dim = ((Rectangle)rootDescriptor()).dimensions();
			if (queryPoint.dimensions()!=dim)
				throw new IllegalArgumentException();
			this.norm = norm;
			skyline = new HashSet<double[]>();
			dominates = new AbstractPredicate<double[]>() {
				public boolean invoke(double[] a, double[] b) {
					for (int i=0; i<dim; i++)
						if (b[i]<a[i])
							return false;
					return true;
				}
			};
		}
		
		public SymmetricSkylineQuery(Queue queue, DoublePoint queryPoint) {
			this(queue, queryPoint, absNorm(queryPoint));		
		}

		public boolean hasNextObject () {
			double [] qp = (double[])queryPoint.getPoint();
			queue : while (!queue.isEmpty()) {
				Candidate candidate = (Candidate)queue.dequeue();
				// inner node
				if (candidate.parentLevel>targetLevel) {
					DoublePointRectangle rect = (DoublePointRectangle) candidate.descriptor();
					double[] ll = (double[])((DoublePoint)rect.getCorner(false)).getPoint();
					double[] up = (double[])((DoublePoint)rect.getCorner(true)).getPoint();
					boolean inCorner = true;
					for (int i=0; inCorner && i<ll.length; i++)
						inCorner &= (ll[i]<qp[i] == up[i]<qp[i]);
					if(inCorner) {
						double[] nllp = norm.invoke((double[])((DoublePoint)rect.getCorner(false)).getPoint());
						double[] nurp = norm.invoke((double[])((DoublePoint)rect.getCorner(true)).getPoint());
						for (int i=0; i<nllp.length; i++)
							if (nurp[i]<nllp[i]) 
								nllp[i]=nurp[i];
						for (double[] sp : skyline) 
							if (dominates.invoke(sp,nllp)) 
								continue queue;
					}
					Node node = (RTree.Node)((IndexEntry)candidate.entry()).get(true);
					Queues.enqueueAll(queue, expand(candidate, node));
				}
				// leaf node
				else {
					double[] np = norm.invoke((double[])((DoublePoint)candidate.entry).getPoint());
					for (double[] sp : skyline) 
						if (dominates.invoke(sp,np)) 
							continue queue;										
					skyline.add(np);
					nextCandidate = candidate;					
					return true;
				}
			}
			return false;
		}


	}
		
	/*********************************************************************/
	/*                       DEBUG FUNCTIONALITY                         */
	/*********************************************************************/
	
	/* (non-Javadoc)
	 * @see xxl.core.indexStructures.ORTree#checkDescriptors(xxl.core.indexStructures.ORTree.IndexEntry)
	 */
	public boolean checkDescriptors (IndexEntry indexEntry) {
		boolean returnValue = true;
		Node node = (Node)indexEntry.get(true);
		Descriptor descriptor = computeDescriptor(node.entries);

		if (!descriptor.equals(indexEntry.descriptor))
			System.out.println("Level "+node.level+": expected: "+descriptor+" actually:"+indexEntry.descriptor);
		if (node.level>0)
			for (Iterator entries = node.entries(); entries.hasNext();)
				if (!checkDescriptors((IndexEntry)entries.next()))
					returnValue = false;

		return returnValue;
	}
}
