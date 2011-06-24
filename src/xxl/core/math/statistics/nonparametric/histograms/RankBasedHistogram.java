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

package xxl.core.math.statistics.nonparametric.histograms;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import xxl.core.cursors.Cursors;
import xxl.core.cursors.groupers.AggregateGrouper;
import xxl.core.functions.Function;
import xxl.core.math.Statistics;
import xxl.core.math.queries.PointQuery;
import xxl.core.util.Distance;

/** This class provides an experimental histogram supporting point queries for categorical data over a nominal scale.
 * <br><p>
 * Since this class is experimental, we apologize for problems with this class.
 * We would please you to report bugs or problems to
 * <a href=mailto:request@xxl-library.de>request@xxl-library.de</a>!
 * </p>
 * <br>
 * Also categorical data over a ordinal scale could be used by loosing the possibility of supporting
 * range queries. Here should be mentioned that range query not means interval query. Interval queries are only supported
 * on data belonging to an interval scale or higher or on data having a total ordering and a defined distance between two
 * points.
 * thus, to support range queries on categorical data a total ordering imposed by a {@link java.util.Comparator comparator}
 * is needed.
 * <br>
 * Categorical data only supports aggregations using the <tt>cumulative frequency distribution (cfd)</tt>,
 * so a cfd is needed for building histogram buckets. The buckets only support aggregations returning
 * the average count (average frequency of cumulated data values) of each bucket.
 * <br>
 * Different strategies to build up a histogram could be used. Each strategy must implement
 * an inner interface {@link ParitioningStrategy}.
 * The following strategies are provided:
 * <br>
 * The {@link MaxBased maximum based strategy} builds up a histogram with <tt>n</tt> buckets by sorting
 * the cfd of the data by its rank (descending by frequency). Every data value of the <tt>n-1</tt> most frequent
 * data values will be assigned to a bucket by its own. The remaining data values will be pooled into the
 * remaing bucket.<br>
 * The {@link OptimizedMaxBased optimized maximum based strategy} builds up a histogram with <tt>n</tt> buckets 
 * by sorting
 * the cfd of the data by its rank (descending by frequency). Every data value of the <tt>n-1</tt> most frequent
 * data values will be assigned to a bucket by its own. But in contrast to the
 * <tt>maximum based strategy</tt> data values with equal frequencies will be combined in one bucket.
 * The remaining data values again will be pooled into the remaing bucket. <br>
 * The {@link MaxDiff maximum differnces strategy} builds up a histogram with <tt>n</tt> buckets by sorting
 * the cfd of the data by its rank (descending by frequency). The buckets will be build up by dividing the data
 * by the (n-1)-biggest differences between two successive objects according to their frequencies.<br>
 * <br>
 * One should remember that categorical data doesn't have a dimension nor a distance.
 * If a domain <tt>D</tt> with categorical data is mapped to the natural numbers <tt>N</tt> this
 * mapping function needs to be restricted to the image of f(D) in N and also needs to be bijective.
 * Moreover, using
 * a mapping function doesn't save memory to store a histogram because typically the mapping function is stored.
 * <br>
 * <b>This class is recommended for use only with categorical data over a nominal scale.</b>
 * Using non-categorical (numerical) data could lead to wrong estimations!
 *
 * @see xxl.core.cursors.groupers.AggregateGrouper
 * @see xxl.core.cursors.groupers.AggregateGrouper.CFDCursor
 * @see xxl.core.math.queries.PointQuery
 * @see xxl.core.math.queries.RangeQuery
 * @see xxl.core.math.queries.WindowQuery
 */

public class RankBasedHistogram implements PointQuery {

	/** Interface providing all functionality a histogram containing nominal data needs
	 */
	public static interface Bucket {

		/** Indicates whether this bucket is <tt>responsible</tt> for this object.
		 * 
		 * @param o Object to verify
		 * @return true if this bucket <tt>contains</tt> the given object
		 */
		public boolean contains(Object o);

		/** Returns the stored aggregation value for this object.
		 * 
		 * @param o object to validate
		 * @return stored aggregation value for the given object
		 */
		public double aggregate(Object o);
	}

	/** This class provides a histogram bucket for nominal (rank-based) data.
	 */
	public static class AverageFrequencyBucket implements Bucket {

		/** stores the objects <tt>contained</tt> in this bucket */
		protected Object[] content;

		/** stores the aggregation value */
		protected double averageCount;

		/** Constructs a new Object of this type.
		 * 
		 * @param content objects belonging to this bucket
		 * @param value aggregation value for this bucket
		 */
		public AverageFrequencyBucket(Object[] content, double value) {
			this.content = content;
			averageCount = value;
		}

		/** Constructs a new Object of this type.
		 * 
		 * @param content single object building this bucket
		 * @param value aggregation value for this bucket
		 */
		public AverageFrequencyBucket(Object content, double value) {
			this(new Object[] { content }, value);
		}

		/** Indicates whether the given object belongs to this bucket.
		 * 
		 * @param o object to test
		 * @return true if the given object belongs to this bucket,
		 * otherwise false
		 */
		public boolean contains(Object o) {
			boolean r = false;
			for (int i = 0; i < content.length; i++) {
				if (o.equals(content[i]))
					r = true;
			}
			return r;
		}

		/** Returns the aggregation value assigned to this bucket.
		 * 
		 * @param o data object to query in this bucket.
		 * For categorical data this parameter will be ignored
		 * @return the aggregation value assigned to this bucket
		 */
		public double aggregate(Object o) {
			return averageCount;
		}

		/** Returns a String representation of this bucket.
		 * 
		 * @return a String representation of this bucket
		 */
		public String toString() {
			String r = "";
			r += "agg=" + averageCount + "\tcontent(" + content.length + "): ";
			for (int i = 0; i < content.length; i++) {
				r += content[i] + " ";
			}
			r += "<";
			return r;
		}
	}

	/** Interface providing a strategy for partitioning nominal data into histogram buckets. */
	public static interface ParitioningStrategy {

		/** Returns the built histogram buckets provided by this strategy.
		 * 
		 * @param aggData data used for building up the buckets given
		 * given by <tt>Object[]</tt> containing the data objects and their
		 * frequencies
		 * @return an array of buckets
		 */
		public Bucket[] partitioning(Iterator aggData);
	}

	/** This class provides a partitioning strategy for rank-based data.
	 * The (n-1) objects that occur most often in the data will be packed
	 * in a single bucket, the rest builds the n-th (the last) bucket.
	  */
	public static class MaxBased implements ParitioningStrategy {

		/** number of buckets to build */
		protected int nob;

		/** Constructs a new Object of this type.
		 * 
		 * @param nob number of buckets to build
		 */
		public MaxBased(int nob) {
			this.nob = nob;
		}

		/** Returns the built up histogram buckets provided by this strategy.
		 * 
		 * @param aggData data used for building up the buckets given
		 * given by <tt>Object[]</tt> containing the data objects and their
		 * frequencies
		 * @return an array of buckets
		 */
		public Bucket[] partitioning(Iterator aggData) {
			Bucket[] r = new Bucket[nob];
			//
			List a = new ArrayList();
			Cursors.toList(aggData, a);
			Collections.sort(a, new Comparator() {
				public int compare(Object o1, Object o2) {
					return ((Comparable) ((Object[]) o2)[1]).compareTo(((Object[]) o1)[1]);
				}
			});
			Iterator sorted = a.iterator();
			int pos = 0;
			double rest = 0.0;
			List restObjects = new ArrayList();
			while (sorted.hasNext()) {
				Object[] temp = (Object[]) sorted.next();
				if (pos < nob - 1) {
					r[pos++] = new AverageFrequencyBucket(temp[0], ((Number) temp[1]).doubleValue());
				} else {
					rest += ((Number) temp[1]).doubleValue();
					restObjects.add(temp[0]);
				}
			}
			r[nob - 1] =
				new AverageFrequencyBucket(Cursors.toArray(restObjects.iterator()), rest / restObjects.size());
			return r;
		}

		/** Returns a String representation of this strategy resp.&nbsp;the
		 * name of the strategy.
		 * 
		 * @return a String representation of this strategy resp.&nbsp;the
		 * name of the strategy.
		 */
		public String toString() {
			return "max-based";
		}
	}

	/** This class provides a partitioning strategy for rank-based data.
	 * The (n-1) objects that occur most often in the data will be packed
	 * in a single bucket, the rest builds the n-th (the last) bucket.
	 */
	public static class OptimizedMaxBased implements ParitioningStrategy {

		/** number of buckets to build */
		protected int nob;

		/** Constructs a new Object of this type.
		 *
		 * @param nob number of buckets to build up
		 */
		public OptimizedMaxBased(int nob) {
			this.nob = nob;
		}

		/** Returns the built up histogram buckets provided by this strategy.
		 * 
		 * @param aggData data used for building up the buckets given
		 * given by <tt>Object[]</tt> containing the data objects and their
		 * frequencies
		 * @return an array of buckets
		 */
		public Bucket[] partitioning(Iterator aggData) {
			Bucket[] r = new Bucket[nob];
			List a = new ArrayList();
			Cursors.toList(aggData, a);
			Collections.sort(a, new Comparator() {
				public int compare(Object o1, Object o2) {
					return ((Comparable) ((Object[]) o2)[1]).compareTo(((Object[]) o1)[1]);
				}
			});
			int lastpos = 0;
			int currentnob = 0;
			//boolean permformRest = false;
			double bufferAgg = 0.0;
			//double rest = 0.0;
			List buffer = new ArrayList();
			for (int i = 0; i < a.size(); i++) {
				Object[] temp = (Object[]) a.get(i);
				if (currentnob < nob - 1) {
					if (!((Object[]) a.get(lastpos))[1].equals(temp[1])) {
						// new bucket with already processed objects
						r[currentnob++] =
							new AverageFrequencyBucket(
								Cursors.toArray(buffer.iterator()),
								bufferAgg / (buffer.size()));
						buffer.clear();
						bufferAgg = 0.0;
					}
					buffer.add(temp[0]);
					bufferAgg += ((Number) temp[1]).doubleValue();
				} else { // the rest
					bufferAgg += ((Number) temp[1]).doubleValue();
					buffer.add(temp[0]);
				}
				lastpos = i;
			}
			r[nob - 1] =
				new AverageFrequencyBucket(Cursors.toArray(buffer.iterator()), bufferAgg / buffer.size());
			return r;
		}

		/** Returns a String representation of this strategy resp.&nbsp;the
		 * name of the strategy.
		 * 
		 * @return a String representation of this strategy resp.&nbsp;the
		 * name of the strategy.
		 */
		public String toString() {
			return "optimized max-based";
		}
	}

	/** This class provides a partitioning strategy for rank-based data.
	 * The (n-1) objects with the biggest difference in their frequencies will be
	 * separators for each bucket.
	 */
	public static class MaxDiff implements ParitioningStrategy {

		/** number of buckets to partition the given data into */
		protected int nob;

		/** Constructs a new Object of this class.
		 * 
		 * @param nob number of buckets to build up
		 */
		public MaxDiff(int nob) {
			this.nob = nob;
		}

		/** Returns the built up histogram buckets provided by this strategy.
		 * 
		 * @param aggData data used for building up the buckets given
		 * given by <tt>Object[]</tt> containing the data objects and their
		 * frequencies
		 * @return an array of buckets
		 */
		public Bucket[] partitioning(Iterator aggData) {
			// Storing and sorting data
			ArrayList a = new ArrayList();
			Cursors.toList(aggData, a);
			Collections.sort(a, new Comparator() {
				public int compare(Object o1, Object o2) {
					return ((Comparable) ((Object[]) o2)[1]).compareTo(((Object[]) o1)[1]);
				}
			});
			// getting indices
			int[] indices = Statistics.maxDiff(a.iterator(), new Distance() {
				public double distance(Object o1, Object o2) {
					return Math.abs(
						((Number) ((Object[]) o1)[1]).doubleValue() - ((Number) ((Object[]) o2)[1]).doubleValue());
				}
			}, nob - 1, false);
			// sorting the indices
			Arrays.sort(indices);
			// init buckets + temp. vars
			Bucket[] r = new Bucket[nob];
			Iterator data = a.iterator();
			int pos = 0;
			int from = 0;
			int to = -1;
			int currentBucket = 0;
			// pos in int[] indices
			for (int metaIndex = 0; metaIndex < indices.length; metaIndex++) {
				from = to + 1;
				to = indices[metaIndex];
				double sum = 0;
				Object[] content = new Object[to - from + 1];
				int c = 0;
				while (data.hasNext() && pos <= to) {
					pos++;
					Object[] temp = (Object[]) data.next();
					// adding frequencies
					sum += ((Number) temp[1]).doubleValue();
					// getting content
					content[c++] = temp[0];
				}
				sum = sum / (to - from + 1);
				r[currentBucket++] = new AverageFrequencyBucket(content, sum);
			}
			// now the rest
			from = to + 1;
			double sum = 0;
			List content = new ArrayList();
			while (data.hasNext()) {
				pos++;
				to++;
				Object[] temp = (Object[]) data.next();
				// adding frequencies
				sum += ((Number) temp[1]).doubleValue();
				// getting content
				content.add(temp[0]);
			}
			sum = sum / (to - from + 1);
			r[currentBucket++] = new AverageFrequencyBucket(Cursors.toArray(content.iterator()), sum);
			//
			return r;
		}

		/** Returns a String representation of this strategy resp.&nbsp;the
		 * name of the strategy.
		 * 
		 * @return a String representation of this strategy resp.&nbsp;the
		 * name of the strategy.
		 */
		public String toString() {
			return "max-diff";
		}
	}

	/** This class provides a partitioning strategy for rank-based data.
	 * The cfd is used to build up the histogram. Each different data object will be
	 * represented by its own bucket.
	 */
	public static class Cfd implements ParitioningStrategy {

		/** Constructs a new Object of this class.
		 */
		public Cfd() {}

		/** Returns the built up histogram buckets provided by this strategy.
		 * 
		 * @param aggData data used for building up the buckets given
		 * given by <tt>Object[]</tt> containing the data objects and their
		 * frequencies
		 * @return an array of buckets
		 */
		public Bucket[] partitioning(Iterator aggData) {
			// Storing and sorting data
			ArrayList a = new ArrayList();
			Cursors.toList(aggData, a);
			Collections.sort(a, new Comparator() {
				public int compare(Object o1, Object o2) {
					return ((Comparable) ((Object[]) o2)[1]).compareTo(((Object[]) o1)[1]);
				}
			});
			// converting tuple to buckets
			Bucket[] r = new Bucket[a.size()];
			Iterator data = a.iterator();
			int pos = 0;
			while (data.hasNext()) {
				Object[] temp = (Object[]) data.next();
				r[pos++] = new AverageFrequencyBucket(new Object[] { temp[0] }, ((Number) temp[1]).doubleValue());
			}
			return r;
		}

		/** Returns a String representation of this strategy resp.&nbsp;the
		 * name of the strategy.
		 * 
		 * @return a String representation of this strategy resp.&nbsp;the
		 * name of the strategy.
		 */
		public String toString() {
			return "cfd";
		}
	}
	
	/* ------------------------------------------------------- */

	/** bucket or bins that build the histogram */
	protected Bucket[] buckets;

	/** stores the used partitioning strategy as a string for verbosing */
	protected String usedStrategy = "";

	/** Constructs a new histogram for categorical data on a nominal scale.
	 *
	 * @param aggregatedData data delivered by an AggregateGrouper as
	 * <tt>Object[]</tt> containing the Object itself and his frequency
	 *
	 * @param strategy used strategy for partitioning
	 */
	public RankBasedHistogram(AggregateGrouper aggregatedData, ParitioningStrategy strategy) {
		buckets = strategy.partitioning(aggregatedData);
		usedStrategy = strategy.toString();
	}

	/** Constructs a new histogram for categorical data on a nominal scale.
	 *
	 * @param iterator discrete data to build the histogram from.
	 * A cfd will build up upon the data by using the {@link xxl.core.cursors.groupers.AggregateGrouper}
	 *
	 * @param strategy used strategy for partitioning
	 */
	public RankBasedHistogram(Iterator iterator, ParitioningStrategy strategy) {
		this(new AggregateGrouper.CFDCursor(iterator), strategy);
	}

	/** Constructs a new histogram for categorical data on a nominal scale.
	 *
	 * @param iterator discrete data to build the histogram from.
	 * A cfd will biuld up upon the data by using the {@link xxl.core.cursors.groupers.AggregateGrouper}
	 * @param representatives Function used by the {@link xxl.core.cursors.groupers.AggregateGrouper}
	 * to build up a cfd.
	 * @param strategy used strategy for partitioning
	 */
	public RankBasedHistogram(Iterator iterator, Function representatives, ParitioningStrategy strategy) {
		this(new AggregateGrouper(iterator, representatives), strategy);
	}

	/** Evaluates the histogram at a given point.
	 * 
	 * @param query query object
	 * @return aggregated value representing the bucket the given data belongs to
	 * @throws IllegalArgumentException if the queried object doesn't belong to any existing bucket
	 */
	public double pointQuery(Object query) throws IllegalArgumentException {
		boolean found = false;
		double r = 0.0;
		
		int i = 0;
		while (!found && (i < buckets.length)) {
			if (buckets[i].contains(query)) {
				found = true;
				r = buckets[i].aggregate(query);
			}
			i++;
		}
		if (!found)
			throw new IllegalArgumentException("Queried object '" + query + "' not found!");
		return r;
	}

	/** Returns a String representation of this histogram.
	 * 
	 * @return a String representation of this histogram
	 */
	public String toString() {
		String r = "";
		r += "RankBasedHistogram ";
		r += "containing " + buckets.length + " buckets\n";
		r += "used strategy:" + usedStrategy;
		r += "\n";
		for (int i = 0; i < buckets.length; i++) {
			r += (i + ":" + buckets[i] + "\n");
		}
		r += "---\n";
		return r;
	}
}
