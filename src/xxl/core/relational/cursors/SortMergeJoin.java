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

package xxl.core.relational.cursors;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.Comparator;

import xxl.core.collections.bags.Bag;
import xxl.core.collections.sweepAreas.BagSAImplementor;
import xxl.core.collections.sweepAreas.ImplementorBasedSweepArea;
import xxl.core.collections.sweepAreas.SweepArea;
import xxl.core.cursors.MetaDataCursor;
import xxl.core.functions.Function;
import xxl.core.predicates.MetaDataPredicate;
import xxl.core.predicates.Not;
import xxl.core.predicates.Predicate;
import xxl.core.predicates.Predicates;
import xxl.core.relational.JoinUtils;
import xxl.core.relational.metaData.AppendedResultSetMetaData;
import xxl.core.relational.metaData.MergedResultSetMetaData;
import xxl.core.relational.metaData.ResultSetMetaDatas;
import xxl.core.relational.metaData.UnifiedResultSetMetaData;
import xxl.core.relational.tuples.Tuple;
import xxl.core.util.metaData.CompositeMetaData;

/**
 * This class realizes a join operator based on the sort-merge paradigm. The
 * two input relations have to be sorted. The algorithm of
 * {@link xxl.core.cursors.joins.SortMergeJoin} is used internally.
 * 
 * <p>There are easy-to-use constructors and others, that need more knowledge
 * about the class {@link SweepArea}. Note: tuples can only find join partners
 * in a sweep-area. So, the predicate and the comparator have to be chosen
 * carefully!</p>
 * 
 * <p>Compared to {@link NestedLoopsJoin} this class does not directly support
 * a Cartesian product.</p>
 */
public class SortMergeJoin extends xxl.core.cursors.joins.SortMergeJoin<Tuple, Tuple> implements MetaDataCursor<Tuple, CompositeMetaData<Object, Object>> {

	/**
	 * An enumeration of constants specifying the join types supported by this
	 * class.
	 */
	public static enum Type {
			
		/**
		 * A constant specifying a theta join. Only the tuples for which the
		 * specified predicate is <code>true</code> will be returned.
		 */
		THETA_JOIN,
		
		/**
		 * A constant specifying a left outer join. The tuples for which the
		 * specified predicate is <code>true</code> as well as all elements of
		 * the <code>cursor1</code> not qualifying concerning the predicate
		 * will be returned. The function <code>newResult</code> is called with
		 * arguments <code>cursor1.peek()</code> and <code>null</code>.
		 */
		LEFT_OUTER_JOIN,
		
		/**
		 * A constant specifying a right outer join. The tuples for which the
		 * specified predicate is <code>true</code> as well as all elements of
		 * the <code>cursor2</code> not qualifying concerning the predicate
		 * will be returned. The function <code>newResult</code> is called with
		 * arguments <code>null</code> and <code>cursor2.peek()</code>.
		 */
		RIGHT_OUTER_JOIN,
		
		/**
		 * A constant specifying a full outer join. The tuples for which the
		 * specified predicate is <code>true</code> as well as all tuples of
		 * the LEFT and RIGHT OUTER_JOIN will be returned.
		 */
		OUTER_JOIN,
		
		/**
		 * A constant specifying a natural join. The tuples are compared using
		 * their common columns.
		 */
		NATURAL_JOIN,
		
		/**
		 * A constant specifying a semi join. The tuples are compared using
		 * their common columns. The results become projected to the columns of
		 * the first input relation.
		 */
		SEMI_JOIN
	}

	// Default implementation for reorganization.
	// Will possibly not work correct for all SortMergeJoins!!!
	/**
	 * This class extends (@link SweepArea) and defines an inverse predicate.
	 * 
	 * @param <E> the type of the elements that can be stored by this
	 *        sweep-area.
	 */
	public static class PredicateBasedSA<E> extends ImplementorBasedSweepArea<E> {
		
		/**
		 * The inverse predicate.
		 */
		protected Predicate<E> inversePredicate;
		
		/**
		 * Construct an new predicate-based sweep-area. 
		 * 
		 * @param bag the given bag.
		 * @param predicate the given predicate.
		 * @param ID the id of this sweep-area.
		 */
		public PredicateBasedSA(Bag<E> bag, Predicate<? super E> predicate, int ID) {
			super(
				new BagSAImplementor<E>(bag),
				ID,
				false, 
				ID == 0 ? 
					(Predicate<? super E>[])new Predicate[] {null, predicate} : 
					(Predicate<? super E>[])new Predicate[] {Predicates.swapArguments(predicate), null},
				ID == 0 ? 
					(Predicate<? super E>[])new Predicate[] {null, new Not<E>(predicate)} : 
					(Predicate<? super E>[])new Predicate[] {Predicates.swapArguments(new Not<E>(predicate)), null}				
			);
		}
	}

	/**
	 * An internal static method computing the relational metadata for all
	 * types of joins except theta joins.
	 *
	 * @param sortedCursor1 the input metadata cursor delivering the elements
	 *        of the first relation.
	 * @param sortedCursor2 the input metadata cursor delivering the elements
	 *        of the second relation.
	 * @param type the type of the join operation. Possible values are the
	 *        elements of the enumeration defined above in this class.
	 * @return relational metadata that is appropriate for the type of join.
	 * @throws IllegalArgumentException if an unsupported join type is given.
	 */
	public static MergedResultSetMetaData computeResultSetMetaData(MetaDataCursor<? extends Tuple, CompositeMetaData<Object, Object>> sortedCursor1, MetaDataCursor<? extends Tuple, CompositeMetaData<Object, Object>> sortedCursor2, Type type) throws IllegalArgumentException {
		switch (type) {
			case THETA_JOIN :
				return new AppendedResultSetMetaData(ResultSetMetaDatas.getResultSetMetaData(sortedCursor1), ResultSetMetaDatas.getResultSetMetaData(sortedCursor2));
			case LEFT_OUTER_JOIN :
			case RIGHT_OUTER_JOIN :
			case OUTER_JOIN :
			case NATURAL_JOIN :
				return new UnifiedResultSetMetaData(ResultSetMetaDatas.getResultSetMetaData(sortedCursor1), ResultSetMetaDatas.getResultSetMetaData(sortedCursor2));
			case SEMI_JOIN :
				return new UnifiedResultSetMetaData(ResultSetMetaDatas.getResultSetMetaData(sortedCursor1));
			default :
				throw new IllegalArgumentException("Undefined type " + type + " specified in used method.");
		}
	}

	/**
	 * An internal static method computing the metadata predicate for all types
	 * of joins except theta joins.
	 *
	 * @param sortedCursor1 the input metadata cursor delivering the elements
	 *        of the first relation.
	 * @param sortedCursor2 the input metadata cursor delivering the elements
	 *        of the second relation.
	 * @param type the type of the join operation. Possible values are the
	 *        elements of the enumeration defined above in this class.
	 * @return a metadata predicate that is appropriate for the type of join.
	 * @throws IllegalArgumentException if an unsupported join type is given.
	 */
	public static MetaDataPredicate<Tuple, CompositeMetaData<Object, Object>> computeMetaDataPredicate(MetaDataCursor<? extends Tuple, CompositeMetaData<Object, Object>> sortedCursor1, MetaDataCursor<? extends Tuple, CompositeMetaData<Object, Object>> sortedCursor2, Type type) throws IllegalArgumentException {
		switch (type) {
			case THETA_JOIN :
				throw new IllegalArgumentException("for using a theta join, a join predicate must be specified");
			case LEFT_OUTER_JOIN :
			case RIGHT_OUTER_JOIN :
			case OUTER_JOIN :
			case NATURAL_JOIN :
				return JoinUtils.naturalJoinMetaDataPredicate(ResultSetMetaDatas.getResultSetMetaData(sortedCursor1), ResultSetMetaDatas.getResultSetMetaData(sortedCursor2));
			case SEMI_JOIN :
				return JoinUtils.semiJoinMetaDataPredicate(JoinUtils.naturalJoinMetaDataPredicate(ResultSetMetaDatas.getResultSetMetaData(sortedCursor1), ResultSetMetaDatas.getResultSetMetaData(sortedCursor2)), new ResultSetMetaData[] {ResultSetMetaDatas.getResultSetMetaData(sortedCursor1), ResultSetMetaDatas.getResultSetMetaData(sortedCursor2)}, 0);
			default :
				throw new IllegalArgumentException("Undefined type specified in used method.");
		}
	}

	/**
	 * An internal variable used for storing the metadata information of this
	 * join operator.
	 */
	protected CompositeMetaData<Object, Object> globalMetaData;

	/**
	 * Advanced constructor for a new sort-merge join.
	 *
	 * <p>Handle with care! Do not use this constructor if you do not know
	 * exactly what you are doing.</p>
	 *
	 * @param sortedCursor1 the input metadata cursor delivering the elements
	 *        of the first relation.
	 * @param sortedCursor2 the input metadata cursor delivering the elements
	 *        of the second relation.
     * @param metaData the relational metadata of the new sort-merge join.
	 * @param sweepArea1 the first sweep-area.
	 * @param sweepArea2 the second sweep-area.
	 * @param comparator a comparator that gets an object from every input
	 *        relation and determines, which tuple has to be inserted into the
	 *        sweep-area first.
	 * @param createTuple a function that maps a list of objects (column
	 *        values) to a new result tuple.
	 *        {@link xxl.core.relational.tuples.ArrayTuple#FACTORY_METHOD} can be
	 *        used.
	 * @param type the type of the join operation. Possible values are the
	 *        elements of the enumeration defined above in this class.
	 */
	public SortMergeJoin(MetaDataCursor<? extends Tuple, CompositeMetaData<Object, Object>> sortedCursor1, MetaDataCursor<? extends Tuple, CompositeMetaData<Object, Object>> sortedCursor2, MergedResultSetMetaData metaData, SweepArea<Tuple,Tuple> sweepArea1, SweepArea<Tuple,Tuple> sweepArea2, Comparator<? super Tuple> comparator, Function<Object, ? extends Tuple> createTuple, Type type) {
		super(
			sortedCursor1,
			sortedCursor2,
			sweepArea1,
			sweepArea2,
			comparator,
			JoinUtils.genericJoinTupleFactory(createTuple, metaData)
		);
		this.type = type == Type.LEFT_OUTER_JOIN ?
			xxl.core.cursors.joins.SortMergeJoin.Type.LEFT_OUTER_JOIN :
			type == Type.RIGHT_OUTER_JOIN ?
				xxl.core.cursors.joins.SortMergeJoin.Type.RIGHT_OUTER_JOIN :
				type == Type.OUTER_JOIN ?
					xxl.core.cursors.joins.SortMergeJoin.Type.OUTER_JOIN :
					xxl.core.cursors.joins.SortMergeJoin.Type.THETA_JOIN;
		
		globalMetaData = new CompositeMetaData<Object, Object>();
		globalMetaData.add(ResultSetMetaDatas.RESULTSET_METADATA_TYPE, metaData);
	}

	/**
	 * Constructs a new sort-merge join.
	 *
	 * <p>The metadata predicate is used to provide the right join metadata! It
	 * is NOT automatically used in the given sweep-areas. This has to be done
	 * explicitely by the developer who specifies the sweep-areas.</p>
	 * 
	 * @param sortedCursor1 the input metadata cursor delivering the elements
	 *        of the first relation.
	 * @param sortedCursor2 the input metadata cursor delivering the elements
	 *        of the second relation.
	 * @param theta a metadata predicate that determines if two tuples qualify
	 *        for the result. This predicate has to provide the relational
	 *        metadata for the new sort-merge join.
	 * @param sweepArea1 the first sweep-area.
	 * @param sweepArea2 the second sweep-area.
	 * @param comparator a comparator that gets an object from every input
	 *        relation and determines, which tuple has to be inserted into the
	 *        sweep-area first.
	 * @param createTuple a function that maps a list of objects (column
	 *        values) to a new result tuple.
	 *        {@link xxl.core.relational.tuples.ArrayTuple#FACTORY_METHOD} can be
	 *        used.
	 * @param type the type of the join operation. Possible values are the
	 *        elements of the enumeration defined above in this class.
	 */
	public SortMergeJoin(MetaDataCursor<? extends Tuple, CompositeMetaData<Object, Object>> sortedCursor1, MetaDataCursor<? extends Tuple, CompositeMetaData<Object, Object>> sortedCursor2, MetaDataPredicate<? super Tuple, CompositeMetaData<Object, Object>> theta, SweepArea<Tuple,Tuple> sweepArea1, SweepArea<Tuple,Tuple> sweepArea2, Comparator<? super Tuple> comparator, Function<Object, ? extends Tuple> createTuple, Type type) {
		this(
			sortedCursor1,
			sortedCursor2,
			(MergedResultSetMetaData)ResultSetMetaDatas.getResultSetMetaData(theta),
			sweepArea1,
			sweepArea2,
			comparator,
			createTuple,
			type
		);
	}
	
	/**
	 * Constructs a new sort-merge join.
	 *
	 * @param sortedCursor1 the input metadata cursor delivering the elements
	 *        of the first relation.
	 * @param sortedCursor2 the input metadata cursor delivering the elements
	 *        of the second relation.
	 * @param sweepArea1 the first sweep-area.
	 * @param sweepArea2 the second sweep-area.
	 * @param comparator a comparator that gets an object from every input
	 *        relation and determines, which tuple has to be inserted into the
	 *        sweep-area first.
	 * @param createTuple a function that maps a list of objects (column
	 *        values) to a new result tuple.
	 *        {@link xxl.core.relational.tuples.ArrayTuple#FACTORY_METHOD} can be
	 *        used.
	 * @param type the type of the join operation. Possible values are the
	 *        elements of the enumeration defined above in this class.
	 */
	public SortMergeJoin(MetaDataCursor<? extends Tuple, CompositeMetaData<Object, Object>> sortedCursor1, MetaDataCursor<? extends Tuple, CompositeMetaData<Object, Object>> sortedCursor2, SweepArea<Tuple,Tuple> sweepArea1, SweepArea<Tuple,Tuple> sweepArea2, Comparator<? super Tuple> comparator, Function<Object, ? extends Tuple> createTuple, Type type) {
		this(
			sortedCursor1,
			sortedCursor2,
			computeResultSetMetaData(sortedCursor1, sortedCursor2, type),
			sweepArea1,
			sweepArea2,
			comparator,
			createTuple,
			type
		);
	}

	/**
	 * Constructs a new sort-merge join.
	 *
	 * <p>The metadata predicate is used to provide the right join metadata AND
	 * is automatically used in the required sweep-areas.</p>
	 * 
	 * @param sortedCursor1 the input metadata cursor delivering the elements
	 *        of the first relation.
	 * @param sortedCursor2 the input metadata cursor delivering the elements
	 *        of the second relation.
	 * @param theta a metadata predicate that determines if two tuples qualify
	 *        for the result. This predicate has to provide the relational
	 *        metadata for the new sort-merge join.
	 * @param newBag a factory method returning a bag that is used inside a
	 *        sweep-area. Both sweep-areas are instances of the inner class
	 *        {@link PredicateBasedSA} defined above.
	 * @param comparator a comparator that gets an object from every input
	 *        relation and determines, which tuple has to be inserted into the
	 *        sweep-area first.
	 * @param createTuple a function that maps a list of objects (column
	 *        values) to a new result tuple.
	 *        {@link xxl.core.relational.tuples.ArrayTuple#FACTORY_METHOD} can be
	 *        used.
	 * @param type the type of the join operation. Possible values are the
	 *        elements of the enumeration defined above in this class.
	 */
	public SortMergeJoin(MetaDataCursor<? extends Tuple, CompositeMetaData<Object, Object>> sortedCursor1, MetaDataCursor<? extends Tuple, CompositeMetaData<Object, Object>> sortedCursor2, MetaDataPredicate<? super Tuple, CompositeMetaData<Object, Object>> theta, Function<?, ? extends Bag<Tuple>> newBag, Comparator<? super Tuple> comparator, Function<Object, ? extends Tuple> createTuple, Type type) {
		this(
			sortedCursor1,
			sortedCursor2, 
			theta,
			new PredicateBasedSA<Tuple>(newBag.invoke(), theta, 0),
			new PredicateBasedSA<Tuple>(newBag.invoke(), theta, 1),
			comparator,
			createTuple,
			type
		);
	}
	
	/**
	 * Constructs a new sort-merge join.
	 *
	 * <p>The predicate is automatically used in the required sweep-areas. The
	 * relational metadata is calculated from the relational metadata of the
	 * input cursors involving the join operator's type.</p>
	 * 
	 * @param sortedCursor1 the input metadata cursor delivering the elements
	 *        of the first relation.
	 * @param sortedCursor2 the input metadata cursor delivering the elements
	 *        of the second relation.
	 * @param theta a predicate that determines if two tuples qualify for the
	 *        result.
	 * @param newBag a factory method returning a bag that is used inside a
	 *        sweep-area. Both sweep-areas are instances of the inner class
	 *        {@link PredicateBasedSA} defined above.
	 * @param comparator a comparator that gets an object from every input
	 *        relation and determines, which tuple has to be inserted into the
	 *        sweep-area first.
	 * @param createTuple a function that maps a list of objects (column
	 *        values) to a new result tuple.
	 *        {@link xxl.core.relational.tuples.ArrayTuple#FACTORY_METHOD} can be
	 *        used.
	 * @param type the type of the join operation. Possible values are the
	 *        elements of the enumeration defined above in this class.
	 */
	public SortMergeJoin(MetaDataCursor<? extends Tuple, CompositeMetaData<Object, Object>> sortedCursor1, MetaDataCursor<? extends Tuple, CompositeMetaData<Object, Object>> sortedCursor2, Predicate<? super Tuple> theta, Function<?, ? extends Bag<Tuple>> newBag, Comparator<? super Tuple> comparator, Function<Object, ? extends Tuple> createTuple, Type type) {
		this(
			sortedCursor1,
			sortedCursor2,
			JoinUtils.thetaJoinMetaDataPredicate(theta, ResultSetMetaDatas.getResultSetMetaData(sortedCursor1), ResultSetMetaDatas.getResultSetMetaData(sortedCursor2)),
			newBag,
			comparator,
			createTuple,
			type
		);
	}

	/**
	 * Constructs a new sort-merge join.
	 *
	 * @param sortedCursor1 the input metadata cursor delivering the elements
	 *        of the first relation.
	 * @param sortedCursor2 the input metadata cursor delivering the elements
	 *        of the second relation.
	 * @param newBag a factory method returning a bag that is used inside a
	 *        sweep-area. Both sweep-areas are instances of the inner class
	 *        {@link PredicateBasedSA} defined above.
	 * @param comparator a comparator that gets an object from every input
	 *        relation and determines, which tuple has to be inserted into the
	 *        sweep-area first.
	 * @param createTuple a function that maps a list of objects (column
	 *        values) to a new result tuple.
	 *        {@link xxl.core.relational.tuples.ArrayTuple#FACTORY_METHOD} can be
	 *        used.
	 * @param type the type of the join operation. Possible values are the
	 *        elements of the enumeration defined above in this class.
	 */
	public SortMergeJoin(MetaDataCursor<? extends Tuple, CompositeMetaData<Object, Object>> sortedCursor1, MetaDataCursor<? extends Tuple, CompositeMetaData<Object, Object>> sortedCursor2, Function<?, ? extends Bag<Tuple>> newBag, Comparator<? super Tuple> comparator, Function<Object, ? extends Tuple> createTuple, Type type) {
		this(
			sortedCursor1,
			sortedCursor2,
			computeMetaDataPredicate(sortedCursor1, sortedCursor2, type),
			newBag,
			comparator,
			createTuple,
			type
		);
	}

	/////////////////////////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////////////////////////
	///////////////// ResultSet Constructors following !!! //////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * Constructs a new sort-merge join.
	 *
	 * <p>The metadata predicate is used to provide the right join metadata! It
	 * is NOT automatically used in the given sweep-areas. This has to be done
	 * explicitely by the developer who specifies the sweep-areas.</p>
	 * 
	 * @param sortedResultSet1 the input result set delivering the elements of
	 *        the first relation.
	 * @param sortedResultSet2 the input result set delivering the elements of
	 *        the second relation.
	 * @param theta a metadata predicate that determines if two tuples qualify
	 *        for the result. This predicate has to provide the relational
	 *        metadata for the new sort-merge join.
	 * @param sweepArea1 the first sweep-area.
	 * @param sweepArea2 the second sweep-area.
	 * @param comparator a comparator that gets an object from every input
	 *        relation and determines, which tuple has to be inserted into the
	 *        sweep-area first.
	 * @param createTuple a function that maps a list of objects (column
	 *        values) to a new result tuple.
	 *        {@link xxl.core.relational.tuples.ArrayTuple#FACTORY_METHOD} can be
	 *        used.
	 * @param type the type of the join operation. Possible values are the
	 *        elements of the enumeration defined above in this class.
	 */
	public SortMergeJoin(ResultSet sortedResultSet1, ResultSet sortedResultSet2, MetaDataPredicate<? super Tuple, CompositeMetaData<Object, Object>> theta, SweepArea<Tuple,Tuple> sweepArea1, SweepArea<Tuple,Tuple> sweepArea2, Comparator<? super Tuple> comparator, Function<Object, ? extends Tuple> createTuple, Type type) {
		this(
			new ResultSetMetaDataCursor(sortedResultSet1),
			new ResultSetMetaDataCursor(sortedResultSet2),
			theta,
			sweepArea1,
			sweepArea2,
			comparator,
			createTuple,
			type
		);
	}
		
	/**
	 * Constructs a new sort-merge join.
	 *
	 * @param sortedResultSet1 the input result set delivering the elements of
	 *        the first relation.
	 * @param sortedResultSet2 the input result set delivering the elements of
	 *        the second relation.
	 * @param sweepArea1 the first sweep-area.
	 * @param sweepArea2 the second sweep-area.
	 * @param comparator a comparator that gets an object from every input
	 *        relation and determines, which tuple has to be inserted into the
	 *        sweep-area first.
	 * @param createTuple a function that maps a list of objects (column
	 *        values) to a new result tuple.
	 *        {@link xxl.core.relational.tuples.ArrayTuple#FACTORY_METHOD} can be
	 *        used.
	 * @param type the type of the join operation. Possible values are the
	 *        elements of the enumeration defined above in this class.
	 */
	public SortMergeJoin(ResultSet sortedResultSet1, ResultSet sortedResultSet2, SweepArea<Tuple,Tuple> sweepArea1, SweepArea<Tuple,Tuple> sweepArea2, Comparator<? super Tuple> comparator, Function<Object, ? extends Tuple> createTuple, Type type) {
		this(
			new ResultSetMetaDataCursor(sortedResultSet1),
			new ResultSetMetaDataCursor(sortedResultSet2),
			sweepArea1,
			sweepArea2,
			comparator,
			createTuple,
			type
		);
	}
		
	/**
	 * Constructs a new sort-merge join.
	 *
	 * <p>The metadata predicate is used to provide the right join metadata AND
	 * is automatically used in the required sweep-areas.</p>
	 * 
	 * @param sortedResultSet1 the input result set delivering the elements of
	 *        the first relation.
	 * @param sortedResultSet2 the input result set delivering the elements of
	 *        the second relation.
	 * @param theta a metadata predicate that determines if two tuples qualify
	 *        for the result. This predicate has to provide the relational
	 *        metadata for the new sort-merge join.
	 * @param newBag a factory method returning a bag that is used inside a
	 *        sweep-area. Both sweep-areas are instances of the inner class
	 *        {@link PredicateBasedSA} defined above.
	 * @param comparator a comparator that gets an object from every input
	 *        relation and determines, which tuple has to be inserted into the
	 *        sweep-area first.
	 * @param createTuple a function that maps a list of objects (column
	 *        values) to a new result tuple.
	 *        {@link xxl.core.relational.tuples.ArrayTuple#FACTORY_METHOD} can be
	 *        used.
	 * @param type the type of the join operation. Possible values are the
	 *        elements of the enumeration defined above in this class.
	 */
	public SortMergeJoin(ResultSet sortedResultSet1, ResultSet sortedResultSet2, MetaDataPredicate<? super Tuple, CompositeMetaData<Object, Object>> theta, Function<?, ? extends Bag<Tuple>> newBag, Comparator<? super Tuple> comparator, Function<Object, ? extends Tuple> createTuple, Type type) {
		this (
			new ResultSetMetaDataCursor(sortedResultSet1),
			new ResultSetMetaDataCursor(sortedResultSet2),
			theta,
			newBag,
			comparator,
			createTuple,
			type
		);
	}

	/**
	 * Constructs a new sort-merge join.
	 *
	 * <p>The predicate is automatically used in the required sweep-areas. The
	 * relational metadata is calculated from the relational metadata of the
	 * input result sets involving the join operator's type.</p>
	 * 
	 * @param sortedResultSet1 the input result set delivering the elements of
	 *        the first relation.
	 * @param sortedResultSet2 the input result set delivering the elements of
	 *        the second relation.
	 * @param theta a predicate that determines if two tuples qualify for the
	 *        result.
	 * @param newBag a factory method returning a bag that is used inside a
	 *        sweep-area. Both sweep-areas are instances of the inner class
	 *        {@link PredicateBasedSA} defined above.
	 * @param comparator a comparator that gets an object from every input
	 *        relation and determines, which tuple has to be inserted into the
	 *        sweep-area first.
	 * @param createTuple a function that maps a list of objects (column
	 *        values) to a new result tuple.
	 *        {@link xxl.core.relational.tuples.ArrayTuple#FACTORY_METHOD} can be
	 *        used.
	 * @param type the type of the join operation. Possible values are the
	 *        elements of the enumeration defined above in this class.
	 */
	public SortMergeJoin(ResultSet sortedResultSet1, ResultSet sortedResultSet2, Predicate<? super Tuple> theta, Function<?, ? extends Bag<Tuple>> newBag, Comparator<? super Tuple> comparator, Function<Object, ? extends Tuple> createTuple, Type type) {
		this(
			new ResultSetMetaDataCursor(sortedResultSet1),
			new ResultSetMetaDataCursor(sortedResultSet2),
			theta,
			newBag,
			comparator,
			createTuple,
			type
		);
	}

	/**
	 * Constructs a new sort-merge join.
	 *
	 * @param sortedResultSet1 the input result set delivering the elements of
	 *        the first relation.
	 * @param sortedResultSet2 the input result set delivering the elements of
	 *        the second relation.
	 * @param newBag a factory method returning a bag that is used inside a
	 *        sweep-area. Both sweep-areas are instances of the inner class
	 *        {@link PredicateBasedSA} defined above.
	 * @param comparator a comparator that gets an object from every input
	 *        relation and determines, which tuple has to be inserted into the
	 *        sweep-area first.
	 * @param createTuple a function that maps a list of objects (column
	 *        values) to a new result tuple.
	 *        {@link xxl.core.relational.tuples.ArrayTuple#FACTORY_METHOD} can be
	 *        used.
	 * @param type the type of the join operation. Possible values are the
	 *        elements of the enumeration defined above in this class.
	 */
	public SortMergeJoin(ResultSet sortedResultSet1, ResultSet sortedResultSet2, Function<?, ? extends Bag<Tuple>> newBag, Comparator<? super Tuple> comparator, Function<Object, ? extends Tuple> createTuple, Type type) {
		this (
			new ResultSetMetaDataCursor(sortedResultSet1),
			new ResultSetMetaDataCursor(sortedResultSet2),
			newBag,
			comparator,
			createTuple,
			type
		);
	}

	/**
	 * Returns the metadata information for this metadata-cursor as a composite
	 * metadata ({@link CompositeMetaData}).
	 *
	 * @return the metadata information for this metadata-cursor as a composite
	 *         metadata ({@link CompositeMetaData}).
	 */
	public CompositeMetaData<Object, Object> getMetaData() {
		return globalMetaData;
	}
}
