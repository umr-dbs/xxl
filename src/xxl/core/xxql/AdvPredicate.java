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

package xxl.core.xxql;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import xxl.core.collections.MapEntry;
import xxl.core.predicates.Predicate;
import xxl.core.relational.tuples.Tuple;
import xxl.core.xxql.AdvTupleCursor.CachingStrategy;
import xxl.core.xxql.columns.Column;

/**
 * A (AdvResultSet)MetaData-aware Predicate on Tuples to be used mainly in JOINS and WHERE-clauses 
 * of the {@link AdvTupleCursor}.<br>
 * 
 * @see Column 
 */
public abstract class AdvPredicate implements Predicate<Tuple>, CorrTuplesReceiver {

	// Columns "used" by this predicate
	protected List<Column> containedColumns = null;
	// AdvPredicates "used" by this predicate (like in AND)
	protected List<AdvPredicate> containedPredicates = null;
	// correlated tuples (i.e. if this is a subquery the current tuple of the 
	// enclosing WHERE-clause - in case of nested subqueries there are several enclosing
	// WHERE-clausis with current tuples, so it's a list
	protected List<MapEntry<AdvResultSetMetaData, Tuple>> corr_tuples=null;
	// the metadata of the cursor containing this predicate.
	protected AdvResultSetMetaData metadata;
	
	
	/**
	 * Add columns "contained" in this predicate so metadata and correlated tuples can be set for them.<br>
	 * Contained Columns are Columns needed to evaluate this Predicate<br>
	 * Example:<br>
	 * <code>col("al1.nameX").EQ(col("al2.nameY"))</code> <br>checks whether the columns "al1.nameX" and
	 * "al2.nameY", that should be present in your Cursor or the correlated Tuples, are equal.<br> 
	 * To map those names to the correct column in the Cursor/Tuple, MetaData and the correlated 
	 * Tuples are needed. They will be set by where() via setMetaData() and setCorrelatedTuples() 
	 * of this AdvPredicate and setMetaData() will pass on the metadata to the contained Columns.
	 * 
	 * @param cols Columns to be contained in this Column
	 * 
	 * @see Column#setCorrelatedTuples(List)
	 */
	// TODO: reicht package protected?
	public void addContainedColumns(Column... cols){
		if(containedColumns == null){
			containedColumns = new ArrayList<Column>(cols.length);
		}
		for(Column col : cols){
			containedColumns.add(col);
		}
	}
	
	/**
	 * Add contained Predicates (like in AND(), OR()...) so metadata and correlated tuples will be 
	 * passed on to them (and they can pass it on to "their" columns)
	 * 
	 * @param preds the AdvPredicates contained in this predicate
	 */
	private void addContainedPredicates(AdvPredicate ... preds){
		if(containedPredicates == null){
			containedPredicates = new ArrayList<AdvPredicate>(preds.length);
		}
		for(AdvPredicate pred : preds){
			containedPredicates.add(pred);
		}
	}
	
	/**
	 * Set the AdvResultSetMetaData of the cursor the contained columns (probably) belong to and 
	 * the new alias of the resulting cursor.<br>
	 * If this is a <b>join</b> Predicate you will have to set the AdvResultSetMetaData of both 
	 * joined Cursors - see {@link #setMetaDatas(AdvResultSetMetaData, AdvResultSetMetaData)}
	 * 
	 * @param metadata AdvResultSetMetaData of the cursor, you're calling 
	 * 					where/select/group_by on with this column
	 * @param newAlias the new alias of the resulting cursor after where/select/group_by 
	 * 					(needed especially if this Column contains a given value from 
	 * 					{@link Column#val(Object, String) val(obj, name)}
	 * 
	 * @see Column#setMetaData(AdvResultSetMetaData, String)
	 * @see #setMetaDatas(AdvResultSetMetaData, AdvResultSetMetaData)
	 */
	// TODO: wuerde package protected reichen?
	public void setMetaData(AdvResultSetMetaData metadata, String newAlias) {
		// the predicate itself may need metadata in case of EXISTS etc
		this.metadata = metadata;
		// and it has to pass the metadata on to the contained columns and predicates.
		if(containedColumns != null) {
			for(Column col : containedColumns)
				col.setMetaData(metadata, newAlias);
		}
		if(containedPredicates != null) {
			for(AdvPredicate pred : containedPredicates)
				pred.setMetaData(metadata, newAlias);
		}
	}
	
	/**
	 * When used as a JOIN-Predicate, set the {@link AdvResultSetMetaData}s of the the left and 
	 * right Cursors that are joined. It will be passed on to contained Columns, so they can map
	 * their given "alias.name" to the appropriate cursor and choose the correct tuple when
	 * invoked.
	 * @param leftMetaData AdvResultSetMetaData of the left cursor being joined
	 * @param rightMetaData AdvResultSetMetaData of the right cursor being joined
	 */
	public void setMetaDatas(AdvResultSetMetaData leftMetaData, AdvResultSetMetaData rightMetaData){
		this.metadata = null; // should really not be needed in a join-predicate (don't want subqueries there)
		if(containedColumns != null) {
			for(Column col : containedColumns)
				col.setMetaDatas(leftMetaData, rightMetaData);
		}
		if(containedPredicates != null) {
			for(AdvPredicate pred : containedPredicates)
				pred.setMetaDatas(leftMetaData, rightMetaData);
		}
	}
	
	protected AdvResultSetMetaData getMetaData(){
		return metadata;
	}
	
	/**
	 * Pass a list of tuples along with their metadata from "outer" cursors in case this is a 
	 * (correlated) subquery. The list has to be sorted from inside to outside (in case of nested 
	 * subqueries). It will be passed on to the contained Columns.<br>
	 * This is meant to be used with EXISTS, ALL and ANY clauses.
	 * 
	 * @param corrTuples a List of tuples with their related metadata from "outer" cursors 
	 * 			for correlated subqueries
	 * 
	 * @see AdvTupleCursor#getCorrTuples()
	 * @see Column#setCorrelatedTuples(List)
	 */
	public void setCorrelatedTuples(List<MapEntry<AdvResultSetMetaData, Tuple>> corrTuples){
		if (corrTuples == null)
			return;
		corr_tuples = corrTuples;
		if(containedColumns != null) {
			for(Column col : containedColumns)
				col.setCorrelatedTuples(corrTuples);
		}
		if(containedPredicates != null) {
			for(AdvPredicate pred : containedPredicates)
				pred.setCorrelatedTuples(corrTuples);
		}
	}
	
	protected List<MapEntry<AdvResultSetMetaData, Tuple>> getCorrelatedTuples(){
		return corr_tuples;
	}
	
	
	/**
	 * creates a logical AND connection between this Predicate and the other Predicate
	 * @param other the other Predicate
	 * @return true if <b>this</b> and <b>other</b> both evaluate to true, false otherwise
	 */
	public AdvPredicate AND(final AdvPredicate other){
		AdvPredicate ret = new AdvPredicate(){
			@Override
			public boolean invoke(Tuple tuple) {
				return AdvPredicate.this.invoke(tuple) && other.invoke(tuple); 
			}
			
			@Override
			public boolean invoke(Tuple lTuple, Tuple rTuple) {
				return AdvPredicate.this.invoke(lTuple, rTuple) && other.invoke(lTuple, rTuple); 
			}
		};
		// add *this* and the other predicate so metadata and correlated tuples will be passed on
		// to them from ret
		ret.addContainedPredicates(this, other);
		return ret;
	}
	
	/**
	 * creates a logical OR connection between this Predicate and the other Predicate
	 * @param other the other Predicate
	 * @return true if at least one of <b>this</b> or <b>other</b> evaluates to true, false otherwise
	 */
	public AdvPredicate OR(final AdvPredicate other){
		AdvPredicate ret = new AdvPredicate(){
			@Override
			public boolean invoke(Tuple tuple) {
				return AdvPredicate.this.invoke(tuple) || other.invoke(tuple); 
			}
			
			@Override
			public boolean invoke(Tuple lTuple, Tuple rTuple) {
				return AdvPredicate.this.invoke(lTuple, rTuple) || other.invoke(lTuple, rTuple); 
			}
		};
		// add *this* and the other predicate so metadata and correlated tuples will be passed on
		// to them from ret
		ret.addContainedPredicates(this, other);
		return ret;
	}
	
	/**
	 * creates a logical XOR connection between this Predicate and the other Predicate
	 * @param other the other Predicate
	 * @return true if <u>either</u> <b>this</b> <u>or</u> <b>other</b> (but not both of them) 
	 * 			evaluates to true, false otherwise.
	 */
	public AdvPredicate XOR(final AdvPredicate other){
		AdvPredicate ret = new AdvPredicate(){
			@Override
			public boolean invoke(Tuple tuple) {
				boolean first = AdvPredicate.this.invoke(tuple);
				boolean second = other.invoke(tuple); 
				return  (first || second) && !(first && second);
			}
			
			@Override
			public boolean invoke(Tuple lTuple, Tuple rTuple) {
				boolean first = AdvPredicate.this.invoke(lTuple, rTuple);
				boolean second = other.invoke(lTuple, rTuple); 
				return  (first || second) && !(first && second);
			}
			
		};
		// add *this* and the other predicate so metadata and correlated tuples will be passed on
		// to them from ret
		ret.addContainedPredicates(this, other);
		return ret;
	}
	
	/**
	 * Creates an AdvPredicate that negates the given AdvPredicate.<br>
	 * <b>Example:</b> <br>
	 * ...where( NOT(col("a1.blah").EQ(val("foo"))) )...
	 * @param pred the predicate to negate
	 * @return false if pred evaluates to true and vice versa
	 */
	public static AdvPredicate NOT(final AdvPredicate pred){
		AdvPredicate ret = new AdvPredicate(){
			@Override
			public boolean invoke(Tuple tuple) {
				return !pred.invoke(tuple);
			}
			
			@Override
			public boolean invoke(Tuple lTuple, Tuple rTuple) {
				return !pred.invoke(lTuple, rTuple);
			}
		};
		// add pred to contained predicates so metadata and correlated tuples will be passed on
		// to them from ret
		ret.addContainedPredicates(pred);
		return ret;
	}
	
	/**
	 * Creates an AdvPredicate that is true if the value is <i>null</i><br>
	 * 
	 * @param col the Column to check for being null
	 * @return true if col is evaluated to null, else false
	 */
	public static AdvPredicate ISNULL(final Column col){
		AdvPredicate ret = new AdvPredicate(){
			@Override
			public boolean invoke(Tuple tuple) {
				return col.invoke(tuple) == null;
			}
			
			@Override
			public boolean invoke(Tuple lTuple, Tuple rTuple) {
				return col.invoke(lTuple, rTuple) == null;
			}
		};
		// add col to contained columns so metadata and correlated tuples will be passed on to it
		ret.addContainedColumns(col);
		return ret;
	}
	
	/**
	 * Creates a EXISTS predicate with a (possibly correlated) subquery. If the subquery returns
	 * any tuples (or just one), i.e. hasNext() returns <i>true</i>, the predicate returns true,
	 * else (there does <i>not <b>exist</b></i> a single tuple in the subquery) it returns false.
	 * <br><b>This may not be used as a join Predicate!</b><br>
	 * <b>Example:</b><br><pre>
	 * <code>// some cursors
	 * AdvTupleCursor cur1 = new AdvTupleCursor(someList, "cur1");
	 * AdvTupleCursor cur2 = new AdvTupleCursor(someOtherList, "cur2");
	 * AdvTupleCursor res = cur1.where( 
	 * 		EXISTS( cur2.where( col("cur2.a").EQ(col("cur1.x")) ) ) 
	 * 	);</code></pre>
	 * This cursor returns all tuples from cur1 with a value in their Column named "x" that
	 * is contained in at least one tuple from cur2 at its column named "a".<br>
	 * <i>Note:</i> <ul><li> Because the subquery needs to be re-evaluated for each new "outer" tuple from 
	 * res' WHERE clause, it can not be cached. Only the first cursor in that query (cur2) will be 
	 * cached to ensure that it supports reset() (it will be resetted for each "outer" tuple).</li>
	 * <li>Subqueries are not really efficient and normally can easily be replaced by a join.</li></ul>
	 * @param subquery The subquery evaluated in the EXISTS clause
	 * @return an AdvPredicate implementing an EXISTS-subquery
	 */
	public static AdvPredicate EXISTS(final AdvTupleCursor subquery){
		// subquery should not cache - except for the first cursor, so it's resettable.
		// so if "only cache if absolutely necessary" via MAYBE_FIRST isn't enforced, we'll enable
		// caching the first cursor (even if the wrapped cursor is resettable)
		if(!subquery.getCachingStrategy().equals(CachingStrategy.MAYBE_FIRST) 
				&& !subquery.getCachingStrategy().equals(CachingStrategy.ONLY_FIRST))
			subquery.setCachingStrategy(CachingStrategy.ONLY_FIRST, true);
		
		AdvPredicate ret = new AdvPredicate() {
			@Override
			public boolean invoke(Tuple tuple) {
				boolean result;
				// current correlated tuple for subquery
				MapEntry<AdvResultSetMetaData, Tuple> newCorrTuple = 
						new MapEntry<AdvResultSetMetaData, Tuple>(getMetaData(), tuple);
				
				// create list of correlated Tuples ("this" tuple and the ones "inherited" from the containing cursor)
				List<MapEntry<AdvResultSetMetaData, Tuple>> corrTuples = new LinkedList<MapEntry<AdvResultSetMetaData,Tuple>>();
				corrTuples.add(newCorrTuple); // current tuple
				if(getCorrelatedTuples() != null)
					corrTuples.addAll(getCorrelatedTuples()); // tuples that were set in this predicate via setCorrelatedTuples()
				
				// set correlated tuples in subquery's CorrTuplesReceivers (predicates, ..)
				for(CorrTuplesReceiver ctr : subquery.getCorrTuplesRec()) {
					ctr.setCorrelatedTuples(corrTuples);
				}
				
				// if the subquery contains at least one tuple, exists is true.
				result = subquery.hasNext();
				
				// reset subquery for next tuple in where..
				subquery.reset();
				
				return result;
			}
			
			@Override
			public boolean invoke(Tuple argument0, Tuple argument1) {
				throw new UnsupportedOperationException("Don't use EXISTS in a JOIN-predicate.");
			}
		};
		
		return ret;
	}
	
	/**
	 * An AdvPredicate that always returns <i>false</i>, regardless of the input tuples.<br>
	 * Probably mostly useful for testing purposes.
	 */
	public static AdvPredicate FALSE = new AdvPredicate() {
		
		@Override
		public boolean invoke(Tuple argument0, Tuple argument1) {
			return false;
		}
		
		@Override
		public boolean invoke(Tuple argument) {
			return false;
		}
	};
	
	/**
	 * An AdvPredicate that always returns <i>true</i>, regardless of the input tuples.<br>
	 * Useful to make an equi-join behave like a cross product or for testing purposes.
	 */
	public static AdvPredicate TRUE = new AdvPredicate() {
		
		@Override
		public boolean invoke(Tuple argument0, Tuple argument1) {
			return true;
		}
		
		@Override
		public boolean invoke(Tuple argument) {
			return true;
		}
	};
	
	/***********************************************************************
	 * "clean" versions of the methods                                     *
	 * (the ones not needed throw exceptions, the other ones are abstract) *
	 ***********************************************************************/
	@Override // this method isn't used anyway
	public boolean invoke(List<? extends Tuple> arguments) {
		throw new UnsupportedOperationException("not implemented");
	}

	@Override // this method isn't used anyway
	public boolean invoke() {
		throw new UnsupportedOperationException("not implemented");
	}

	/**
	 * Invokes this Predicate on the given Tuple. The standard use case for this are WHERE-clauses:
	 * If the given Predicate evaluates to true, the tuple will be the next tuple returned by 
	 * next(), if it evaluates to false, the tuple will be discarded.
	 * <b>A simple Example:</b> <code>AdvTupleCursor res = cur1.where(col("a").LEQ(val("42")))</code><br>
	 * LEQ returns an AdvPredicate that compares the given Columns and returns true if the first 
	 * Column (col("a")) is less or equals the second Column(val("42")), so only Tuples with a 
	 * value of 42 or smaller in their Column named "a" will be returned by the where-operator. 
	 * @param argument the tuple to be evaluated
	 */
	@Override
	public abstract boolean invoke(Tuple argument);

	/**
	 * Invokes this Predicate on the given Tuples. The standard use case for this are 
	 * JOIN predicates. If the Predicates evaluates to true, the tuples will be concatenated
	 * and returned by the join-operator, if it evaluates to false, that combination of tuples
	 * will be ignored.<br>
	 * <b>A simple Example:</b> <code>AdvTupleCursor res = cur1.join(cur2, col("cur1.a").EQ(col("cur2.x")));
	 * </code><br> EQ returns a Predicate that compares the given Columns, in this case column "a" 
	 * from leftTuple (because leftTuple is from cur1) and column "b" from rightTuple, and if these
	 * Columns are equal, leftTuple and rightTuple will be 
	 * {@link AdvTupleCursor#concatTuples(Tuple, Tuple, int, int) concatenated} and returned by the
	 * cursor res.<br>
	 * @param leftTuple tuple of the left cursor being joined (cur1 in example)
	 * @param rightTuple tuple of the right cursor being joined (cur2 in example)
	 */
	@Override
	public abstract boolean invoke(Tuple leftTuple, Tuple rightTuple); 
	
}
