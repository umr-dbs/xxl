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

import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import xxl.core.collections.MapEntry;
import xxl.core.cursors.AbstractCursor;
import xxl.core.cursors.Cursor;
import xxl.core.cursors.Cursors;
import xxl.core.cursors.MetaDataCursor;
import xxl.core.cursors.wrappers.IteratorCursor;
import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;
import xxl.core.relational.cursors.ResultSetMetaDataCursor;
import xxl.core.relational.metaData.ColumnMetaData;
import xxl.core.relational.metaData.ResultSetMetaDatas;
import xxl.core.relational.tuples.ArrayTuple;
import xxl.core.relational.tuples.Tuple;
import xxl.core.util.metaData.CompositeMetaData;
import xxl.core.xxql.columns.Column;
import xxl.core.xxql.columns.ColumnUtils;


/**
 * A Cursor wrapping another Cursor/Iterator/Iterable to a Tuple Cursor
 * with AdvResultSetMetaData and CompositeMetaData. The central class of "LINQ for XXL" with
 * join, union, where, select, etc as methods.
 *
 */
@SuppressWarnings("static-access") // shut up, dummy, we're doing this on purpose
public class AdvTupleCursor extends AbstractCursor<Tuple> 
	implements MetaDataCursor<Tuple, CompositeMetaData<Object, Object>>, Iterable<Tuple>{
	
	/**
	 * metadata, compatible to ResultSetMetaData - has to match the tuples contained in this cursor!
	 */
	AdvResultSetMetaData metadata;
	
	/**
	 * The compositeMetaData required by the cursors in the relational package
	 */
	CompositeMetaData<Object, Object> compositeMetaData=null;
	
	/**
	 * An Object of a class implementing the operators (join, where, select etc).<br>
	 * Might be changes with setOperatorImpl() to allow using of custom operator implementations.
	 */
	protected OperatorImplementation operatorImpl = OperatorImplementation.getOperatorImpl();
	
	/**
	 * The wrapped Cursor
	 */
	protected Cursor<?> wrappedCursor=null; 
	
	/**
	 * This Function is used to create a AdvTuple from a given Object (e.g. for mapping a given 
	 * Cursor's elements to an AdvTuple returned by this Cursor)
	 */
	protected Function<Object, Tuple> objToTuple;
	
	/**
	 * If caching isn't disabled, the elements of the wrapped Cursor will be mapped to AdvTuple 
	 * and saved in this list.
	 */
	protected List<Tuple> tuples = new LinkedList<Tuple>();
	
	
	/**
	 * While this is true (and if caching isn't disabled) we will fill the 
	 * {@link AdvTupleCursor#tuples tuples List} on each next()
	 */
	protected boolean firstRun=true; 
	
	/**
	 * true, if the cursor has already delivered an element (i.e. it's too late to change the 
	 * caching strategy)
	 */
	protected boolean inUse=false;
	
	/**
	 * If the {@link AdvTupleCursor#tuples tuples List} is done (after first "run" of the cursor)
	 * this will be an iterator from it used in this cursors 
	 * {@link AdvTupleCursor#hasNextObject() hasNextObject()} and
	 * {@link AdvTupleCursor#nextObject() nextObject()} methods. On
	 * {@link AdvTupleCursor#reset() reset()} a new iterator is obtained from the list.
	 */
	protected Iterator<Tuple> tupleIt; 
	
	/** 
	 * If this is true, the tuples-List will not be used, i.e. the query will be re-evaluated 
	 * after reset(). That is necessary if this is a (potentially correlated) subquery.
	 */
	protected boolean doNotCache = true;
	
	// if true, we'll cache right father on SMART caching
	boolean isExpensive = false;
	
	/**
	 * A list of Objects implementing {@link CorrTuplesReceiver} (atm only Predicates from WHERE).<br>
	 * In this list all CorrTupleReceivers of this Query (this Cursor and it's parents) are collected
	 * so EXISTS etc can set the correlated Tuples in their (possibly correlated) subqueries.
	 */
	List<CorrTuplesReceiver> corrTuplesReceivers = new LinkedList<CorrTuplesReceiver>();
	
	/**
	 *  the "parents" of this Cursor. for example on <br>
	 *  <code> cur1.join(cur2, pred, "blah").select(...)</code><br>
	 *  cur1 and cur2 are parents of the cursor returned by join, which itself is the parent of
	 *  the cursor returned by select.<br>
	 *  References to them are needed to pass on information to all Cursors within a query.<br>
	 *  This is used to disable caching for the whole Cursor on subqueries.<br>
	 *  The "root" cursor(s) of a query have parents set to null (in the example above cur1 and 
	 *  cur2 are the roots).
	 */
	AdvTupleCursor[] parents=null;
	
	/**
	 * The child of this cursor. For example on <br>
	 *  <code> cur1.join(cur2, pred, "blah").select(...)</code><br>
	 *  the cursor returned by select is the child of the one returned by join, which is the child
	 *  of cur1 and cur2.
	 */
	List<AdvTupleCursor> children=null;
	
	/**
	 * The Caching-Strategy to be used in a query's {@link AdvTupleCursor}s:<br/>
	 * <ul>
	 * 	<li><b>ALL:</b> Cache within all operators of a query</li>
	 * 	<li><b>NONE:</b> Don't cache at all</li>
	 * 	<li><b>ONLY_FIRST:</b> Only cache the first cursor so the Elements of the Input-Cursor don't 
	 * 			have to be mapped to Tuples again after reset(). This is default.</li>
	 * 	<li><b>MAYBE_FIRST:</b> Cache the first cursor if its wrapped cursor is not resettable</li>
	 * 	<li><b>ONLY_LAST:</b> Only cache the last cursor. Makes sense for "inner" cursors of joins, 
	 * 			differences, intersections</li>
	 * 	<li><b>SMART:</b> Cache the last cursor of the query and also the last cursors queries that are 
	 * 			joined/intersected/subtracted with/from this query</li>
	 * <li><b>SMART_NOTLAST:</b> Like SMART, but don't cache the last cursor in the query
	 * </ul>
	 * <h3>Example:</h3> 
	 * Consider the following Code: <pre><code> 
	 * List{@code <SomeType>} mylist = ...; // some list containing Objects of an arbitrary class 
	 * AdvTupleCursor fooCur = new AdvTupleCursor(someList, "foo"); 
	 * AdvTupleCursor barCur = new AdvTupleCursor(mylist, "bar")
	 * 	.where( col("bar.x").LEQ(val(42)) )
	 * 	.join(<b>fooCur</b>, col("foo.a").EQ(col("bar.y"), "joinedCursor")
	 * 	.select("selectedCursor", col("foo.a"), col("bar.x"));
	 * 
	 * barCur.setCachingStrategy(CachingStrategy.ALL); // or NONE, SMART, ...
	 * </code></pre>
	 *  
	 * <i>barCur</i> will consist of multiple AdvTupleCursors: The first one is the new cursor created 
	 * from mylist, the second one is the one created by where(..), the third one is created by 
	 * the join with the "foo" cursor, which itself is the fifth one, and the fourth (and in this case last) 
	 * one is the one created by select (with the name "selectedCursor").<br>
	 * With the <b>ALL</b> caching strategy, all five cursors will have an internal list containing their tuples
	 * (it's created in the first run before the first reset(), after that the cursor will simply iterate
	 * through that list). This is pretty fast, but needs most memory (all those lists have some 
	 * overhead..) and doesn't make any sense most of the time.<br>
	 * With the <b>NONE</b> caching strategy, none of the cursors will use such an internal list, so on 
	 * each reset() the cursors (especially the "foo"-cursor, which is reset() <i>n</i> times in the join..) 
	 * need to be re-evaluated. This can be quite expensive, especially for the initial mapping of 
	 * the elements in the list to {@link Tuple}s and in joins, differences etc. But it needs least 
	 * memory.<br>
	 * <i>NOTE</i>: If the input-cursor/iterator of the first cursor is not resettable, you should use the 
	 * MAYBE_FIRST-strategy, if you want to get an resettable cursor. See below for operations that need
	 * a resettable cursor.<br>
	 * With the <b>ONLY_FIRST</b> caching strategy only the first cursor ("bar") saves it's tuples in 
	 * an internal list, so the mapping from <i>SomeType</i> to {@link Tuple} doesn't have to be done
	 * again after each reset(). Joins etc are still expensive.<br>
	 * With the <b>MAYBE_FIRST</b> strategy the first cursor <i>might</i> be cached - or rather 
	 * <i>will be cached if it is not resettable</i>. So this strategy guarantees a resettable 
	 * cursor with least overhead.<br>
	 * The <b>ONLY_LAST</b> strategy will cache only the last cursor in your query, so you can 
	 * iterate and reset it efficiently as often as you like.<br>
	 * With the <b>SMART</b> caching strategy, the last cursor and heavily used (=resetted often) cursors 
	 * will be cached. Specifically, the inner cursors in joins, intersections and differences will
	 * be cached. In the example above the "foo" cursor (because it's an inner join partner) and the
	 * cursor returned by select(..) (because it's the last one) would be cached. <br>
	 * <b>SMART_NOTLAST</b> is similar, but it doesn't cache the last cursor, so in this example only
	 * the "foo" cursor would be cached.<br><br>
	 * 
	 * The SMART strategies may both (but SMART_NOTLAST less then pure SMART) need much memory 
	 * because of the overhead of the Lists containing the tuples. However, they should generally 
	 * need a lot less memory than ALL while being just as fast.  
	 * <br><br>
	 * As a rule of thumb it makes sense to perform "heavy" caching (SMART) on a cursor if it
	 * needs to be resettable and it either needs to be resetted multiple times or it contains 
	 * several expensive calculations (joins, differences, intersections, aggregates, subqueries, ..)
	 * and needs to be reset() at least one time.<br><br>
	 *  
	 * <ul>
	 * 	<li>The cursor is an <b>inner</b> join/difference/intersect-partner ("foo" in the foregoing example) it will
	 * 		be resetted several times => cache it!</li>
	 * 	<li>If the cursor is used in a subquery (EXISTS/ALL/ANY in WHERE-clause) it needs to be resettable,
	 * 		but it may not cache anything but the first cursor (because of correlation) => use ONLY_FIRST
	 * 		if your input to the first-cursor is an Iterator or non-resettable Cursor! (This is default anyway, so
	 * 		just don't change it if you don't know what you're doing)</li>
	 * <ul>
	 * 
	 */
	// TODO: SMART weitergedacht koennte ausserdem bei reset von gecachetem cursor j die listen in 
	// cursorn i mit i<j geloescht werden - solange die nicht geclont wurden...
	public enum CachingStrategy {
		ALL,
		NONE,
		ONLY_FIRST,
		MAYBE_FIRST,
		ONLY_LAST,
		SMART,
		SMART_NOTLAST
	}
	
	public enum JOIN_TYPE {
		/**
		 * A constant specifying a theta-join. Only the tuples for which the
		 * specified predicate is <code>true</code> will be returned.
		 */
		THETA_JOIN,
		
		/**
		 * A constant specifying a left outer-join. The tuples for which the
		 * specified predicate is <code>true</code> as well as all elements of
		 * <code>input0</code> not qualifying concerning the predicate will be
		 * returned. The function <code>newResult</code> is called with an
		 * element of <code>input0</code> and the <code>null</code> value.
		 */
		LEFT_OUTER_JOIN,
		
		/**
		 * A constant specifying a right outer-join. The tuples for which the
		 * specified predicate is <code>true</code> as well as all elements of
		 * <code>input1</code> not qualifying concerning the predicate will be
		 * returned. The function <code>newResult</code> is called with an
		 * element of <code>input1</code> and the <code>null</code> value.
		 */
		RIGHT_OUTER_JOIN,
		
		/**
		 * A constant specifying a full outer-join. The tuples for which the
		 * specified predicate is <code>true</code> as well as all tuples
		 * additionally returned by the left and right outer-join will be
		 * returned.
		 */
		OUTER_JOIN
	};
	
	/**
	 * The caching strategy used by this cursor.
	 * 
	 * @see CachingStrategy
	 */
	CachingStrategy cacheStrat = null; // has to be set via setCachingStrategy(), so doNotCache etc are set properly
	
	
	/**
	 * Constructor that takes an arbitrary Cursor, a Function that maps its Objects to Tuples, 
	 * Metadata for <b>the resulting</b> tuples. If you want this Cursor to have a new alias, use 
	 * {@link AdvResultSetMetaData#clone(String newAlias)} on your existing metadata.<br>
	 * <i>Note</i>: This is the constructor that is also be used, explicitly or via 
	 * {@link AdvTupleCursor#factorFromCursorWithTuples(Cursor, AdvResultSetMetaData, AdvTupleCursor...)
	 * factorFromCursorWithTuples()}, by our operators like join, select, where, ...
	 * @param cur some Cursor
	 * @param mapping Maps Objects from Cur to Tuple 
	 * @param metadata Metadata appropriate for the resulting Tuples
	 * @param parents "{@link #parents}" of this cursor (if any)
	 */
	public AdvTupleCursor(Cursor<?> cur, Function<Object, Tuple> mapping, AdvResultSetMetaData metadata, AdvTupleCursor... parents){
		this(cur, mapping, metadata, null, parents);
	}
	
	/**
	 * Constructor that takes an arbitrary Cursor, a Function that maps its Objects to Tuples, 
	 * Metadata for <b>the resulting</b> tuples. If you want this Cursor to have a new alias, use 
	 * {@link AdvResultSetMetaData#clone(String newAlias)} on your existing metadata.<br>
	 * <i>Note</i>: This is the constructor that is also be used, explicitly or via 
	 * {@link AdvTupleCursor#factorFromCursorWithTuples(Cursor, AdvResultSetMetaData, AdvTupleCursor...)
	 * factorFromCursorWithTuples()}, by our operators like join, select, where, ...
	 * @param cur some Cursor
	 * @param mapping Maps Objects from Cur to Tuple 
	 * @param metadata Metadata appropriate for the resulting Tuples
	 * @param operatorImpl an implementation of operators, in case you want to provide your own 
	 * 		(<i>null</i> for default operator - e.g. {@link OperatorImplementation#getOperatorImpl()} or the left parent's 
	 * 		implementation, if any)
	 * @param parents "{@link #parents}" of this cursor (if any)
	 */
	public AdvTupleCursor(Cursor<?> cur, Function<Object, Tuple> mapping, AdvResultSetMetaData metadata, 
			OperatorImplementation operatorImpl, AdvTupleCursor... parents)
	{
		this.metadata = metadata; 
		this.objToTuple = mapping;
		this.wrappedCursor = cur;
		if(operatorImpl != null)
			this.operatorImpl = operatorImpl;
		if(parents != null && parents.length>0){
			this.parents = parents;
			for(AdvTupleCursor parent : parents){
				// add this cursor to parents children-list
				parent.addChild(this); 
				// add parents correlated tuples receivers to this cursors list
				this.corrTuplesReceivers.addAll(parent.getCorrTuplesRec());
			}
			if(operatorImpl == null){
				// use the same operator implementation as the left parent if not told otherwise
				this.setOperatorImpl(parents[0].operatorImpl, false);
			}
			
			// use same caching-strategy as (left) parent
			this.setCachingStrategy(parents[0].getCachingStrategy(), false);
			// TODO: hier koennte man ganz smarten kram machen und die strategien beider eltern
			// betrachten und die bessere (ueber)nehmen oder sowas
		} else // set default caching strategy
			this.setCachingStrategy(CachingStrategy.ONLY_FIRST, false);
	}
	
	/**
	 * Creates an AdvTupleCursor from a list of AdvTuples and appropriate metadata.<br>
	 * If you want this Cursor to have a new alias, use 
	 * {@link AdvResultSetMetaData#clone(String newAlias)} on your existing metadata.<br>
	 * (mainly useful for cloning)
	 * @param list a List of AdvTuples
	 * @param metadata Metadata suitable for those tuples
	 * @param operImpl the {@link OperatorImplementation} to be used in this cursor (or null for default)
	 */
	public AdvTupleCursor(List<Tuple> list, AdvResultSetMetaData metadata, OperatorImplementation operImpl){
		this.tuples = list;
		this.metadata = metadata;
		this.firstRun = false;
		this.tupleIt = tuples.iterator();
		// this cursor has no parents and does cache (with the given list)
		this.setCachingStrategy(CachingStrategy.ONLY_FIRST, true);
		if(operImpl != null)
			this.operatorImpl = operImpl;
	}
	
	/**
	 * A constructor that an AdvTupleCursor from a 
	 * <b>nonempty</b> Cursor of arbitrary type<br>
	 * <b>Don't</b> use this Constructor for Cursors containing Tuples (e.g. from a join)! 
	 * We have a factory Method ({@link AdvTupleCursor#factorFromCursorWithAdvTuples(Cursor, AdvResultSetMetaData, String) 
	 * factorFromCursorWithAdvTuples()} ) for that purpose that is safer, especially with empty cursors
	 * (like from a join creating an empty cursor because the Predicate is never met).<br>
	 * The Cursor will contain Tuples with one column named "value" that containing the elements of <b>cur</b>
	 * @param cur Cursor to be wrapped
	 * @param alias an Alias
	 */
	public AdvTupleCursor(Cursor<?> cur, String alias) {
		this(cur, alias, null);
	}

	/**
	 * A constructor that an AdvTupleCursor from a 
	 * <b>nonempty</b> Cursor of arbitrary type<br>
	 * <b>Don't</b> use this Constructor for Cursors containing Tuples (e.g. from a join)! 
	 * We have a factory Method ({@link AdvTupleCursor#factorFromCursorWithAdvTuples(Cursor, AdvResultSetMetaData, String) 
	 * factorFromCursorWithAdvTuples()} ) for that purpose that is safer, especially with empty cursors
	 * (like from a join creating an empty cursor because the Predicate is never met).<br>
	 * The Cursor will contain Tuples with one column named "value" that containing the elements of <b>cur</b>
	 * @param cur Cursor to be wrapped
	 * @param alias an Alias
	 * @param operImpl the {@link OperatorImplementation} to be used in this cursor (or null for default)
	 */
	public AdvTupleCursor(Cursor<?> cur, String alias, OperatorImplementation operImpl) {
		wrappedCursor = cur;
		wrappedCursor.open();
		if(!cur.hasNext())
			throw new RuntimeException("Cursor "+alias+" is empty!");
		Object example = cur.peek();
		// if cur is *not* some kind of Cursor<Tuple>
		if(!(example instanceof Tuple)) {
			ColumnMetaData cmd = AdvResultSetMetaData.createColumnMetaData(example.getClass(), "value", alias);
			metadata = new AdvResultSetMetaData(alias, cmd);
			objToTuple = new AbstractFunction<Object, Tuple>() {
				private static final long serialVersionUID = 1L;

				@Override
				public Tuple invoke(Object argument) {
					return new ArrayTuple(argument);
				}
			};
		} else { // cur *is* some kind of Cursor<Tuple> 
			throw new RuntimeException("You really want to use factorFromCursorWithAdvTuples() instead of this constructor.");
		}
		// this cursor has no parents and should probably cache (unless set otherwise later)
		this.setCachingStrategy(CachingStrategy.NONE, false);
		if(operImpl != null)
			this.operatorImpl = operImpl;
	}
	
	
	/**
	 * This factory creates an {@link AdvTupleCursor} for {@link Iterable}s (Lists, ...) containing 
	 * arbitrary types and does an "automagical" mapping:
	 * <ul>
	 * 	<li> If the elements are Objects of a <b>primitive type</b> (or pseudo-Primitive like 
	 * 		Integer, String, ...) the Tuples in the resulting Cursor will just contain one element 
	 * 		called "value".</li>
	 * 	<li> If the elements contain a more <b>complex type</b>, the public(!) <b>getters</b> 
	 * 		(get*() and is*()) will be used to obtain values. The columns will be called according 
	 * 		to the getter: The value retrieved from getFoo() will be in the column "foo".</li>
	 * 	<li> If the elements are <b>Arrays</b> (of equal length!), value i from the array (arr[i]) 
	 * 		will be value i+1 in the Tuple (because tuples start counting at 1) and will be called
	 * 		{@code"col<i>"} with i >= 1 (e.g. "col1" or "col42")</li>
	 * </ul>
	 * @param it an Iterable providing the elements
	 * @param alias an Alias for the created {@link AdvTupleCursor}
	 * @return an AdvTupleCursor wrapping it
	 */
	@SuppressWarnings("unchecked")
	public static AdvTupleCursor automagicallyCreateTupleCursor(Iterable<?> it, String alias){
		return automagicallyCreateTupleCursor(new IterableCursor(it), alias);
	}
	
	/**
	 * This factory creates an {@link AdvTupleCursor} for Iterators/Cursors containing arbitrary
	 * types and does an "automagical" mapping:
	 * <ul>
	 * 	<li> If the elements are Objects of a <b>primitive type</b> (or pseudo-Primitive like 
	 * 		Integer, String, ...) the Tuples in the resulting Cursor will just contain one element 
	 * 		called "value".</li>
	 * 	<li> If the elements contain a more <b>complex type</b>, the public(!) <b>getters</b> 
	 * 		(get*() and is*()) will be used to obtain values. The columns will be called according 
	 * 		to the getter: The value retrieved from getFoo() will be in the column "foo".</li>
	 * 	<li> If the elements are <b>Arrays</b> (of equal length!), value i from the array (arr[i]) 
	 * 		will be value i+1 in the Tuple (because tuples start counting at 1) and will be called
	 * 		{@code"col<i>"} with i >= 1 (e.g. "col1" or "col42")</li>
	 *  </ul>
	 * @param it an Iterator/Cursor providing the elements
	 * @param alias an Alias for the created {@link AdvTupleCursor}
	 * @return an AdvTupleCursor wrapping it
	 */
	public static AdvTupleCursor automagicallyCreateTupleCursor(Iterator<?> it, String alias){
		Cursor<?> cur = Cursors.wrap(it);
		cur.open();
		if(!cur.hasNext())
			throw new RuntimeException("Cursor "+alias+" is empty!");
		Object example = cur.peek();
		// if cur is *not* some kind of Cursor<Tuple>
		if(!example.getClass().isArray()){
			MapEntry<List<Method>, AdvResultSetMetaData> tmp = createAndSetMetaDataFromClass(example.getClass(), alias);
			Function<Object, Tuple> mapping = createDefaultMapping(tmp.getKey());
			
			return new AdvTupleCursor(cur, mapping, tmp.getValue(), (AdvTupleCursor[])null);
		} else { // is Array
			AdvResultSetMetaData md = createMetadataForArray(example, alias);
			Function<Object, Tuple> mapping;
			try {
				mapping = createArrayMapping(md.getColumnCount());
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
			return new AdvTupleCursor(cur, mapping, md, (AdvTupleCursor[])null);
		}
	}
	
	/**
	 * A constructor that automagically creates an AdvTupleCursor from a 
	 * <b>nonempty</b> Iterator of arbitrary type.<br>
	 * The first element of the cursor will be inspected (vie peek()) to generate the mapping
	 * from the elements to AdvTuple - this will of course fail if there are no elements to peek.
	 * 
	 * @param it an Iterator to be wrapped
	 * @param alias an Alias
	 */
	public AdvTupleCursor(Iterator<?> it, String alias) {
		this(it, alias, null);
	}

	/**
	 * A constructor that automagically creates an AdvTupleCursor from a 
	 * <b>nonempty</b> Iterator of arbitrary type.<br>
	 * The first element of the cursor will be inspected (vie peek()) to generate the mapping
	 * from the elements to AdvTuple - this will of course fail if there are no elements to peek.
	 * 
	 * @param it an Iterator to be wrapped
	 * @param alias an Alias
	 * @param operImpl the {@link OperatorImplementation} to be used in this cursor (or null for default)
	 */
	public AdvTupleCursor(Iterator<?> it, String alias, OperatorImplementation operImpl) {
		this(new IteratorCursor<Object>(it), alias, operImpl);
	}
	
	/**
	 * A constructor that automagically creates an AdvTupleCursor from a 
	 * <b>nonempty</b> Iterable (almost any Java Container) of arbitrary type
	 * 
	 * @param it an Iterable to be wrapped
	 * @param alias an Alias
	 */
	public <T> AdvTupleCursor(String alias, T... a) {
		this(Arrays.asList(a), alias, null);
	}
	
	/**
	 * A constructor that automagically creates an AdvTupleCursor from a 
	 * <b>nonempty</b> Iterable (almost any Java Container) of arbitrary type
	 * 
	 * @param it an Iterable to be wrapped
	 * @param alias an Alias
	 */
	public AdvTupleCursor(Iterable<?> it, String alias) {
		this(it, alias, null);
	}

	/**
	 * A constructor that automagically creates an AdvTupleCursor from a 
	 * <b>nonempty</b> Iterable (almost any Java Container) of arbitrary type
	 * 
	 * @param it an Iterable to be wrapped
	 * @param alias an Alias
	 * @param operImpl the {@link OperatorImplementation} to be used in this cursor (or null for default)
	 */
	public AdvTupleCursor(Iterable<?> it, String alias, OperatorImplementation operImpl) {
		this(new IterableCursor<Object>(it), alias, operImpl);
	}
	
	/**
	 * Wraps a ResultSet (most probably from JDBC, i.e. from a query to a database) into an
	 * AdvTupleCursor. <br>
	 * By default it does not cache its tuples - its {@link CachingStrategy} is set to <i>NONE</i>
	 * in contrast to most other constructors that set an ONLY_FIRST caching strategy.<br>
	 * That's because ResultSets can contain a lot of elements and creating Tuples from them is
	 * quite easy. You may change this setting via 
	 * {@link AdvTupleCursor#setCachingStrategy(CachingStrategy, boolean) setCachingStrategy()}, of course.
	 * 
	 * @param resultset the ResultSet to be  wrapped
	 * @param alias the alias of the AdvTupleCursor created
	 */
	public AdvTupleCursor(ResultSet resultset, String alias) {
		this(resultset, alias, null);
	}

	/**
	 * Wraps a ResultSet (most probably from JDBC, i.e. from a query to a database) into an
	 * AdvTupleCursor. <br>
	 * By default it does not cache its tuples - its {@link CachingStrategy} is set to <i>NONE</i>
	 * in contrast to most other constructors that set an ONLY_FIRST caching strategy.<br>
	 * That's because ResultSets can contain a lot of elements and creating Tuples from them is
	 * quite easy. You may change this setting via 
	 * {@link AdvTupleCursor#setCachingStrategy(CachingStrategy, boolean) setCachingStrategy()}, of course.
	 * 
	 * @param resultset the ResultSet to be  wrapped
	 * @param alias the alias of the AdvTupleCursor created
	 * @param operImpl the {@link OperatorImplementation} to be used in this cursor (or null for default)
	 */
	public AdvTupleCursor(ResultSet resultset, String alias, OperatorImplementation operImpl) {
		try {
			this.metadata = new AdvResultSetMetaData(resultset.getMetaData(), alias);	
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
		this.wrappedCursor = new ResultSetMetaDataCursor(resultset);
		this.objToTuple = new AbstractFunction<Object, Tuple>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple invoke(Object argument) {
				return (Tuple)argument;
			}
		};
		// don't cache (unless requested otherwise later), just get the tuples from the DB again
		// but set to MAYBE_FIRST to indicate that this cursor is resettable
		this.setCachingStrategy(CachingStrategy.MAYBE_FIRST, false);
		if(operImpl != null)
			this.operatorImpl = operImpl;
	}
	
	/**
	 * Creates a Tuple from an Object using the mapping function {@link AdvTupleCursor#objToTuple objToTuple}
	 * @param o an Object
	 * @return a Tuple created from that Object
	 */
	protected Tuple createTuple(Object o) {
		return objToTuple.invoke(o);
	}
	
	
	/**
	 * Factory Function that wraps a Cursor containing AdvTuples (e.g. from a join, ...) to an AdvTupleCursor.<br>
	 * If you want this to have a specific new alias, make sure your metadata already has got it 
	 * (via {@link AdvResultSetMetaData#clone(String newAlias)} or 
	 * {@link AdvResultSetMetaData#concat(AdvResultSetMetaData first, AdvResultSetMetaData second, String newAlias)}.<br>
	 * The <b>parents</b> is/are the AdvTupleCursor(s) providing the tuples processed by this cursor.<br>
	 * <b>Example:</b> <code>cur1.join(cur2, col("x").EQ(col("y"), "foo").where(col("z").LEQ(val(4711)))</code>
	 * cur1 and cur2 are the parents of the cursor returned by <i>join</i>, which itself is the parent of the 
	 * cursor returned by <i>where</i>.
	 * @param cur the Cursor to be wrapped
	 * @param metadata appropriate Metadata for the AdvTuples
	 * @param parents "parents" of this cursor (if any)
	 * @return an AdvTupleCursor containing the elements of cur
	 */
	@SuppressWarnings("serial")
	public static AdvTupleCursor factorFromCursorWithTuples(Cursor<Tuple> cur, AdvResultSetMetaData metadata, AdvTupleCursor... parents){
		Function<Object, Tuple> id = new AbstractFunction<Object, Tuple>() {
			@Override // no transformation needed, ID-function
			public Tuple invoke(Object argument) {
				return (Tuple)argument;
			}
		};
		
		return new AdvTupleCursor(cur, id, metadata, parents);
	}
	
	/**
	 * Sorts Cursor this by the given comperator. (useful for sorting and grouping)<br>
	 * <b>The Cursor will be <u>reset</u>!</b> (Wouldn't make much sense not to reset anyway..)
	 * @param comp the Comparator
	 */
	protected void internal_sort(Comparator<Tuple> comp){
		inUse=true; // so reset does something
		reset(); // reset to make sure all elements are in List
		
		// interne liste sortieren - waere etwas schneller mit ner arraylist, da die elemente
		// in nen array kopiert, dann sortiert und dann zurueck in die liste kopiert werden...
		// und das kopieren ist von array zu array schneller. (auf die idee, bei ner arraylist
		// direkt aufm internen array zu sortieren ist sun - aehhh oracle. - aber noch nicht 
		// gekommen...)
		Collections.sort(tuples, comp); 
		tupleIt = tuples.iterator();
	}
	
	@Override
	protected boolean hasNextObject() {
		// set inUse to true - the first element has been determined, changing the caching strategy 
		// now would be a bit painful.. (probably not impossible, though.. feel free to improve this :))
		inUse = true;
		// tupleIt is not used with subqueries, because they need to be re-evaluated every time
		// in case they are correlated
		if(firstRun || doNotCache) { 
			return wrappedCursor.hasNext();
		} else
			return tupleIt.hasNext();
	}
	
	@Override
	protected Tuple nextObject() {
		Tuple tmp;
		// do not cache results in subquery: it might be correlated, so it has to be re-evaluated
		// on every new run. however this could be optimized by finding out it the subquery is really
		// correlated. also only the cursor first where-clause and later need to be re-evaluated
		if(doNotCache) {
			tmp = createTuple(wrappedCursor.next());
		} else if(firstRun) { // on the first run copy tuple to tuples-List
			tmp = createTuple(wrappedCursor.next());
			tuples.add(tmp);
		} else
			tmp = tupleIt.next();
		
		return tmp;
	}
	
	@Override
	public void reset() throws UnsupportedOperationException {
		if(inUse == false)
			return;
		if(doNotCache){
			wrappedCursor.reset();
		} else {
			if(firstRun) { // if the cursor is reset on first run..
		
				// copy all remaining tuples (if any) to tuples-List
				while(wrappedCursor.hasNext()){
					tuples.add(createTuple(wrappedCursor.next()));
				}
				// FIXME: sollte man den schliessen?
				// wrappedCursor.close();
				firstRun = false;
			} 
			
			// fetch new iterator, so it starts again from the beginning of the list
			tupleIt = tuples.iterator(); 
		}
		super.reset();
	}
	
	@Override
	public boolean supportsReset() {
		if(doNotCache && !wrappedCursor.supportsReset())
			return false;
		else
			return true;
	}

	/**
	 * Returns the AdvResultSetMetaData of this Cursor, which is compatible to 
	 * JDBC's ResultSetMetaData
	 * 
	 * @return the (Adv)ResultSetMetaData of this Cursor
	 */
	public AdvResultSetMetaData getResultSetMetaData() {
		return metadata;
	}
	

	
	/**
	 * Get CompositeMetaData<Object, Object> containing ResultSetMetaData for this Cursor.
	 * Especially needed by the Cursors of the relational package.
	 * 
	 * @return CompositeMetaData<Object, Object> containing ResultSetMetaData for this Cursor
	 */
	@Override
	public CompositeMetaData<Object, Object> getMetaData() {
		// solange den quatsch keiner braucht, muessen wir ihn auch nicht initialisieren.
		if(compositeMetaData == null) { 
			String type = ResultSetMetaDatas.RESULTSET_METADATA_TYPE;
			compositeMetaData = new CompositeMetaData<Object, Object>();
			compositeMetaData.add(type, metadata);
		}
		return compositeMetaData;
	}
	
	/**
	 * Returns this Cursor (as it is itself an Iterator containing Tuples).<br>
	 * If it has already been used it will be {@link #reset()} before being returned to make sure
	 * the returned Iterator iterates all Tuples of this Cursor.<br>
	 * It doesn't make to much sense to use this Method directly, but it enables for-each loops
	 * for this class.<br>
	 * <b>Example:</b>
	 * <pre><code> AdvTupleCursors cur = ... ; // some AdvTupleCursor
	 * for(Tuple tup : cur){
	 * 	System.out.println(tup);
	 * }</code></pre>
	 * <i><b>Note:</b></i> It is <b>not possible to nest two</b> of this iterators from the <b>same 
	 * cursor</b> (or if one cursor is a part of the other cursor). This means, the following will
	 * produce strange behavior:
	 * <pre><code> AdvTupleCursors cur1 = ... ; // some AdvTupleCursor
	 * AdvTupleCursor cur2 = cur1.where(...); // cur2 contains cur1!
	 * for(Tuple t1 : cur1){
	 * 	for(Tuple t2 : cur2) {
	 * 		System.out.println(t1+" "+t2);
	 * 	}
	 * }</code></pre>
	 * If you want something like this you have to {@link #clone()}!
	 * @return an Iterator containing Tuples, i.e. <b>this</b> 
	 */
	@Override
	public Iterator<Tuple> iterator() {
		if(inUse)
			reset();
		return this;
	}
	
	/**
	 * Returns a clone of this Cursor with given Alias. Will fail if this cursor does not 
	 * {@link #supportsReset() support reset()} And <b>it will reset your cursor!</b>.<br>
	 * This is especially useful for self-joins: joining a cursor with itself (or a query that 
	 * shares some cursors with this one like <code><i>AdvTupleCursors atc1 = cur1.where(...); 
	 * AdvTupleCursor atc2 = cur1.select(...);</i></code>) is impossible (or rather: it is possible, but 
	 * you'll get strange results).<br>
	 * But if you clone the cursors before joining it'll work.<br>
	 * <b>Example:</b>
	 * <code><pre> AdvTupleCursor cur1 = ...; // a AdvTupleCursor
	 * AdvTupleCursor atc1 = cur1.where(...);
	 * AdvTupleCursor atc2 = cur1.clone("cur1clone").select(...);
	 * AdvTupleCursor join = atc1.join(atc2, col("cur1.a").LEQ(col("cur1clone.b")));</pre></code> 
	 * @param alias an Alias
	 * @return a clone of this Cursor
	 */
	public AdvTupleCursor clone(String alias) {
		
		if(!this.supportsReset()){
			throw new RuntimeException("Can't clone a Cursor that doesn't support reset()!");
			// it doesn't support reset() if the wrapped cursor isn't resettable 
			// and this cursor doesn't cache
		}
		List<Tuple> newList=null; // list of tuples for the new cloned cursor

		// reset() this cursor to make sure a cached cursor has it's tuples-list filled and
		// other cursors are before their first element
		inUse=true; // so reset() does something
		reset();
		if(doNotCache){ // this cursor doesn't cache so copy each element into list
			newList = new LinkedList<Tuple>();
			while(this.hasNext()){
				newList.add(this.next());
			}
			reset(); // reset again so the cursor again is before the first element
		} else { // this cursor does cache, so just copy the list
			newList = new ArrayList<Tuple>(tuples);
		}
		
		AdvResultSetMetaData newMD;
		if(alias == null)
			newMD = this.metadata;
		else
			newMD = this.metadata.clone(alias);
		
		return new AdvTupleCursor(newList, newMD, operatorImpl){
			@Override
			public void setCachingStrategy(CachingStrategy strat, boolean recursive) {
				super.setCachingStrategy(strat, recursive);
				// make sure caching is not disabled - the cloned cursor relies on its tuples list
				doNotCache=false; 
			}
		};
	} 
	
	/**
	 * Add a "correlated Tuple Receiver" to the Cursors corrTuplesReceivers list.
	 * That's a list of Objects implementing {@link CorrTuplesReceiver} (atm only Predicates from WHERE).<br>
	 * In this list all CorrTupleReceivers of this Query (this Cursor and it's parents) are collected
	 * so EXISTS etc can set the correlated Tuples in the Predicates of their (possibly correlated) 
	 * subqueries.
	 *
	 * @param ctr A "Corrolated Tuples Receiver", for example an {@link AdvPredicate}
	 * 
	 * @see AdvPredicate#setCorrelatedTuples(List)
	 */
	// needs to be public so classes from the columns package can use it
	public void addCorrTuplesRec(CorrTuplesReceiver ctr){
		corrTuplesReceivers.add(ctr);
	}
	
	/**
	 * Returns a list of Objects implementing {@link CorrTuplesReceiver} (atm only Predicates from WHERE).<br>
	 * In this list all CorrTupleReceivers of this Query (this Cursor and it's parents) are collected
	 * so EXISTS etc can set the correlated Tuples in the Predicates of their (possibly correlated) 
	 * subqueries.
	 * 
	 * @return a List of "Corrolated Tuples Receivers", for example {@link AdvPredicate}s
	 * 
	 * @see AdvPredicate#setCorrelatedTuples(List)
	 */
	public List<CorrTuplesReceiver> getCorrTuplesRec(){
		return corrTuplesReceivers;
	}

	/**
	 * Sets the {@link CachingStrategy} for this cursor and maybe its parental cursors 
	 * (if recursive is <i>true</i>). The CachingStrategy determines whether the cursor saves its tuples
	 * in an intern list in the first run, so it doesn't have to compute them again after reset().<br>
	 * Settings recursive to <i>false</i> doesn't make too much sense for the SMART strategies, however: if
	 * this cursor is a join/difference/intersection, the right parent's caching will be enabled. <br>
	 * See {@link CachingStrategy} for an extensive explanation of the different strategies and 
	 * what they're good for.
	 * 
	 * @param strat the {@link CachingStrategy} to be set
	 * @param recursive if <i>true</i>, this strategy will also be set to the {@link AdvTupleCursor#parents parental} cursors
	 * 
	 * @see CachingStrategy
	 */
	public void setCachingStrategy(CachingStrategy strat, boolean recursive){
		
		// TODO: maybe allow setting a new caching strategy directly after reset()? 
		// - that would quite difficult.
		if(inUse){
			throw new RuntimeException("It's currently impossible to change the caching strategy of"
					+" a cursor that has already delivered an element");
		}
		
		cacheStrat = strat;
		
		switch(strat){
			case NONE : 
				doNotCache = true;
				break;
			case ALL :
				doNotCache = false;
				break;
			case ONLY_FIRST : 
				if(parents == null)
					doNotCache = false;
				else
					doNotCache = true;
				break;
			case MAYBE_FIRST:
				if(parents == null && !wrappedCursor.supportsReset())
					doNotCache = false;
				else
					doNotCache = true;
				break;
			case ONLY_LAST : 
				doNotCache = false;
				// pass on NONE to parents
				strat = CachingStrategy.NONE;
				break;
			case SMART :
				if(children == null) // cache the last cursor
					doNotCache = false;
				else // this might be set to false by a child afterwards 
					doNotCache = true; // (if this is an inner join/difference/intersect partner)
			case SMART_NOTLAST :
				doNotCache = true; // don't cache last cursor. otherwise like SMART.
		}
		
		if(recursive && parents != null){
			for(AdvTupleCursor parent : parents){
				parent.setCachingStrategy(strat, recursive);
			}
		}
		
		if(strat.equals(CachingStrategy.SMART) || strat.equals(CachingStrategy.SMART_NOTLAST)){
			if(isExpensive && parents != null && parents.length==2) { // join, difference, intersect
				// enable caching for inner join/.. partner because it will be reset() really often
				parents[1].doNotCache = false; 
			}
		}
	}
	
	/**
	 * Returns the CachingStrategy of this cursor. See {@link CachingStrategy} for an extensive
	 * explanation of the different strategies and what they're good for.
	 * 
	 * @return this cursor's caching strategy
	 */
	public CachingStrategy getCachingStrategy(){
		return cacheStrat;
	}
	
	/**
	 * Sets a custom implementation of operators (join, where, ...) for this query, e.g. for this
	 * cursor, its {@link #parents}, their parents, ...<br>
	 * If you don't want to change the implementation for the whole query but only for this cursor
	 * use {@link #setOperatorImpl(OperatorImplementation, boolean) setOperatorImpl(impl, <b>false</b>)}
	 * @param impl the alternative implementation of operators
	 */
	public void setOperatorImpl(OperatorImplementation impl){
		setOperatorImpl(impl, true);
	}
	
	/**
	 * Sets a custom implementation of operators (join, where, ...) for this cursor.<br>
	 * If recursive is <i>true</i> it's set for the whole query, if not just for this cursor.
	 * @param impl the alternative implementation of operators
	 * @param recursive if <i>true</i> it will be set in the whole query, else just for this cursor
	 */
	public void setOperatorImpl(OperatorImplementation impl, boolean recursive){
		this.operatorImpl = impl;
		if(recursive){
			for(AdvTupleCursor parent : parents)
				parent.setOperatorImpl(impl, recursive);
		}
	}
	
	
	/**
	 * Register a "child" at this cursor
	 * <b>Example:</b> <code>cur1.join(cur2, col("x").EQ(col("y"), "foo").where(col("z").LEQ(val(4711)))</code>
	 * The cursor returned by <i>join</i> is a child of both cur1 and cur2 and the cursor returned 
	 * by <i>where</i> is a child of the cursor returned by <i>join</i>.
	 * @param child an AdvTupleCursor that is to be registered as a child of this cursor
	 */
	void addChild(AdvTupleCursor child){
		if(children == null){
			// most cursors will have only one child
			children = new ArrayList<AdvTupleCursor>(1);
		}
		children.add(child);
	}
	
// ##########################################################################
// ab hier: Voodoo fuer "automagisches" Mapping beliebiger Klassen auf Tupel #
// ##########################################################################
	
	protected LinkedList<Method> getters;
	
	/**
	 * Automagically sets creates and sets MetaData and a list of getters in this Cursor for Objects
	 * of the given Class.<br> 
	 * The mapping itself should be created with {@link AdvTupleCursor#createDefaultMapping() createDefaultMapping()} 
	 * and will use the list of getters created by this Method.<br>
	 * If cl is a (pseudo)-<b>primitive</b> type (Float, Integer, ... , Boolean, Byte, Character, String) 
	 * the tuples will have only one column named "<b>value</b>".<br>
	 * If the class it is not primitive, it will be scanned for getters and for each getter there 
	 * will be a column with the getters suffix, the first character will be lowercase
	 * (i.e. the column for "getFoo()" will be called "foo"). 
	 *  
	 * @param cl the type of the Objects this Cursor is to wrap
	 * @param alias this Cursor's alias
	 * 
	 * @see AdvTupleCursor#createDefaultMapping()
	 */
	protected static MapEntry<List<Method>, AdvResultSetMetaData> createAndSetMetaDataFromClass(Class<?> cl, String alias){
		AdvResultSetMetaData metadata=null;
		LinkedList<Method> getters = null; 
		// special cases: primitive Types
		boolean prim=false;
		if(cl.isPrimitive()) // it's a "real" primitive type
			prim=true;
		// it's derived from Number (covers Float, Integer, ... and similar)
		else if(java.lang.Number.class.isAssignableFrom(cl)) 
			prim = true;
		else if(cl.getName().startsWith("java.lang.")) { // check for Boolean, Byte, Character and String
			if(cl.getSimpleName().equals("Boolean") || cl.getSimpleName().equals("Byte") 
					|| cl.getSimpleName().equals("Character") || cl.getSimpleName().equals("String")) 
				prim = true;
		}
		// System.out.println("prim="+prim);
		if(prim){ // it's (more or less) primitive
			getters = null;
			metadata = new AdvResultSetMetaData(alias, AdvResultSetMetaData.createColumnMetaData(cl, "value", alias) );
			// primitive types have the special column-name "value"
			// metadata.addMappingAliasNameIndex("value", alias, 1); not needed, is done by constructor
		} else { // complex type -> traverse the getters
			// System.out.println("Class: "+cl.getName());
			Method[] methods = cl.getMethods();
			LinkedList<ColumnMetaData> cmds = new LinkedList<ColumnMetaData>(); // ColumnMetaDatas for each column..
			getters = new LinkedList<Method>(); 
			
			for(Method method : methods){
				if(method.getName().equals("getClass")) // we don't want the class..
					continue;
				if(method.getReturnType().equals(void.class))
					continue; // we're not interested in methods returning void..
				// System.out.println("Method: "+method.getName());
				if(method.getName().startsWith("get") && method.getGenericParameterTypes().length == 0) {
					getters.add(method); // put into getters-list
					String name = method.getName().substring(3); // remove prefix "get"
					name = name.substring(0, 1).toLowerCase()+name.substring(1); // make first letter lowercase
					// create appropriate ColumnMetaData and add it to list..
					Class<?> type = getObjectType(method.getReturnType());
					cmds.add( AdvResultSetMetaData.createColumnMetaData(type, name, alias) );
				} else if(method.getName().startsWith("is") && method.getGenericParameterTypes().length == 0) {
					getters.add(method); // put into getters-list
					String name = method.getName().substring(2); // remove prefix "is"
					name = name.substring(0, 1).toLowerCase()+name.substring(1); // make first letter lowercase
					// create appropriate ColumnMetaData and add it to list..
					Class<?> type = getObjectType(method.getReturnType());
					cmds.add( AdvResultSetMetaData.createColumnMetaData(type, name, alias) );
				}
			}
			
			if(cmds.size() == 0)
				throw new RuntimeException("Couldn't get attributes of Class "+cl.getSimpleName()
				                            +", because it has no getters and is not (pseudo-)primitive");

			ColumnMetaData[] cmdArr = cmds.toArray(new ColumnMetaData[cmds.size()]);
			metadata = new AdvResultSetMetaData(alias, cmdArr);
		} // !prim
		return new MapEntry<List<Method>, AdvResultSetMetaData>(getters, metadata);
	}
	
	/**
	 * Returns the Object-type for a primitive type (or just the given type if it isn't primitive)
	 * @param type a type/class, that might be primitive
	 * @return the according non-primitive type
	 */
	@SuppressWarnings("unchecked")
	protected static Class getObjectType(Class<?> type){
		if(!type.isPrimitive())
			return type;
		
		if(type.equals(byte.class))
			return Byte.class;
		else if(type.equals(short.class))
			return Short.class;
		else if(type.equals(int.class))
			return Integer.class;
		else if(type.equals(long.class))
			return Long.class;
		else if(type.equals(float.class))
			return Float.class;
		else if(type.equals(double.class))
			return Double.class;
		else if(type.equals(boolean.class))
			return Boolean.class;
		else if(type.equals(char.class))
			return Character.class;
		
		throw new RuntimeException("Unknown primitive type "+type.getName());
	}
	
	/**
	 * Returns a function that creates a tuple from an Object. Needs MetaData and the getters-list
	 * that may be generated with {@link AdvTupleCursor#createAndSetMetaDataFromClass(Class, String)}
	 */
	protected static Function<Object, Tuple> createDefaultMapping(final List<Method> getters) {  
		if(getters != null) { // => complex type
			return new AbstractFunction<Object, Tuple>() {
				private static final long serialVersionUID = -5940693227875126138L; 
	
				@Override
				public Tuple invoke(Object obj) {
					Object[] elems = new Object[getters.size()];
					int i=0;
					for(Method m : getters){ // traverse getters and put result into an object array
						try {
							elems[i++] = m.invoke(obj, (Object[])null);
						} catch (Exception e) {
							e.printStackTrace();
							throw new RuntimeException("Can't use getter on Object", e);
						} 
					}
					// create an arraytuple with that object-array and return it
					return new ArrayTuple(elems);
				}
			};
		} else { // primitive type
			return new AbstractFunction<Object, Tuple>() {
				
				private static final long serialVersionUID = 6138923494410694623L;

				@Override 
				public Tuple invoke(Object obj){
					// the tuple contains only one element
					return new ArrayTuple(obj);
				}
				
			};
		}
	}
	
	protected static AdvResultSetMetaData createMetadataForArray(Object arr, String alias){
		if(!arr.getClass().isArray())
			throw new RuntimeException("This is not an Array!");
		int length = Array.getLength(arr);
		ColumnMetaData[] cmds = new ColumnMetaData[length];
		
		for(int i=0;i<length;i++){
			Object tmp = Array.get(arr, i);
			cmds[i] = AdvResultSetMetaData.createColumnMetaData(tmp.getClass(), "col"+(i+1), alias);
		}
			
		return null;
	}
	
	protected static Function<Object, Tuple> createArrayMapping(final int length){
		return new AbstractFunction<Object, Tuple>() {
			private static final long serialVersionUID = 8401428384322811855L;
			
			@Override
			public Tuple invoke(Object arr){
				Object[] ret = new Object[length];
				try{
					for(int i=0;i<length;i++)
						ret[i] = Array.get(arr, i);
				} catch (IndexOutOfBoundsException e) {
					throw new RuntimeException("Seems like your arrays"
							+" are not of equal length: "+e.getMessage(), e);
				}
				
				return new ArrayTuple(ret);
			}
		};
		
	}
	
// ######################################################################
// ab hier: Methoden fuer LINQ-artige Funktionalitaet (join, select, ...) #
// ######################################################################

	/**
	 * Returns a Tuple that is a concatenation of the given Tuples.
	 * Useful for join().
	 * 
	 * @param t1 first Tuple (if null, all columns will be null for outer join)
	 * @param t2 second Tuple (if null, all columns will be null for outer join)
	 * @param size1 number of colums in the first tuple
	 * @param size2 number of colums in the second tuple
	 * @return t1++t2
	 */
	public static Tuple concatTuples(Tuple t1, Tuple t2, int size1, int size2){
		Object elems[] = new Object[size1+size2];
		
		if(t1 != null){
			for(int i=1;i<=size1;i++) {
				elems[i-1] = t1.getObject(i);
			}
		} else { // t1 == null -> null colums from outer join
			for(int i=1;i<=size1;i++) {
				elems[i-1] = null;
			}
		}
		
		if(t2 != null){
			for(int i=1;i<=size2;i++) {
				elems[size1+i-1] = t2.getObject(i);
			}
		} else { // t2 == null -> null columns from outer join
			for(int i=1;i<=size2;i++) {
				elems[size1+i-1] = null;
			}
		}
		
		return new ArrayTuple(elems);
	}
	
	/**
	 * Joins this cursor and atc with a standard inner theta-join with given predicate.
	 * @param other the cursor this cursor is to be joined with
	 * @param pred the join-predicate
	 * @return the joined {@link AdvTupleCursor}
	 */
	// default theta-join
	public AdvTupleCursor join(AdvTupleCursor other, AdvPredicate pred){
		return join(null, other, pred);
	}
	
	/**
	 * Joins this cursor and another cursor with a standard inner theta-join with given predicate.
	 * @param alias the new alias for the resulting cursor
	 * @param other the cursor this cursor is to be joined with
	 * @param pred the join-predicate
	 * @return the joined {@link AdvTupleCursor}
	 */
	public AdvTupleCursor join(String alias, AdvTupleCursor other, AdvPredicate pred){
		AdvTupleCursor ret = operatorImpl.join(this, other, alias, pred);
		ret.isExpensive = true; // set isExpensive, so SMART caching caches atc2
		return ret;
	}
	
	/**
	 * Joins this cursor with the other cursor with the given Predicate. The join type 
	 * (theta, outer, left/right outer join) may be specified.
	 * @param other the cursor this cursor is to be joined with
	 * @param pred the join-predicate
	 * @param type the {@link JOIN_TYPE} of this cursor
	 * @return joined {@link AdvTupleCursor}
	 */
	public AdvTupleCursor join(AdvTupleCursor other, AdvPredicate pred, JOIN_TYPE type){
		return join(null, other, pred, type);
	}
	
	/**
	 * Joins this cursor with the other cursor with the given Predicate. The join type 
	 * (theta, outer, left/right outer join) may be specified.
	 * @param alias the new alias for the resulting cursor
	 * @param other the cursor this cursor is to be joined with
	 * @param pred the join-predicate
	 * @param type the {@link JOIN_TYPE} of this cursor
	 * @return joined {@link AdvTupleCursor}
	 */
	public AdvTupleCursor join(String alias, AdvTupleCursor atc, AdvPredicate pred, JOIN_TYPE type){
		AdvTupleCursor ret = operatorImpl.join(this, atc, alias, pred, type);
		ret.isExpensive = true; // set isExpensive, so SMART caching caches atc2
		return ret;
	}
	
	/**
	 * Like the SQL SELECT clause. You can select existing Columns from this cursor
	 * (with {@link ColumnUtils#col(String) col()}) or create new ones by combining existing
	 * Columns (for example with {@link Column#ADD(Column, String)}) or getting values via
	 * reflection from existing Columns (for example with {@link ColumnUtils#colOBJCALL()} 
	 * or even insert new static values with {@link ColumnUtils#val(Object, String)}.
	 * @param columns {@link Column}s you want to select/create
	 * @return an {@link AdvTupleCursor} containing the selected/created Columns
	 */
	public AdvTupleCursor select(Column... columns ) {
		return select(null, columns);
	}
	
	
	/**
	 * Like the SQL SELECT clause. You can select existing Columns from this cursor
	 * (with {@link ColumnUtils#col(String) col()}) or create new ones by combining existing
	 * Columns (for example with {@link Column#ADD(Column, String)}) or getting values via
	 * reflection from existing Columns (for example with {@link ColumnUtils#colOBJCALL()} 
	 * or even insert new static values with {@link ColumnUtils#val(Object, String)}.
	 * @param columns {@link Column}s you want to select/create
	 * @return an {@link AdvTupleCursor} containing the selected/created Columns
	 */
	public AdvTupleCursor selectIndex(Column... columns ) {
		return operatorImpl.selectIndex(this, null, columns);
	}
	
	/**
	 * Like the SQL SELECT clause. You can select existing Columns from this cursor
	 * (with {@link ColumnUtils#col(String) col()}) or create new ones by combining existing
	 * Columns (for example with {@link Column#ADD(Column, String)}) or getting values via
	 * reflection from existing Columns (for example with {@link ColumnUtils#colOBJCALL()} 
	 * or even insert new static values with {@link ColumnUtils#val(Object, String)}.
	 * 
	 * @param alias the new alias for the resulting Cursor
	 * @param columns {@link Column}s you want to select/create
	 * @return an {@link AdvTupleCursor} containing the selected/created Columns
	 */
	public AdvTupleCursor select(String alias, Column ...columns ) {
		return operatorImpl.select(this, alias, columns);
	}

	/**
	 * Filters the tuples by the given predicate. If the predicate evaluates to true for a tuple
	 * it's contained in the resulting cursor, if it does not it isn't contained.
	 * @param predicate the predicate of the filter
	 * @return the filtered cursor
	 */
	public AdvTupleCursor where(AdvPredicate predicate) {
		return operatorImpl.where(this, predicate);  
	}

	/**
	 * Unions this cursor with another cursor. The cursors need to have the same schema, i.e. the 
	 * column count needs to be identical and the names and types of each column need to be 
	 * identical.
	 * @param other the cursor to be unioned with
	 * @return a cursor containing the tuples of both this cursor and the other cursor
	 */
	public AdvTupleCursor union(AdvTupleCursor other) {
		return union(null, other);
	}
	
	/**
	 * Unions this cursor with another cursor. The cursors need to have the same schema, i.e. the 
	 * column count needs to be identical and the names and types of each column need to be 
	 * identical.
	 * @param alias the alias for the resulting cursor
	 * @param other the cursor to be unioned with
	 * @return a cursor containing the tuples of both this cursor and the other cursor
	 */
	public AdvTupleCursor union(String alias, AdvTupleCursor atc) {
		return operatorImpl.union(this, atc, alias);  
	}

	/**
	 * Groups the Cursor by the given Columns and performs the given aggregations. The resulting 
	 * cursor's tuples will contain the Columns from group and the new columns created by the 
	 * aggregations. <u>The Colums to be grouped need to map to an index within the tuples, i.e.
	 * you may <b>not use val() or reflection Columns</b> here.</u> If you want to group by values
	 * obtained by reflection you have to insert them into the tuple with 
	 * {@link #select(Column...) select(cols)} first!<br>
	 * <b>Note:</b> To conveniently specify the Columns to be grouped by you may use 
	 * {@link ColumnUtils#PROJ(Column...)} so you don't have to create an Array yourself. 
	 * @param group the Columns to be grouped by (use {@link ColumnUtils#PROJ(Column...)})
	 * @param aggregations aggregations (like count, avg, ...) to be performed on the cursor
	 * @return a cursor containing the Columns from group and the new columns created by the 
	 * 		aggregations.
	 * 
	 * @see ColumnUtils#PROJ(Column...)
	 */
	public AdvTupleCursor groupBy(Column[] group, 
			AggregateColumn... aggregations)
	{
		return groupBy(null, group, aggregations);
	}
	
	/**
	 * Groups the Cursor by the given Columns and performs the given aggregations. The resulting 
	 * cursor's tuples will contain the Columns from group and the new columns created by the 
	 * aggregations. <u>The Colums to be grouped need to map to an index within the tuples, i.e.
	 * you may <b>not use val() or reflection Columns</b> here.</u> If you want to group by values
	 * obtained by reflection you have to insert them into the tuple with 
	 * {@link #select(Column...) select(cols)} first!<br>
	 * <b>Note:</b> To conveniently specify the Columns to be grouped by you may use 
	 * {@link ColumnUtils#PROJ(Column...)} so you don't have to create an Array yourself. 
	 * @param alias the new alias for the resulting cursor
	 * @param group the Columns to be grouped by (use {@link ColumnUtils#PROJ(Column...)})
	 * @param aggregations aggregations (like count, avg, ...) to be performed on the cursor
	 * @return a cursor containing the Columns from group and the new columns created by the 
	 * 		aggregations.
	 * 
	 * @see ColumnUtils#PROJ(Column...)
	 */
	public AdvTupleCursor groupBy(String alias, Column[] group, 
			AggregateColumn... aggregations) 
	{
		
		return operatorImpl.groupBy(this, alias, group, aggregations);  
	}

	/**
	 * Eliminates duplicate Tuples
	 * @return a cursor containing the same tuples as this cursor, but each tuple only once
	 */
	public AdvTupleCursor distinct() {
		return operatorImpl.distinct(this);
	}

	/**
	 * Removes tuples from this cursor that are contained in the other cursor.<br>
	 * The cursors need to have the same schema, i.e. the column count needs to be identical and 
	 * the names and types of each column need to be identical.<br>
	 * If all is true, all tuples from this cursor that are contained in the other one are removed,
	 * else only as many tuples of each set of duplicate tuples are removed as are returned in the
	 * other cursor.<br>
	 * <b>Example:</b> if this cursor contains 3 tuples ("a", "b", "c") and the other cursor 
	 * contains one tuple ("a", "b", "c"), the resulting cursor will contain 2 tuples 
	 * ("a", "b", "c") if all is false or no tuple ("a", "b", "c") if all is true.
	 * @param other the cursor that is subtracted from this one
	 * @param all see above
	 * @return a cursor containing only elements not contained in the other cursor (if all is true)
	 * 	or containing less elements that are also contained in the other cursor (if all is false)
	 */
	public AdvTupleCursor difference(AdvTupleCursor other, boolean all) {
		AdvTupleCursor ret = operatorImpl.difference(this, other, all);
		ret.isExpensive=true; // set isExpensive, so SMART caching caches atc2
		return ret;
	}
	
	/**
	 * Removes all tuples from this cursor that are contained in the other cursor.<br>
	 * The cursors need to have the same schema, i.e. the column count needs to be identical and 
	 * the names and types of each column need to be identical.<br>
	 * @param other the cursor that is subtracted from this one
	 * @return a cursor containing elements that are contained in this cursor, but not in the 
	 * 		other cursor
	 */
	public AdvTupleCursor difference(AdvTupleCursor other) {
		AdvTupleCursor ret = operatorImpl.difference(this,other);
		ret.isExpensive=true; // set isExpensive, so SMART caching caches atc2
		return ret;
	}
	
	/**
	 * Calculates an intersection of this cursor and the other cursor, i.e. the resulting cursor
	 * will only contain elements contained in this cursor and the other cursor.<br>
	 * The cursors need to have the same schema, i.e. the column count needs to be identical and 
	 * the names and types of each column need to be identical.<br>
	 * @param other the cursor this one is intersected with
	 * @return a cursor containing only elements contained in both this and the other cursor
	 */
	public AdvTupleCursor intersect(AdvTupleCursor other){
		AdvTupleCursor ret = operatorImpl.intersect(this,other);
		ret.isExpensive=true; // set isExpensive, so SMART caching caches atc2
		return ret;
	}
	
	/**
	 * Sorts the Tuples of this cursor by the given {@link Column}s in ascending order.<br>
	 * It is sorted by the order of the Columns: It's sorted primary by the first Column, 
	 * secundarily by the second Column and so on.
	 * @param cols Columns to sort by
	 * @return a sorted Cursor
	 * 
	 * @see #orderBy(boolean, Column...) orderBy(asc, cols) if you want to order in descending order
	 */
	// default ascending - like in SQL
	public AdvTupleCursor orderBy(Column... cols){
		return orderBy(true, cols);
	}
	
	/**
	 * Sorts the Tuples of this cursor by the given {@link Column}s.<br>
	 * It is sorted by the order of the Columns: It's sorted primary by the first Column, 
	 * secundarily by the second Column and so on.
	 * @param asc if false the cursor will be sorted in descending order, else ascending
	 * @param cols Columns to sort by
	 * @return a sorted Cursor
	 */ 
	public AdvTupleCursor orderBy(boolean asc, Column... cols){
		return operatorImpl.orderBy(this, asc, cols);
	}
	
	/**
	 * Returns the first count Tuples of the Cursor. Probably only useful with {@link #orderBy}()
	 * @param count how many tuples to be returned
	 * @return an {@link AdvTupleCursor} returning the <i>count</i> first elements of this cursor
	 */
	public AdvTupleCursor top(int count){
		return operatorImpl.top(this, count);
	}
	
	/**
	 * Returns an {@link Iterable} for one column of this Cursor. This may be used in a for-each 
	 * loop.<br>
	 * <b>Example:</b>
	 * <code><pre> List<Foo> l1; // a list of Foos
	 * // a query on that list, for example using reflection, maybe joining 
	 * // some other cursor, doesn't matter...
	 * AdvTupleCursor cur = new AdvTupleCursor(l1, "l1")
	 * 		.where( colOBJCALL(col("l1.value"), "getA").LEQ(val(42)) )
	 * 		.join(...);
	 * // now I want the elements from l1 that are left after that query
	 * for( Object o : cur.getIterableForColumn(col("l1.value")) ){
	 * 		Foo f = (Foo)o;
	 * 		f.someMethod();
	 * }</pre></code>
	 * <i><b>Note:</b></i> It is <b>not possible to <i>nest</i> two</b> of this iterators from the <b>same 
	 * cursor</b> (or if one cursor is a part of the other cursor). This means, the following will
	 * produce strange behavior:
	 * <pre><code> AdvTupleCursors cur1 = ... ; // some AdvTupleCursor
	 * AdvTupleCursor cur2 = cur1.where(...); // cur2 contains cur1!
	 * for( Tuple t1 : cur1.getIterableForColumn(col("a")) ){
	 * 	for( Tuple t2 : cur2.getIterableForColumn(col("b")) ) {
	 * 		System.out.println(t1+" "+t2);
	 * 	}
	 * }</code></pre>
	 * If you want something like this you have to {@link #clone()}!
	 */
	public Iterable<Object> getIterableForColumn(Column col){
		return operatorImpl.getIterableForColumn(this, col);
	}
	
	public AdvTupleCursor expand(Column col) {
		return NewLINQFunctions.expand(this,col);
		
	}
	public AdvTupleCursor newSelect(Column... columns ){
		Projection prj = new Projection(this.getResultSetMetaData(), "newSelect", columns);
		return new AdvTupleCursor(this, prj, prj.getMetadata(), this);
	}
}


