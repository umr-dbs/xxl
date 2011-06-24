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

package xxl.core.spatial.cursors;

import java.util.Iterator;

import xxl.core.cursors.Cursor;
import xxl.core.cursors.wrappers.IteratorCursor;
import xxl.core.functions.Function;
import xxl.core.functions.Tuplify;
import xxl.core.predicates.Predicate;
import xxl.core.spatial.predicates.OverlapsPredicate;

/**
 *	NestedLoops distance similarity-join for Points.
 *
 *	The use-case contains a detailed example for a similarity-join on one or two input Point sets.
 *
 */
public class NestedLoopsJoin extends xxl.core.cursors.joins.NestedLoopsJoin {

	/** Creates a new NestedLoopsJoin.
	 *
	 * @param input0 the input-Cursor that is traversed in the "outer"
	 * 		loop.
	 * @param input1 the input-Cursor that is traversed in the "inner"
	 * 		loop.
	 * @param newCursor a parameterless function that delivers a new Cursor, when the
	 * 		Cursor <code>input1</code> cannot be reset, i.e.
	 * 		<code>input1.reset()</code> will cause a
	 * 		{@link java.lang.UnsupportedOperationException}.
	 * @param predicate the Predicate used to determin whether a tuple is a result-tuple
	 * @param newResult a factory-method (Function) that takes two parameters as argument
	 * 		and is invoked on each tuple where the predicate's evaluation result
	 * 		is <tt>true</tt>, i.e. on each qualifying tuple before it is
	 * 		returned to the caller concerning a call to <tt>next()</tt>.
	 */
	public NestedLoopsJoin(Cursor input0, Cursor input1, Function newCursor, Predicate predicate, Function newResult) {
		super(input0, input1, newCursor, predicate, newResult);
	}

	/** Creates a new NestedLoopsJoin.
	 *
	 * @param input0 the input-Iterator that is traversed in the "outer"
	 * 		loop.
	 * @param input1 the input-Iterator that is traversed in the "inner"
	 * 		loop.
	 * @param newCursor a parameterless function that delivers a new Cursor, when the
	 * 		Cursor <code>input1</code> cannot be reset, i.e.
	 * 		<code>input1.reset()</code> will cause a
	 * 		{@link java.lang.UnsupportedOperationException}.
	 * @param predicate the Predicate used to determin whether a tuple is a result-tuple
	 * @param newResult a factory-method (Function) that takes two parameters as argument
	 * 		and is invoked on each tuple where the predicate's evaluation result
	 * 		is <tt>true</tt>, i.e. on each qualifying tuple before it is
	 * 		returned to the caller concerning a call to <tt>next()</tt>.
	 */
	public NestedLoopsJoin(Iterator input0, Iterator input1, Function newCursor, Predicate predicate, Function newResult) {
		this(new IteratorCursor(input0), new IteratorCursor(input1), newCursor, predicate, newResult);
	}

	/** Creates a new NestedLoopsJoin. Uses OverlapsPredicate.
	 *
	 * @param input0 the input-Cursor that is traversed in the "outer"
	 * 		loop.
	 * @param input1 the input-Cursor that is traversed in the "inner"
	 * 		loop.
	 * @param newCursor a parameterless function that delivers a new Cursor, when the
	 * 		Cursor <code>input1</code> cannot be reset, i.e.
	 * 		<code>input1.reset()</code> will cause a
	 * 		{@link java.lang.UnsupportedOperationException}.
	 * @param newResult a factory-method (Function) that takes two parameters as argument
	 * 		and is invoked on each tuple where the predicate's evaluation result
	 * 		is <tt>true</tt>, i.e. on each qualifying tuple before it is
	 * 		returned to the caller concerning a call to <tt>next()</tt>.
	 */
	public NestedLoopsJoin(Cursor input0, Cursor input1, Function newCursor, Function newResult) {
		this(input0, input1, newCursor, OverlapsPredicate.DEFAULT_INSTANCE, newResult);
	}

	/** Creates a new NestedLoopsJoin-Object. Uses OverlapsPredicate.
	 *
	 * @param input0 the input-Cursor that is traversed in the "outer"
	 * 		loop.
	 * @param input1 the input-Cursor that is traversed in the "inner"
	 * 		loop.
	 * @param newResult a factory-method (Function) that takes two parameters as argument
	 * 		and is invoked on each tuple where the predicate's evaluation result
	 * 		is <tt>true</tt>, i.e. on each qualifying tuple before it is
	 * 		returned to the caller concerning a call to <tt>next()</tt>.
	 */
	public NestedLoopsJoin(Cursor input0, Cursor input1, Function newResult){
		super(input0, input1, OverlapsPredicate.DEFAULT_INSTANCE, newResult, xxl.core.cursors.joins.NestedLoopsJoin.Type.THETA_JOIN);
	}

	/** Creates a new NestedLoopsJoin. newResult is set to Tuplify.DEFAULT_INSTANCE. Uses OverlapsPredicate.
	 *
	 * @param input0 the input-Cursor that is traversed in the "outer"
	 * 		loop.
	 * @param input1 the input-Cursor that is traversed in the "inner"
	 * 		loop.
	*/
	public NestedLoopsJoin(Cursor input0, Cursor input1){
		this(input0, input1, Tuplify.DEFAULT_INSTANCE);
	}
}
