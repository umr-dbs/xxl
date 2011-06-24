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

package xxl.core.predicates;

import java.util.List;

import xxl.core.cursors.Subquery;

/**
 * This class provides a prototypical implementation of the exist-predicate.
 * The <code>invoke</code> method of this class returns <code>true</code>, if
 * the subquery delivers a non-empty result with the applied arguments.
 * 
 * <p>For example consider the implementation of the query:
 * <pre>
 *   SELECT (Integer2)
 *   FROM Cursor 2
 *   WHERE EXIST (SELECT Integer1
 *                FROM Cursor 1
 *                WHERE Integer1=Integer2)
 * </pre>
 * by using an <code>ExistPredicate</code> instance
 * <code><pre>
 *   System.out.println("Cursor 1: integers 8 to 15");
 *   Cursor&lt;Integer&gt; cursor1 = Cursors.wrap(new Enumerator(8,16));
 *   
 *   System.out.println("Cursor 2: integers 9 to 19");
 *   Cursor&lt;Integer&gt; cursor2 = Cursors.wrap(new Enumerator(9,20));
 *   
 *   Predicate&lt;Integer&gt; pred = new Equal&lt;Integer&gt;();
 *   BindingPredicate&lt;Integer&gt; bindPred = new BindingPredicate&lt;Integer&gt;(pred, Arrays.asList(1));
 *   
 *   Filter&lt;Integer&gt; sel = new Filter&lt;Integer&gt;(cursor1, bindPred);
 *   
 *   Subquery&lt;Integer&gt; sub = new Subquery&lt;Integer&gt;(
 *       sel,
 *       Arrays.asList(bindPred),
 *       new int[][] {
 *           new int[] {1}
 *       }
 *   );
 *   
 *   Predicate&lt;Integer&gt; exist0 = new ExistPredicate&lt;Integer&gt;(sub);
 *   
 *   Filter&lt;Integer&gt; cursor = new Filter&lt;Integer&gt;(cursor2, exist0);
 *   
 *   System.out.println("Cursor: result");
 *   
 *   Cursors.println(cursor);
 * </code></pre></p>
 *
 * @param <P> the type of the predicate's parameters.
 */
public class ExistPredicate<P> extends AbstractPredicate<P> {

	/**
	 * The subquery used in the exist-predicate.
	 */
	protected Subquery<? super P> subquery;

	/**
	 * Creates a new exist-predicate. The <code>invoke</code> method returns
	 * <code>true</code>, if the subquery delivers a non-empty result with the
	 * applied arguments.
	 *
	 * @param subquery the subquery used in the exist-predicate.
	 */
	public ExistPredicate(Subquery<? super P> subquery) {
		this.subquery = subquery;
	}

	/**
	 * Set the subquery of the exist-predicate.
	 *
	 * @param subquery the subquery used in the exist-predicate.
	 */
	public void setSubquery(Subquery<? super P> subquery) {
		this.subquery = subquery;
	}

	/**
	 * Test whether the subquery of the predicate delivers a non-empty result
	 * with the applied arguments.
	 *
	 * @param arguments the arguments to be applied to the underlying
	 *        predicate.
	 * @return the result of the underlying predicate's <code>invoke</code>
	 *         method that is called with applied arguments.
	 */
	@Override
	public boolean invoke(List<? extends P> arguments) {
		if (arguments == null)
			return invoke((P)null);
		subquery.bind(arguments);
		subquery.reset();
		if (subquery.hasNext()) {
			subquery.next();
			return true;
		}
		return false;
	}
}
