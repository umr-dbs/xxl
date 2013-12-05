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

import java.util.List;

import xxl.core.collections.MapEntry;
import xxl.core.relational.tuples.Tuple;
import xxl.core.xxql.columns.Column;

/**
 * Classes should implement this interface, if they need Correlated Tuples. <br>
 * Examples are Column and Predicate.
 *
 */
public interface CorrTuplesReceiver {
	
	/**
	 * Pass a list of tuples along with their metadata from "outer" cursors in case this is a 
	 * (correlated) subquery. The list has to be sorted from inside to outside (in case of nested 
	 * subqueries). It will be passed on to the contained Columns.<br>
	 * This is meant to be called from invoke() of EXISTS, ALL and ANY clauses.
	 * 
	 * @param corrTuples a List of tuples with their related metadata from "outer" cursors 
	 * 			for correlated subqueries
	 * 
	 * @see AdvTupleCursor#getCorrTuples()
	 * @see Column#setCorrelatedTuples(List)
	 */
	public void setCorrelatedTuples(List<MapEntry<AdvResultSetMetaData, Tuple>> corrTuples);
}
