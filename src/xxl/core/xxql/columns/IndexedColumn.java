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

package xxl.core.xxql.columns;

import xxl.core.relational.tuples.Tuple;
import xxl.core.xxql.AdvResultSetMetaData;
import xxl.core.xxql.columns.Column;

public class IndexedColumn extends Column {

	private Long counter;
	public IndexedColumn(String name, long start){
		super(name);
		counter = start;
	}
	@Override
	public Object invoke(Tuple tuple) {
		return counter++;
	}
	@Override
	public void setMetaData(AdvResultSetMetaData metadata,	String newAlias) {
		
		try {
			this.columnMetaData = AdvResultSetMetaData.createColumnMetaData(Long.class, columnAlias, newAlias);
		} catch(Exception e){
			throw new RuntimeException(e);
		}
		
	}

	@Override
	public void setMetaDatas(AdvResultSetMetaData leftMetaData,
			AdvResultSetMetaData rightMetaData) {
		// TODO @Daniel col.getMetadata() wirft ne exception da ja die col noch nicht "komplett" initialissiert wurde
		
		
		try {
			this.columnMetaData = AdvResultSetMetaData.createColumnMetaData(Long.class, columnAlias, null);
		} catch(Exception e){
			throw new RuntimeException(e);
		}
	}
}

