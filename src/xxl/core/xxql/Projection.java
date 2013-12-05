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

import xxl.core.functions.AbstractFunction;
import xxl.core.relational.metaData.ColumnMetaData;
import xxl.core.relational.tuples.ArrayTuple;
import xxl.core.relational.tuples.Tuple;
import xxl.core.xxql.AdvResultSetMetaData;
import xxl.core.xxql.columns.Column;

//TODO Refactoren zu AbstractFunction<Tuple, Tuple>
public class Projection extends AbstractFunction<Object, Tuple> {

	//private AdvTupleCursor src;
	private String newTableAlias;
	private Column[] cols;
	private AdvResultSetMetaData metadata;

	
	public Projection(AdvResultSetMetaData oldMetadata, String newTableAlias, Column[] cols) {
		super();
		//this.src = src;
		this.newTableAlias = newTableAlias;
		this.cols = cols;
		setMetaData(oldMetadata);
	}

	private void setMetaData(AdvResultSetMetaData oldMetadata) {
		if (newTableAlias == null)
			newTableAlias = oldMetadata.getAlias();
		for (Column col : cols) {
			col.setMetaData(oldMetadata, newTableAlias);
		}
		ColumnMetaData[] cmds = new ColumnMetaData[cols.length];
		for (int i = 0; i < cmds.length; i++) {
			cmds[i] = cols[i].getColumnMetaData();
		}
		this.metadata = new AdvResultSetMetaData(newTableAlias,
				cmds);
	}

	public AdvResultSetMetaData getMetadata() {
		return metadata;
	}

	@Override
	public Tuple invoke(Object argument) {
		Tuple tuple = (Tuple) argument;
		Object[] elems = new Object[cols.length];
		for (int i = 0; i < cols.length; i++) {
			elems[i] = cols[i].invoke(tuple);
		}

		return new ArrayTuple(elems);
	}
	// public static AdvTupleCursor select(AdvTupleCursor src, String
	// newTableAlias, final Column ...cols ){
	// // if no alias was given use the old one
	// if(newTableAlias == null)
	// newTableAlias = src.getResultSetMetaData().getAlias();
	// for(Column col : cols){
	// col.setMetaData(src.getResultSetMetaData(), newTableAlias);
	// }
	// Function<Object, Tuple> mapping = new AbstractFunction<Object,Tuple>(){
	// private static final long serialVersionUID = 1L;
	//
	// @Override
	// public Tuple invoke(Object argument) {
	// Tuple tuple = (Tuple)argument;
	// Object[] elems = new Object[cols.length];
	// for(int i = 0; i < cols.length; i++){
	// elems[i] = cols[i].invoke(tuple);
	// }
	//
	// return new ArrayTuple(elems);
	// }
	// };
	//
	// ColumnMetaData[] cmds = new ColumnMetaData[cols.length];
	// for(int i=0;i<cmds.length;i++){
	// cmds[i] = cols[i].getColumnMetaData();
	// }
	// AdvResultSetMetaData metadata = new AdvResultSetMetaData(newTableAlias,
	// cmds);
	// return new AdvTupleCursor(src, mapping, metadata, src);
	// }

}
