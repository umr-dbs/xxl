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

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.TypeVariable;
import java.sql.SQLException;
import java.util.Iterator;

import xxl.core.functions.Functional.BinaryFunction;
import xxl.core.functions.Functional.UnaryFunction;
import xxl.core.relational.metaData.ColumnMetaData;
import xxl.core.relational.tuples.ArrayTuple;
import xxl.core.relational.tuples.Tuple;
import xxl.core.xxql.AdvResultSetMetaData;
import xxl.core.xxql.AdvTupleCursor;
import xxl.core.xxql.columns.Column;

public class NewLINQFunctions {

	/**
		 * Test Method
		 * @param function
		 * @param arg1
		 * @param arg2
		 * @return
		 */
		//TODO: Methadaten
		public static <O> Column tupleFunction(final String name, 
				final UnaryFunction<Tuple, O> function){
			return new Column(function, name, null){
				O obj;
				@Override
				public Object invoke(Tuple tuple) {	
					obj = function.invoke(tuple);
					return obj;
				}
				@Override
				public void setMetaData(AdvResultSetMetaData metadata,
						String newAlias) {
					Class cls = null;
					try {
						TypeVariable[] typevars = function.getClass().getTypeParameters();
						System.out.println(typevars.length);
						for(TypeVariable tv : typevars){
							System.out.println("TADA: " + tv.toString());
						}
						
						String className = ((ParameterizedType) (function.getClass().getGenericInterfaces()[0])).getActualTypeArguments()[1].toString();
						System.out.println("NLF: " +className);
						if (className.startsWith("class")) {
							className = className.substring(6);
						}else if(className.contains("<")){
							className = className.substring(0, className.indexOf("<"));
						}
						System.out.println(className);
						cls = Class.forName(className);
					} catch (ClassNotFoundException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					columnMetaData = AdvResultSetMetaData.createColumnMetaData(cls,
							this.columnAlias, name);
				}
			}; 
		}

	/**
	 * Test Method
	 * @param function
	 * @param arg1
	 * @param arg2
	 * @return
	 */
	public static <I, O> Column function(String name, 
			final UnaryFunction< I ,  O > function, final  Column arg0){
		return new Column(function, name, null){
			@Override
			public void setMetaData(AdvResultSetMetaData metadata,
					String newAlias) {
				super.setMetaData(metadata, newAlias);
				// pass meta data to argument cols 
				arg0.setMetaData(metadata, newAlias);
				this.useLeftTuple =  arg0.useLeftTuple; 
			}
			
		@Override
		public void setMetaDatas(AdvResultSetMetaData leftMetaData,
				AdvResultSetMetaData rightMetaData) {
			// TODO Auto-generated method stub
			super.setMetaDatas(leftMetaData, rightMetaData);
			arg0.setMetaDatas(leftMetaData, rightMetaData);
			this.useLeftTuple =  arg0.useLeftTuple; 
		}
			
			@Override
			public Object invoke(Tuple tuple) {
				Object argument = arg0.invoke(tuple);
				return function.invoke((I)argument);
			}
		}; 
	}

	/**
	 * Test Method
	 * @param function
	 * @param arg1
	 * @param arg2
	 * @return
	 */
	public static <I1, I2, O> Column function(String name, 
			final BinaryFunction< I1 , I2,   O > function, final  Column arg0, final Column arg1){
		return new Column(function, name, null){
			@Override
			public void setMetaData(AdvResultSetMetaData metadata,
					String newAlias) {
				super.setMetaData(metadata, newAlias);
				// pass meta data to argument cols 
				arg0.setMetaData(metadata, newAlias);
				arg1.setMetaData(metadata, newAlias);
			}
			@Override
			public Object invoke(Tuple tuple) {
				Object argument0 = arg0.invoke(tuple);
				Object argument1 = arg1.invoke(tuple);
				return function.invoke((I1)argument0, (I2)argument1);
			}
		}; 
	}

	public static AdvTupleCursor expand(final AdvTupleCursor src,
			final Column column) {
		String newTableAlias = null;
		if(newTableAlias == null)
			newTableAlias = src.getResultSetMetaData().getAlias();
		
		int columnscount = 0;
		try {
			columnscount = src.getResultSetMetaData().getColumnCount();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		Column[] columns = new Column[columnscount];
		for(int i = 0; i < columns.length; i++){
			columns[i] = new Column(i+1);
			columns[i].setMetaData(src.getResultSetMetaData(), newTableAlias);
		}
		column.setMetaData(src.getResultSetMetaData(), newTableAlias);
		
		ColumnMetaData[] cmds = new ColumnMetaData[columnscount+1];
		for(int i=0; i < cmds.length-1; i++){
				cmds[i] = columns[i].getColumnMetaData();
		}
		cmds[cmds.length-1] = AdvResultSetMetaData.createColumnMetaData(Object.class, "expand", newTableAlias);
		
		
		AdvResultSetMetaData metadata = new AdvResultSetMetaData(newTableAlias, cmds);
		
		
		
		
		
		AdvTupleCursor ret = new AdvTupleCursor(src, null, metadata, src){
			Iterator colDataToExpand = null;
			Tuple actTuple;
			Object actValue = null;
			@Override
			protected boolean hasNextObject() {
				if(actTuple == null){
					actTuple = src.next();
				}
				if (colDataToExpand== null){
					//System.out.println((actTuple.getObject(col.getColumnIndex())).getClass());
					colDataToExpand = ((java.util.AbstractCollection) (actTuple.getObject(column.getColumnIndex()))).iterator();
				}
				
				if(colDataToExpand.hasNext()){
					return true;
				}else{
					if(src.hasNext()){
						actTuple = src.next();
						colDataToExpand = null;
						return hasNextObject();
					}else{
						return false;
					}
				}
			}

			@Override
			protected Tuple nextObject() {
				if(!hasNextObject()){
					throw new NullPointerException("DING DONG");
				}
				Object actValue = colDataToExpand.next();
				Tuple leftTuple = new ArrayTuple(actValue);
//				System.out.println("!" + actTuple);
//				System.out.println("*" + leftTuple);
				Tuple retTuple = concatTuples(actTuple,leftTuple, actTuple.getColumnCount(), leftTuple.getColumnCount());
				return retTuple;
			}
			
		};
		//METADATA !!!
		return ret;
	}

}
