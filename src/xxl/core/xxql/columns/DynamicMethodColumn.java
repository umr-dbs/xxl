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

/**
 * 
 */
package xxl.core.xxql.columns;

import java.lang.reflect.Method;

import xxl.core.relational.tuples.Tuple;
import xxl.core.xxql.AdvResultSetMetaData;

public class DynamicMethodColumn extends ReflectionColumn {
	private static final long serialVersionUID = 1L;
	private final Column callingObjectColumn;
	Method m = null;
	private final String methodName;
	
	public DynamicMethodColumn(String name, String methodName, Column col,
			Column[] cols) {
		super(name);
		this.methodName = methodName;
		this.callingObjectColumn = col;
		this.argumentsColumns = cols;
	}

	@Override
	public Object invoke(Tuple tuple) {
		Object obj = callingObjectColumn.invoke(tuple);
		Object[] args = new Object[argumentsColumns.length];
		for (int i = 0; i < args.length; i++) {
			args[i] = argumentsColumns[i].invoke(tuple);
		}
		try {
			return m.invoke(obj, args);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void setMetaData(AdvResultSetMetaData metadata, String newAlias) {
		// this.metadata = metadata;
		// set metadata for contained columns
		argumentsTypes = new Class[containedColumns.size()];
		callingObjectColumn.setMetaData(metadata, newAlias);
		if (containedColumns != null) {
			for (Column col : containedColumns) {
				col.setMetaData(metadata, newAlias);
			}
			for (int i = 0; i < containedColumns.size(); i++) {
				try {
					argumentsTypes[i] = Class
							.forName(containedColumns.get(i).columnMetaData
									.getColumnClassName());
					// TODO ALLE Primitiven mappings hierrein ???
					/*
					 * if(parameterTypes[i] == Boolean.class){ parameterTypes[i]
					 * = boolean.class; }
					 */
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
		}

		try {
			this.clazz = Class.forName(callingObjectColumn.columnMetaData
					.getColumnClassName());
			m = getSuitableMethod(methodName, argumentsTypes, false);
			// Class.forName(col.columnMetaData.getColumnClassName()).getMethod(methodName,
			// parameterTypes);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		if (m == null) {
			throw new RuntimeException("can't find a suitable method for "
					+ methodName + " with given parameters");
		}
	//	System.out.println( m.getReturnType().getSimpleName());
		Class<?> claZZ = convertPrimitivToWrapperClass(m.getReturnType().getSimpleName()) ;
		if (claZZ == null){
			claZZ = m.getReturnType();
		}
		columnMetaData = AdvResultSetMetaData.createColumnMetaData(claZZ, this.columnAlias, newAlias);
	}
	
	
	 private Class<?> convertPrimitivToWrapperClass(String className){
		Class<?> claZZ = null;
		if(className.equals("int")){
			claZZ = Integer.class;
		}else if (className.equals("int")){
			claZZ = Long.class;
		}else if (className.equals("boolean")){
			claZZ = Boolean.class;
		}else if (className.equals("float")){ 
			claZZ = Float.class;
		}else if (className.equals("double")){
			claZZ = Double.class;
		}else if (className.equals("char")){
			claZZ = Character.class;
		}else if (className.equals("short")){
			claZZ = Short.class;
		}else if (className.equals("byte")){
			claZZ = Byte.class;
		}else if (className.equals("long")){
			claZZ = Long.class;
		}
		return claZZ;
	}

	@Override
	public void setMetaDatas(AdvResultSetMetaData leftMetaData,
			AdvResultSetMetaData rightMetaData) {
		argumentsTypes = new Class[containedColumns.size()];
		callingObjectColumn.setMetaDatas(leftMetaData, rightMetaData);
		if (containedColumns != null) {
			for (Column col : containedColumns) {
				col.setMetaDatas(leftMetaData, rightMetaData);
			}
			for (int i = 0; i < containedColumns.size(); i++) {
				try {
					argumentsTypes[i] = Class
							.forName(containedColumns.get(i).columnMetaData
									.getColumnClassName());
					// TODO ALLE Primitiven mappings hierrein ???
					/*
					 * if(parameterTypes[i] == Boolean.class){ parameterTypes[i]
					 * = boolean.class; }
					 */
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
		}
		try {
			this.clazz = Class.forName(callingObjectColumn.columnMetaData
					.getColumnClassName());
			m = getSuitableMethod(methodName, argumentsTypes, false);
			// Class.forName(col.columnMetaData.getColumnClassName()).getMethod(methodName,
			// parameterTypes);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		if (m == null) {
			throw new RuntimeException("can't find a suitable method for "
					+ methodName + " with given parameters");
		}
		columnMetaData = AdvResultSetMetaData.createColumnMetaData(m
				.getReturnType(), this.columnAlias, "");
	}
}
