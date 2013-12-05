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

public class StaticMethodColumn extends ReflectionColumn {
	private static final long serialVersionUID = 1L;
	Method m = null;
	private  String methodName;
	public StaticMethodColumn(String name, Class<?> clazz, String methodName,
			Column[] cols) {
		super(name);
		this.clazz = clazz;
		this.methodName = methodName;
		this.argumentsColumns = cols;
	}

	
	
	@Override
	public Object invoke(Tuple tuple) {
		
		Object[] args = new Object[argumentsColumns.length];
		for (int i = 0; i < args.length; i++) {
			args[i] = argumentsColumns[i].invoke(tuple);
		}
		try {
			return m.invoke(null, args);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void setMetaData(AdvResultSetMetaData metadata, String newAlias) {
		// this.metadata = metadata;
		// set metadata for contained columns
		argumentsTypes = new Class[containedColumns.size()];
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
		/*
		 * try {
		 * 
		 * // clazz.getMethod(methodName, parameterTypes); } catch (Exception e)
		 * { throw new RuntimeException(e); }
		 */
		// we want a static method -> last parameter true
		m = getSuitableMethod(methodName, argumentsTypes, true);
		if (m == null) {
			throw new RuntimeException("can't find a suitable method for "
					+ methodName + " with given parameters");
		}
		columnMetaData = AdvResultSetMetaData.createColumnMetaData(m
				.getReturnType(), this.columnAlias, newAlias);
	}

	@Override
	public void setMetaDatas(AdvResultSetMetaData leftMetaData,
			AdvResultSetMetaData rightMetaData) {
		argumentsTypes = new Class[containedColumns.size()];
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
		/*
		 * try {
		 * 
		 * // clazz.getMethod(methodName, parameterTypes); } catch (Exception e)
		 * { throw new RuntimeException(e); }
		 */
		// we want a static method -> last parameter true
		m = getSuitableMethod(methodName, argumentsTypes, true);
		if (m == null) {
			throw new RuntimeException("can't find a suitable method for "
					+ methodName + " with given parameters");
		}
		columnMetaData = AdvResultSetMetaData.createColumnMetaData(m
				.getReturnType(), this.columnAlias, "");
	}
}
