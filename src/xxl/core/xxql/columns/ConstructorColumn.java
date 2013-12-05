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

import java.lang.reflect.Constructor;

import xxl.core.relational.tuples.Tuple;
import xxl.core.xxql.AdvResultSetMetaData;

public class ConstructorColumn extends ReflectionColumn {
	/**
	 * 
	 */
	private static final long serialVersionUID = 2582893362937366123L;
	Constructor<?> c = null;
	public ConstructorColumn(String name, Class<?> clazz,
			Column[] cols) {
		super(name);
		this.clazz = clazz;
		this.argumentsColumns = cols;
	}

	@Override
	public Object invoke(Tuple tuple) {
		Object[] args = new Object[argumentsColumns.length];
		for (int i = 0; i < args.length; i++) {
			args[i] = argumentsColumns[i].invoke(tuple);
		}

		/*
		 * das wird doch schon in setMetaData gemacht?! try { c =
		 * clazz.getConstructor(parameterTypes); } catch (Exception e) { throw
		 * new RuntimeException(e); }
		 */
		try {
			return c.newInstance(args);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void setMetaData(AdvResultSetMetaData metadata, String newAlias) {
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
		c = getSuitableConstructor(argumentsTypes);
		if (c == null) {
			throw new RuntimeException("can't find a suitable constructor for "
					+ clazz.getName() + " with given parameters");
		}
		columnMetaData = AdvResultSetMetaData.createColumnMetaData(clazz,
				this.columnAlias, newAlias);
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
		c = getSuitableConstructor(argumentsTypes);
		// c = clazz.getConstructor(parameterTypes);
		if (c == null) {
			throw new RuntimeException("can't find a suitable constructor for "
					+ clazz.getName() + " with given parameters");
		}
		columnMetaData = AdvResultSetMetaData.createColumnMetaData(clazz,
				this.columnAlias, "");
	}
}
