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

import java.util.LinkedList;
import java.util.List;

import xxl.core.math.functions.AggregationFunction;
import xxl.core.math.functions.MetaDataAggregationFunction;
import xxl.core.relational.metaData.ColumnMetaDatas;
import xxl.core.relational.tuples.Tuple;
import xxl.core.util.metaData.CompositeMetaData;
import xxl.core.xxql.columns.Column;

/**
 * Aggregation functions to be used with {@link AdvTupleCursor#groupBy(Column[], MetaDataAggregationFunction...)}
 */
public class AggregateUtils {

	@SuppressWarnings("serial")
	public static AggregateColumn COUNT(final String newColumnname){
		return  new AggregateColumn(
				new AggregationFunction<Tuple, Object>(){
					@Override
					public Object invoke(Object aggr, Tuple value) {
						Long aggregate = (Long)aggr;
						return aggregate == null ?
								value == null ? 0 : 1 
										:
											value == null ? aggregate : aggregate+1;
					}}
		) {
			protected CompositeMetaData<Object, Object> globalMetaData = new CompositeMetaData<Object, Object>();
			{
				globalMetaData.add(ColumnMetaDatas.COLUMN_METADATA_TYPE, AdvResultSetMetaData.createColumnMetaData(Long.class, newColumnname, null)
				);
			}

			@Override
			public CompositeMetaData<Object, Object> getMetaData() {
				return globalMetaData;
			}

			@Override
			public void setMetaData(AdvResultSetMetaData metadata,
					String newAlias) {
				// TODO Auto-generated method stub
				
			}
		};
	}

	@SuppressWarnings("serial")
	public static AggregateColumn MAX(final Column col, final String newColumnname){
		return  new AggregateColumn(
				new AggregationFunction<Tuple, Object>(){
					@Override
					public Object invoke(Object aggr, Tuple value) {
						Long actValue = (Long) col.invoke(value);
						return aggr == null ? actValue : Math.max((Long)aggr,actValue);
					}}
		) {
			protected CompositeMetaData<Object, Object> globalMetaData = new CompositeMetaData<Object, Object>();
			{
				globalMetaData.add(ColumnMetaDatas.COLUMN_METADATA_TYPE, AdvResultSetMetaData.createColumnMetaData(Long.class, newColumnname, null)
				);
			}

			@Override
			public CompositeMetaData<Object, Object> getMetaData() {
				return globalMetaData;
			}

			@Override
			public void setMetaData(AdvResultSetMetaData metadata,
					String newAlias) {
				col.setMetaData(metadata, newAlias);
				
			}
		};
	}

	@SuppressWarnings("serial")
	public static AggregateColumn MIN(final Column col, final String newColumnname){
		return  new AggregateColumn(
				new AggregationFunction<Tuple, Object>(){
					@Override
					public Object invoke(Object aggr, Tuple value) {
						Long actValue = (Long) col.invoke(value);
						return aggr == null ? actValue : Math.min((Long)aggr,actValue);
					}}
		) {
			protected CompositeMetaData<Object, Object> globalMetaData = new CompositeMetaData<Object, Object>();
			{
				globalMetaData.add(ColumnMetaDatas.COLUMN_METADATA_TYPE, AdvResultSetMetaData.createColumnMetaData(Long.class, newColumnname, null)
				);
			}

			@Override
			public CompositeMetaData<Object, Object> getMetaData() {
				return globalMetaData;
			}

			@Override
			public void setMetaData(AdvResultSetMetaData metadata,
					String newAlias) {
				col.setMetaData(metadata, newAlias);
				
			}
		};
	}

	@SuppressWarnings("serial")
	public static AggregateColumn SUM(final Column col, final String newColumnname){
		return  new AggregateColumn(
				new AggregationFunction<Tuple, Object>(){
					@Override
					public Object invoke(Object aggr, Tuple value) {
						Number actValue = (Number) col.invoke(value);
						return aggr == null ? actValue : ((Number)aggr).doubleValue() + actValue.doubleValue();
					}}
		) {
			protected CompositeMetaData<Object, Object> globalMetaData = new CompositeMetaData<Object, Object>();
			{
				globalMetaData.add(ColumnMetaDatas.COLUMN_METADATA_TYPE, AdvResultSetMetaData.createColumnMetaData(Double.class, newColumnname, null)
				);
			}

			@Override
			public CompositeMetaData<Object, Object> getMetaData() {
				return globalMetaData;
			}

			@Override
			public void setMetaData(AdvResultSetMetaData metadata,
					String newAlias) {
				col.setMetaData(metadata, newAlias);
				
			}
		};
	}
	@SuppressWarnings({"serial", "unchecked"})
	public static AggregateColumn LIST(final Column col, final String newColumnname){
		return  new AggregateColumn(
				new AggregationFunction<Tuple, Object>(){

					@Override
					public Object invoke(Object aggregate, Tuple value) {
						if(aggregate == null){
							aggregate = new LinkedList();
						}
						((List) aggregate).add(col.invoke(value));
						return aggregate;
					}}
		) {
			protected CompositeMetaData<Object, Object> globalMetaData = new CompositeMetaData<Object, Object>();
			{
				globalMetaData.add(ColumnMetaDatas.COLUMN_METADATA_TYPE, AdvResultSetMetaData.createColumnMetaData(List.class, newColumnname, null));
			}

			@Override
			public CompositeMetaData<Object, Object> getMetaData() {
				return globalMetaData;
			}

			@Override
			public void setMetaData(AdvResultSetMetaData metadata,
					String newAlias) {
				col.setMetaData(metadata, newAlias);
				
			}
		};
	}





	public static AggregateColumn[] AGGR(AggregateColumn ... aggrcolumns ){
		return aggrcolumns;
	}
}
