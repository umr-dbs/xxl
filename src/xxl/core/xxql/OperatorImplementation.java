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


import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;



import xxl.core.collections.bags.Bag;
import xxl.core.cursors.AbstractCursor;
import xxl.core.cursors.Cursor;
import xxl.core.cursors.differences.NestedLoopsDifference;
import xxl.core.cursors.filters.Filter;
import xxl.core.cursors.joins.NestedLoopsJoin;
import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;
import xxl.core.relational.cursors.NestedLoopsDistinct;
import xxl.core.relational.metaData.ColumnMetaData;
import xxl.core.relational.metaData.ColumnMetaDatas;
import xxl.core.relational.metaData.ResultSetMetaDatas;
import xxl.core.relational.tuples.ArrayTuple;
import xxl.core.relational.tuples.Tuple;
import xxl.core.util.metaData.CompositeMetaData;
import xxl.core.xxql.columns.Column;

/**
 * This class implements all relational operators used by {@link AdvTupleCursor} as static 
 * functions.<br>
 * You may extend (via inheritance) this class to provide your own implementation of the operators 
 * and tell AdvTupleCursor to use it (via 
 * {@link AdvTupleCursor#setOperatorImpl(OperatorImplementation)} or in it's constructors)
 */
public class OperatorImplementation {
	
	// HA! singleton! (see "gang of four" books if you don't know what this is and are seriously 
	// interested - or wikipedia)
	private static OperatorImplementation stdOperatorImplSingleton = null;
	private OperatorImplementation(){}
	
	/**
	 * Returns Object of {@link OperatorImplementation} - always the same Object, as it doesn't 
	 * make any sense to have multiple of them: it only contains static functions anyway.<br>
	 * Yes, this is a Singleton. 
	 */
	public static OperatorImplementation getOperatorImpl(){
		if(stdOperatorImplSingleton == null)
			stdOperatorImplSingleton = new OperatorImplementation();
		
		return stdOperatorImplSingleton;
	}
	
	public static AdvTupleCursor select(AdvTupleCursor src, String newTableAlias, final Column ...cols ){
		// if no alias was given use the old one
		if(newTableAlias == null)
			newTableAlias = src.getResultSetMetaData().getAlias();
		for(Column col : cols){
			col.setMetaData(src.getResultSetMetaData(), newTableAlias);
		}
		Function<Object, Tuple> mapping = new AbstractFunction<Object,Tuple>(){
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple invoke(Object argument) {
				Tuple tuple = (Tuple)argument;
				Object[] elems = new Object[cols.length];
				for(int i = 0; i < cols.length; i++){
					elems[i] = cols[i].invoke(tuple);
				}

				return new ArrayTuple(elems);
			}
		};
		
		ColumnMetaData[] cmds = new ColumnMetaData[cols.length];
		for(int i=0;i<cmds.length;i++){
				cmds[i] = cols[i].getColumnMetaData();
		}
        AdvResultSetMetaData metadata = new AdvResultSetMetaData(newTableAlias, cmds);
		return new AdvTupleCursor(src, mapping, metadata, src);
	}

	/**
	 * Adds additional column with a name "index", which can be accessed via ColumnUtils.col("index"). 
	 * This column holds actual position of the tuple in the provided iterator.    
	 * @param src
	 * @param newTableAlias
	 * @param cols
	 * @return
	 */
	public static AdvTupleCursor selectIndex(AdvTupleCursor src, String newTableAlias, final Column ...cols ){
		// if no alias was given use the old one
		if(newTableAlias == null)
			newTableAlias = src.getResultSetMetaData().getAlias();
		for(Column col : cols){
			col.setMetaData(src.getResultSetMetaData(), newTableAlias);
		}
		Function<Object, Tuple> mapping = new AbstractFunction<Object,Tuple>(){
			private static final long serialVersionUID = 1L;
			private int counter = 0;
			
			@Override
			public Tuple invoke(Object argument) {
				Tuple tuple = (Tuple)argument;
				Object[] elems = new Object[cols.length+1];
				for(int i = 0; i < cols.length; i++){
					elems[i] = cols[i].invoke(tuple);
				}
				elems[elems.length-1] = new Integer(counter); 
				counter++;
				return new ArrayTuple(elems);
			}
		};
		
		ColumnMetaData[] cmds = new ColumnMetaData[cols.length+1];
		for(int i=0;i<cols.length;i++){
				cmds[i] = cols[i].getColumnMetaData();
		}
		cmds[cmds.length-1] = AdvResultSetMetaData.createColumnMetaData(Integer.class, "index", newTableAlias);
        AdvResultSetMetaData metadata = new AdvResultSetMetaData(newTableAlias, cmds);
		return new AdvTupleCursor(src, mapping, metadata, src);
	}
	
	
	
	
	public static AdvTupleCursor where(AdvTupleCursor src, AdvPredicate predicate){
		// supports reset
		// set metadata and correlated tuples in predicate so it can pass them on to "its" Columns
		predicate.setMetaData(src.getResultSetMetaData(), null);
		
		Filter<Tuple> filter = new Filter<Tuple>(src, predicate);
		AdvTupleCursor ret = AdvTupleCursor.factorFromCursorWithTuples(filter, src.getResultSetMetaData(), src);
		// add our predicate to the "Correlated Tuples Receivers" of the cursor
		ret.addCorrTuplesRec(predicate);
		return ret;
	}
	
	public static AdvTupleCursor join(AdvTupleCursor src1, AdvTupleCursor src2, String newTableAlias, AdvPredicate predicate){
		return join_impl(src1, src2, newTableAlias, predicate, NestedLoopsJoin.Type.THETA_JOIN);
	}
	
	public static AdvTupleCursor join(AdvTupleCursor src1, AdvTupleCursor src2, 
										String newTableAlias, AdvPredicate predicate, AdvTupleCursor.JOIN_TYPE type){
		
		NestedLoopsJoin.Type jtype = NestedLoopsJoin.Type.THETA_JOIN;
		
		switch(type){ // we duplicated the types in AdvTupleCursor so NestedLoopsJoin.Type 
				// needs not be imported by user
			case LEFT_OUTER_JOIN : 
				jtype = NestedLoopsJoin.Type.LEFT_OUTER_JOIN;
				break;
			case RIGHT_OUTER_JOIN : 
				jtype = NestedLoopsJoin.Type.RIGHT_OUTER_JOIN;
				break;
			case OUTER_JOIN : 
				jtype = NestedLoopsJoin.Type.OUTER_JOIN;
				break;
			case THETA_JOIN :  
				jtype = NestedLoopsJoin.Type.THETA_JOIN;
				break;
		}
		return join_impl(src1, src2, newTableAlias, predicate, jtype);
	}
	
	
	protected static AdvTupleCursor join_impl(AdvTupleCursor src1, AdvTupleCursor src2, 
								String newTableAlias, AdvPredicate predicate, NestedLoopsJoin.Type type)
	{
		AdvResultSetMetaData metadata = 
			AdvResultSetMetaData.concat(src1.getResultSetMetaData(), src2.getResultSetMetaData(), newTableAlias);
		
		final int size1;
		final int size2;
		try {
			size1 = src1.getResultSetMetaData().getColumnCount();
			size2 = src2.getResultSetMetaData().getColumnCount();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
		
		Function<Tuple, Tuple> concat = new AbstractFunction<Tuple, Tuple>(){

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple invoke(Tuple t1, Tuple t2) {
				return AdvTupleCursor.concatTuples(t1, t2, size1, size2);
			}
		};
		
		// set left and right metadata in predicate, so the containend columns can decide whether 
		// to use the left or the right tuple in invoke()
		predicate.setMetaDatas(src1.getResultSetMetaData(), src2.getResultSetMetaData());
		
		
		NestedLoopsJoin<Tuple, Tuple> join = new NestedLoopsJoin<Tuple, Tuple>(src1, src2, null, predicate, concat, type);
		//TODO sollte hier nicht ret.addCorrTuplesRec(predicate); rein ?!?
		return AdvTupleCursor.factorFromCursorWithTuples(join, metadata, src1, src2); 
	}
	
	

	@SuppressWarnings("unchecked")
	public static AdvTupleCursor union(AdvTupleCursor src1, AdvTupleCursor src2, String newTableAlias) {
		// supports reset
		AdvResultSetMetaData metadata;
		if(newTableAlias != null && !newTableAlias.equals(""))
			 metadata = src1.getResultSetMetaData().clone(newTableAlias);
		else
			metadata = src1.getResultSetMetaData();
		
		// check whether metadatas are compatible
		if(!checkMetaDatasEquivalence(src1.getResultSetMetaData(), src2.getResultSetMetaData())){
			throw new RuntimeException("Can't calculate union of "+src1.getResultSetMetaData().getAlias()
					+" and "+src2.getResultSetMetaData().getAlias()+" because their schemas are not compatible");
		}
		
		// don't use relational.Union because its metadata-checks won't work with our metadata 
		// (because we  might have Objects that don't have a SQL equivalent in our tuples)
		ResettableSequentializer<Tuple> union = new ResettableSequentializer<Tuple>(src1, src2);
		return AdvTupleCursor.factorFromCursorWithTuples(union, metadata, src1, src2); 
	}

	public static AdvTupleCursor distinct(AdvTupleCursor src) {
		// supports reset()
		AdvResultSetMetaData metadata = src.getResultSetMetaData();
		
		int memSize = 32;
		int objectSize = 4;
		
		NestedLoopsDistinct distinct = new NestedLoopsDistinct(src, memSize, objectSize);
		
		return AdvTupleCursor.factorFromCursorWithTuples(distinct, metadata, src);
	}
	
	/**
	 * Checks whether two (Adv)ResultSetMetaDatas are equivalent, e.g. you may perfrom UNION, 
	 * DIFFERENCE or INTERSECT on them.<br><br>
	 * We don't use {@link ResultSetMetaDatas#RESULTSET_METADATA_COMPARATOR}. because it checks the 
	 * java.sql.types and precision and we don't support them because it's impossible for non-basic 
	 * types. We check by comparing the columnCount and the columnClassName (i.e. the full java 
	 * class name). 
	 * 
	 * @param rsmd1 first ResultSetMetaData
	 * @param rsmd2 first ResultSetMetaData
	 * @return true if rsmd1 and rsmd2 are equivalent
	 */
	public static boolean checkMetaDatasEquivalence(ResultSetMetaData rsmd1, ResultSetMetaData rsmd2){
		try { // kind of stolen from ResultSetMetaDatas, but changed to compare the actual java types
			// and we just care if they're equal.
			if(rsmd1.getColumnCount() != rsmd2.getColumnCount())
				return false;
			int compare;
			
			for (int column = 1; column <= rsmd1.getColumnCount(); column++) {
				// compare the column names
				compare = rsmd1.getColumnName(column).compareToIgnoreCase(rsmd2.getColumnName(column));
				if (compare != 0)
					return false;
				
				// compare the (java!)-type
				if(!rsmd1.getColumnClassName(column).equals(rsmd2.getColumnClassName(column)))
					return false;
			}
			return true;
		}
		catch (SQLException sqle) {
			throw new RuntimeException("relational metadata information cannot be compared because of the following SQL exception : " 
					+sqle.getMessage());
		}
	}
	
	public static AdvTupleCursor difference(AdvTupleCursor src1, AdvTupleCursor src2) {
		return difference(src1, src2, true);
	}
	
	public static AdvTupleCursor difference(AdvTupleCursor src1, AdvTupleCursor src2, boolean all) {
		// supports reset
		AdvResultSetMetaData metadata = src1.getResultSetMetaData();
		
		// check whether metadatas are compatible
		if(!checkMetaDatasEquivalence(src1.getResultSetMetaData(), src2.getResultSetMetaData())){
			throw new RuntimeException("Can't calculate difference between "+src1.getResultSetMetaData().getAlias()
					+" and "+src2.getResultSetMetaData().getAlias()+" because their schemas are not compatible");
		}
		
		int memSize = 32;
		int objectSize = 4;
		NestedLoopsDifference<Tuple> difference = 
			new NestedLoopsDifference<Tuple>(src1, src2, memSize, objectSize, all); 

		return AdvTupleCursor.factorFromCursorWithTuples(difference, metadata, src1, src2);
	}
	
	public static AdvTupleCursor groupBy(AdvTupleCursor src, String newTableAlias, final Column[] proj, AggregateColumn[] metaDataAggregationFunctions){
		// (probably) supports reset
		
		// if no alias was given use the old one
		if(newTableAlias == null)
			newTableAlias = src.getResultSetMetaData().getAlias();
		
		//MetaDaten weiterreichen
		for(Column col : proj){
			col.setMetaData(src.getResultSetMetaData(), newTableAlias);
		}
		
		for(AggregateColumn col : metaDataAggregationFunctions){
			col.setMetaData(src.getResultSetMetaData(), newTableAlias);
		}

		//Teil 1: Berechnen der Partitionen
		//berechnet den key um die tuples zu partitionieren
		Function<Tuple, Tuple> mapping = new Function<Tuple, Tuple>() {

			@Override
			public Tuple invoke(List<? extends Tuple> arguments) {
				throw new RuntimeException();
			}

			@Override
			public Tuple invoke() {
				throw new RuntimeException();
			}

			@Override
			public Tuple invoke(Tuple argument) {
				List<Object> objects = new LinkedList<Object>();
				for(Column col: proj){
					objects.add(col.invoke(argument));
				}
				return ArrayTuple.FACTORY_METHOD.invoke(objects);
			}

			@Override
			public Tuple invoke(Tuple argument0, Tuple argument1) {
				throw new RuntimeException();
			}

		};
		
		//Zeug fuer den NestedLoopsGrouper
		Map<Object, Bag<Tuple>> map = new HashMap<Object, Bag<Tuple>>();
		int memSize = 32;
		int objectSize = 4;
		int keySize = 8;

		//Partitionierung
		// FIXME: das ding ist kaputt: schmeisst nullpointer-exception bei reset() weil bagIterator == null!
		xxl.core.relational.cursors.NestedLoopsGrouper grouper = new xxl.core.relational.cursors.NestedLoopsGrouper(
				src, mapping, map, memSize, objectSize, keySize);
		

		//Teil 2 Auswertung der Partitionen
		Function<Object, ? extends Tuple> createOutputTuple = ArrayTuple.FACTORY_METHOD;

		GroupAggregator aggregator = new GroupAggregator(
				grouper, 
				metaDataAggregationFunctions,
				proj, 
				createOutputTuple);
		
		//Metadaten des neuen AdvTupleCursor 
		ColumnMetaData[] cmds = new ColumnMetaData[proj.length + metaDataAggregationFunctions.length];
		for(int i=0;i<proj.length;i++){
				cmds[i] = proj[i].getColumnMetaData();
				try {
					System.out.println(cmds[i].getColumnName());
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
		
		CompositeMetaData<Object, Object> mtd;
		for(int i=proj.length;i<cmds.length;i++){
			mtd = (CompositeMetaData<Object, Object>) metaDataAggregationFunctions[i-proj.length].getMetaData();
			
			cmds[i] =  (ColumnMetaData) mtd.get(ColumnMetaDatas.COLUMN_METADATA_TYPE);
		}
		
		AdvResultSetMetaData metadata = new AdvResultSetMetaData(newTableAlias, cmds);
		
		return AdvTupleCursor.factorFromCursorWithTuples(aggregator, metadata, src); 
	}
	


	public static AdvTupleCursor intersect(AdvTupleCursor src1, AdvTupleCursor src2) {
		// supports reset
		AdvResultSetMetaData metadata = src1.getResultSetMetaData();
		// check whether metadatas are compatible
		if(!checkMetaDatasEquivalence(src1.getResultSetMetaData(), src2.getResultSetMetaData())){
			throw new RuntimeException("Can't calculate intersection of "+src1.getResultSetMetaData().getAlias()
					+" and "+src2.getResultSetMetaData().getAlias()+" because their schemas are not compatible");
		}
		
		// we don't use the relational NestedLoopsIntersection because we don't like their MetaData 
		// comparison, because it checks the java.sql.types and we don't support them because it's
		// impossible for non-basic types. we need our own type checking on the metadatas checking 
		// the java type (see above) to be sure!
		// XXLs NestedLoopsIntersection is buggy!
		// NestedLoopsIntersection<Tuple> intersect = new NestedLoopsIntersection<Tuple>(src1, src2);
		Intersection<Tuple> intersect = new Intersection<Tuple>(src1, src2);
		return AdvTupleCursor.factorFromCursorWithTuples(intersect, metadata, src1, src2);
	}

	@SuppressWarnings("unchecked")
	public static AdvTupleCursor orderBy(AdvTupleCursor src, final boolean asc, final Column ... cols) {
		if(cols == null || cols.length<1){
			throw new RuntimeException("You need to specify at least one Column to sort!");
		}
		
		for(Column col : cols){
			col.setMetaData(src.getResultSetMetaData(), null);
		}
		
		final Comparator comps[] = new Comparator[cols.length]; // comparators for each column
		// initialize comparators
		for(int i=0;i<cols.length;i++){
			try {
				comps[i] = Column.createComp(cols[i], cols[i]);
			} catch (Exception e) {
				throw new RuntimeException("Can't sort by Column "+cols[i].columnAlias+" because: "
						+e.getMessage(),e);
			}
		}
		// the comparator for the whole tuple (iterates through the columns)
		final Comparator<Tuple> comp = new Comparator<Tuple>() {
			@Override
			public int compare(Tuple t1, Tuple t2) {	
				for(int i=0;i<cols.length;i++){
					int tmp = comps[i].compare(cols[i].invoke(t1), cols[i].invoke(t2));
					if(tmp != 0) // if the colums aren't equal
						return (asc == true) ? tmp : -1*tmp;
				}
				// if we've come this far, the tuples are equal.
				return 0;
			}
		};
		
		AdvTupleCursor ret = new AdvTupleCursor(src, id, src.getResultSetMetaData(), src){
			
			{
				doNotCache = false;
			}
			
			@Override
			public void setCachingStrategy(CachingStrategy strat, boolean recursive) {
				super.setCachingStrategy(strat, recursive);
				this.doNotCache=false; // make sure this cursor does cache!
			}
			
			@Override
			protected boolean hasNextObject() {
				// when hasNext() or next() is called the first time, we cache everything into the 
				// internal list and sort.
				if(firstRun){ 
					// this will cache all elements in the list, sort them an reset the cursor
					internal_sort(comp);
					// TODO: als optimierung koennte man jetzt einen "ist sortiert" flag setzen und
					// bei joins, gruppierungen etc die sorted varianten nehmen
				} 
				return super.hasNextObject();
			}
		};
		return ret;
	}
	
	public Iterable<Object> getIterableForColumn(final AdvTupleCursor cur, final Column col){
		
		col.setMetaData(cur.getResultSetMetaData(), null);
		
		return new Iterable<Object>(){
			
			Cursor<Object> internCursor = new AbstractCursor<Object>() {

				@Override
				protected boolean hasNextObject() {
					return cur.hasNext();
				}

				@Override
				protected Object nextObject() {
					return col.invoke(cur.next());
				}
				
				@Override
				public void reset() throws UnsupportedOperationException {
					cur.reset();
					super.reset();
				}
				
				@Override
				public boolean supportsReset() {
					return cur.supportsReset();
				}
				
			};
			
			@Override
			public Iterator<Object> iterator() {
				internCursor.reset();
				return internCursor;
			}
			
		};
	}

	public static AdvTupleCursor top(AdvTupleCursor src, final int limit) {
		if(limit<0){
			throw new RuntimeException("top() must not be invoked with a negative count");
		}
		AdvTupleCursor ret = new AdvTupleCursor(src, id, src.getResultSetMetaData(), src){
			int count = 0;
			@Override
			protected boolean hasNextObject() {
				if(count == limit)
					return false;
				
				count++;
				
				return super.hasNextObject();
			}
			@Override
			public void reset() throws UnsupportedOperationException {
				count=0;
				super.reset();
			}
		};
		return ret;
	}
	
	// brauchen wir mind. 2x, also lieber in die klasse packen
	static Function<Object, Tuple> id = new AbstractFunction<Object, Tuple>() {
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple invoke(Object argument) {
			return (Tuple)argument;
		}
	};
	
}
