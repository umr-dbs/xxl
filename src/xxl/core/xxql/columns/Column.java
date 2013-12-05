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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import xxl.core.collections.MapEntry;
import xxl.core.comparators.ComparableComparator;
import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;
import xxl.core.predicates.AbstractPredicate;
import xxl.core.predicates.Predicate;
import xxl.core.relational.metaData.ColumnMetaData;
import xxl.core.relational.tuples.Tuple;
import xxl.core.xxql.AdvPredicate;
import xxl.core.xxql.AdvResultSetMetaData;
import xxl.core.xxql.AdvTupleCursor;
import xxl.core.xxql.CorrTuplesReceiver;
import xxl.core.xxql.AdvTupleCursor.CachingStrategy;

/**
 * This class represents a single value/object, among other things to be used in 
 * {@link AdvPredicate predicates} and to select and create new Columns in 
 * {@link AdvTupleCursor#select(Column...) select}. <br>
 * The object is retrieved by calling {@link #invoke(Tuple)} or {@link #invoke(Tuple, Tuple)} and
 * most commonly a Column will return the Object contained in one specific column of the tuple
 * (thus the name).<br>
 * But this class (or subclasses of this class) is also used for static values like 
 * {@link ColumnUtils#val(Object)}, calculated values like {@link #ADD(Column)} that create a new
 * Column from to given Columns, values obtained via reflection from a method-call on an Object 
 * from a column of the tuple (see {@link ReflectionColumn}) and to access values of 
 * "correlated tuples" in subqueries like EXISTS etc.
 */
public class Column extends AbstractFunction<Tuple, Object> implements CorrTuplesReceiver {

	private static final long serialVersionUID = 1L;
	
	public enum SubQueryType { // FIXME: evtl nach ColumnUtils um imports zu sparen
		ALL, ANY
	}
	
	// either a value (from val()) or a bound object from an outer cursor/tuple
	protected Object			boundObject			= null;
	// index of this Column within a tuple (<1 if that doesn't apply)
	private int					columnIndex;
	// metadata for this Column
	protected ColumnMetaData	columnMetaData;
	// contained Colums (e.g. if this Column is composed like from ADD)
	protected List<Column>		containedColumns	= null;
	// if true, this Column projects it's values from a correlated tuple
	private boolean				fromCorrTuple		= false;

	// a flag to indicate we're using a bound object (from val() or from a correlated tuple)
	private boolean				haveBoundObject		= false;
	// the name of this column
	public String				columnAlias;
	// the new name of this column (for renaming columns)
	private String				newColumnAlias;

	// if this is used in a join, we'll have a left and a right tuple, but a normal Column uses
	// only one of them. CONCAT, ADD, ... may "use" both, but they just pass them on to normal colums
	// this specifies whether to evaluate the left or the right tuple in a join
	//TODO: getter oder wieder protected
	public boolean			useLeftTuple		= true;

	// TODO: evtl alle Konstrkutoren private ?!? - wenn dann package protected fuer ColumnUtils
	// FIXME: brauchen wir wirklich so viele konstruktoren?
	public Column() {
		this(0, null, null, null);
	}

	public Column(int columnIndex) {
		this(columnIndex, null, null, null);

	}

	public Column(int columnIndex, String newColumnAlias) {
		this(columnIndex, null, newColumnAlias, null);
	}

	public Column(String columnAlias) {
		this(0, columnAlias, null, null);
	}
	
	public Column(String columnAlias, String newColumnAlias) {
		this(0, columnAlias, newColumnAlias, null);
	}
	
	private Column(int column, String columnAlias, String newColumnAlias, ColumnMetaData columnMetaData) {
		this.columnIndex = column;
		this.columnAlias = columnAlias;
		this.newColumnAlias = newColumnAlias;
		this.columnMetaData = columnMetaData;
	}

	protected Column(Object val, String columnAlias){
		this(val, columnAlias, null);
	}
	
	protected Column(Object val, String columnAlias, ColumnMetaData cmd) {
		this(0, columnAlias, columnAlias, cmd);
		boundObject = val;
		haveBoundObject = true;
	}

	
	Column(String columnAlias, ColumnMetaData cmd) {
		this(0, columnAlias, null, cmd);
	}
	
	/**
	 * Returns an the Object represented by this Column. That may be the value of one column of 
	 * the given tuple, a correlated tuple, a value calculated from contained columns or 
	 * just a fixed value.
	 */
	@Override
	public Object invoke(Tuple tuple) {
		if (columnIndex > 0) {
			// System.out.println(this.name+": "+tuple.getObject(column).toString());
			return tuple.getObject(columnIndex);
		} else if (haveBoundObject) { // check ob != null ist suboptimal, falls
			// der wert der spalte einfach null
			// war..
			return boundObject;
		} else {
			throw new RuntimeException(
					"You haven't specified a valid Column! (" + columnAlias + ")");
		}
	}

	/**
	 * Returns an the Object represented by this Column. That may be the value of one column of 
	 * the given tuples, a value calculated from contained columns or just a fixed value.
	 */
	@Override
	public Object invoke(Tuple left, Tuple right) {
		if (useLeftTuple) {
			return invoke(left);
		} else {
			return invoke(right);
		}
	}
	
	/**
	 * Set the AdvResultSetMetaData of the cursor this column (probably) belongs
	 * to and the new alias of the resulting cursor.<br>
	 * It also passes on the MetaData to it's "contained" Colums (e.g. if this
	 * Column represents a concatenation of two other Columns it's passed on to
	 * them). If you're using this in a <b>join</b> Predicate you will have to
	 * set the AdvResultSetMetaData of both joined Cursors - just invoke this
	 * Method with each of them, the metadata with the first match for the
	 * internal name will be used.
	 * 
	 * @param metadata
	 *            AdvResultSetMetaData of the cursor, you're calling
	 *            where/select/group_by on with this column
	 * @param newAlias
	 *            the new alias of the resulting cursor after
	 *            where/select/group_by (needed especially if this Column
	 *            contains a given value from {@link Column#val(Object, String)
	 *            val(obj, name)})
	 */
	public void setMetaData(AdvResultSetMetaData metadata, String newAlias) {
		
		// set metadata for contained columns
		if (containedColumns != null) {
			for (Column col : containedColumns) {
				col.setMetaData(metadata, newAlias);
			}
		}

		// handle metadata for this column
		if (columnMetaData == null) {
			// in the first case the "bound object" is a value from val() - and
			// that should not be
			// null or we can't create metadata for it.. also this is the only
			// case that needs the new alias
			if (boundObject != null) {
				columnMetaData = AdvResultSetMetaData.createColumnMetaData(
						boundObject.getClass(), this.columnAlias, newAlias);
			} else if (columnIndex > 0) { // the column was specified by the index
				ColumnMetaData tmp = metadata.getColumnMetaData(columnIndex);
				if (newColumnAlias == null) {
					columnMetaData = tmp;
				} else {
					try {
						columnMetaData = 
							AdvResultSetMetaData.createColumnMetaData(
								Class.forName(tmp.getColumnClassName()), 
								newColumnAlias, tmp.getTableName()
							);
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
				}
			} else { // column was specified by "alias.name"
				if (columnAlias == null || columnAlias.equals("")) {
					throw new RuntimeException(
							"You didn't specify a name, index or value for the column!");
				}
				int index = metadata.getIndexByAliasAndName(columnAlias);
				// System.out.println("aliasname: "+name+" index: "+index);
				if (index > 0) {
					columnIndex = index;
					ColumnMetaData tmp = metadata.getColumnMetaData(index);
					if (newColumnAlias == null) {
						columnMetaData = tmp;
					} else {
						try {
							columnMetaData = 
								AdvResultSetMetaData.createColumnMetaData(
									Class.forName(tmp.getColumnClassName()),
									newColumnAlias, tmp.getTableName()
								);
						} catch (Exception e) {
							throw new RuntimeException(e);
						}
					}
				} // else: see setCorrelatedTuples()
			}
		} else { // if the columnMetaData was already set
			try { // but didn't have the tablename/alias specified yet (from
				// CONCAT etc)
				if (columnMetaData.getTableName().equals("")
						&& newAlias != null && !newAlias.equals("")) {
					ColumnMetaData cmd = columnMetaData;
					// build a new one with the old class, (column)name and the
					// new alias
					columnMetaData = 
						AdvResultSetMetaData.createColumnMetaData(
							Class.forName(cmd.getColumnClassName()), 
							cmd.getColumnName(), 
							newAlias
						);
				}
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}

	/**
	 * When used in a JOIN-Predicate, set the {@link AdvResultSetMetaData}s of
	 * the the left and right Cursors that are joined. So the Column can decide
	 * whether to use the left or the right tuple in
	 * {@link Column#invoke(Tuple, Tuple) invoke(left, right)}.<br>
	 * The metadatas will be also be passed on to contained Columns.
	 * 
	 * @param leftMetaData
	 *            AdvResultSetMetaData of the left cursor being joined
	 * @param rightMetaData
	 *            AdvResultSetMetaData of the right cursor being joined
	 */
	public void setMetaDatas(AdvResultSetMetaData leftMetaData, AdvResultSetMetaData rightMetaData){
		// set metadata for contained columns
		if (containedColumns != null) {
			for (Column col : containedColumns) {
				col.setMetaDatas(leftMetaData, rightMetaData);
			}
		}
		
		if(columnIndex>0){
			throw new RuntimeException("You may NOT specify a Column with its index" 
					+" (col("+columnIndex+")) in a join predicate, because it's not clear"
					+" if it refers to the left or right Cursor being joined.");
		}

		if (haveBoundObject) { // this is a value from val()
			// the columnMetaData is needed in ADD etc (specifically checkForNumber) and the
			// comparators (createComp) to find out the Columns class
			if(columnMetaData == null)
				columnMetaData = AdvResultSetMetaData.createColumnMetaData(boundObject.getClass(), 
						this.columnAlias, "");
		} else { // this is not a value from val()
			if (columnAlias == null || columnAlias.equals("")) {
				throw new RuntimeException(
						"You didn't specify a name for a Column in a join-Predicate!");
			}

			int index = -1;
			index = leftMetaData.getIndexByAliasAndName(columnAlias);
			if (index > 0) { // the given alias.name matches to a column of the
				// left tuple, so use it
				int checkIndex = rightMetaData.getIndexByAliasAndName(columnAlias);
				if (checkIndex > 0) {
					throw new RuntimeException("You can't use \""
							+columnAlias+"\" to specify a column in the "
							+ "join-predicate, because it maps to a column in both cursors!");
				}
				// the columnMetaData is needed in ADD etc (specifically
				// checkForNumber) and the
				// comparators (createComp) to find out the Columns class
				columnMetaData = leftMetaData.getColumnMetaData(index);
				useLeftTuple = true;
			} else { // if it's not in the left cursor, it should be in the
				// right one..
				index = rightMetaData.getIndexByAliasAndName(columnAlias);
				if (index < 1) {
					throw new RuntimeException(
							"You didn't specify a valid name for a Column in a join-Predicate! ("
									+ columnAlias + ")");
				}

				columnMetaData = rightMetaData.getColumnMetaData(index);
				useLeftTuple = false; // use the right tuple.
			}

			columnIndex = index;
		}
	}
	
	/**
	 * Add columns "contained" in this one so metadata and correlated tuples can
	 * be set for them.<br>
	 * You have contained columns with CONCAT(), ADD() etc.<br>
	 * <b>Example:</b><br>
	 * <code>col("al1.nameX").CONCAT(col("al2.nameY"), "foo")</code> creates a
	 * new Column named foo from the Columns "al1.nameX" and "al2.nameY" that
	 * should be present in your cursor or the correlated tuples.<br>
	 * Metadata will be set for this Column (in select/where/...), but not for
	 * "al1.nameX" and "al2.nameY" which actually need the metadata to map to
	 * the right column.. So "foo" gets a list of those Columns and when
	 * setMetaData() or setCorrelatedTuples() is called upon "foo", it will call
	 * it on "al1.nameX" and "al2.nameY".
	 * 
	 * @param cols Columns to be contained in this Column
	 */
	public void addContainedColumns(Column... cols) {
		if (containedColumns == null) {
			containedColumns = new ArrayList<Column>(cols.length);
		}
			containedColumns.addAll(Arrays.asList(cols));
	}
	
	/**
	 * Pass a list of tuples along with their metadata from "outer" cursors in
	 * case this is a (correlated) subquery. The list has to be sorted from
	 * inside to outside (in case of nested subqueries). With the name given to
	 * this column the object will be fetched from the the first tuple with a
	 * column with according alias and name and this object will be bound to
	 * this column.<br>
	 * This is meant to be used with EXISTS, ALL and ANY clauses.
	 * 
	 * @param corrTuples
	 *            a List of tuples with their related metadata from "outer"
	 *            cursors for correlated subqueries
	 * 
	 * @see AdvTupleCursor#getCorrTuples()
	 */
	public void setCorrelatedTuples(
			List<MapEntry<AdvResultSetMetaData, Tuple>> corrTuples) {
		if (corrTuples == null) {
			return;
		}
		// set correlated Tuples for contained Columns
		if (containedColumns != null) {
			for (Column col : containedColumns) {
				col.setCorrelatedTuples(corrTuples);
			}
		}

		// fromCorrTuple => bound object is from a correlated tuple and may be overwritten
		// || this column is not fetched from the "regular" tuples and is not a "val" either
		// so we try if might be from a correlated tuple
		if (fromCorrTuple || columnIndex < 1 && !haveBoundObject) {
			// try to find the object to be bound in the correlated tuples -
			// sorted from inside to outside
			for (MapEntry<AdvResultSetMetaData, Tuple> ent : corrTuples) {
				int index = ent.getKey().getIndexByAliasAndName(this.columnAlias);
				if (index > 0) { // a valid index => get boundObject from
					// associated tuple and we're done.
					// TODO: possible optimization: if a corr tuple has been
					// used once, remember which tuple in the
					// corrTuples-list it was (and which attribute of that
					// tuple) and access it directly next time
					boundObject = ent.getValue().getObject(index);
					columnMetaData = ent.getKey().getColumnMetaData(index); // get the metadata
					haveBoundObject = true;
					fromCorrTuple = true;
					return;
				}
			}
		}
	}
	
	/**
	 * @return the index of the column in a tuple this Column represents
	 */
	public int getColumnIndex() {
		return columnIndex;
	}
	
	/**
	 * @return the {@link ColumnMetaData} for this Column, if it has a name.
	 * @throws a RuntimeException if this Column has no name (e.g. an unnamed val())
	 */
	public ColumnMetaData getColumnMetaData() {
		return columnMetaData;
	}

/* *********************************
 * Comparisons (EQ, LEQ, GT, ....) * 
 ***********************************/
	
	/**
	 * Returns an {@link AdvPredicate} comparing this Column with another Column for <b>EQ</b>uality
	 * @param col the other Column
	 */
	public AdvPredicate EQ(final Column col) {
		return EQ(col, null);
	}

	/**
	 * Returns an {@link AdvPredicate} comparing this Column with another Column for <b>EQ</b>uality
	 * with a custom {@link Comparator}
	 * @param col the other Column
	 * @param cmp your custom comparator (null for default)
	 */
	@SuppressWarnings("unchecked")
	public AdvPredicate EQ(final Column col, final Comparator cmp) {

		AdvPredicate ret = new AdvPredicate() {

			Comparator<Object>	comp	= cmp;

			@Override
			// normale case (where(), ...)
			public boolean invoke(Tuple tuple) {
				Object o1 = Column.this.invoke(tuple);
				Object o2 = col.invoke(tuple);
				if (comp == null) {
					try { // try to use a real comparator
						comp = createComp(Column.this, col);
					} catch(Exception e){
						// if that fails just use plain equals
						comp = equalsPseudoComp;
					}
				}
				return comp.compare(o1, o2) == 0;
			}

			@Override
			// case of join predicate
			public boolean invoke(Tuple lTuple, Tuple rTuple) {
				Object o1 = Column.this.invoke(lTuple, rTuple);
				Object o2 = col.invoke(lTuple, rTuple);
				if (comp == null) {
					try { // try to use a real comparator
						comp = createComp(Column.this, col);
					} catch(Exception e){
						// if that fails just use plain equals
						comp = equalsPseudoComp;
					}
				}
				return comp.compare(o1, o2) == 0;
			}

		};

		// add *this* and col as contained Columns so they will have metadata
		// and correlated tuples set later on (in where() or join())
		ret.addContainedColumns(this, col);
		return ret;

	}

	/**
	 * Returns an AdvPredicate performing  a comparison for <b>EQ</b>uality with a (possibly correlated) 
	 * ANY/ALL-subquery.<br> 
	 * <b>Note:</b> The subquery must return a cursor with <i>only a single column</i>!<br>
	 * <b>Example</b>:
	 * <code><pre> SELECT * FROM c1 
	 * 	WHERE c1.a1 = ANY (SELECT b1 FROM c2)</pre></code>
	 * will be something like: <br>
	 * <code>
	 * c1.where( col("c1.a1").EQ( SubQueryType.ANY, c2.select(col("c2.b1")) ) )</code>
	 * 
	 * @param type the {@link SubQueryType} of the subquery (SubQueryType.ANY or SubQueryType.ALL)
	 * @param subquery the subquery itself (an AdvTupleCursor), possibly correlated
	 * @return an appropriate AdvPredicate to be used in where()
	 */
	public AdvPredicate EQ(final SubQueryType type, final AdvTupleCursor subquery) {
		return EQ(type, subquery, null);
	}

	

	/**
	 * Returns an AdvPredicate performing a comparison for <b>EQ</b>uality with a custom comparator with a 
	 * (possibly correlated) ANY/ALL-subquery.<br> 
	 * <b>Note:</b> The subquery must return a cursor with <i>only a single column</i>!<br>
	 * 
	 * @param type the {@link SubQueryType} of the subquery (SubQueryType.ANY or SubQueryType.ALL)
	 * @param subquery the subquery itself (an AdvTupleCursor), possibly correlated
	 * @param cmp a {@link Comparator} to be used or <b>null</b> to use a standard comparator
	 * @return an appropriate AdvPredicate to be used in where()
	 * 
	 * @see #EQ(SubQueryType, AdvTupleCursor) EQ(type, subquery) for a small example how ANY/ALL-subqueries are used
	 */
	@SuppressWarnings( { "serial", "unchecked" })
	public AdvPredicate EQ(final SubQueryType type, final AdvTupleCursor subquery, 
					final Comparator cmp) 
	{
		Predicate<Object> pred = new AbstractPredicate<Object>() {
			Comparator<Object>	comp	= cmp;

			@Override
			public boolean invoke(Object o1, Object o2) {
				if (comp == null) { // if no comparator was set create one.
					try {
						String outerclassname = Column.this.columnMetaData.getColumnClassName();
						String innerclassname = subquery.getResultSetMetaData().getColumnClassName(1);
						// if they're of the same class plain equals is ok
						if (comp == null) {
							try { // try to use a real comparator
								comp = createComp(outerclassname, innerclassname);
							} catch(Exception e){
								// if that fails just use plain equals
								comp = equalsPseudoComp;
							}
						}
					} catch (Exception e) {
						throw new RuntimeException("Can't compare "
								+ Column.this.columnAlias + " and the subquery "
								+ subquery.getResultSetMetaData().getAlias()
								+ " because: " + e.getMessage(), e);
					}
				}
				return comp.compare(o1, o2) == 0;
			}
		};
		return any_all_subquery(type, subquery, pred);
	}
	
	/**
	 * Returns an {@link AdvPredicate} comparing this Column with another Column for being 
	 * <b>N</b>ot <b>EQ</b>ual
	 * @param col the other Column
	 */
	public AdvPredicate NEQ(final Column col) {
		return NEQ(col, null);
	}

	/**
	 * Returns an {@link AdvPredicate} comparing this Column with another Column for being 
	 * <b>N</b>ot <b>EQ</b>ual with a custom {@link Comparator}
	 * @param col the other Column
	 * @param cmp your custom comparator (null for default)
	 */
	@SuppressWarnings("unchecked")
	public AdvPredicate NEQ(final Column col, final Comparator cmp) {
		return AdvPredicate.NOT(EQ(col, cmp));
	}

	/**
	 * Returns an AdvPredicate performing a comparison for <b>N</b>ot being <b>EQ</b>ual with a 
	 * (possibly correlated) ANY/ALL-subquery.<br> 
	 * <b>Note:</b> The subquery must return a cursor with <i>only a single column</i>!<br>
	 * 
	 * @param type the {@link SubQueryType} of the subquery (SubQueryType.ANY or SubQueryType.ALL)
	 * @param subquery the subquery itself (an AdvTupleCursor), possibly correlated
	 * @return an appropriate AdvPredicate to be used in where()
	 * 
	 * @see #EQ(SubQueryType, AdvTupleCursor) EQ(type, subquery) for a small example how ANY/ALL-subqueries are used
	 */
	public AdvPredicate NEQ(final SubQueryType type, final AdvTupleCursor subquery) {
		return NEQ(type, subquery, null);
	}

	/**
	 * Returns an AdvPredicate performing a comparison for <b>N</b>ot being <b>EQ</b>ual with a custom comparator 
	 * with a (possibly correlated) ANY/ALL-subquery.<br> 
	 * <b>Note:</b> The subquery must return a cursor with <i>only a single column</i>!<br>
	 * 
	 * @param type the {@link SubQueryType} of the subquery (SubQueryType.ANY or SubQueryType.ALL)
	 * @param subquery the subquery itself (an AdvTupleCursor), possibly correlated
	 * @param cmp a {@link Comparator} to be used or <b>null</b> to use a standard comparator
	 * @return an appropriate AdvPredicate to be used in where()
	 * 
	 * @see #EQ(SubQueryType, AdvTupleCursor) EQ(type, subquery) for a small example how ANY/ALL-subqueries are used
	 */
	@SuppressWarnings("unchecked")
	public AdvPredicate NEQ(final SubQueryType type, final AdvTupleCursor subquery, 
					final Comparator cmp) 
	{
		return AdvPredicate.NOT(EQ(type, subquery, cmp));
	}
	
	/**
	 * Returns an {@link AdvPredicate} checking if this Column's value is <b>G</b>reater <b>T</b>han the other
	 * Column's value.
	 * @param col the other Column
	 */
	public AdvPredicate GT(final Column col) {
		return GT(col, null);
	}

	/**
	 * Returns an {@link AdvPredicate} checking if this Column's value is <b>G</b>reater <b>T</b>han than the other
	 * Column's value with a custom {@link Comparator}.
	 * @param col the other Column
	 * @param cmp your custom comparator (null for default)
	 */
	@SuppressWarnings("unchecked")
	public AdvPredicate GT(final Column col, final Comparator cmp) {

		AdvPredicate ret = new AdvPredicate() {
			// wenn man nen eigenen comparator setzen wollte, koennte man das
			// einfach hier tun und in
			// invoke wuerd sich nix aendern
			Comparator	comp	= cmp;

			@Override
			// normale case (where(), ...)
			public boolean invoke(Tuple tuple) {
				Object o1 = Column.this.invoke(tuple);
				Object o2 = col.invoke(tuple);
				if (comp == null) {
					try {
						comp = createComp(Column.this, col);
					} catch (Exception e) {
						throw new RuntimeException("Can't compare "
								+ Column.this.columnAlias + " and " + col.columnAlias
								+ " because: " + e.getMessage(), e);
					}
				}
				return comp.compare(o1, o2) > 0;
			}

			@Override
			// case of join predicate
			public boolean invoke(Tuple lTuple, Tuple rTuple) {
				Object o1 = Column.this.invoke(lTuple, rTuple);
				Object o2 = col.invoke(lTuple, rTuple);
				if (comp == null) {
					try {
						comp = createComp(Column.this, col);
					} catch (Exception e) {
						throw new RuntimeException("Can't compare "
								+ Column.this.columnAlias + " and " + col.columnAlias
								+ " because: " + e.getMessage(), e);
					}
				}
				return comp.compare(o1, o2) > 0;
			}

		};

		// add *this* and col as contained Columns so they will have metadata
		// and correlated tuples set later on (in where() or join())
		ret.addContainedColumns(this, col);
		return ret;
	}

	/**
	 * Returns an AdvPredicate checking if this Columns value is  <b>G</b>reater <b>T</b>han ANY/ALL 
	 * values of the (possibly correlated) subquery with a custom {@link Comparator}.<br> 
	 * <b>Note:</b> The subquery must return a cursor with <i>only a single column</i>!<br>
	 * 
	 * @param type the {@link SubQueryType} of the subquery (SubQueryType.ANY or SubQueryType.ALL)
	 * @param subquery the subquery itself (an AdvTupleCursor), possibly correlated
	 * @return an appropriate AdvPredicate to be used in where()
	 * 
	 * @see #EQ(SubQueryType, AdvTupleCursor) EQ(type, subquery) for a small example how ANY/ALL-subqueries are used
	 */
	public AdvPredicate GT(final SubQueryType type, final AdvTupleCursor subquery) {
		return GT(type, subquery, null);
	}

	/**
	 * Returns an AdvPredicate checking if this Columns value is  <b>G</b>reater <b>T</b>han ANY/ALL 
	 * values of the (possibly correlated) subquery.<br> 
	 * <b>Note:</b> The subquery must return a cursor with <i>only a single column</i>!<br>
	 * 
	 * @param type the {@link SubQueryType} of the subquery (SubQueryType.ANY or SubQueryType.ALL)
	 * @param subquery the subquery itself (an AdvTupleCursor), possibly correlated
	 * @param cmp a {@link Comparator} to be used or <b>null</b> to use a standard comparator
	 * @return an appropriate AdvPredicate to be used in where()
	 * 
	 * @see #EQ(SubQueryType, AdvTupleCursor) EQ(type, subquery) for a small example how ANY/ALL-subqueries are used
	 */
	@SuppressWarnings( { "serial", "unchecked" })
	public AdvPredicate GT(final SubQueryType type, final AdvTupleCursor subquery, final Comparator cmp){
		// a simple predicate to be used by any_all_subquery
		Predicate<Object> pred = new AbstractPredicate<Object>() {
			Comparator	comp	= cmp;

			@Override
			public boolean invoke(Object o1, Object o2) {
				if (comp == null) {
					try {
						String outerclassname = Column.this.columnMetaData.getColumnClassName();
						String innerclassname = subquery.getResultSetMetaData().getColumnClassName(1);
						comp = createComp(outerclassname, innerclassname);
					} catch (Exception e) {
						throw new RuntimeException("Can't compare "
								+ Column.this.columnAlias + " and the subquery "
								+ subquery.getResultSetMetaData().getAlias()
								+ " because: " + e.getMessage(), e);
					}
				}
				return comp.compare(o1, o2) > 0;
			}
		};
		return any_all_subquery(type, subquery, pred);
	}
	
	/**
	 * Returns an {@link AdvPredicate} checking if this Column's value is <b>L</b>ess <b>T</b>han the other
	 * Column's value.
	 * @param col the other Column
	 */
	public AdvPredicate LT(final Column col) {
		return LT(col, null);
	}

	/**
	 * Returns an {@link AdvPredicate} checking if this Column's value is <b>L</b>ess <b>T</b>han than the other
	 * Column's value with a custom {@link Comparator}.
	 * @param col the other Column
	 * @param cmp your custom comparator (null for default)
	 */
	@SuppressWarnings("unchecked")
	public AdvPredicate LT(final Column col, final Comparator cmp) {

		AdvPredicate ret = new AdvPredicate() {
			// wenn man nen eigenen comparator setzen wollte, koennte man das
			// einfach hier tun und in
			// invoke wuerd sich nix aendern
			Comparator	comp	= cmp;

			@Override
			// normale case (where(), ...)
			public boolean invoke(Tuple tuple) {
				Object o1 = Column.this.invoke(tuple);
				Object o2 = col.invoke(tuple);
				if (comp == null) {
					try {
						comp = createComp(Column.this, col);
					} catch (Exception e) {
						throw new RuntimeException("Can't compare "
								+ Column.this.columnAlias + " and " + col.columnAlias
								+ " because: " + e.getMessage(), e);
					}
				}
				return comp.compare(o1, o2) < 0;
			}

			@Override
			// case of join predicate
			public boolean invoke(Tuple lTuple, Tuple rTuple) {
				Object o1 = Column.this.invoke(lTuple, rTuple);
				Object o2 = col.invoke(lTuple, rTuple);
				if (comp == null) {
					try {
						comp = createComp(Column.this, col);
					} catch (Exception e) {
						throw new RuntimeException("Can't compare "
								+ Column.this.columnAlias + " and " + col.columnAlias
								+ " because: " + e.getMessage(), e);
					}
				}
				return comp.compare(o1, o2) < 0;
			}

		};

		// add *this* and col as contained Columns so they will have metadata
		// and correlated tuples set later on (in where() or join())
		ret.addContainedColumns(this, col);
		return ret;
	}

	/**
	 * Returns an AdvPredicate checking if this Columns value is  <b>L</b>ess <b>T</b>han ANY/ALL 
	 * values of the (possibly correlated) subquery.<br> 
	 * <b>Note:</b> The subquery must return a cursor with <i>only a single column</i>!<br>
	 * 
	 * @param type the {@link SubQueryType} of the subquery (SubQueryType.ANY or SubQueryType.ALL)
	 * @param subquery the subquery itself (an AdvTupleCursor), possibly correlated
	 * @return an appropriate AdvPredicate to be used in where()
	 * 
	 * @see #EQ(SubQueryType, AdvTupleCursor) EQ(type, subquery) for a small example how ANY/ALL-subqueries are used
	 */
	public AdvPredicate LT(final SubQueryType type, final AdvTupleCursor subquery) {
		return LT(type, subquery, null);
	}

	/**
	 * Returns an AdvPredicate checking if this Columns value is  <b>L</b>ess <b>T</b>han ANY/ALL 
	 * values of the (possibly correlated) subquery with a custom {@link Comparator}.<br> 
	 * <b>Note:</b> The subquery must return a cursor with <i>only a single column</i>!<br>
	 * 
	 * @param type the {@link SubQueryType} of the subquery (SubQueryType.ANY or SubQueryType.ALL)
	 * @param subquery the subquery itself (an AdvTupleCursor), possibly correlated
	 * @param cmp a {@link Comparator} to be used or <b>null</b> to use a standard comparator
	 * @return an appropriate AdvPredicate to be used in where()
	 * 
	 * @see #EQ(SubQueryType, AdvTupleCursor) EQ(type, subquery) for a small example how ANY/ALL-subqueries are used
	 */
	@SuppressWarnings( { "serial", "unchecked" })
	public AdvPredicate LT(final SubQueryType type, final AdvTupleCursor subquery, Comparator cmp){
		Predicate<Object> pred = new AbstractPredicate<Object>() {
			Comparator	comp	= null;

			@Override
			public boolean invoke(Object o1, Object o2) {
				if (comp == null) {
					try {
						String outerclassname = Column.this.columnMetaData.getColumnClassName();
						String innerclassname = subquery.getResultSetMetaData().getColumnClassName(1);
						comp = createComp(outerclassname, innerclassname);
					} catch (Exception e) {
						throw new RuntimeException("Can't compare "
								+ Column.this.columnAlias + " and the subquery "
								+ subquery.getResultSetMetaData().getAlias()
								+ " because: " + e.getMessage(), e);
					}
				}
				return comp.compare(o1, o2) < 0;
			}
		};
		return any_all_subquery(type, subquery, pred);
	}
	
	/**
	 * Returns an {@link AdvPredicate} checking if this Column's value is 
	 * <b>G</b>reater or <b>EQ</b>ual than the other Column's value.
	 * @param col the other Column
	 */
	public AdvPredicate GEQ(final Column col) {
		return GEQ(col, null);
	}

	/**
	 * Returns an {@link AdvPredicate} checking if this Column's value is <b>G</b>reater 
	 * or <b>EQ</b>ual than the other Column's value with a custom {@link Comparator}.
	 * @param col the other Column
	 * @param cmp your custom comparator (null for default)
	 */
	@SuppressWarnings("unchecked")
	public AdvPredicate GEQ(final Column col, final Comparator cmp) {
		return AdvPredicate.NOT(LT(col, cmp));
	}

	/**
	 * Returns an AdvPredicate checking if this Columns value is <b>G</b>reater or <b>EQ</b>ual
	 * than ANY/ALL values of the (possibly correlated) subquery.<br> 
	 * <b>Note:</b> The subquery must return a cursor with <i>only a single column</i>!<br>
	 * 
	 * @param type the {@link SubQueryType} of the subquery (SubQueryType.ANY or SubQueryType.ALL)
	 * @param subquery the subquery itself (an AdvTupleCursor), possibly correlated
	 * @return an appropriate AdvPredicate to be used in where()
	 * 
	 * @see #EQ(SubQueryType, AdvTupleCursor) EQ(type, subquery) for a small example how ANY/ALL-subqueries are used
	 */
	public AdvPredicate GEQ(final SubQueryType type, final AdvTupleCursor subquery) {
		return GEQ(type, subquery, null);
	}

	/**
	 * Returns an AdvPredicate checking if this Columns value is  <b>G</b>reater or <b>EQ</b>ual
	 * than ANY/ALL values of the (possibly correlated) subquery with a custom {@link Comparator}.<br> 
	 * <b>Note:</b> The subquery must return a cursor with <i>only a single column</i>!<br>
	 * 
	 * @param type the {@link SubQueryType} of the subquery (SubQueryType.ANY or SubQueryType.ALL)
	 * @param subquery the subquery itself (an AdvTupleCursor), possibly correlated
	 * @param cmp a {@link Comparator} to be used or <b>null</b> to use a standard comparator
	 * @return an appropriate AdvPredicate to be used in where()
	 * 
	 * @see #EQ(SubQueryType, AdvTupleCursor) EQ(type, subquery) for a small example how ANY/ALL-subqueries are used
	 */
	@SuppressWarnings("unchecked")
	public AdvPredicate GEQ(final SubQueryType type, final AdvTupleCursor subquery, 
			final Comparator cmp) 
	{
		return AdvPredicate.NOT(LT(type, subquery, cmp));
	}
	
	/**
	 * Returns an {@link AdvPredicate} checking if this Column's value is <b>L</b>ess 
	 * or <b>EQ</b>ual than the other Column's value.
	 * @param col the other Column
	 * @param cmp your custom comparator (null for default)
	 */
	public AdvPredicate LEQ(final Column col) {
		return LEQ(col, null);
	}

	/**
	 * Returns an {@link AdvPredicate} checking if this Column's value is <b>L</b>ess 
	 * or <b>EQ</b>ual than the other Column's value with a custom {@link Comparator}.
	 * @param col the other Column
	 * @param cmp your custom comparator (null for default)
	 */
	@SuppressWarnings("unchecked")
	public AdvPredicate LEQ(final Column col, final Comparator cmp) {
		return AdvPredicate.NOT(GT(col, cmp));
	}

	/**
	 * Returns an AdvPredice checking if this Columns value is  <b>L</b>ess or <b>EQ</b>ual
	 * than ANY/ALL values of the (possibly correlated) subquery.<br> 
	 * <b>Note:</b> The subquery must return a cursor with <i>only a single column</i>!<br>
	 * 
	 * @param type the {@link SubQueryType} of the subquery (SubQueryType.ANY or SubQueryType.ALL)
	 * @param subquery the subquery itself (an AdvTupleCursor), possibly correlated
	 * @return an appropriate AdvPredicate to be used in where()
	 * 
	 * @see #EQ(SubQueryType, AdvTupleCursor) EQ(type, subquery) for a small example how ANY/ALL-subqueries are used
	 */
	public AdvPredicate LEQ(final SubQueryType type, final AdvTupleCursor subquery) {
		return LEQ(type, subquery, null);
	}

	/**
	 * Returns an AdvPredice checking if this Columns value is  <b>L</b>ess or <b>EQ</b>ual
	 * than ANY/ALL values of the (possibly correlated) subquery with a custom {@link Comparator}.<br> 
	 * <b>Note:</b> The subquery must return a cursor with <i>only a single column</i>!<br>
	 * 
	 * @param type the {@link SubQueryType} of the subquery (SubQueryType.ANY or SubQueryType.ALL)
	 * @param subquery the subquery itself (an AdvTupleCursor), possibly correlated
	 * @param cmp a {@link Comparator} to be used or <b>null</b> to use a standard comparator
	 * @return an appropriate AdvPredicate to be used in where()
	 * 
	 * @see #EQ(SubQueryType, AdvTupleCursor) EQ(type, subquery) for a small example how ANY/ALL-subqueries are used
	 */
	@SuppressWarnings("unchecked")
	public AdvPredicate LEQ(final SubQueryType type, final AdvTupleCursor subquery, 
					final Comparator cmp)
	{
		return AdvPredicate.NOT(GT(type, subquery, cmp));
	}
	
	/**
	 * Returns an {@link AdvPredicate} checking if this {@link Column}s value as a String (using 
	 * {@link Object#toString()}) matches with the given regular expression.<br>
	 * The regular expression uses the java.util.regex classes, so see {@link Pattern} for
	 * details on how the regex has to look like.
	 * 
	 * @param regex the regular expression used for comparison
	 * @return an AdvPredicate checking if this Columns value as a String matches the given regex
	 */
	public AdvPredicate LIKE(final String regex){
		AdvPredicate ret = new AdvPredicate() {
			private Pattern p;
			
			{
				try {
					p = Pattern.compile(regex);
				} catch (PatternSyntaxException e) {
					throw new RuntimeException("You used an illegal regular expression pattern in "
							+"LIKE("+regex+"): "+e.getMessage(), e);
				}
			}
			
			@Override
			// normale case (where(), ...)
			public boolean invoke(Tuple tuple) {
				Object o = Column.this.invoke(tuple);
				Matcher m = p.matcher(o.toString());
				return m.matches();
			}

			@Override
			// case of join predicate
			public boolean invoke(Tuple lTuple, Tuple rTuple) {
				Object o = Column.this.invoke(lTuple, rTuple);
				Matcher m = p.matcher(o.toString());
				return m.matches();
			}

		};
		
		return ret;
	}
	
/* ***************************************************
 * Operations (mostly arithmetic like ADD, MUL, ...) *
 * ***************************************************/
	
	/**
	 * @param col the Column this Column will be concatenated with
	 * @return a Column containing a String that is a concatenaton of the strings (Object.toString)
	 * from this Column and col
	 */
	public Column CONCAT(Column col) {
		return CONCAT(col, null);
	}

	/**
	 * @param col the Column this Column will be concatenated with
	 * @param name the created Columns name
	 * @return a Column with given name containing a String that is a concatenaton of the strings 
	 * (Object.toString) from this Column and col
	 */
	@SuppressWarnings("serial")
	public Column CONCAT(final Column col, String name) {
		final boolean noName = (name == null);
		if (name == null) {
			name = "concat";
		}
		// den alias setzen wir spaeter in setMetaData() .. bisschen hackish,
		// aber mir faellt nix besseres ein :/
		final ColumnMetaData cmd = AdvResultSetMetaData.createColumnMetaData(
				String.class, name, null);
		Column ret = new Column(name, cmd) {

			@Override
			public ColumnMetaData getColumnMetaData() {
				if(noName)
					throw new RuntimeException("If you want to use CONCAT to create a new column"
							+" in in a tuple (e.g. in select()), give it a name, i.e. use" 
							+" CONCAT(col, name)!");
				else
					return super.getColumnMetaData();
			}
			
			@Override
			public String invoke(Tuple tuple) {

				return Column.this.invoke(tuple).toString().concat(
						col.invoke(tuple).toString());
			}

			@Override
			// fuer den join-fall (columns koennen aus linkem oder rechtem tupel
			// sein)
			public Object invoke(Tuple left, Tuple right) {
				return Column.this.invoke(left, right).toString()
				.concat(col.invoke(left, right).toString());
			}
		};
		// add *this* and col as contained Columns so they'll get metadata and
		// correlated tuples
		ret.addContainedColumns(this, col);
		return ret;
	}
	
	/**
	 * Performs summation of this Column's value with <b>col</b>'s value.<br>
	 * <i>Both need to contain Numbers, i.e. evaluate to values of a class 
	 * <b>derived from {@link java.lang.Number}</b>!</i>
	 * 
	 * @param col
	 * @return a Column containing the result of the calculation as a {@link Double}
	 */
	public Column ADD(final Column col) {
		return ADD(col, null);
	}
	
	/**
	 * Performs summation ("plus") of this Column's value with <b>col</b>'s value.<br>
	 * <i>Both need to contain Numbers, i.e. evaluate to values of a class 
	 * <b>derived from {@link java.lang.Number}</b>!</i>
	 * 
	 * @param col
	 * @param name name of the new Column (if it's to be an actual column in a tuple)
	 * @return a Column containing the result of the calculation as a {@link Double}
	 */
	public Column ADD(final Column col, String name) {
		Function<Number, Double> op = new AbstractFunction<Number, Double>() {
			private static final long	serialVersionUID	= 1L;

			@Override
			public Double invoke(Number n1, Number n2) {
				return n1.doubleValue() + n2.doubleValue();
			}
		};
		return arithm_op(this, col, op, name);
	}
	
	/**
	 * Performs subtraction ("minus") of this Column's value with <b>col</b>'s value.<br>
	 * <i>Both need to contain Numbers, i.e. evaluate to values of a class 
	 * <b>derived from {@link java.lang.Number}</b>!</i>
	 * 
	 * @param col
	 * @return a Column containing the result of the calculation as a {@link Double}
	 */
	public Column SUB(final Column col) {
		return SUB(col, null);
	}

	/**
	 * Performs subtraction ("minus") of this Column's value with <b>col</b>'s value.<br>
	 * <i>Both need to contain Numbers, i.e. evaluate to values of a class 
	 * <b>derived from {@link java.lang.Number}</b>!</i>
	 * 
	 * @param col
	 * @param name name of the new Column (if it's to be an actual column in a tuple)
	 * @return a Column containing the result of the calculation as a {@link Double}
	 */
	public Column SUB(final Column col, String name) {
		Function<Number, Double> op = new AbstractFunction<Number, Double>() {
			private static final long	serialVersionUID	= 1L;

			@Override
			public Double invoke(Number n1, Number n2) {
				return n1.doubleValue() - n2.doubleValue();
			}
		};
		return arithm_op(this, col, op, name);
	}

	/**
	 * Performs multiplication of this Column's value with <b>col</b>'s value.<br>
	 * <i>Both need to contain Numbers, i.e. evaluate to values of a class 
	 * <b>derived from {@link java.lang.Number}</b>!</i>
	 * 
	 * @param col
	 * @return a Column containing the result of the calculation as a {@link Double}
	 */
	public Column MUL(final Column col) {
		return MUL(col, null);
	}
	
	/**
	 * Performs multiplication of this Column's value with <b>col</b>'s value.<br>
	 * <i>Both need to contain Numbers, i.e. evaluate to values of a class 
	 * <b>derived from {@link java.lang.Number}</b>!</i>
	 * 
	 * @param col
	 * @param name name of the new Column (if it's to be an actual column in a tuple)
	 * @return a Column containing the result of the calculation as a {@link Double}
	 */
	public Column MUL(final Column col, String name) {
		Function<Number, Double> op = new AbstractFunction<Number, Double>() {
			private static final long	serialVersionUID	= 1L;

			@Override
			public Double invoke(Number n1, Number n2) {
				return n1.doubleValue() * n2.doubleValue();
			}
		};
		return arithm_op(this, col, op, name);
	}
	
	/**
	 * Performs division of this Column's value by <b>col</b>'s value.<br>
	 * <i>Both need to contain Numbers, i.e. evaluate to values of a class 
	 * <b>derived from {@link java.lang.Number}</b>!</i>
	 * 
	 * @param col
	 * @return a Column containing the result of the calculation as a {@link Double}
	 */
	public Column DIV(final Column col) {
		return DIV(col, null);
	}

	/**
	 * Performs division of this Column's value by <b>col</b>'s value.<br>
	 * <i>Both need to contain Numbers, i.e. evaluate to values of a class 
	 * <b>derived from {@link java.lang.Number}</b>!</i>
	 * 
	 * @param col
	 * @param name name of the new Column (if it's to be an actual column in a tuple)
	 * @return a Column containing the result of the calculation as a {@link Double}
	 */
	public Column DIV(final Column col, String name) {
		Function<Number, Double> op = new AbstractFunction<Number, Double>() {
			private static final long	serialVersionUID	= 1L;

			@Override
			public Double invoke(Number n1, Number n2) {
				return n1.doubleValue() / n2.doubleValue();
			}
		};
		return arithm_op(this, col, op, name);
	}
	
	/**
	 * Performs an exponentiation with this Column's value being the base and
	 * <b>col</b>'s value being the exponent.<br>
	 * <i>Both need to contain Numbers, i.e. evaluate to values of a class 
	 * <b>derived from {@link java.lang.Number}</b>!</i>
	 * 
	 * @param col
	 * @return a Column containing the result of the calculation as a {@link Double}
	 */
	public Column POW(final Column col) {
		return POW(col, null);
	}

	/**
	 * Performs an exponentiation with this Column's value being the base and
	 * <b>col</b>'s value being the exponent.<br>
	 * <i>Both need to contain Numbers, i.e. evaluate to values of a class 
	 * <b>derived from {@link java.lang.Number}</b>!</i>
	 * 
	 * @param col
	 * @param name name of the new Column (if it's to be an actual column in a tuple)
	 * @return a Column containing the result of the calculation as a {@link Double}
	 */
	public Column POW(final Column col, String name) {
		Function<Number, Double> op = new AbstractFunction<Number, Double>() {
			private static final long	serialVersionUID	= 1L;

			@Override
			public Double invoke(Number n1, Number n2) {
				return Math.pow(n1.doubleValue(), n2.doubleValue());
			}
		};
		return arithm_op(this, col, op, name);
	}
	
	
/* ****************************************************************************
 * Helping functions that make implementing operations and comparisons easier *
 ******************************************************************************/
	
	/**
	 * Creates an ALL/ANY subquery on the given cursor with the given Predicate.<br>
	 * <b>Example</b>:<br>
	 * In AdvPredicate EQ(type, subquery) you just create a Predicate that
	 * compares two Objects(!) for equality and pass it on to this method: <br>
	 * <code><pre> public AdvPredicate simple_EQ(final SubQueryType type, final AdvTupleCursor subquery){
	 * 	Predicate<Object> pred = new AbstractPredicate<Object>() {
	 * 		{@code @Override}
	 * 		public boolean invoke(Object o1, Object o2) {
	 *			return o1.equals(o2);
	 *		}
	 * 	};
	 * 	return any_all_subquery(type, subquery, pred);
	 * } </pre></code>
	 * 
	 * @param type
	 *            the {@link SubQueryType} of the subquery (SubQueryType.ANY or
	 *            SubQueryType.ALL)
	 * @param subquery
	 *            the subquery itself, possibly correlated
	 * @param pred
	 *            the predicate used to compare "outer" and "inner" Objects
	 * @return an appropriate AdvPredicate to be used in where()
	 */
	protected AdvPredicate any_all_subquery(final SubQueryType type, final AdvTupleCursor subquery,
			final Predicate<Object> pred) 
	{
		// subquery should not cache - except for the first cursor, so it's resettable.
		// so if "only cache if absolutely necessary" via MAYBE_FIRST isn't
		// enforced, we'll enable caching the first cursor (even if the wrapped cursor 
		// is resettable)
		if (!subquery.getCachingStrategy().equals(CachingStrategy.MAYBE_FIRST)
				&& !subquery.getCachingStrategy().equals(CachingStrategy.ONLY_FIRST)) {
			subquery.setCachingStrategy(CachingStrategy.ONLY_FIRST, true);
		}

		AdvPredicate ret = new AdvPredicate() {

			@Override
			public boolean invoke(Tuple tuple) {

				/* ********************************
				 * correlated tuples for subquery *
				 **********************************/
				// the current tuple
				MapEntry<AdvResultSetMetaData, Tuple> newCorrTuple = 
					new MapEntry<AdvResultSetMetaData, Tuple>(getMetaData(), tuple);

				// create list of correlated Tuples (current tuple and the ones
				// "inherited" from the containing cursor)
				List<MapEntry<AdvResultSetMetaData, Tuple>> corrTuples = 
						new LinkedList<MapEntry<AdvResultSetMetaData, Tuple>>();
				corrTuples.add(newCorrTuple); // add current tuple first
				if (getCorrelatedTuples() != null) {
					corrTuples.addAll(getCorrelatedTuples()); // tuples that were set in this 
								// predicate via setCorrelatedTuples()
				}

				// set correlated tuples in subquery's CorrTuplesReceivers (predicates, ..)
				for (CorrTuplesReceiver ctr : subquery.getCorrTuplesRec()) {
					ctr.setCorrelatedTuples(corrTuples);
				}

				/* ***********************
				 * actual implementation * 
				 *************************/

				try {
					if (subquery.getResultSetMetaData().getColumnCount() != 1) {
						throw new RuntimeException("Subqueries used with ANY/ALL are supposed to"
								+" return just a single value per Tuple!");
					}
				} catch (SQLException e) {
					throw new RuntimeException(e);
				}
				/*
				 * if this is an ANY-subquery the result is false by default,
				 * because just one "true" tuple from the inner cursor is
				 * sufficient for this ANY-clause to be "true" if this is an
				 * ALL-subquery the result is true by default, because just one
				 * "false" tuple from the inner cursor is sufficient for this
				 * ALL-clause to be "false"
				 */
				boolean result = type == SubQueryType.ANY ? false : true;
				// the "outer" object - from the current tuple WHERE
				Object outerObj = Column.this.invoke(tuple);

				while (subquery.hasNext()) {
					// the "inner" Object from the subquery - remember the Tuple
					// should have only one column, so it's at position 1
					Object innerObj = subquery.next().getObject(1);

					// in case of ANY: on first pred returning true, result is
					// set to true and we break
					// in case of ALL: on first pred returning false, result is
					// set to false and we break
					if (pred.invoke(outerObj, innerObj) != result) {
						result = !result;
						break;
					}
				}

				subquery.reset(); // reset subquery for next tuple

				return result;
			}

			@Override
			public boolean invoke(Tuple argument0, Tuple argument1) {
				throw new UnsupportedOperationException(
						"Don't use ANY or ALL in a JOIN-predicate.");
			}

		};

		// add *this* as contained Column so it will have metadata
		// and correlated tuples set later on (in where())
		ret.addContainedColumns(this);
		return ret;
	}
	
	/**
	 * Creates a {@link Comparator} for Objects of the given classes, if they're
	 * {@link Comparable} (if they're not, an Exception will be thrown). <br>
	 * If they're Numbers, they will be casted to {@link Number} and their
	 * double values will be compared.<br>
	 * If they're not numbers one class needs to be a subclass of the other
	 * class (or they need to be of the same class) or they can't be compared.
	 * 
	 * @param cl1 type of the left object to be compared
	 * @param cl2 type of the right object to be compared
	 * @return a Comparator comparing Objects of type cl1 and cl2 with cl1 as
	 *         left argument and cl2 as right argument
	 * @throws Exception if the given classes can't be compared
	 */
	@SuppressWarnings("unchecked")
	static Comparator<Object> createComp(Class<?> cl1, Class<?> cl2)
			throws Exception {
		if (!Comparable.class.isAssignableFrom(cl1)) {
			throw new Exception("Class " + cl1.getName()
					+ " is not comparable!");
		}
		if (!Comparable.class.isAssignableFrom(cl2)) {
			throw new Exception("Class " + cl2.getName()
					+ " is not comparable!");
		}
		Comparator<Object> cmp = null;

		if (cl1.isAssignableFrom(cl2)) { // cl2 subclass of cl1 (or same class)
			cmp = new Comparator<Object>() {

				@Override
				public int compare(Object obj1, Object obj2) {
					if(obj1 == null){ // we don't want nullpointer-exceptions
						if(obj2 == null)
							return 0;
						return -1; // null is smaller than anything else
					} else if(obj2 == null)
						return 1;
					Comparable o1 = (Comparable) obj1;
					Comparable o2 = (Comparable) obj2;
					
					return o1.compareTo(o2);
				}
			};
		} else if (cl2.isAssignableFrom(cl1)) { // cl1 subclass of cl2
			cmp = new Comparator<Object>() {

				@Override
				public int compare(Object obj1, Object obj2) {
					if(obj1 == null){
						if(obj2 == null)
							return 0;
						return 1;
					} else if(obj2 == null)
						return -1;
					
					Comparable o1 = (Comparable) obj1;
					Comparable o2 = (Comparable) obj2;
					return -1 * o2.compareTo(o1);
				}
			};
		} else if (Number.class.isAssignableFrom(cl1)
				&& Number.class.isAssignableFrom(cl2)) {
			// both are numbers
			cmp = new Comparator<Object>() {

				@Override
				public int compare(Object o1, Object o2) {
					if(o1 == null){ 
						if(o2 == null)
							return 0;
						return -1; 
					} else if(o2 == null)
						return 1;
					
					Number n1 = (Number) o1;
					Number n2 = (Number) o2;
					return ComparableComparator.DOUBLE_COMPARATOR
						.compare(n1.doubleValue(), n2.doubleValue());
				}
			};
		}
		if (cmp != null) {
			return cmp;
		} else {
			throw new Exception("Can't compare those types: " + cl1.getName()
					+ " and " + cl2.getName());
		}
	}

	/**
	 * Creates a {@link Comparator} for Objects contained in the given Columns,
	 * if they're {@link Comparable} (if they're not, an Exception will be
	 * thrown). <br>
	 * If they're Numbers, they will be casted to {@link Number} and their
	 * double values will be compared.<br>
	 * If they're not numbers one class needs to be a subclass of the other
	 * class (or they need to be of the same class) or they can't be compared.
	 * 
	 * @param col1
	 *            Column containing the left object to be compared
	 * @param col2
	 *            Column containing the right object to be compared
	 * @return a Comparator comparing Objects of given types with col1's Object
	 *         as left argument and col2's Object as right argument
	 * @throws Exception
	 *             if the given classes can't be compared
	 */
	public static Comparator<Object> createComp(Column col1, Column col2)
			throws Exception {
		Class<?> cl1;
		Class<?> cl2;
		try {
			cl1 = Class.forName(col1.columnMetaData.getColumnClassName());
			cl2 = Class.forName(col2.columnMetaData.getColumnClassName());
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return createComp(cl1, cl2);
	}

	/**
	 * Creates a {@link Comparator} for Objects of the given classes, if they're
	 * {@link Comparable} (if they're not, an Exception will be thrown). <br>
	 * If they're Numbers, they will be casted to {@link Number} and their
	 * double values will be compared.<br>
	 * If they're not numbers one class needs to be a subclass of the other
	 * class (or they need to be of the same class) or they can't be compared.
	 * 
	 * @param cl1
	 *            class name of the left object to be compared
	 * @param cl2
	 *            class name of the right object to be compared
	 * @return a Comparator comparing Objects of type cl1 and cl2 with cl1 as
	 *         left argument and cl2 as right argument
	 * @throws Exception
	 *             if the given classes can't be compared
	 */
	static Comparator<Object> createComp(String className1, String className2)
			throws Exception {
		Class<?> cl1;
		Class<?> cl2;
		try {
			cl1 = Class.forName(className1);
			cl2 = Class.forName(className2);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return createComp(cl1, cl2);
	}
	
	/**
	 * Pseudo-Comparator that compares two Objects for equality and returns 0 if
	 * they're equal and 1 if they aren't. To be used with EQ and NEQ when both
	 * Objects are of the same type.
	 */
	public static Comparator<Object> equalsPseudoComp = new Comparator<Object>() {
		@Override
		public int compare(Object o1, Object o2) {
			if(o1 == null || o2 == null) // prevent nullpointer exception
				return (o1 == o2) ? 0 : 1; // if o2 is also null they're equal, else they're not
			
			return o1.equals(o2) ? 0 : 1;
		}
	};
	
	/**
	 * Used to implement arithmetic operations with two parameters like ADD,
	 * SUB, ...<br>
	 * You just have to supply a Function that does the actual operation.<br>
	 * The name will be the name of the new Column (in case you want to create a
	 * new one in select). The name may be <i>null</i> if the resulting Column
	 * is just to be used in an arithmetic operation or a comparison - but in
	 * that case getColumnMetaData() will throw an Exception to make sure it
	 * really isn't used in select.<br>
	 * The Function <b>op</b> should have {@code invoke(Number n1, Number n2)}
	 * implemented to perform the operation.<br>
	 * <b>Example:</b><br>
	 * 
	 * <pre>
	 * <code> // for ADD
	 * Function<Number, Double> op = new AbstractFunction<Number, Double>() {
	 * 	{@code @Override}
	 * 	public Double invoke(Number n1, Number n2) {
	 * 		return n1.doubleValue() + n2.doubleValue();
	 * 	}
	 * };</pre</code>
	 * 
	 * @param col1 the first Column
	 * @param col2 the second Column on that
	 * @param op a Function performing the operation
	 * @param name the name for the resulting Column (or <i>null</i> if none is needed)
	 * @return a Column performing the arithmetic operation specified in op on col1 and col2
	 */
	protected static Column arithm_op(final Column col1, final Column col2,
			final Function<Number, Double> op, String name) 
	{
		final boolean noName = (name == null); // indicates that no name was
		// provided for this operation
		if (noName) { // if this is a temporal column for a comparison
			name = "val_from_arith_op"; // bogus-name so createColumnMetaData doesn't explode
		}

		// columnmetadata for this Column - the alias will be set later in setMetaData().. 
		// this is a bit hackish but i haven't got a better idea on how to do this
		final ColumnMetaData cmd = 
			AdvResultSetMetaData.createColumnMetaData(Double.class, name, null);
		
		Column ret = new Column(name, cmd) {
			private static final long	serialVersionUID	= 1L;
			// indicates whether the columns have already been checked for
			// really containing numbers
			boolean						checked				= false;

			@Override
			public ColumnMetaData getColumnMetaData() {
				if (noName) {
					// like val: if no name was given this may only be used in
					// comparisons, arithmetic
					// operations etc, but not to create a column for a tuple
					// (in select)!
					throw new UnsupportedOperationException(
							"If you want to use an arithmetic operation"
								+ " (ADD, SUB, ...) to create a real column within a tuple, give"
								+" it a name (i.e. use ADD(col, name) etc)!");
				} else {
					return super.getColumnMetaData();
				}
			}

			@Override
			public Object invoke(Tuple tuple) {
				// make sure the columns contain numbers - this check can't be done any sooner
				// (e.g. when creating this Column) because we have to be sure 
				// col.setMetaData(..) has been called: the Columns (generally) don't "know" 
				// their type before that
				if (!checked) { 
					checkForNumber(col1, col2);
					checked = true;
				}
				Number val1 = (Number) col1.invoke(tuple);
				Number val2 = (Number) col2.invoke(tuple);

				return op.invoke(val1, val2);
			}

			@Override
			// the join-case: columns may be from the left or right tuple
			public Object invoke(Tuple left, Tuple right) {
				if (!checked) { // make sure the columns contain numbers
					checkForNumber(col1, col2);
					checked = true;
				}
				Number val1 = (Number) col1.invoke(left, right);
				Number val2 = (Number) col2.invoke(left, right);

				return op.invoke(val1, val2);
			}
		};
		// add col1 and col2 as contained Columns so they'll get metadata and
		// correlated tuples
		ret.addContainedColumns(col1, col2);
		return ret;
	}

	/**
	 * Checks whether the values stored in the {@link Column}s are subclasses of
	 * {@link java.lang.Number}.<br>
	 * If they're not, no arithmetic operations like ADD can be performed onthem.
	 * 
	 * @param col1
	 *            the first Column to be checked
	 * @param col2
	 *            the second Column to be checked
	 */
	protected static void checkForNumber(Column col1, Column col2) {
		Class<?> cl1;
		Class<?> cl2;
		try {
			cl1 = Class.forName(col1.columnMetaData.getColumnClassName());
			cl2 = Class.forName(col2.columnMetaData.getColumnClassName());
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		if (!Number.class.isAssignableFrom(cl1)) {
			throw new RuntimeException(col1.columnAlias + " contains a value of type "
					+ cl1.getName() + " that is not a Number!");
		}
		if (!Number.class.isAssignableFrom(cl2)) {
			throw new RuntimeException(col2.columnAlias + " contains a value of type "
					+ cl2.getName() + " that is not a Number!");
		}

	}
}
