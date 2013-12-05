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

import java.util.List;

import xxl.core.collections.MapEntry;
import xxl.core.relational.metaData.ColumnMetaData;
import xxl.core.relational.tuples.Tuple;
import xxl.core.xxql.AdvPredicate;
import xxl.core.xxql.AdvResultSetMetaData;
import xxl.core.xxql.AdvTupleCursor;

/**
 * A class containing several static functions that deal with {@link Column}s
 */
@SuppressWarnings("serial")
public class ColumnUtils {

	/**
	 * Creates a Column of the given class by calling its constructor with the given arguments.<br>
	 * If no suitable constructor for the given arguments is found, an Exception will be thrown.
	 * This takes the first constructor with matching parameters, so it might not be the same that
	 * would be called in "real java code". And varargs constructors might not work.<br>
	 * <i>Example:</i><br> If there is a constructor <code>Foo(Bar x)</code> and one 
	 * <code>Foo(Object x)</code> and you try to invoke it with a <code>Bar</code> argument, 
	 * but (by coincidence) the <code>Foo(Object x)</code> constructor - that won't fail when
	 * being invoked with a Bar-object - is tried first, it will be used. Emulating the "original"
	 * java behaviour would be really difficult as javas reflection API has no useful functionality
	 * to return the constructor (or Method) that would be used when being invoked with given 
	 * parameters: You have to iterate through the method and try to find one that fits..<br>
	 * However, in many/most cases this should work as expected.<br>
	 * <b>Note:</b> To make sure to call the constructor you want you can cast the arguments via
	 * {@link #colCAST(Column, Class)}: if the arguments match the exact constructor signature the
	 * right constructor will be used.
	 * @param clazz you want a new Object of this class.
	 * @param arguments the arguments to invoke the constructor with
	 * @return a Column returning objects of clazz created with the given arguments
	 */
	public static Column colNEW(Class<?> clazz, Column... arguments) {
		return ColumnUtils.colNEW(null, clazz, arguments);
	}

	/**
	 * Creates a Column of the given class by calling its constructor with the given arguments.<br>
	 * If no suitable constructor for the given arguments is found, an Exception will be thrown.
	 * This takes the first constructor with matching parameters, so it might not be the same that
	 * would be called in "real java code". And varargs constructors might not work.<br>
	 * <i>Example:</i> If there is a constructor <code>Foo(Bar x)</code> and one 
	 * <code>Foo(Object x)</code> and you try to invoke it with a <code>Bar</code> argument, 
	 * but (by coincidence) the <code>Foo(Object x)</code> constructor - that won't fail when
	 * being invoked with a Bar-object - is tried first, it will be used. Emulating the "original"
	 * java behaviour would be really difficult as javas reflection API has no useful functionality
	 * to return the constructor (or Method) that would be used when being invoked with given 
	 * parameters: You have to iterate through the method and try to find one that fits..<br>
	 * However, in many/most cases this should work as expected.<br>
	 * <b>Note:</b> To make sure to call the method you want you can cast the arguments via
	 * {@link #colCAST(Column, Class)}: if the arguments match the exact method signature the
	 * right method will be used.<br>
	 * <b>Note:</b> To make sure to call the constructor you want you can cast the arguments via
	 * {@link #colCAST(Column, Class)}: if the arguments match the exact constructor signature the
	 * right constructor will be used.
	 * 
	 * @param name the name of the new Column (needed to create a new column in a Tuple)
	 * @param clazz you want a new Object of this class.
	 * @param arguments the arguments to invoke the constructor with
	 * @return a Column returning objects of clazz created with the given arguments
	 */
	public static Column colNEW(String name, final Class<?> clazz, final Column... cols) {
		// indicates that no name was provided for this operation
		final boolean noName = (name == null); 
		if (noName) { // if this is a temporal column for a comparison
			name = "REFLECTION_NEW"; // bogus-name so createColumnMetaData doesn't explode
		}

		Column ret = new ConstructorColumn(name, clazz, cols);
		ret.addContainedColumns(cols);
		return ret;
	}

	/**
	 * Creates a Column executing a Method with the given name and arguments on the Object
	 * returned by col.<br>
	 * If no suitable (non-void) method for the given arguments is found, an Exception will be 
	 * thrown. This takes the first method with matching parameters, so it might not be the same 
	 * that would be called in "real java code". And varargs methods might not work.<br>
	 * <i>Example:</i> If there is a method <code>foo(Bar x)</code> and one 
	 * <code>foo(Object x)</code> and you try to invoke it with a <code>Bar</code> argument, 
	 * but (by coincidence) the <code>foo(Object x)</code> method - that won't fail when
	 * being invoked with a Bar-object - is tried first, it will be used. Emulating the "original"
	 * java behaviour would be really difficult as javas reflection API has no useful functionality
	 * to return the Method that would be used when being invoked with given 
	 * parameters: You have to iterate through the method and try to find one that fits..<br>
	 * However, in many/most cases this should work as expected.<br>
	 * <b>Note:</b> To make sure to call the method you want you can cast the arguments via
	 * {@link #colCAST(Column, Class)}: if the arguments match the exact method signature the
	 * right method will be used.
	 * 
	 * @param col Column returning the Object the Method will be called on
	 * @param methodName the name of the Method to be used
	 * @param arguments the arguments to invoke the method with
	 * @return a Column returning objects returned by the method invoked with the given arguments
	 */
	public static Column colOBJCALL(Column col, String methodName, Column... cols) {
		return ColumnUtils.colOBJCALL(null, col, methodName, cols);
	}

	/**
	 * Creates a Column executing a Method with the given name and arguments on the Object
	 * returned by col.<br>
	 * If no suitable (non-void) method for the given arguments is found, an Exception will be 
	 * thrown. This takes the first method with matching parameters, so it might not be the same 
	 * that would be called in "real java code". And varargs methods might not work.<br>
	 * 
	 * <i>Example:</i> If there is a method <code>foo(Bar x)</code> and one 
	 * <code>foo(Object x)</code> and you try to invoke it with a <code>Bar</code> argument, 
	 * but (by coincidence) the <code>foo(Object x)</code> method - that won't fail when
	 * being invoked with a Bar-object - is tried first, it will be used. Emulating the "original"
	 * java behaviour would be really difficult as javas reflection API has no useful functionality
	 * to return the Method that would be used when being invoked with given 
	 * parameters: You have to iterate through the method and try to find one that fits..<br>
	 * However, in many/most cases this should work as expected.<br>
	 * <b>Note:</b> To make sure to call the method you want you can cast the arguments via
	 * {@link #colCAST(Column, Class)}: if the arguments match the exact method signature the
	 * right method will be used.
	 * 
	 * @param name the name of the returned Column
	 * @param col Column returning the Object the Method will be called on
	 * @param methodName the name of the Method to be used
	 * @param arguments the arguments to invoke the method with
	 * @return a Column returning objects returned by the method invoked with the given arguments
	 */
	public static Column colOBJCALL(String name, final Column col,
			final String methodName, final Column... cols) 
	{
		final boolean noName = (name == null); // indicates that no name was
												// provided for this operation
		if (noName) { // if this is a temporal column for a comparison
			name = "REFLECTION_OBJCALL"; // bogus-name so createColumnMetaData
											// doesn't explode
		}

		Column ret = new DynamicMethodColumn(name, methodName, col, cols);
		ret.addContainedColumns(cols);
		return ret;
	}

	/**
	 * Creates a Column executing a static function with the given methodname of the given class  
	 * with the given arguments.<br>
	 * If no suitable (non-void) static function for the given arguments is found, an Exception 
	 * will be thrown. This takes the first method with matching parameters, so it might not be 
	 * the same that would be called in "real java code". And varargs methods might not work.<br>
	 * 
	 * <i>Example:</i> If there is a method <code>foo(Bar x)</code> and one 
	 * <code>foo(Object x)</code> and you try to invoke it with a <code>Bar</code> argument, 
	 * but (by coincidence) the <code>foo(Object x)</code> method - that won't fail when
	 * being invoked with a Bar-object - is tried first, it will be used. Emulating the "original"
	 * java behaviour would be really difficult as javas reflection API has no useful functionality
	 * to return the Method that would be used when being invoked with given 
	 * parameters: You have to iterate through the method and try to find one that fits..<br>
	 * However, in many/most cases this should work as expected.<br>
	 * <b>Note:</b> To make sure to call the method you want you can cast the arguments via
	 * {@link #colCAST(Column, Class)}: if the arguments match the exact method signature the
	 * right method will be used.
	 * 
	 * @param clazz the class containing the static method
	 * @param methodName the name of the static Method to be used
	 * @param arguments the arguments to invoke the method with
	 * @return a Column returning objects returned by the method invoked with the given arguments
	 */
	public static Column colSTATICCALL(Class<?> clazz, String methodName, Column... cols) {
		return ColumnUtils.colSTATICCALL(null, clazz, methodName, cols);
	}

	/**
	 * Creates a Column executing a static function with the given methodname of the given class  
	 * with the given arguments.<br>
	 * If no suitable (non-void) static function for the given arguments is found, an Exception 
	 * will be thrown. This takes the first method with matching parameters, so it might not be 
	 * the same that would be called in "real java code". And varargs methods might not work.<br>
	 * 
	 * <i>Example:</i> If there is a method <code>foo(Bar x)</code> and one 
	 * <code>foo(Object x)</code> and you try to invoke it with a <code>Bar</code> argument, 
	 * but (by coincidence) the <code>foo(Object x)</code> method - that won't fail when
	 * being invoked with a Bar-object - is tried first, it will be used. Emulating the "original"
	 * java behaviour would be really difficult as javas reflection API has no useful functionality
	 * to return the Method that would be used when being invoked with given 
	 * parameters: You have to iterate through the method and try to find one that fits..<br>
	 * However, in many/most cases this should work as expected.<br>
	 * <b>Note:</b> To make sure to call the method you want you can cast the arguments via
	 * {@link #colCAST(Column, Class)}: if the arguments match the exact method signature the
	 * right method will be used.
	 * 
	 * @param name the name of the returned Column
	 * @param clazz the class containing the static method
	 * @param methodName the name of the static Method to be used
	 * @param arguments the arguments to invoke the method with
	 * @return a Column returning objects returned by the method invoked with the given arguments
	 */
	public static Column colSTATICCALL(String name, final Class<?> clazz,
			final String methodName, final Column... arguments) 
	{
		final boolean noName = (name == null); // indicates that no name was
												// provided for this operation
		if (noName) { // if this is a temporal column for a comparison
			name = "REFLECTION_STATICCALL"; // bogus-name so
											// createColumnMetaData doesn't
											// explode
		}

		Column ret = new StaticMethodColumn(name, clazz, methodName, arguments);
		ret.addContainedColumns(arguments);
		return ret;
	}

	/**
	 * This {@link Column} wraps another Column and casts it to the given type.<br>
	 * Particularly the {@link ColumnMetaData} of this Column will be according to the given type.
	 * This is useful for the reflectiving Columns to make sure the right Method/Constructor is
	 * used (just cast the arguments to the exact signature of the method/constructor).  
	 * @param col the Column to be casted
	 * @param type the class to be casted to
	 * @return a Column casting col to the given type
	 */
	public static Column colCAST(final Column col, final Class<?> type){
		return new Column(){
			
			@Override
			public int getColumnIndex() {
				
				return col.getColumnIndex();
			}

			@Override // sollte so passen
			public ColumnMetaData getColumnMetaData() {
				
				col.getColumnMetaData(); // maybe this will throw an exception because no name was given.. 
				// if it didn't throw an exception just return the proper metadata
				return columnMetaData;
			}

			@Override
			public Object invoke(Tuple left, Tuple right) {
				// cast the element to make sure it is of the requested type
				return type.cast( col.invoke(left, right) );
			}

			@Override
			public Object invoke(Tuple tuple) {
				// cast the element to make sure it is of the requested type
				return type.cast( col.invoke(tuple) );
			}

			@Override
			public void setCorrelatedTuples(List<MapEntry<AdvResultSetMetaData,Tuple>> corrTuples){
				col.setCorrelatedTuples(corrTuples);
			}

			@Override
			public void setMetaData(AdvResultSetMetaData metadata,	String newAlias) {
				// TODO @Daniel col.getMetadata() wirft ne exception da ja die col noch nicht "komplett" initialissiert wurde
				col.setMetaData(metadata, newAlias);
				try {
					this.columnMetaData = AdvResultSetMetaData.createColumnMetaData(type, col.getColumnMetaData().getColumnName(), null);
				} catch(Exception e){
					throw new RuntimeException(e);
				}
				
			}

			@Override
			public void setMetaDatas(AdvResultSetMetaData leftMetaData,
					AdvResultSetMetaData rightMetaData) {
				// TODO @Daniel col.getMetadata() wirft ne exception da ja die col noch nicht "komplett" initialissiert wurde
				
				col.setMetaDatas(leftMetaData, rightMetaData);
				try {
					this.columnMetaData = AdvResultSetMetaData.createColumnMetaData(type, col.getColumnMetaData().getColumnName(), null);
				} catch(Exception e){
					throw new RuntimeException(e);
				}
			}
			
		}; 
	}
	
	/**
	 * Creates a (pseudo)-Column with a value (the given Object).<br>
	 * <b>This should only be used with predicates (like EQ etc)</b> or
	 * operations like CONCAT or ADD etc and <b>not</b> to create a "real"
	 * column within a Tuple (e.g. in select())!
	 * 
	 * @param value
	 *            the Object to be contained in this pseudo-Column
	 * @return a Column with the given value
	 */
	public static Column val(final Object obj) {
		if(obj == null) // if obj is null we can't generate metadata.. 
			throw new RuntimeException("Don't use val() with a null value! Use valNULL() instead!"); 
		return new Column(obj, "val(" + obj.toString() + ")"){
			
			@Override
			public ColumnMetaData getColumnMetaData() {
				// make sure this val is only used as intended by the user..
				// ATTENTION: this means that for "internal" use, like
				// checkForNumber, the columnmetadata
				// needs to be accessed directly!
				
				
				throw new UnsupportedOperationException(
						"If you want to use val() to create a real"
								+ " column within a tuple, give it a name (i.e. use val(obj, name))!");
			}
		};
	}
	

	public static Column val(Object obj, String columnAlias) {
		if(obj == null) // if obj is null we can't generate metadata.. 
			throw new RuntimeException("Don't use val() with a null value! Use valNULL() instead!"); 
		return new Column(obj, columnAlias);
	}
	
	/**
	 * Creates a Column containing the value <i>null</i>. You have to specify the type though, to make
	 * our internal MetaDatas happy and, if this is to be used in a reflection Column (like 
	 * colOBJCALL etc), to make it possible to find the suitable method that expects this type.
	 * 
	 * @param type type of the resulting Column - i.e. the type of the object/pointer that is null.
	 * @param name a name for this Column
	 * @return a Column containing <i>null</i> with given type in the metadata
	 */
	public static Column valNULL(Class<?> type, String name){
		ColumnMetaData cmd = AdvResultSetMetaData.createColumnMetaData(type, name, "");
		return new Column(null, name, cmd);
	}
	
	/**
	 * Creates a {@link Column} containing the value <i>null</i>. You have to specify the type though, to make
	 * our internal MetaDatas happy and, if this is to be used in a reflection Column (like 
	 * colOBJCALL etc), to make it possible to find the suitable method that expects this type.
	 * 
	 * @param type type of the resulting Column - i.e. the type of the object/pointer that is null.
	 * @param name a name for this Column
	 * @return a Column containing <i>null</i> with given type in the metadata
	 */
	public static Column valNULL(Class<?> type){
		ColumnMetaData cmd = AdvResultSetMetaData.createColumnMetaData(type, "valNULL", "");
		return new Column(null, "valNULL", cmd){

			@Override
			public ColumnMetaData getColumnMetaData() {
				throw new UnsupportedOperationException(
						"If you want to use valNULL() to create a real"
								+ " column within a tuple, give it a name (i.e. use valNULL(class, name))!");
			}
		};
	}
	
	/**
	 * Returns a {@link Column} projecting the <b>i</b>-th Element from a tuple.<br>
	 * May not be used in joins! (Because then it's not clear whether it should be applied to
	 * the left or the right tuple being joined)
	 * @param i index of the column to be projected
	 * @return a Column projecting the <b>i</b>-th Element from a tuple. 
	 */
	public static Column col(int i) {
		return new Column(i);
	}

	/**
	 * Returns a {@link Column} projecting the <b>i</b>-th Element from a tuple.<br>
	 * May not be used in joins! (Because then it's not clear whether it should be applied to
	 * the left or the right tuple being joined)
	 * @param i index of the column to be projected
	 * @return a Column projecting the <b>i</b>-th Element from a tuple. 
	 */
	public static Column col(int i, String newColumnName) {
		return new Column(i, newColumnName);
	}

	/**
	 * Returns a {@link Column} projecting the element with given columnName from a Tuple.
	 * 
	 * @param name the columnName of the column to be projected
	 * @return a Column projecting the <b>i</b>-th Element from a tuple. 
	 */
	public static Column col(String name) {
		return new Column(name);
	}

	/**
	 * Returns a {@link Column} projecting the element with given columnName from a Tuple.
	 * Also the returned Column will have a new name, so this can be used (in 
	 * {@link AdvTupleCursor#select(Column...) select()}) to rename a Column.
	 * 
	 * @param name the columnName of the column to be projected
	 * @return a Column projecting the <b>i</b>-th Element from a tuple. 
	 */
	public static Column col(String name, String newName) {
		return new Column(name, newName);
	}
	
	/**
	 * Just returns an array of {@link Column}s  containing the given columns. To be used with
	 * {@link AdvTupleCursor#groupBy(Column[], xxl.core.math.functions.MetaDataAggregationFunction[]) 
	 * groupBy(Column[], MetaDataAggregationFunction[])} because it's only possible to use varargs 
	 * once within one function.<br>
	 * <b>Example:</b>
	 * <code>cur.groupBy(PROJ( col("a"), col("c") ), COUNT("cnt"), MAX(col("b"), "maxB"))</code>
	 * @param columns columns to be put in an array
	 * @return an array of Column containing the given columns
	 */
	public static Column[] PROJ(Column... columns) {
		return columns;
	}
	
	/**
	 * This {@link Column} calculates the square root of <b>col</b><br>
	 * <b>col</b> needs to contain Numbers, of course.
	 * @param col the Column that provides the number to be square-rooted
	 * @return a Column containing the square root of col's value (as Double)
	 */
	public static Column SQRT(final Column col) {
		return SQRT(col, null);
	}

	/**
	 * This {@link Column} calculates the square root of <b>col</b><br>
	 * <b>col</b> needs to contain Numbers, of course.
	 * 
	 * @param col the Column that provides the number to be square-rooted
	 * @param name the name of this Column (useful for new actual columns within a tuple like
	 * 		in select() or groupBy())
	 * @return a Column containing the square root of col's value (as Double)
	 */
	public static Column SQRT(final Column col, String name) {
		// this is the only arithm. operation performed on one column (ADD etc
		// use two, of course)
		// so no arithm_op or similar is used
		final boolean noName = (name == null); // indicates that no name was
		// provided for this operation
		if (noName) { // if this is a temporal column for a comparison
			name = "val_from_arith_op"; // bogus-name so createColumnMetaData
			// doesn't explode
		}

		// columnmetadata for this Column - the alias will be set later in
		// setMetaData().. this is
		// a bit hackish but i haven't got a better idea on how to do this
		final ColumnMetaData cmd = AdvResultSetMetaData.createColumnMetaData(
				Double.class, name, null);
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
									+ " (ADD, SUB, ...) to create a real column within a tuple, give it a name"
									+ " (i.e. use ADD(col, name) etc)!");
				} else {
					return super.getColumnMetaData();
				}
			}

			@Override
			public Object invoke(Tuple tuple) {
				if (!checked) {
					// yes, this is a bit hackish, but i'm not writing a new
					// check just for sqrt
					checkForNumber(col, col);
					checked = true;
				}
				Number val = (Number) col.invoke(tuple);

				return Math.sqrt(val.doubleValue());
			}

			@Override
			// the join-case: columns may be from the left or right tuple
			public Object invoke(Tuple left, Tuple right) {
				if (!checked) {
					checkForNumber(col, col);
					checked = true;
				}
				Number val = (Number) col.invoke(left, right);

				return Math.sqrt(val.doubleValue());
			}
		};
		// add col as contained Column so it'll get metadata and correlated
		// tuples
		ret.addContainedColumns(col);
		return ret;
	}
	
	/**
	 * A conditional {@link Column}: If the given {@link AdvPredicate} IF evaluates to <i>true</i>
	 * the Column THEN will be returned, else the Column ELSE will be returned.
	 * @param IF the condition
	 * @param THEN Column to return if the condition is met for the current tuple(s)
	 * @param ELSE Column to return if the condition is not met for the current tuple(s)
	 * @return THEN or ELSE, determined by IF
	 */
	public static Column IfThenElse(final AdvPredicate IF, final Column THEN, final Column ELSE){
		return IfThenElse(null, IF, THEN, ELSE);
	}
	
	/**
	 * A conditional {@link Column}: If the given {@link AdvPredicate} IF evaluates to <i>true</i>
	 * the Column THEN will be returned, else the Column ELSE will be returned.
	 * @param name the name of the resulting Column (useful for new actual columns within a tuple like
	 * 		in select() or groupBy())
	 * @param IF the condition 
	 * @param THEN Column to return if the condition is met for the current tuple(s)
	 * @param ELSE Column to return if the condition is not met for the current tuple(s)
	 * @return THEN or ELSE, determined by IF
	 */
	public static Column IfThenElse(String name, final AdvPredicate IF, final Column THEN, final Column ELSE){
		final boolean noName = name == null;
		final String columnName = noName ? "ifthenelse" : name;
		
		Column ret = new Column(columnName) {
			private static final long	serialVersionUID	= 1L;
			
			private Class<?> getCommonType(Class<?> clazz1, Class<?> clazz2){
				while(!clazz1.isAssignableFrom(clazz2)){       
						clazz1 = clazz1.getSuperclass();       
				}                                              
				return clazz1;
			}
			
			@Override
			public void setMetaData(AdvResultSetMetaData metadata, String newAlias) {
				// set metadata for contained columns
				if (containedColumns != null) {
					for (Column col : containedColumns) {
						col.setMetaData(metadata, newAlias);
					}
				}
				// set metadata for IF-predicate
				IF.setMetaData(metadata, newAlias);
				
				Class<?> thenType; 
				Class<?> elseType;
				try {
					thenType = Class.forName(THEN.columnMetaData.getColumnClassName());
					elseType = Class.forName(ELSE.columnMetaData.getColumnClassName());
				} catch (Exception e) {
					if(noName)
						throw new RuntimeException("Error while setting metadata for IfThenElse: "
								+e.getMessage(), e);
					else
						throw new RuntimeException("Error while setting metadata for IfThenElse \""
								+columnName+"\": "+e.getMessage(), e);
				} 
				Class<?> cl = getCommonType(thenType, elseType);
				columnMetaData = AdvResultSetMetaData.createColumnMetaData(cl, columnName, newAlias);
			}
			
			@Override
			public void setMetaDatas(AdvResultSetMetaData leftMetaData, AdvResultSetMetaData rightMetaData) {
				// set metadata for contained columns
				if (containedColumns != null) {
					for (Column col : containedColumns) {
						col.setMetaDatas(leftMetaData, rightMetaData);
					}
				}
				// set metadatas in IF-predicate
				IF.setMetaDatas(leftMetaData, rightMetaData);
				
				Class<?> thenType; 
				Class<?> elseType;
				try {
					thenType = Class.forName(THEN.columnMetaData.getColumnClassName());
					elseType = Class.forName(ELSE.columnMetaData.getColumnClassName());
				} catch (Exception e) {
					if(noName)
						throw new RuntimeException("Error while setting metadata for IfThenElse: "
								+e.getMessage(), e);
					else
						throw new RuntimeException("Error while setting metadata for IfThenElse \""
								+columnName+"\": "+e.getMessage(), e);
				} 
				Class<?> cl = getCommonType(thenType, elseType);
				columnMetaData = AdvResultSetMetaData.createColumnMetaData(cl, columnName, null);
			}
			
			@Override
			public ColumnMetaData getColumnMetaData() {
				if (noName) {
					// like val: if no name was given this may only be used in
					// comparisons, arithmetic
					// operations etc, but not to create a column for a tuple
					// (in select)!
					throw new UnsupportedOperationException(
							"If you want to use IfThenElse to create a real Column within a Tuple,"
									+ " give it a name!");
				} else {
					return super.getColumnMetaData();
				}
			}

			@Override
			public Object invoke(Tuple tuple) {
				if(IF.invoke(tuple))
					return THEN.invoke(tuple);
				else
					return ELSE.invoke(tuple);
			}

			@Override
			// the join-case: columns may be from the left or right tuple
			public Object invoke(Tuple left, Tuple right) {
				if(IF.invoke(left, right))
					return THEN.invoke(left, right);
				else
					return ELSE.invoke(left, right);
			}
		};
		// add then and else as contained Columns so they'll get metadata and correlated tuples
		ret.addContainedColumns(THEN, ELSE);
		return ret;
	}

	
	public static Column indexCol() {
		return indexCol("index", 0);
	}
	public static Column indexCol(long start) {
		return indexCol("index", start);
	}
	public static Column indexCol(String name) {
		return new IndexedColumn(name, 0);
	}
	public static Column indexCol(String name, long start) {
		return new IndexedColumn(name, start);
	}

}
