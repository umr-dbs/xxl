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

package xxl.core.util.reflect;

import java.io.PrintStream;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Map.Entry;

import xxl.core.cursors.Cursor;
import xxl.core.cursors.Cursors;
import xxl.core.cursors.MetaDataCursor;
import xxl.core.cursors.filters.Dropper;
import xxl.core.cursors.filters.Filter;
import xxl.core.cursors.filters.Taker;
import xxl.core.cursors.joins.NestedLoopsJoin;
import xxl.core.cursors.mappers.Mapper;
import xxl.core.cursors.sorters.ShuffleCursor;
import xxl.core.cursors.sources.ArrayCursor;
import xxl.core.cursors.sources.Enumerator;
import xxl.core.cursors.sources.SingleObjectCursor;
import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;
import xxl.core.functions.FunctionMetaDataFunction;
import xxl.core.functions.NTuplify;
import xxl.core.predicates.Predicate;
import xxl.core.relational.metaData.AppendedResultSetMetaData;
import xxl.core.relational.metaData.ColumnMetaDataResultSetMetaData;
import xxl.core.relational.metaData.ResultSetMetaDatas;
import xxl.core.relational.metaData.StoredColumnMetaData;
import xxl.core.relational.tuples.ArrayTuple;
import xxl.core.relational.tuples.Tuple;
import xxl.core.util.WrappingRuntimeException;
import xxl.core.util.metaData.CompositeMetaData;
import xxl.core.util.timers.Timer;
import xxl.core.util.timers.TimerUtils;

/**
 * This framework allows it to write applications which 
 * are easily parameterizable via command line options.
 * Additionally, automatic testing of such applications
 * is easily possible. The results of test runs are
 * returned as a MetaDataCursor with relational meta
 * data (ResultSetMetaData).<p>
 * 
 * To support this TestFramework, a test class do not
 * have to support many things. First: all public
 * static fields which are not final, can be set via
 * this framework (see method processParameters). For each
 * field, the test class can also support a description,
 * which has to be inside a public static String field
 * with the name of the field plus "Description". The
 * class can also offer a range of valid parameters
 * (a field of the same type postfix "Min", resp. "Max").<p>
 * 
 * A compatible test class should call processParameters
 * as first instruction inside the main method.<p>
 * 
 * For automatic testing, the test class can put measurements
 * into the static list of the TestFramework class. If the
 * class provides relational meta data for these measurements,
 * then the automatic testing returns a MetaDataCursor, else
 * it returns a normal Cursor which contains a list. To provide
 * meta data, the test class has to offer a method 
 * <code>public static ResultSetMetaData getReturnRSMD()</code>.
 * To build the meta data, the classes from {@link xxl.core.relational.metaData}
 * are extremely useful.<p>
 * 
 * To get good input parameters for the tests, the test class 
 * can provide a method
 * <code>public static Iterator getTestValues(String fieldName)</code>
 * This method has to return an Iterator of valid test values 
 * for the given fieldName. If null is returned, then the 
 * automatic test environment generates values:
 * <ul>
 * <li>For int and double fields: generate values between 
 * 	fieldNameMin and fieldNameMax</li>
 * <li>For boolean fields: true and false (if in mode 
 * 	testAllBooleanCombinations, else only the default value
 * 	is tested).</li>
 * </ul>
 * If no values can be generated, then only the single default value 
 * is used for testing.
 */
public class TestFramework {

	/**
	 * This list can be used by applications to hand over
	 * some measurements to the testClass method. Use
	 * list.add to do this.
	 */
	public static List list;
	
	/**
	 * Function which gets a Cursor and returns a Cursor where
	 * the Objects are randomly permutated. This Function can
	 * be used to parameterize the method testClass below.
	 */
	public static Function shuffleCursorFunction = new AbstractFunction() {
		public Object invoke (Object o) {
			Cursor cursor = (Cursor) o;
			return new ShuffleCursor(cursor);
		}
	};

	/**
	 * Function which gets a Cursor and returns a Cursor where
	 * the order of the Objects is inverse. This Function can
	 * be used to parameterize the method testClass below.
	 */
	public static Function reverseCursorFunction = new AbstractFunction() {
		public Object invoke (Object cursor) {
			Object [] allObjects = Cursors.toArray((Cursor) cursor);
			return new ArrayCursor(allObjects, new Enumerator(allObjects.length-1, -1));
		}
	};

	/**
	 * Returns true, if the type of a field is supported by
	 * the TestFramework.
	 * @param field given field.
	 * @return true, iff the type is supported.
	 */
	private static boolean isTypeSupported(Field field) {
		int modifiers = field.getModifiers();
		return
			Reflections.convertStringToDifferentTypeSupported(field.getType()) &&
			Modifier.isStatic(modifiers) &&
			Modifier.isPublic(modifiers) &&
			!Modifier.isFinal(modifiers);
	}

	/**
	 * Returns a map with the supported fields of a class cl.
	 * @param cl Class with static fields (the test class).
	 * @return a map with (fieldName, defaultValue) entries.
	 */
	private static Map getSupportedFields(Class cl) {
		Map map = new HashMap();
		Field fields[] = cl.getFields();
		for (int i=0; i<fields.length; i++) {
			try {
				Field field = fields[i];
				if (isTypeSupported(field)) {
					String fieldName = field.getName();
					// System.out.println(i+": "+fieldName);
					Object value = field.get(null);
					map.put(fieldName, value);
				}
			}
			catch (IllegalAccessException e) {}
		}
		
		return map;
	}

	/**
	 * Returns an Iterator with the field names of all
	 * supported public static not final fields of the class cl. 
	 * @param cl Class which field names will be returned.
	 * @return Iterator with the field names.
	 */
	private static Iterator getSupportedFieldNames(Class cl) {
		final Field fields[] = cl.getFields();
		return new Iterator() {
			int i=-1;
			Object next=null;
			public boolean hasNext() {
				if (next==null) {
					i++; // go to the next field
					while (i<fields.length) {
						if (isTypeSupported(fields[i])) {
							next = fields[i].getName();
							break;
						}
						i++;
					}
				}
				return i<fields.length;
			}
			public Object next() {
				if (next==null)
					if (!hasNext())
						throw new NoSuchElementException();
				Object ret = next;
				next = null;
				return ret;
			}
			public void remove() {
				throw new UnsupportedOperationException(); 
			}
		};
	}

	/**
	 * Writes the entries of a map into a String. Each map entry
	 * (key,value) is converted into key=value.
	 * @param map Map containing command line options
	 * @return a String representation of the map.
	 */
	public static String mapToArgString(Map map) {
		StringBuffer sb = new StringBuffer();
		
		Iterator it = map.entrySet().iterator();
		
		while (it.hasNext()) {
			Entry e = (Entry) it.next();
			sb.append(e.getKey().toString());
			sb.append("=");
			Object value = e.getValue();
			if (value!=null) {
				sb.append(value.toString());
				sb.append(" ");
			}
		}
		
		return sb.toString();
	}

	/**
	 * Writes the parameters given by args (key=value) into a map. To do this,
	 * there is a type conversion needed. Therefore, each key value has to
	 * be a field inside a given class cl. The type of this field is used for the 
	 * conversion.
	 * @param args String array containing key=value Strings.
	 * @param cl Class used for type conversion.
	 * @return Map containing (key,value) pairs (values have the correct type).
	 */
	public static Map argStringArrayToMap(String args[], Class cl) {
		Map map = new HashMap();
		
		for (int i=0; i<args.length; i++) {
			int index = args[i].indexOf('=');
			String fieldName = args[i].substring(0,index);
			
			try {
				Field field = cl.getField(fieldName);
				Class clF = field.getType();
				String value = args[i].substring(index+1);
				Object o = Reflections.convertStringToDifferentType(value, clF); 
				map.put(fieldName, o);
			}
			catch (NoSuchFieldException ex) {}
		}
		
		return map;
	}

	/**
	 * Processes the parameters given by the args array and writes the
	 * values into public static variables of class cl. If a PrintStream
	 * is given, then the current parameterset is sent to this PrintStream.
	 * If something like "help" is given as first parameter, then all possible
	 * parameters of a class are printed on a help screen. Then, the given
	 * description is outputed first.
	 * @param description Textual description of the class
	 * @param cl Class which needs the parameters.
	 * @param args Given parameterset.
	 * @param ps PrintStream for output of current parameterset.
	 * @return true, if the calling programm should proceed. false if
	 * 	the help screen was shown and processing further may not be the 
	 * 	right choice.
	 */
	public static boolean processParameters(String description, Class cl, String args[], PrintStream ps) {
		
		if (args.length>0) {
			if (
				args[0].equalsIgnoreCase("help") || 
				args[0].equalsIgnoreCase("-help") || 
				args[0].equalsIgnoreCase("--help") || 
				args[0].equalsIgnoreCase("?") ||
				args[0].equalsIgnoreCase("-?") || 
				args[0].equalsIgnoreCase("--?") || 
				args[0].equalsIgnoreCase("/?")) { 
				
				System.out.println(description);
				System.out.println("Parameters:\n");
				
				Iterator it = getSupportedFieldNames(cl);
				while (it.hasNext()) { 
					String fieldName = (String) it.next();
					try {
						Field origField = cl.getField(fieldName);
						System.out.print(fieldName+
							" ("+origField.getType().getName()+
							", default: "+origField.get(null));
						
						try {
							Field minField = cl.getField(fieldName+"Min");
							System.out.print(", min: "+minField.get(null));
						}
						catch (NoSuchFieldException e) {}
						
						try {
							Field maxField = cl.getField(fieldName+"Max");
							System.out.print(", max: "+maxField.get(null));
						}
						catch (NoSuchFieldException e) {}
						
						System.out.println(")");
						Field descField = cl.getField(fieldName+"Description");
						System.out.println("\t"+descField.get(null));
					}
					catch (NoSuchFieldException e) {}
					catch (IllegalAccessException e) {}
				}
				return false;
			}
			
			Map map = argStringArrayToMap(args, cl);
			Reflections.setStaticFields(map, cl);
		}
		
		// Output the current parameterset
		if (ps!=null) {
			System.out.println("Current Parameterset:");
			Map map = getSupportedFields(cl);
	
			ps.println(mapToArgString(map));
			ps.println();
		}
		
		// Important for classes which are testable
		// if list!=null then the API is in batch test mode
		if (list==null)
			list = new ArrayList();
		
		return true;
	}

	/**
	 * Runs a series of tests of the class cl.
	 * @param cl Class to be tested.
	 * @param inputParamCursor Input parameters for the tests. 
	 * @param testName Name of the test.
	 * @return Cursor or MetaDataCursor containing the results of the tests. 
	 * 	There are a lot of useful columns inside the result.
	 */
	public static Cursor testClass(final Class cl, MetaDataCursor inputParamCursor, 
			final String testName) {
		final ResultSetMetaData rsmd = ResultSetMetaDatas.getResultSetMetaData(inputParamCursor);
		ResultSetMetaData returnRSMD = null;
		int columnsFromTestClass = 0;
		
		try {
			Method m = cl.getMethod("getReturnRSMD",new Class[0]);
			returnRSMD = (ResultSetMetaData) m.invoke(null, new Object[0]);
			if (returnRSMD!=null) {
				columnsFromTestClass = returnRSMD.getColumnCount();
				returnRSMD = new AppendedResultSetMetaData(rsmd,returnRSMD);
			}
			else
				returnRSMD = rsmd;
		}
		catch (SQLException e) {
			throw new RuntimeException("Function getReturnRSMD returned an invalid ResultSetMetaData");
		}
		catch (NoSuchMethodException e) {
			throw new WrappingRuntimeException(e);
		}
		catch (InvocationTargetException e) {
			throw new WrappingRuntimeException(e);
		}
		catch (IllegalAccessException e) {
			throw new WrappingRuntimeException(e);
		}
		
		if (returnRSMD!=null) 
			returnRSMD = 
				new AppendedResultSetMetaData(
					returnRSMD,
					new ColumnMetaDataResultSetMetaData(
						new StoredColumnMetaData(false, false, true, false, ResultSetMetaData.columnNullable, true, 9, "freeMemoryBeforeTest", "freeMemoryBeforeTest", "", 9, 0, "", "", Types.INTEGER, true, false, false),
						new StoredColumnMetaData(false, false, true, false, ResultSetMetaData.columnNullable, true, 9, "totalMemory", "totalMemory", "", 9, 0, "", "", Types.INTEGER, true, false, false),
						new StoredColumnMetaData(false, false, true, false, ResultSetMetaData.columnNullable, true, 9, "maxMemory", "maxMemory", "", 9, 0, "", "", Types.INTEGER, true, false, false),
						new StoredColumnMetaData(false, false, true, false, ResultSetMetaData.columnNullable, true, 9, "freeMemoryAfterTest", "freeMemoryAfterTest", "", 9, 0, "", "", Types.INTEGER, true, false, false),
						new StoredColumnMetaData(false, false, true, false, ResultSetMetaData.columnNullable, true, 9, "availableProcessors", "availableProcessors", "", 9, 0, "", "", Types.INTEGER, true, false, false),
						new StoredColumnMetaData(false, true, true, false, ResultSetMetaData.columnNullable, false, 30, "JavaVM", "JavaVM", "", 0, 0, "", "", Types.VARCHAR, true, false, false),
						new StoredColumnMetaData(false, true, true, false, ResultSetMetaData.columnNullable, false, 30, "OS", "OS", "", 0, 0, "", "", Types.VARCHAR, true, false, false),
						new StoredColumnMetaData(false, true, true, false, ResultSetMetaData.columnNullable, false, 30, "Username", "Username", "", 0, 0, "", "", Types.VARCHAR, true, false, false),
						new StoredColumnMetaData(false, true, true, false, ResultSetMetaData.columnNullable, false, 30, "CurrentDateTime", "CurrentDateTime", "", 0, 0, "", "", Types.VARCHAR, true, false, false),
						new StoredColumnMetaData(false, true, true, false, ResultSetMetaData.columnNullable, false, 30, "TestName", "TestName", "", 0, 0, "", "", Types.VARCHAR, true, false, false),
						new StoredColumnMetaData(false, true, true, false, ResultSetMetaData.columnNullable, false, 30, "TestStartDate", "TestStartDate", "", 0, 0, "", "", Types.VARCHAR, true, false, false),
						new StoredColumnMetaData(false, true, true, false, ResultSetMetaData.columnNullable, false, 30, "JavaClass", "JavaClass", "", 0, 0, "", "", Types.VARCHAR, true, false, false),
						new StoredColumnMetaData(false, true, true, false, ResultSetMetaData.columnNullable, false, 50, "PackageName", "PackageName", "", 0, 0, "", "", Types.VARCHAR, true, false, false),
						new StoredColumnMetaData(false, true, true, false, ResultSetMetaData.columnNullable, false, 50, "Hostname", "Hostname", "", 0, 0, "", "", Types.VARCHAR, true, false, false),
						new StoredColumnMetaData(false, true, true, false, ResultSetMetaData.columnNullable, false, 20, "HostIP", "HostIP", "", 0, 0, "", "", Types.VARCHAR, true, false, false),
						new StoredColumnMetaData(false, false, true, false, ResultSetMetaData.columnNullable, true, 15, "TimeForMainMethod", "TimeForMainMethod", "", 15, 0, "", "", Types.DOUBLE, true, false, false),
						new StoredColumnMetaData(false, false, true, false, ResultSetMetaData.columnNullable, false, 1, "ExceptionOccured", "ExceptionOccured", "", 1, 0, "", "", Types.BIT,  true, false, false),
						new StoredColumnMetaData(false, true, true, false, ResultSetMetaData.columnNullable, false, 200, "ExceptionText", "ExceptionText", "", 0, 0, "", "", Types.VARCHAR, true, false, false)
					)
				);
		
		String machineName = "";
		String machineIP ="127.0.0.1";
		try {
			InetAddress thisMachine = InetAddress.getLocalHost();
			machineName = thisMachine.getCanonicalHostName();
			machineIP = thisMachine.getHostAddress();
		}
		catch (UnknownHostException e) {}
		
		final SimpleDateFormat sfd = new SimpleDateFormat("dd.MM.yy HH:mm:ss,SSS");
		final String testStartDate = sfd.format(new Date());
		final String className = cl.getName();
		final String packageName = cl.getPackage().getName();
		final String hostName = machineName;
		final String hostIP = machineIP;
		final Runtime runtime = Runtime.getRuntime();

		final ResultSetMetaData retRSMD = returnRSMD;
		final int finalColumnsFromTestClass = columnsFromTestClass;
		final Timer timer = (Timer) TimerUtils.FACTORY_METHOD.invoke();
		TimerUtils.warmup(timer);
		final long zeroTime = TimerUtils.getZeroTime(timer);
		
		Function mapFunction = new AbstractFunction() {
			public Object invoke(Object o) {
				Tuple t = (Tuple) o;

				list = new ArrayList();
				try {
					// The first field is the testNumber!
					for (int column=1; column<=rsmd.getColumnCount(); column++) {
						list.add(t.getObject(column));
						if (column>1) {
							String fieldName = rsmd.getColumnName(column);
							Field field = cl.getField(fieldName);
							field.set(null, t.getObject(column));
						}
					}
				}
				catch (SQLException e) {
					throw new WrappingRuntimeException(e);
				}
				catch (NoSuchFieldException e) {
					throw new WrappingRuntimeException(e);
				}
				catch (IllegalAccessException e) {
					throw new WrappingRuntimeException(e);
				}
				
				Throwable throwable=null;

				long freeMemBefore = runtime.freeMemory();
				long stopTime = -1;
				// Run a garbage collection
				System.gc();
				int numberOfColumnsBefore = list.size();

				try {
					Method m = cl.getMethod("main",new Class[]{String[].class});
					timer.start();
					m.invoke(null, new Object[]{new String[0]});
					stopTime = timer.getDuration();
				}
				catch (NoSuchMethodException e) {
					throw new WrappingRuntimeException(e);
				}
				catch (IllegalAccessException e) {
					throw new WrappingRuntimeException(e);
				}
				catch (InvocationTargetException e) {
					stopTime = timer.getDuration();
					throwable = e.getTargetException();
					while (throwable instanceof InvocationTargetException)
						throwable = ((InvocationTargetException) throwable).getTargetException();
						
				}
				catch (Throwable thr) {
					stopTime = timer.getDuration();
					throwable = thr;
				}

				int numberOfColumnsAfter = list.size();
				int addColumns = finalColumnsFromTestClass - (numberOfColumnsAfter-numberOfColumnsBefore);
				while (addColumns-->0)
					list.add(null);

				list.add(new Long(freeMemBefore));
				list.add(new Long(runtime.totalMemory()));
				list.add(new Long(runtime.maxMemory()));
				list.add(new Long(runtime.freeMemory()));
				list.add(new Integer(runtime.availableProcessors()));
				
				list.add(System.getProperty("java.vm.vendor")+" "+System.getProperty("java.vm.name")+" "+System.getProperty("java.vm.version"));
				list.add(System.getProperty("os.name"));
				list.add(System.getProperty("user.name"));			
				list.add(sfd.format (new Date()));

				list.add(testName);
				list.add(testStartDate);
				list.add(className);
				list.add(packageName);
				list.add(hostName);
				list.add(hostIP);
				
				list.add(new Double(TimerUtils.getTimeInSeconds(timer, zeroTime, stopTime)));
				list.add(new Boolean(throwable!=null));
				if (throwable==null)
					list.add("");
				else {
					StringBuffer sb = new StringBuffer(throwable.toString());
					StackTraceElement ste[] = throwable.getStackTrace();
					for (int i=0; i<ste.length; i++)
						sb.append("\n"+ste[i].toString());
					list.add(sb.toString());
				}
				// list.add(new Integer(count++));
				
				if (retRSMD==null)
					return list;
				else
					return new ArrayTuple(list.toArray(), retRSMD);
			}
		};
		
		if (returnRSMD==null) {
			return new xxl.core.cursors.mappers.Mapper(
				mapFunction,
				inputParamCursor
			);
		}
		else {
			return new xxl.core.relational.cursors.Mapper(
				new FunctionMetaDataFunction(mapFunction) {
					CompositeMetaData<Object, Object> globalMetaData = new CompositeMetaData<Object, Object>();
					{
						globalMetaData.add(ResultSetMetaDatas.RESULTSET_METADATA_TYPE, retRSMD);
					}
					public Object getMetaData() {
						return globalMetaData;
					}
				},
				inputParamCursor
			);
		}
	}

	/**
	 * Runs automatic tests for class cl using automatically generated input 
	 * parameters. It is possible to perform only some tests, not all.
	 * @param cl Class to be tested.
	 * @param startTestNumber First test number (usually 1)
	 * @param endTestNumber Last test number (or -1 if all tests should be performed).
	 * @param testName Name of the test (also returned inside the Cursor).
	 * @param testAllBooleanCombinations If true, all public static boolean (not final)
	 * 	fields are tested with true and false. If false, only the default value is
	 * 	used for testing.
	 * @param generateFiltersFunction If this function is not null, then the Function
	 * 	gets the fieldName and a Cursor which contains all values that will be tested.
	 * 	The Function has to return a Cursor, which contains the values that will
	 * 	really be tested. For example, the Function have a Filter inside.
	 * @param filterPredicate If this predicate is not null, then the Predicate is
	 * 	used as a filter for the input data cursor. The predicate gets an input tuple 
	 *  and has to return true iff the tuple really should be used as input.
	 * @param getWrapperCursor If this function is not null, then the Function is
	 * 	called with the Cursor which contains all combination of values that
	 *  will be tested. The Function has to return a Cursor again which then is
	 *  really used. The main purpose for this Function is to reorder the tests,
	 *  so that the tests can be repeated in different orders. The input Cursor itself
	 *  contains Object arrays.
	 * @param varyParameterWithName Determines if a parameter is varied at all.
	 * 	If the predicate called with a parameter name returns false, then this
	 * 	parameter is not varied.
	 * @return Cursor or MetaDataCursor containing the results of the tests. 
	 * 	There are a lot of useful additional columns inside the result.
	 */
	public static Cursor testClass(Class cl, int startTestNumber, int endTestNumber, String testName, 
			boolean testAllBooleanCombinations, Function generateFiltersFunction,
			Function getWrapperCursor, Predicate filterPredicate, Predicate varyParameterWithName
	) {
		
		MetaDataCursor inputCursor = constructInputMetaDataCursor(
			cl, testAllBooleanCombinations, generateFiltersFunction, 
			getWrapperCursor, varyParameterWithName
		);
		
		if (startTestNumber<1)
			throw new RuntimeException("startTestNumber has to be at least 1");
		
		if (startTestNumber>1)
			inputCursor = 
				Cursors.wrapToMetaDataCursor(
					new Dropper(inputCursor, startTestNumber-1),
					inputCursor.getMetaData()
				);
		
		if (endTestNumber>0)
			inputCursor = 
				Cursors.wrapToMetaDataCursor(
					new Taker(inputCursor, endTestNumber-startTestNumber+1),
					inputCursor.getMetaData()
				);
		
		if (filterPredicate!=null)
			inputCursor = 
				Cursors.wrapToMetaDataCursor(
					new Filter(inputCursor, filterPredicate),
					inputCursor.getMetaData()
				);
		
		Cursor resultCursor = testClass(cl, inputCursor, testName);
		return resultCursor;
	}

	/**
	 * Constructs input data for a test of class cl. All combination
	 * of allowed values are used and the cartesian product of them
	 * is computed.
	 * @param cl Class to be tested. 
	 * @param testAllBooleanCombinations If true, all public static boolean (not final)
	 * 	fields are tested with true and false. If false, only the default value is
	 * 	used for testing.
	 * @param generateFiltersFunction If this function is not null, then the Function
	 * 	gets the fieldName and a Cursor which contains all values that will be tested.
	 * 	The Function has to return a Cursor, which contains the values that will
	 * 	really be tested. For example, the Function have a Filter inside.
	 * @param getWrapperCursor If this function is not null, then the Function is
	 * 	called with the Cursor which contains all combination of values that
	 *  will be tested. The Function has to return a Cursor again which then is
	 *  really used. The main purpose for this Function is to reorder the tests,
	 *  so that the tests can be repeated in different orders. The input Cursor itself
	 *  contains Object arrays.
	 * @param varyParameterWithName Determines if a parameter is varied at all.
	 * 	If the predicate called with a parameter name returns false, then this
	 * 	parameter is not varied.
	 * @return MetaDataCursor with test parameters.
	 */
	public static MetaDataCursor constructInputMetaDataCursor(
		Class cl, boolean testAllBooleanCombinations, 
		Function generateFiltersFunction, Function getWrapperCursor,
		Predicate varyParameterWithName
	) {
		List cursors = new ArrayList();
		List metaDatas = new ArrayList();
		
		try {
			Iterator itFields = getSupportedFieldNames(cl);
			while (itFields.hasNext()) {
				Cursor cursor = null; // must be resetable
				String fieldName = (String) itFields.next();
				Field origField = cl.getField(fieldName);
				Object value = origField.get(null);
				
				Iterator it = null;
				if (varyParameterWithName.invoke(fieldName)) {
					final Class clv = value.getClass();
					
					try {
						Method m = cl.getMethod("getTestValues",new Class[]{String.class});
						it = (Iterator) m.invoke(null, new Object[]{fieldName});
					}
					catch (NoSuchMethodException e) {
						throw new WrappingRuntimeException(e);
					}
					catch (InvocationTargetException e) {
						throw new WrappingRuntimeException(e);
					}
					catch (IllegalAccessException e) {
						throw new WrappingRuntimeException(e);
					}
					if (it==null) {
						if (testAllBooleanCombinations && (clv==boolean.class || clv==Boolean.class)) {
							it = new ArrayCursor(new Object[]{Boolean.FALSE, Boolean.TRUE});
						}
						else {
							Object valuesArray=null;
							try {
								Field valuesField = cl.getField(fieldName+"Values");
								valuesArray = valuesField.get(null);
							}
							catch (NoSuchFieldException e) {}
							catch (IllegalAccessException e) {
								throw new WrappingRuntimeException(e);
							}
							
							if (valuesArray!=null) {
								it = Reflections.typedArrayCursor(valuesArray);
							}
							else {
								Object min, max;
								try {
									Field minField = cl.getField(fieldName+"Min");
									min = minField.get(null);
									Field maxField = cl.getField(fieldName+"Max");
									max = maxField.get(null);
									
									if (Reflections.isIntegralType(clv)) {
										Number minN = (Number) min;
										Number maxN = (Number) max;
										
										it =
											new Mapper(
												Reflections.getNumberTypeConversionFunction(clv),
												new Enumerator(minN.intValue(), maxN.intValue()+1)
											);
									}
									else if (Reflections.isRealType(clv)) {
										final Number minN = (Number) min;
										final Number maxN = (Number) max;
										final double factor = 2.0;
										
										it =
											new Mapper(
												Reflections.getNumberTypeConversionFunction(clv),
												new Iterator() {
													double currentValue=minN.doubleValue();
													public boolean hasNext() {
														return currentValue<maxN.doubleValue();
													}
													public Object next() {
														if (!hasNext())
															throw new NoSuchElementException();
														Object ret = new Double(currentValue);
														currentValue *= factor;
														return ret;
													}
													public void remove() {
														throw new UnsupportedOperationException();
													}
												}
											);
									}
								}
								catch (NoSuchFieldException e) {}
								catch (IllegalAccessException e) {
									throw new WrappingRuntimeException(e);
								}
							}
						}
					}
				}
				if (it==null) {
					if (value!=null)
						cursor = new SingleObjectCursor(value); // is resetable
					else
						throw new RuntimeException("No input parameter cursor could be constructed");
				}
				
				if (cursor==null) {
					Object o[] = Cursors.toArray(it);
					cursor = new ArrayCursor(o); // is resetable
				}
				if (generateFiltersFunction!=null)
					cursor = (Cursor) generateFiltersFunction.invoke(fieldName, cursor);

				cursors.add(cursor);
				metaDatas.add(
					new ColumnMetaDataResultSetMetaData(
						new StoredColumnMetaData(false, false, true, false, ResultSetMetaData.columnNullable, true, 0, fieldName, fieldName, "", 0, 0, "", "", xxl.core.relational.Types.getSqlType(xxl.core.relational.Types.getJavaTypeCode(value.getClass().getCanonicalName())), true, false, false)
					)
				);
			}
		}
		catch (NoSuchFieldException e) {
			throw new WrappingRuntimeException(e);
		}
		catch (IllegalAccessException e) {
			throw new WrappingRuntimeException(e);
		}
		
		if (cursors.size()==0)
			return null;
		
		// Cartesian product of all Cursors inside cursors.
		
		// First, include a test number
		Cursor cursor = (Cursor) cursors.get(0);
		
		for (int i=1; i<cursors.size(); i++) {
			cursor = new NestedLoopsJoin(
				cursor,
				(Cursor) cursors.get(i),
				NTuplify.DEFAULT_INSTANCE
			);
		}

		if (getWrapperCursor!=null)
			cursor = (Cursor) getWrapperCursor.invoke(cursor);
		
		Object omds[] = metaDatas.toArray();
		ResultSetMetaData mds[] = new ResultSetMetaData[omds.length+1];
		mds[0] = new ColumnMetaDataResultSetMetaData(
			new StoredColumnMetaData(false, false, true, false, ResultSetMetaData.columnNullable, true, 9, "TestNumber", "TestNumber", "", 9, 0, "", "", Types.INTEGER, true, false, false)
		);
		System.arraycopy(omds, 0, mds, 1, omds.length); 
		ResultSetMetaData rsmd = new AppendedResultSetMetaData(mds);
		CompositeMetaData<Object, Object> metadata = new CompositeMetaData<Object, Object>();
		metadata.add(ResultSetMetaDatas.RESULTSET_METADATA_TYPE, rsmd);
		
		return
			Cursors.wrapToMetaDataCursor(
				new Mapper(
					new AbstractFunction() {
						long count=1;
						public Object invoke (Object o1) {
							ArrayList<Object> list = new ArrayList<Object>();
							list.add(new Long(count++));
							if (o1 instanceof Object[])
								for (Object o : (Object[])o1)
									list.add(o);
							else
								list.add(o1);
							return ArrayTuple.FACTORY_METHOD.invoke(list);
						}
					},
					cursor
				),
				metadata);
	}

	/**
	 * Returns the number of tests which are automatically generated.
	 * @param cl Class to be tested.
	 * @param testAllBooleanCombinations If true, all public static boolean (not final)
	 * 	fields are tested with true and false. If false, only the default value is
	 * 	used for testing.
	 * @param generateFiltersFunction If this function is not null, then the Function
	 * 	gets the fieldName and a Cursor which contains all values that will be tested.
	 * 	The Function has to return a Cursor, which contains the values that will
	 * 	really be tested. For example, the Function have a Filter inside.
	 * @param varyParameterWithName Determines if a parameter is varied at all.
	 * 	If the predicate called with a parameter name returns false, then this
	 * 	parameter is not varied.
	 * @return the number of tests.
	 */
	public static int countTestCasesForClass(Class cl, boolean testAllBooleanCombinations, 
		Function generateFiltersFunction, Predicate varyParameterWithName
	) {
		MetaDataCursor inputCursor = 
			constructInputMetaDataCursor(
					cl, testAllBooleanCombinations, 
					generateFiltersFunction, null, varyParameterWithName
			);
		return Cursors.count(inputCursor);
	}

	/**
	 * Returns the number of tests which are automatically generated.
	 * @param cl Class to be tested.
	 * @param testAllBooleanCombinations If true, all public static boolean (not final)
	 * 	fields are tested with true and false. If false, only the default value is
	 * 	used for testing.
	 * @param varyParameterWithName Determines if a parameter is varied at all.
	 * 	If the predicate called with a parameter name returns false, then this
	 * 	parameter is not varied.
	 * @return the number of tests.
	 */
	public static int countTestCasesForClass(Class cl, boolean testAllBooleanCombinations, Predicate varyParameterWithName) {
		return countTestCasesForClass(cl, testAllBooleanCombinations, null, varyParameterWithName);
	}

	/**
	 * Returns the content of the static list with the
	 * metadata. 
	 * @param rsmd Metadata for the list usually got by a call
	 * 	to classname.getReturnRSMD().
	 * @return
	 */
	public static Tuple getListAsTuple(ResultSetMetaData rsmd) {
		if (rsmd!=null)
			return new ArrayTuple(list.toArray(), rsmd);
		else
			return null;
	}
}
