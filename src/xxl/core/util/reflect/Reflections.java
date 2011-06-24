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

import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.Map.Entry;

import xxl.core.cursors.Cursor;
import xxl.core.cursors.mappers.Mapper;
import xxl.core.cursors.sources.Enumerator;
import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;
import xxl.core.functions.Identity;

/**
 * This class provides some static methods which make heavily use
 * of the Java reflection mechanism.
 */
public class Reflections {
	/**
	 * No instance of this class allowed, because it only contains
	 * static methods.
	 */
	private Reflections() {
		// private access in order to ensure non-instantiability
	}

	/**
	 * Converts a given String into a type which is given by
	 * its class object.
	 * @param s String containing a value.
	 * @param type Class object of the desired return type.
	 * @return the value parsed from the String.
	 */
	public static Object convertStringToDifferentType(String s, Class<?> type) {
		return type==String.class ?
			s :
			type==StringBuffer.class ?
				new StringBuffer(s) :
				type==Integer.TYPE || type==Integer.class ?
					new Integer(s) :
					type==Byte.TYPE || type==Byte.class ?
						new Byte(s) :
						type==Short.TYPE || type==Short.class ?
							new Short(s) :
							type==Long.TYPE || type==Long.class ?
								new Long(s) :
								type==Float.TYPE || type==Float.class ?
									new Float(s) :
									type==Double.TYPE || type==Double.class ?
										new Double(s) :
										type==Boolean.TYPE || type==Boolean.class ?
											new Boolean(s) :
											type==Character.TYPE || type==Character.class ?
												new Character(s.charAt(0)) :
												null;
	}

	/**
	 * Determines if the type is supported by the convertStringToDifferentType
	 * method above.
	 * @param type Class object of the desired return type.
	 * @return true iff the type is supported.
	 */
	public static boolean convertStringToDifferentTypeSupported(Class<?> type) {
		return 
			type==String.class ||
			type==StringBuffer.class ||
			type==Integer.TYPE || type==Integer.class ||
			type==Byte.TYPE || type==Byte.class ||
			type==Short.TYPE || type==Short.class ||
			type==Long.TYPE || type==Long.class ||
			type==Float.TYPE || type==Float.class ||
			type==Double.TYPE || type==Double.class ||
			type==Boolean.TYPE || type==Boolean.class ||
			type==Character.TYPE || type==Character.class;
	}

	/**
	 * Sets the static fields of a class cl with
	 * values which are given by a map.
	 * @param map Map containing the values to be set.
	 * @param cl Given class.
	 */
	public static void setStaticFields(Map<String, Object> map, Class<?> cl) {
		for (Entry<String, Object> e : map.entrySet())
			try {
				cl.getField(e.getKey()).set(null, e.getValue());
			}
			catch (IllegalAccessException ex) {
				// ignore
			}
			catch (NoSuchFieldException ex) {
				// ignore
			}
	}

	/**
	 * Returns a function which converts Number types into the
	 * desired Wrapper class type, which is given by returnClass.
	 * @param returnClass Desired return Class. If a primitive
	 * 	class is given, then also the associated wrapper class is
	 * 	used.
	 * @return The conversion function.
	 */
	public static Function<Number, ? extends Number> getNumberTypeConversionFunction(final Class<?> returnClass) {
		return returnClass == Byte.TYPE || returnClass == Byte.class ?
			new AbstractFunction<Number, Byte>() {
				@Override
				public Byte invoke(Number n) {
					return n.byteValue();
				}
			} :
			returnClass == Short.TYPE || returnClass == Short.class ?
				new AbstractFunction<Number, Short>() {
					@Override
					public Short invoke(Number n) {
						return n.shortValue();
					}
				} :
				returnClass == Integer.TYPE || returnClass == Integer.class ?
					new AbstractFunction<Number, Integer>() {
						@Override
						public Integer invoke(Number n) {
							return n.intValue();
						}
					} :
					returnClass == Long.TYPE || returnClass == Long.class ?
						new AbstractFunction<Number, Long>() {
							@Override
							public Long invoke(Number n) {
								return n.longValue();
							}
						} :
						returnClass == Float.TYPE || returnClass == Float.class ?
							new AbstractFunction<Number, Float>() {
								@Override
								public Float invoke(Number n) {
									return n.floatValue();
								}
							} :
							returnClass == Long.TYPE || returnClass == Long.class ?
								new AbstractFunction<Number, Long>() {
									@Override
									public Long invoke(Number n) {
										return n.longValue();
									}
								} :
								new Identity<Number>();
	}

	/**
	 * Returns true iff the given class is a integral number type. 
	 * @param clv Given class
	 * @return true iff the given class is a integral number type.
	 */
	public static boolean isIntegralType(Class<?> clv) {
		return 
			clv == Integer.TYPE || clv == Short.TYPE || clv == Byte.TYPE || clv == Long.TYPE ||
			clv == Integer.class || clv == Short.class || clv == Byte.class || clv == Long.class;
	}

	/**
	 * Returns true iff the given class is a real number type. 
	 * @param clv Given class
	 * @return true iff the given class is a real number type.
	 */
	public static boolean isRealType(Class<?> clv) {
		return 
			clv == Float.TYPE || clv == Double.TYPE ||
			clv == Float.class || clv == Double.class;
	}

	/**
	 * Calls the main method of a class cl with a given parameter array.
	 * @param cl The class.
	 * @param args String array with parameters for the main method.
	 */
	public static void callMainMethod(Class<?> cl, String[] args) {
		try {
			cl.getMethod("main", new Class[]{String[].class}).invoke(null, new Object[]{args});
		}
		catch (SecurityException e) {
			// ignore
		}
		catch (NoSuchMethodException e) {
			// ignore
		}
		catch (IllegalArgumentException e) {
			// ignore
		}
		catch (IllegalAccessException e) {
			// ignore
		}
		catch (InvocationTargetException e) {
			// ignore
		}
	}

	/**
	 * Returns a Cursor iterating over the elements of the
	 * array. This method is applicable for all kinds of arrays.
	 * @param valuesArray Array of arbitrary base type.
	 * @return Cursor iterating over the elements of the array.
	 */
	public static Cursor<Object> typedArrayCursor(final Object valuesArray) {
		return
			new Mapper<Integer, Object>(
				new AbstractFunction<Integer, Object> () {
					@Override
					public Object invoke(Integer index) {
						return Array.get(valuesArray, index);
					}
				},
				new Enumerator(Array.getLength(valuesArray))
			);
	}
}
