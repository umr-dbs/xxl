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

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

import xxl.core.xxql.AdvResultSetMetaData;

public abstract class ReflectionColumn extends Column {

	private static final long serialVersionUID = 1L;
	
	protected Class<?>[]	argumentsTypes;
	protected Class<?>	clazz;
	protected Column[]	argumentsColumns;

	public ReflectionColumn(String name) {
		super(name);
	}

	public abstract void setMetaData(AdvResultSetMetaData metadata, String newAlias);
	public abstract void setMetaDatas(AdvResultSetMetaData leftMetaData, AdvResultSetMetaData rightMetaData);
	
	@SuppressWarnings("unchecked")
	protected Constructor getSuitableConstructor(Class<?>[] pmt) {
	
		try {
			Constructor ret = clazz.getConstructor(pmt);
			return ret;
		} catch (Exception e) {
			// don't care, if it wasn't found this way try smarter..
		}
		for(Class cls : pmt){
			System.out.println(cls);
		}
		Constructor[] constructors = clazz.getConstructors();
		for(Constructor cls : constructors){
			System.out.println(cls);
		}
		constrs: for (Constructor c : constructors) {
			Class<?>[] curPmt = c.getParameterTypes();
			// if the method hasn't even got the same number of arguments we
			// continue
			if (curPmt.length != pmt.length) {
				continue;
			}
			// check parametertypes for compatibility
			for (int i = 0; i < pmt.length; i++) {
				if (curPmt[i].isPrimitive()) {
					// TODO: noch nicht ganz sauber mit zahlen, die evtl
					// automatisch gecasted werden...
					/*
					 * fuer zahlentypen (double, float, int, long, short, char)
					 * gilt: Methode mit ... frisst ... double : alles float :
					 * alles ausser double int : nur char, short und int long :
					 * alle ausser double und float short : nur short char : nur
					 * char
					 */
					if (!getPrimitiveType(pmt[i]).equals(curPmt[i])) {
						continue; // type doesn't match for this parameter: try
						// next.
					}
				} else {
					Class<?> tmp = getObjectType(pmt[i]);
					// if expected class is superclass of given class (from pmt)
					// it matches
					if (!curPmt[i].isAssignableFrom(tmp)) {
						continue constrs;
					}
				}
			}
			// if Method m has passed all those checks it matches.
			return c;
		}
		return null;
	}
	
	
	protected Method getSuitableMethod(String methodName, Class<?>[] pmt, boolean wantStatic) {
		// TODO: this is far from perfect
		Method[] methods = clazz.getMethods();
	
		try {
			Method ret = clazz.getMethod(methodName, pmt);
			// if either the found method is static or we're not looking for a static method anyway
			if(Modifier.isStatic(ret.getModifiers()) || !wantStatic)
				return ret;
		} catch (Exception e) {
			// don't care, if it wasn't found this way try smarter..
		}
	
		mthds: for (Method m : methods) {
			Class<?>[] curPmt = m.getParameterTypes();
			// if the method hasn't even got the same name or number of
			// arguments we continue
			if (!m.getName().equals(methodName) || curPmt.length != pmt.length) {
				continue;
			}
			// if the found method is *not* static, but we want a static one, try next.
			if(!Modifier.isStatic(m.getModifiers()) && wantStatic)
				continue;
			// void-methods are not really helpful
			if(m.getReturnType().equals(void.class))
				continue;
			// check parametertypes for compatibility
			for (int i = 0; i < pmt.length; i++) {
				if (curPmt[i].isPrimitive()) {
					// TODO: noch nicht ganz sauber mit zahlen, die evtl
					// automatisch gecasted werden...
					/*
					 * fuer zahlentypen (double, float, int, long, short, char)
					 * gilt: Methode mit ... frisst ... double : alles float :
					 * alles ausser double int : nur char, short und int long :
					 * alle ausser double und float short : nur short char : nur
					 * char
					 */
					if (!getPrimitiveType(pmt[i]).equals(curPmt[i])) {
						continue; // type doesn't match for this parameter: try
						// next.
					}
				} else {
					Class<?> tmp = getObjectType(pmt[i]);
					// if expected class is superclass of given class (from pmt)
					// it matches
					if (!curPmt[i].isAssignableFrom(tmp)) {
						continue mthds;
					}
				}
			}
			// if Method m has passed all those checks it matches.
			return m;
		}
		return null;
	}
	/**
	 * Returns the Object-type for a primitive type (or just the given type if
	 * it isn't primitive)
	 * 
	 * @param type
	 *            a type/class, that might be primitive
	 * @return the according non-primitive type
	 */
	@SuppressWarnings("unchecked")
	protected
	static Class getObjectType(Class<?> type) {
		if (!type.isPrimitive()) {
			return type;
		}

		if (type.equals(byte.class)) {
			return Byte.class;
		} else if (type.equals(short.class)) {
			return Short.class;
		} else if (type.equals(int.class)) {
			return Integer.class;
		} else if (type.equals(long.class)) {
			return Long.class;
		} else if (type.equals(float.class)) {
			return Float.class;
		} else if (type.equals(double.class)) {
			return Double.class;
		} else if (type.equals(boolean.class)) {
			return Boolean.class;
		} else if (type.equals(char.class)) {
			return Character.class;
		}

		throw new RuntimeException("Unknown primitive type " + type.getName());
	}

	@SuppressWarnings("unchecked")
	protected
	static Class getPrimitiveType(Class<?> type) {
		if (type.isPrimitive()) {
			return type;
		}

		if (type.equals(Byte.class)) {
			return byte.class;
		} else if (type.equals(Short.class)) {
			return short.class;
		} else if (type.equals(Integer.class)) {
			return int.class;
		} else if (type.equals(Long.class)) {
			return long.class;
		} else if (type.equals(Float.class)) {
			return float.class;
		} else if (type.equals(Double.class)) {
			return double.class;
		} else if (type.equals(Boolean.class)) {
			return boolean.class;
		} else if (type.equals(Character.class)) {
			return char.class;
		}

		throw new RuntimeException("Unknown primitive type " + type.getName());
	}
	
}
