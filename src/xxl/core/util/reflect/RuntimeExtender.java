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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.io.Serializable;
import java.util.HashMap;

/**
 * This class allows to extend a class at runtime in order to have a MetaData
 * field and matching setter and getter-methods. Existing objects can be
 * modified that way at runtime as well.
 */
public class RuntimeExtender {
	
	/**
	 * Subclass of ClassLoader needed to call the protected methos definaClass.
	 */
	public static class ExtenderClassLoader extends ClassLoader {
		Class load (String name, byte[] b, int off, int len) throws ClassFormatError {			
			return	super.defineClass(name,b,off,len);
		}
	}
		
	final static byte[] extenderByteCodeStart = {
			-54, -2, -70, -66, 0, 0, 0, 46, 0, 45, 1, 0
	};
				
	final static byte[] extenderByteCodeMiddle = {	
		7, 0, 1, 1, 0
	};
	
	final static byte[] extenderByteCodeEnd = {
			7, 0, 3, 1, 0, 30, 120, 120,
			108, 47, 99, 111, 114, 101, 47, 117, 116, 105, 108, 47, 77, 101,
			116, 97, 68, 97, 116, 97, 80, 114, 111, 118, 105, 100, 101, 114, 7,
			0, 5, 1, 0, 20, 106, 97, 118, 97, 47, 105, 111, 47, 83, 101, 114,
			105, 97, 108, 105, 122, 97, 98, 108, 101, 7, 0, 7, 1, 0, 8, 109,
			101, 116, 97, 68, 97, 116, 97, 1, 0, 18, 76, 106, 97, 118, 97, 47,
			108, 97, 110, 103, 47, 79, 98, 106, 101, 99, 116, 59, 1, 0, 6, 60,
			105, 110, 105, 116, 62, 1, 0, 3, 40, 41, 86, 1, 0, 4, 67, 111, 100,
			101, 12, 0, 11, 0, 12, 10, 0, 4, 0, 14, 1, 0, 15, 76, 105, 110,
			101, 78, 117, 109, 98, 101, 114, 84, 97, 98, 108, 101, 1, 0, 18,
			76, 111, 99, 97, 108, 86, 97, 114, 105, 97, 98, 108, 101, 84, 97,
			98, 108, 101, 1, 0, 4, 116, 104, 105, 115, 1, 0, 10, 76, 69, 120,
			116, 101, 110, 100, 101, 114, 59, 1, 0, 11, 103, 101, 116, 77, 101,
			116, 97, 68, 97, 116, 97, 1, 0, 20, 40, 41, 76, 106, 97, 118, 97,
			47, 108, 97, 110, 103, 47, 79, 98, 106, 101, 99, 116, 59, 12, 0, 9,
			0, 10, 9, 0, 2, 0, 22, 1, 0, 11, 115, 101, 116, 77, 101, 116, 97,
			68, 97, 116, 97, 1, 0, 21, 40, 76, 106, 97, 118, 97, 47, 108, 97,
			110, 103, 47, 79, 98, 106, 101, 99, 116, 59, 41, 86, 1, 0, 10, 114,
			101, 97, 100, 79, 98, 106, 101, 99, 116, 1, 0, 30, 40, 76, 106, 97,
			118, 97, 47, 105, 111, 47, 79, 98, 106, 101, 99, 116, 73, 110, 112,
			117, 116, 83, 116, 114, 101, 97, 109, 59, 41, 86, 1, 0, 10, 69,
			120, 99, 101, 112, 116, 105, 111, 110, 115, 1, 0, 19, 106, 97, 118,
			97, 47, 105, 111, 47, 73, 79, 69, 120, 99, 101, 112, 116, 105, 111,
			110, 7, 0, 29, 1, 0, 32, 106, 97, 118, 97, 47, 108, 97, 110, 103,
			47, 67, 108, 97, 115, 115, 78, 111, 116, 70, 111, 117, 110, 100,
			69, 120, 99, 101, 112, 116, 105, 111, 110, 7, 0, 31, 1, 0, 25, 106,
			97, 118, 97, 47, 105, 111, 47, 79, 98, 106, 101, 99, 116, 73, 110,
			112, 117, 116, 83, 116, 114, 101, 97, 109, 7, 0, 33, 1, 0, 17, 100,
			101, 102, 97, 117, 108, 116, 82, 101, 97, 100, 79, 98, 106, 101,
			99, 116, 12, 0, 35, 0, 12, 10, 0, 34, 0, 36, 1, 0, 6, 115, 116,
			114, 101, 97, 109, 1, 0, 27, 76, 106, 97, 118, 97, 47, 105, 111,
			47, 79, 98, 106, 101, 99, 116, 73, 110, 112, 117, 116, 83, 116,
			114, 101, 97, 109, 59, 1, 0, 10, 105, 110, 105, 116, 105, 97, 108,
			105, 122, 101, 12, 0, 26, 0, 27, 10, 0, 2, 0, 41, 1, 0, 10, 83,
			111, 117, 114, 99, 101, 70, 105, 108, 101, 1, 0, 13, 69, 120, 116,
			101, 110, 100, 101, 114, 46, 106, 97, 118, 97, 0, 33, 0, 2, 0, 4,
			0, 2, 0, 6, 0, 8, 0, 1, 0, 0, 0, 9, 0, 10, 0, 0, 0, 5, 0, 1, 0, 11,
			0, 12, 0, 1, 0, 13, 0, 0, 0, 47, 0, 1, 0, 1, 0, 0, 0, 5, 42, -73,
			0, 15, -79, 0, 0, 0, 2, 0, 16, 0, 0, 0, 6, 0, 1, 0, 0, 0, 42, 0,
			17, 0, 0, 0, 12, 0, 1, 0, 0, 0, 5, 0, 18, 0, 19, 0, 0, 0, 1, 0, 20,
			0, 21, 0, 1, 0, 13, 0, 0, 0, 47, 0, 1, 0, 1, 0, 0, 0, 5, 42, -76,
			0, 23, -80, 0, 0, 0, 2, 0, 16, 0, 0, 0, 6, 0, 1, 0, 0, 0, 54, 0,
			17, 0, 0, 0, 12, 0, 1, 0, 0, 0, 5, 0, 18, 0, 19, 0, 0, 0, 1, 0, 24,
			0, 25, 0, 1, 0, 13, 0, 0, 0, 62, 0, 2, 0, 2, 0, 0, 0, 6, 42, 43,
			-75, 0, 23, -79, 0, 0, 0, 2, 0, 16, 0, 0, 0, 10, 0, 2, 0, 0, 0, 62,
			0, 5, 0, 63, 0, 17, 0, 0, 0, 22, 0, 2, 0, 0, 0, 6, 0, 18, 0, 19, 0,
			0, 0, 0, 0, 6, 0, 9, 0, 10, 0, 1, 0, 2, 0, 26, 0, 27, 0, 2, 0, 28,
			0, 0, 0, 6, 0, 2, 0, 30, 0, 32, 0, 13, 0, 0, 0, 61, 0, 1, 0, 2, 0,
			0, 0, 5, 43, -74, 0, 37, -79, 0, 0, 0, 2, 0, 16, 0, 0, 0, 10, 0, 2,
			0, 0, 0, 66, 0, 4, 0, 67, 0, 17, 0, 0, 0, 22, 0, 2, 0, 0, 0, 5, 0,
			18, 0, 19, 0, 0, 0, 0, 0, 5, 0, 38, 0, 39, 0, 1, 0, 1, 0, 40, 0,
			27, 0, 2, 0, 28, 0, 0, 0, 6, 0, 2, 0, 30, 0, 32, 0, 13, 0, 0, 0,
			62, 0, 2, 0, 2, 0, 0, 0, 6, 42, 43, -73, 0, 42, -79, 0, 0, 0, 2, 0,
			16, 0, 0, 0, 10, 0, 2, 0, 0, 0, 70, 0, 5, 0, 71, 0, 17, 0, 0, 0,
			22, 0, 2, 0, 0, 0, 6, 0, 18, 0, 19, 0, 0, 0, 0, 0, 6, 0, 38, 0, 39,
			0, 1, 0, 1, 0, 43, 0, 0, 0, 2, 0, 44
	};

	private static ExtenderClassLoader classloader = new ExtenderClassLoader();
	
	/**
	 * A HashMap containing {@link java.lang.Class classes} which have already
	 * been generated at runtime.
	 */
	protected static HashMap classes = new HashMap();
	
	/**
	 * A HashMap containing the serialized form of prototype objects
	 * from classes for which an instance was extended at runtime.
	 */	
	protected static HashMap prototypes = new HashMap();
	
	/** 
	 * Converts a string to an array of bytes.
	 * 
	 * @param s the string to convert.
	 * @return the resulting array.
	 */
	public static byte[] StringToByteArray(String s) {
		byte [] res = new byte[s.length()];
		for (int i=0; i<res.length; i++)
			res[i] = (byte)s.charAt(i);
		return res;		
	}
	
	/** 
	 * Extends a class at runtime. The resulting class supports 
	 * <code>public void setMetaData(Object)</code> and 
	 * <code>public Object getMetaData()</code> which both can be 
	 * used by using javas reflection mechanism {@link #main(String [])}. 
	 * 
	 * @param cl the class to extend
	 * @return a new class which is a subclass of <code>cl</code>
	 */
	public static Class extendClass(Class cl) {
		try {
			String name = cl.getName().replace('.','/');
			if (classes.containsKey(name))
				return (Class)classes.get(name);
			String classname = cl.getName().replace('.','_')+"_Extended";
			byte [] code = new byte[extenderByteCodeStart.length+extenderByteCodeMiddle.length+extenderByteCodeEnd.length+name.length()+classname.length()+2];
			int pos=0;
			System.arraycopy(extenderByteCodeStart,0,code,pos,extenderByteCodeStart.length);
		  code[pos+=extenderByteCodeStart.length]=(byte)classname.length();
		  byte [] classnamea = StringToByteArray(classname);
		  System.arraycopy(classnamea,0,code,pos+=1,classnamea.length);	  
		  System.arraycopy(extenderByteCodeMiddle,0,code,pos+=classnamea.length,extenderByteCodeMiddle.length);
		  code[pos+=extenderByteCodeMiddle.length]=(byte)name.length();
		  byte [] namea = StringToByteArray(name);
		  System.arraycopy(namea,0,code,pos+=1,namea.length);	  
		  System.arraycopy(extenderByteCodeEnd,0,code,pos+=namea.length,extenderByteCodeEnd.length);
			Class result = classloader.load(classname,code,0,code.length);
			classes.put(name,result);
			return result;
		}
		catch (Exception e) {
		}
		return null;
	}
	
	/**
	 * Returns a modified version of the given object which is a member
	 * of the subclass defined in {@link #extendClass(Class)}. 
	 * 
	 * @param object the source object
	 * @return a new object which is a modified version of <source>object</source>
	 */
	public static Object extendObject(Serializable object) {
		try {
			final Class classOfObject = extendClass(object.getClass());	
			String name = classOfObject.getName();
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			(new ObjectOutputStream(baos)).writeObject(object);
			byte [] serializedObject = baos.toByteArray();
			byte [] prototype;
			if (prototypes.containsKey(name)) {
				prototype = (byte [])prototypes.get(name);
			}	
			else {
				Object prototypeinstance = classOfObject.newInstance();
				ByteArrayOutputStream baos2 = new ByteArrayOutputStream();
				(new ObjectOutputStream(baos2)).writeObject(prototypeinstance);
				prototype = baos2.toByteArray();
				prototypes.put(name,prototype);
			}
			byte [] serializedNewObject = new byte[serializedObject.length+48+name.length()];
			System.arraycopy(prototype,0,serializedNewObject,0,54+name.length());
			System.arraycopy(serializedObject,7,serializedNewObject,54+name.length(),serializedObject.length-7);
			serializedNewObject[serializedNewObject.length-1] = (byte)112;
			ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(serializedNewObject)) {
				protected Class resolveClass (ObjectStreamClass desc) throws IOException, ClassNotFoundException {
					if (desc.getName().equals(classOfObject.getName()))   
						return classOfObject;  
					else 
						return super.resolveClass(desc);					
				}		
			};			
			return in.readObject();
		}
		catch (Exception e) {
		}
		return null;			
	}
}
