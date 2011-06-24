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

package xxl.core.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintStream;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import xxl.core.io.NullOutputStream;
import xxl.core.io.converters.Converter;
import xxl.core.util.metaData.CompositeMetaData;
import xxl.core.util.metaData.MetaDataException;

/**
 * The <code>XXLSystem</code> class contains system related methods. For
 * example there are static methods to determine the memory size of an object.
 * It cannot be instantiated.
 *
 * @see java.util.HashSet
 * @see java.util.Set
 * @see java.lang.reflect.Array
 * @see java.lang.reflect.Field
 * @see java.lang.reflect.Modifier
 */
public class XXLSystem {

	/**
	 * Don't let anyone instantiate this class. 
	 */
	private XXLSystem() {
		// ensure non-instantiability
	}

	/**
	 * The "standard" null stream. This stream is already open and ready to
	 * accept output data. Typically this stream corresponds to suppress any
	 * display output. This stream fills the gap in {@link java.lang.System}
	 * providing "only" standard {@link java.io.PrintStream print streams}
	 * for standard out and errors but no "null sink".
	 * 
	 * @see PrintStream#println()
	 */
	public static final PrintStream NULL = new PrintStream( NullOutputStream.NULL);

	/**
	 * The default size of a reference in memory.
	 */
	public final static int REFERENCE_MEM_SIZE = 8;
	
	/**
	 * This class is a Wrapper for objects with the intention to distinguish
	 * two objects if and only if they refer to the same object. That means for
	 * any reference values <code>x</code> and <code>y</code>, the method
	 * <code>equals()</code> returns <code>true</code> if and only if
	 * <code>x</code> and <code>y</code> refer to the same object
	 * (<code>x==y</code> has the value <code>true</code>).
	 * 
	 * <p>It is used in the method
	 * {@link #sizeOf(Object, int, Set, Set, Set, int)} where attributes of the
	 * object to be analyzed are inserted in a set with the intention to create
	 * an exact image of the memory allocated for this given object. Each
	 * non-primitive attribute is wrapped using this class before inserting it
	 * into the set, so the set contains only one instance of each traversed
	 * class during determining the object size. Because of wrapping the
	 * non-primitive attributes the <code>equals</code>-method of this class is
	 * used, in contradiction to inserting the objects directly in the set, the
	 * attributes's own <code>equals</code>-method will be used, but this may
	 * lead to duplicates, like the following example demonstrates:
	 * <pre><code>
	 * 	class Test {
	 * 
	 * 		public Integer a, b;
	 *
	 * 		public Test(Integer a, Integer b) {
	 * 			this.a = a;
	 * 			this.b = b;
	 * 		}
	 *
	 * 		public static void main(String[] args) throws Exception {
	 * 			Test test = new Test(new Integer(7), new Integer(7));
	 * 			System.out.println("a == b ? " + test.a == test.b);
	 * 			System.out.println("a.equals(b) ? " + test.a.equals(test.b));
	 * 			System.out.println("getObjectSize(test) = " + XXLSystem.getObjectSize(test));
	 * 		}
	 * 	}
	 * </code></pre>
	 * The output of this short example is:
	 * <pre>
	 * 	a==b ? false
	 * 	a.equals(b) ? true
	 * 	getObjectSize(test) = 32
	 * </pre>
	 * If the attributes inserted into the HashSet were not wrapped,
	 * the returned object size would only be 28, because
	 * the attribute <tt>a</tt> was detected to be equal to attribute <tt>b</tt>,
	 * and therefore only a reference pointing to the same object would be
	 * saved in memory.
	 * 
	 * @see #sizeOf(Object, int, Set, Set, Set, int)
	 */
	protected static final class Wrapper {

		/**
		 * The wrapped object.
		 */
		public final Object object;

		/**
		 * Creates a new Wrapper
		 * by wrapping the given object.
		 *
		 * @param object the object to be wrapped.
		 */
		public Wrapper(Object object) {
			this.object = object;
		}

		/**
		 * The <tt>equals</tt> method for class <code>Object</code> implements
		 * the most discriminating possible equivalence relation on objects;
		 * that is, for any reference values <code>x</code> and <code>y</code>,
		 * this method returns <code>true</code> if and only if <code>x</code> and
		 * <code>y</code> refer to the same object (<code>x==y</code> has the
		 * value <code>true</code>).
		 *
		 * @param wrapper the reference object with which to compare.
		 * @return  <code>true</code> if this object is the same as
		 * 		the wrapper argument; <code>false</code> otherwise.
		 */
		@Override
		public boolean equals(Object wrapper) {
			return this == wrapper ? true : wrapper == null || !(wrapper instanceof Wrapper) ? false : object == ((Wrapper)wrapper).object;
		}

		/**
		 * Returns a hash code value for the wrapped object.
		 * This method is supported for the benefit
		 * of hashtables such as those provided by
		 * <code>java.util.Hashtable</code>.
		 *
		 * @return a hash code value for this object.
		 * @see java.lang.Object#hashCode()
		 * @see java.util.Hashtable
		 */
		@Override
		public int hashCode () {
			return object.hashCode();
		}
		
		@Override
		public String toString() {
			return "Wrapper(" + object.toString() + ")";
		}
		
	}

	/**
	 * The size of an object is computed recursively, by determining the
	 * attributes of the class the object is an instance of. If the given
	 * object is an array the sizes of the array's components are computed
	 * recursively and the resulting memory size is incremented by 4 bytes
	 * (needed to save the length information of an array). Otherwise the
	 * algorithm determines the attributes (fields) of the object's class and
	 * its super classes. If an attribute's type is primitive, the according
	 * size is added to the object's memory size. Otherwise the attribute is an
	 * objects and its memory size is calculated recursively and added to the
	 * object's memory size.
	 * 
	 * <p><b>Overview:</b><br><br>
	 * 
	 * <table>
	 * <tr><th align=left colspan=3 style="font-weight:normal">size of primitive types</th></tr>
	 * <tr><td>byte, boolean</td><td>&nbsp;:&nbsp;</td><td>1 byte</td></tr>
	 * <tr><td>short, char</td><td>&nbsp;:&nbsp;</td><td>2 bytes</td></tr>
	 * <tr><td>int, float</td><td>&nbsp;:&nbsp;</td><td>4 bytes</td></tr>
	 * <tr><td>long, double</td><td>&nbsp;:&nbsp;</td><td>8 bytes</td></tr>
	 * </table><br><br>
	 * 
	 * <table>
	 * <tr><th align=left colspan=3 style="font-weight:normal">size of references depends on the platform</th></tr>
	 * <tr><td>32-bit platform</td><td>&nbsp;:&nbsp;</td><td>4 bytes</td></tr>
	 * <tr><td>64-bit platform</td><td>&nbsp;:&nbsp;</td><td>8 bytes</td></tr>
	 * </table><br><br>
	 * 
	 * size of primitive arrays: 4 bytes + (array.length * size of primitive type)<br><br>
	 * 
	 * size of object arrays : 4 bytes + &Sigma;(size of components)<br><br>
	 * 
	 * All traversed attributes are inserted in a set, which represents an
	 * exact image of the allocated memory concerning the given object.<p>
	 * 
	 * @param object the object the memory size is to be determined.
	 * @param referenceSize the memory size of a reference.
	 * @param processedObjects a set containing the traversed attributes
	 *        (required for cycle detection).
	 * @param excludedClasses a set of classes which are excluded during memory
	 *        measurement.
	 * @param excludedFields a set of fields which are excluded during memory
	 *        measurement.
	 * @param excludedFieldModifiers a bitmap identifying the
	 *        {@link Modifier modifiers} of the fields that should be excluded
	 *        from memory measurement.
	 * @return the memory size of the specified object.
	 * @throws IllegalAccessException if a field of the given object cannot be
	 *         accessed.
	 */
	public static int sizeOf(Object object, int referenceSize, Set<? super Wrapper> processedObjects, Set<Class<?>> excludedClasses, Set<Field> excludedFields, int excludedFieldModifiers) throws IllegalAccessException {
		int size = 0;
		
		// Circle detection
		if (object == null || !processedObjects.add(new Wrapper(object)))
			return size;
		
		Class<?> objectClass = object.getClass(); // refresh object-reference
		if (objectClass.isArray()) {
			int length = Array.getLength(object);
			size += 4; // allocated for saving length
			objectClass = objectClass.getComponentType();
			if (objectClass.isPrimitive())
				size += objectClass == Boolean.TYPE ? Math.ceil(length / 8.0) : length * (objectClass == Byte.TYPE ? 1 : objectClass == Character.TYPE || objectClass == Short.TYPE ? 2 : objectClass == Integer.TYPE || objectClass == Float.TYPE ? 4 : 8);
			else
				for (int i = 0; i < length; i++)
					size += sizeOf(Array.get(object, i), referenceSize, processedObjects, excludedClasses, excludedFields, excludedFieldModifiers);
		}
		else
			for (; objectClass != null; objectClass = objectClass.getSuperclass())
				for (Field field : objectClass.getDeclaredFields()) {
					objectClass = field.getType();
					if (!excludedClasses.contains(objectClass) && !excludedFields.contains(field) && (field.getModifiers() & excludedFieldModifiers) == 0)
						if (objectClass.isPrimitive())
							size += objectClass == Boolean.TYPE || objectClass == Byte.TYPE ? 1 : objectClass == Character.TYPE || objectClass == Short.TYPE ? 2 : objectClass == Integer.TYPE || objectClass == Float.TYPE ? 4 : 8;
						else {
							field.setAccessible(true);
							size += referenceSize + sizeOf(field.get(object), referenceSize, processedObjects, excludedClasses, excludedFields, excludedFieldModifiers);
						}
				}
		return size;
	}

	/**
	 * The size of an object is computed recursively, by determining the
	 * attributes of the class the object is an instance of. If the given
	 * object is an array the sizes of the array's components are computed
	 * recursively and the resulting memory size is incremented by 4 bytes
	 * (needed to save the length information of an array). Otherwise the
	 * algorithm determines the attributes (fields) of the object's class and
	 * its super classes. If an attribute's type is primitive, the according
	 * size is added to the object's memory size. Otherwise the attribute is an
	 * objects and its memory size is calculated recursively and added to the
	 * object's memory size.
	 * 
	 * <p><b>Overview:</b><br><br>
	 * 
	 * <table>
	 * <tr><th align=left colspan=3 style="font-weight:normal">size of primitive types</th></tr>
	 * <tr><td>byte, boolean</td><td>&nbsp;:&nbsp;</td><td>1 byte</td></tr>
	 * <tr><td>short, char</td><td>&nbsp;:&nbsp;</td><td>2 bytes</td></tr>
	 * <tr><td>int, float</td><td>&nbsp;:&nbsp;</td><td>4 bytes</td></tr>
	 * <tr><td>long, double</td><td>&nbsp;:&nbsp;</td><td>8 bytes</td></tr>
	 * </table><br><br>
	 * 
	 * <table>
	 * <tr><th align=left colspan=3 style="font-weight:normal">size of references depends on the platform</th></tr>
	 * <tr><td>32-bit platform</td><td>&nbsp;:&nbsp;</td><td>4 bytes</td></tr>
	 * <tr><td>64-bit platform</td><td>&nbsp;:&nbsp;</td><td>8 bytes</td></tr>
	 * </table><br><br>
	 * 
	 * size of primitive arrays: 4 bytes + (array.length * size of primitive type)<br><br>
	 * 
	 * size of object arrays : 4 bytes + &Sigma;(size of components)<br><br>
	 * 
	 * All traversed attributes are inserted in a set, which represents an
	 * exact image of the allocated memory concerning the given object.<p>
	 * 
	 * @param object the object the memory size is to be determined.
	 * @param referenceSize the memory size of a reference.
	 * @param processedObjects a set containing the traversed attributes
	 *        (required for cycle detection).
	 * @param excludedClasses a set of classes which are excluded during memory
	 *        measurement.
	 * @param excludedFields a set of fields which are excluded during memory
	 *        measurement.
	 * @param excludedFieldModifiers the {@link Modifier modifiers} of the
	 *        fields that should be excluded from memory measurement.
	 * @return the memory size of the specified object.
	 * @throws IllegalAccessException if a field of the given object cannot be
	 *         accessed.
	 */
	public static int sizeOf(Object object, int referenceSize, Set<? super Wrapper> processedObjects, Set<Class<?>> excludedClasses, Set<Field> excludedFields, int... excludedFieldModifiers) throws IllegalAccessException {
		int bitset = 0;
		for (int excludedFieldModifier : excludedFieldModifiers)
			bitset |= excludedFieldModifier;
		return sizeOf(object, referenceSize, processedObjects, excludedClasses, excludedFields, bitset);
	}
	
	/**
	 * The main memory size of the given object is computed recursively, by
	 * determining the attributes of the class the object is an instance of.
	 * The size of references on different platforms (i.e. 32-bit and 64-bit
	 * platforms respectively) can be specified separately. For determining the
	 * main memory size of the given object only static attributes are excluded
	 * from the memory measurement.
	 * 
	 * @param object the object the memory size is to be determined.
	 * @param referenceSize the memory size of a reference.
	 * @param processedObjects a set containing the traversed attributes
	 *        (required for cycle detection).
	 * @param excludedClasses a set of classes which are excluded during memory
	 *        measurement.
	 * @param excludedFields a set of fields which are excluded during memory
	 *        measurement.
	 * @return the memory size of the specified object.
	 * @throws IllegalAccessException if a field of the given object cannot be
	 *         accessed.
	 */
	public static int memSizeOf(Object object, int referenceSize, Set<? super Wrapper> processedObjects, Set<Class<?>> excludedClasses, Set<Field> excludedFields) throws IllegalAccessException {
		return sizeOf(object, referenceSize, processedObjects, excludedClasses, excludedFields, Modifier.STATIC);
	}

	/**
	 * The main memory size of the given object is computed recursively, by
	 * determining the attributes of the class the object is an instance of.
	 * The size of references on different platforms (i.e. 32-bit and 64-bit
	 * platforms respectively) can be specified separately. For determining the
	 * main memory size of the given object only static attributes are excluded
	 * from the memory measurement.
	 * 
	 * @param object the object the memory size is to be determined.
	 * @param referenceSize the memory size of a reference.
	 * @param processedObjects a set containing the traversed attributes
	 *        (required for cycle detection).
	 * @return the memory size of the specified object.
	 * @throws IllegalAccessException if a field of the given object cannot be
	 *         accessed.
	 */
	@SuppressWarnings("unchecked")
	public static int memSizeOf(Object object, int referenceSize, Set<? super Wrapper> processedObjects) throws IllegalAccessException {
		return memSizeOf(object, referenceSize, processedObjects, Collections.EMPTY_SET, Collections.EMPTY_SET);
	}

	/**
	 * The main memory size of the given object is computed recursively, by
	 * determining the attributes of the class the object is an instance of.
	 * The size of references on different platforms (i.e. 32-bit and 64-bit
	 * platforms respectively) can be specified separately. For determining the
	 * main memory size of the given object only static attributes are excluded
	 * from the memory measurement.
	 * 
	 * @param object the object the memory size is to be determined.
	 * @param referenceSize the memory size of a reference.
	 * @param excludedClasses a set of classes which are excluded during memory
	 *        measurement.
	 * @param excludedFields a set of fields which are excluded during memory
	 *        measurement.
	 * @return the memory size of the specified object.
	 * @throws IllegalAccessException if a field of the given object cannot be
	 *         accessed.
	 */
	public static int memSizeOf(Object object, int referenceSize, Set<Class<?>> excludedClasses, Set<Field> excludedFields) throws IllegalAccessException {
		return memSizeOf(object, referenceSize, new HashSet<Wrapper>(), excludedClasses, excludedFields);
	}

	/**
	 * The main memory size of the given object is computed recursively, by
	 * determining the attributes of the class the object is an instance of.
	 * The size of references on different platforms (i.e. 32-bit and 64-bit
	 * platforms respectively) can be specified separately. For determining the
	 * main memory size of the given object only static attributes are excluded
	 * from the memory measurement.
	 * 
	 * @param object the object the memory size is to be determined.
	 * @param referenceSize the memory size of a reference.
	 * @return the memory size of the specified object.
	 * @throws IllegalAccessException if a field of the given object cannot be
	 *         accessed.
	 */
	public static int memSizeOf(Object object, int referenceSize) throws IllegalAccessException {
		return memSizeOf(object, referenceSize, new HashSet<Wrapper>());
	}

	/**
	 * The main memory size of the given object is computed recursively, by
	 * determining the attributes of the class the object is an instance of.
	 * The size is computed for a 32-bit platform, i.e. a reference allocates
	 * 8 bytes of main memory. For determining the main memory size of the
	 * given object only static attributes are excluded from the memory
	 * measurement.
	 * 
	 * @param object the object the memory size is to be determined.
	 * @return the memory size of the specified object.
	 * @throws IllegalAccessException if a field of the given object cannot be
	 *         accessed.
	 */
	public static int memSizeOf(Object object) throws IllegalAccessException {
		return memSizeOf(object, 4);
	}
	
	/**
	 * This method computes the (secondary) memory size of the given object. To
	 * get the memory size of the specified object, a reference to this object
	 * is needed. If this reference is <code>null</code>, the trivial case, the
	 * returned object size is 0 bytes. Otherwise
	 * {@link #sizeOf(Object, int, Set, Set, Set, int)} is called, which
	 * analyzes the class this object is an instance of, the super classes and
	 * especially the memory size allocated for each non-static and
	 * non-transient attribute of these classes is determined and saved in a
	 * set. For further information of saving the objects in the set see
	 * {@link Wrapper}.
	 * 
	 * @see #sizeOf(Object, int, Set, Set, Set, int)
	 *
	 * @param object the object the memory size is to be determined.
	 * @param processedObjects a set containing the traversed attributes
	 *        (required for cycle detection).
	 * @param excludedClasses a set of classes which are excluded during memory
	 *        measurement.
	 * @param excludedFields a set of fields which are excluded during memory
	 *        measurement.
	 * @return the memory size of the specified object.
	 * @throws IllegalAccessException if a field of the given object cannot be
	 *         accessed.
	 */
	public static int getObjectSize(Object object, Set<? super Wrapper> processedObjects, Set<Class<?>> excludedClasses, Set<Field> excludedFields) throws IllegalAccessException {
		return sizeOf(object, REFERENCE_MEM_SIZE, processedObjects, excludedClasses, excludedFields, Modifier.STATIC, Modifier.TRANSIENT);
	}
	
	/**
	 * This method computes the (secondary) memory size of the given object. To
	 * get the memory size of the specified object, a reference to this object
	 * is needed. If this reference is <code>null</code>, the trivial case, the
	 * returned object size is 0 bytes. Otherwise
	 * {@link #sizeOf(Object, int, Set, Set, Set, int)} is called, which
	 * analyzes the class this object is an instance of, the super classes and
	 * especially the memory size allocated for each non-static and
	 * non-transient attribute of these classes is determined and saved in a
	 * set. For further information of saving the objects in the set see
	 * {@link Wrapper}.
	 * 
	 * @see #sizeOf(Object, int, Set, Set, Set, int)
	 *
	 * @param object the object the memory size is to be determined.
	 * @param processedObjects a set containing the traversed attributes
	 *        (required for cycle detection).
	 * @return the memory size of the specified object.
	 * @throws IllegalAccessException if a field of the given object cannot be
	 *         accessed.
	 */
	@SuppressWarnings("unchecked")
	public static int getObjectSize(Object object, Set<? super Wrapper> processedObjects) throws IllegalAccessException {
		return getObjectSize(object, processedObjects, Collections.EMPTY_SET, Collections.EMPTY_SET);
	}
	
	/**
	 * This method computes the (secondary) memory size of the given object. To
	 * get the memory size of the specified object, a reference to this object
	 * is needed. If this reference is <code>null</code>, the trivial case, the
	 * returned object size is 0 bytes. Otherwise
	 * {@link #sizeOf(Object, int, Set, Set, Set, int)} is called, which
	 * analyzes the class this object is an instance of, the super classes and
	 * especially the memory size allocated for each non-static and
	 * non-transient attribute of these classes is determined and saved in a
	 * set. For further information of saving the objects in the set see
	 * {@link Wrapper}.
	 * 
	 * @see #sizeOf(Object, int, Set, Set, Set, int)
	 *
	 * @param object the object the memory size is to be determined.
	 * @param excludedClasses a set of classes which are excluded during memory
	 *        measurement.
	 * @param excludedFields a set of fields which are excluded during memory
	 *        measurement.
	 * @return the memory size of the specified object.
	 * @throws IllegalAccessException if a field of the given object cannot be
	 *         accessed.
	 */
	public static int getObjectSize(Object object, Set<Class<?>> excludedClasses, Set<Field> excludedFields) throws IllegalAccessException {
		return getObjectSize(object, new HashSet<Wrapper>(), excludedClasses, excludedFields);
	}

	/**
	 * This method computes the (secondary) memory size of the given object. To
	 * get the memory size of the specified object, a reference to this object
	 * is needed. If this reference is <code>null</code>, the trivial case, the
	 * returned object size is 0 bytes. Otherwise
	 * {@link #sizeOf(Object, int, Set, Set, Set, int)} is called, which
	 * analyzes the class this object is an instance of, the super classes and
	 * especially the memory size allocated for each non-static and
	 * non-transient attribute of these classes is determined and saved in a
	 * set. For further information of saving the objects in the set see
	 * {@link Wrapper}.
	 * 
	 * @see #sizeOf(Object, int, Set, Set, Set, int)
	 *
	 * @param object the object the memory size is to be determined.
	 * @return the memory size of the specified object.
	 * @throws IllegalAccessException if a field of the given object cannot be
	 *         accessed.
	 */
	public static int getObjectSize(Object object) throws IllegalAccessException {
		return getObjectSize(object, new HashSet<Wrapper>());
	}

	/**
	 * Serializes an object and returns its byte representation.
	 * 
	 * @param o input object to be serialized.
	 * @return byte array containing the byte representation.
	 */
	public static byte[] serializeObject(Serializable o) {
		try {
			ByteArrayOutputStream output = new ByteArrayOutputStream();
			ObjectOutputStream out = new ObjectOutputStream(output);
			out.writeObject(o);
			out.flush();
			out.close();
			return output.toByteArray();
		}
		catch (IOException e) {
			throw new WrappingRuntimeException(e);
		}
	}
	
	/**
	 * Deserializes a byte array to an object and returns it.
	 * @param b byte array containing the byte representation of an object.
	 * @return deserialized object.
	 */
	public static Object deserializeObject(byte b[]) {
		try {
			ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(b));
			Object o = in.readObject();
			in.close();
			return o;
		}
		catch (IOException e) {
			throw new WrappingRuntimeException(e);
		}
		catch (ClassNotFoundException e) {
			throw new WrappingRuntimeException(e);
		}
	}

	/**
	 * Converts an object using the given converter and returns its byte
	 * representation.
	 * 
	 * @param <T> the type of the object to be converted.
	 * @param o input object to be serialized.
	 * @param converter the converter used for converting the object.
	 * @return byte array containing the byte representation.
	 */
	public static <T> byte[] convertObject(T o, Converter<? super T> converter) {
		try {
			ByteArrayOutputStream output = new ByteArrayOutputStream();
			DataOutputStream out = new DataOutputStream(output);
			converter.write(out, o);
			out.flush();
			out.close();
			return output.toByteArray();
		}
		catch (IOException e) {
			throw new WrappingRuntimeException(e);
		}
	}
	
	/**
	 * Reconverts a byte array to an object using the given converter and
	 * returns it.
	 * 
	 * @param <T> the type of the object to be reconverted.
	 * @param b byte array containing the byte representation of an object.
	 * @param converter the converter used for reconverting the object.
	 * @return reconverted object.
	 */
	public static <T> T reconvertObject(byte b[], Converter<T> converter) {
		try {
			DataInputStream in = new DataInputStream(new ByteArrayInputStream(b));
			T o = converter.read(in);
			in.close();
			return o;
		}
		catch (IOException e) {
			throw new WrappingRuntimeException(e);
		}
	}

	/**
	 * Clones an object whether or not the clone method is protected (via reflection).
	 * If a clone is not possible, then a RuntimeException is thrown.
	 * @param o to be cloned.
	 * @return the cloned Object.
	 */
	public static Object cloneObject(Object o) {
		try {
			Method m = o.getClass().getMethod("clone");
			m.setAccessible(true);
			return m.invoke(o);
		}
		catch (Exception e) {
			throw new WrappingRuntimeException(e);
		}
	}

	/**
	 * Determines iff the mainmaker called the current class. 
	 * @return true iff the mainmaker called the current class.
	 */
	public static boolean calledFromMainMaker() {
		try {
			throw new RuntimeException();
		}
		catch (Exception e) {
			StackTraceElement st[] = e.getStackTrace();
			for (int i=0; i<st.length; i++)
				if (st[i].getClassName().toLowerCase().indexOf("mainmaker")>=0)
					return true;
			return false;
		}
	}

	/**
	 * Returns the outpath that has been passed to java via the -D Option.
	 * @return String - the outpath
	 */
	public static String getOutPath() {
		String s = System.getProperty("xxloutpath");
		if (s==null)
			throw new RuntimeException("xxloutpath has not been given as a parameter. Use java -Dxxloutpath=...");
		return s; 
	}

	/**
	 * Returns the rootpath that has been passed to java via the -D Option.
	 * @return String - the outpath
	 */
	public static String getRootPath() {
		String s = System.getProperty("xxlrootpath");
		if (s==null)
			throw new RuntimeException("xxlrootpath has not been given as a parameter. Use java -Dxxlrootpath=...");
		return s; 
	}

	/**
	 * Constructs a directory inside the outpath with the desired subdirectory.
	 * The partial names of the subdirectory have to be passed inside the subdirs
	 * array.
	 * The path does not have a file separator at the end.
	 * @param subdirs partial names of the path
	 * @return whole path (contains a file separator at the end).
	 */
	public static String getOutPath(String subdirs[]) {
		StringBuffer sb = new StringBuffer(XXLSystem.getOutPath());
		for (int i=0; i<subdirs.length; i++)
			sb.append(File.separator + subdirs[i]);
		String s = sb.toString();
		File f = new File(s);
		f.mkdirs();
		return s;
	}

	/** 
	 * Returns a data path inside xxl/data, which points to
	 * a certain subdirectory.
	 * The partial names of the subdirectory have to be passed inside the subdirs
	 * array.
	 * The path does not have a file separator at the end.
	 * 
	 * @param subdirs string containing subpath
	 * @return whole path (contains a file separator at the end).
	 */
	public static String getDataPath(String subdirs[]) {
		StringBuffer sb = new StringBuffer(XXLSystem.getRootPath());
		sb.append(File.separator + "data");
		for (int i=0; i<subdirs.length; i++)
			sb.append(File.separator + subdirs[i]);
		return sb.toString();
	}


	/**
	 * Returns the version of the Java RTE (only the major version
	 * number).
	 *
	 * @return returns the Java RTE major version number
	 */
	public static double getJavaVersion() {
		String s = System.getProperty("java.version");
		int pos = s.indexOf('.');
		if (pos>=0) {
			pos = s.indexOf('.',pos+1);
			if (pos>0)
				s = s.substring(0,pos);
		}
		return Double.parseDouble(s);
	}
	
	/**
	 * Tries to get a value from the meta data of given object, specified by the key. 
	 * If the object does not implement the CompositeMetaData interface or the key
	 * is not found, null is returned. 
	 * @see xxl.core.util.metaData.CompositeMetaData
	 *  
	 * @param <I> the type o the key.
	 * @param key the specified key
	 * @param object an object
	 * @return the value specified by the key.
	 */
	@SuppressWarnings("unchecked")
	public static <I> Object getValueFromMD (I key, Object object) {
		if (object == null || !(object instanceof CompositeMetaData))
			return null;
		try {
			return ((CompositeMetaData<? super I, ? extends Object>)object).get(key);	
		}
		catch (MetaDataException e) {
			return null;
		}		
	}
}
