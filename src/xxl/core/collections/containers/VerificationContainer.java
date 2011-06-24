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

package xxl.core.collections.containers;

import java.util.NoSuchElementException;

import xxl.core.functions.Function;

/**
 * Verifies each write operation of a container, that means for example after 
 * an insert, the object is retrieved again with get and both objects are compared 
 * (using the equals method from Object). If an operation fails 
 * a RuntimeException is thrown. 
 */
public class VerificationContainer extends ConstrainedDecoratorContainer {

	/**
	 * A name which can be given the VerificationContainer.
	 */
	private String name;

	/**
	 * Internal numbering of actions that write data (insert/update/remove).
	 */
	public long numberOfActions;

	/**
	 * Constructs a new VerificationContainer.
	 * @param container The container to be wrapped.
	 * @param name A name which is used inside the text of exception (useful to
	 * 		set if using multiple VerificationContainers).
	 */
	public VerificationContainer(Container container, String name) {
		super(container);
		this.name = name;
		numberOfActions = 0;
	}

	/**
	 * Removes an Object and makes some tests if it really has been removed. 
	 * @param id an identifier of an object.
	 * @throws NoSuchElementException if an object with an identifier
	 * <tt>id</tt> is not in the container.
	 */
	public void remove(Object id) throws NoSuchElementException {
		int sizeBefore = size();
		super.remove(id);
		int sizeAfter = size();
		if (sizeAfter!=sizeBefore-1)
			throw new RuntimeException("VerificationContainer "+name+"("+numberOfActions+"): failed inside remove (number of objects)");
		if (contains(id))
			throw new RuntimeException("VerificationContainer "+name+"("+numberOfActions+"): failed inside remove");
		numberOfActions++;
	}

	/**
	 * Reserves space for an Object and tests if the identifier is
	 * returned by the contains method.
	 * @param getObject A parameterless function providing the object for
	 * 			that an id should be reserved.
	 * @return the reserved id.
	 */
	public Object reserve(Function getObject) {
		Object o = super.reserve(getObject);
		numberOfActions++;
		return o;
	}

	/**
	 * Updates an object and tests if the retrieved object equals the original object.
	 * @param id identifier of the element.
	 * @param object the new object that should be associated to
	 *        <tt>id</tt>.
	 * @param unfix signals whether the object can be removed from the
	 *        underlying buffer.
 	 * @throws NoSuchElementException if an object with an identifier
	 *         <tt>id</tt> does not exist in the container.
	 */
	public void update(Object id, Object object, boolean unfix)
		throws NoSuchElementException {
		int sizeBefore = size();
		super.update(id, object, unfix);
		int sizeAfter = size();
		if (sizeAfter!=sizeBefore)
			throw new RuntimeException("VerificationContainer "+name+"("+numberOfActions+"):failed inside update (number of objects)");
		Object restored = get(id);
		if (!restored.equals(object))
			throw new RuntimeException("VerificationContainer "+name+"("+numberOfActions+"): failed inside update (objects are not equal)");
		numberOfActions++;
	}

	/**
	 * Inserts an Object and tests some integrity constraints.
	 * @param object is the new object.
	 * @param unfix signals whether the object can be removed from the
	 *        underlying buffer.
	 * @return the identifier of the object.
	 */
	public Object insert(Object object, boolean unfix) {
		int sizeBefore = size();
		Object id = super.insert(object, unfix);
		int sizeAfter = size();
		if (sizeAfter!=sizeBefore+1)
			throw new RuntimeException("VerificationContainer "+name+"("+numberOfActions+"):failed inside insert (number of objects)");
		Object restored = get(id);
		if (!restored.equals(object))
			throw new RuntimeException("VerificationContainer "+name+"("+numberOfActions+"): failed inside insert (objects are not equal)");
		numberOfActions++;
		return id;
	}
}
