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

package xxl.core.cursors.visual;

/**
 * This interface defines a set of opeations that should be implemented by an
 * object, to enhance this object by the possibility to be controlled remotely.
 */
public interface Controllable {

	/**
	 * Initializes a controllable object and prepares for the start of its life
	 * cycle.
	 */
	public abstract void init();

	/**
	 * Starts or resumes the life cycle of a controllable object.
	 */
	public abstract void go();

	/**
	 * Pauses the life cycle of a controllable object.
	 */
	public abstract void pause ();

	/**
	 * Lets a controllable object perfom a given number of steps of its life
	 * cycle (optional operation).
	 * 
	 * @param steps the number of steps the controllable object will be allowed
	 *        to resume its life cycle.
	 * @throws UnsupportedOperationException if the {@link #go(int)} operation is
	 *         not supported by the controllable object.
	 */
	public abstract void go(int steps) throws UnsupportedOperationException;

	/**
	 * Indicates whether the controllable object supports the
	 * {@link #go(int)} operation or not.
	 * 
	 * @return <tt>true</tt> if the controllable object supports the
	 *         {@link #go(int)} operation, otherwise <tt>false</tt>.
	 */
	public abstract boolean supportsGoSteps();

	/**
	 * Resets the controllable object to the beginning of its life cycle
	 * (optional operation).
	 * 
	 * @throws UnsupportedOperationException if the {@link #reset()} operation is
	 *         not supported by the controllable object.
	 */
	public abstract void reset() throws UnsupportedOperationException;

	/**
	 * Indicates whether the controllable object supports the
	 * {@link #reset()} operation or not.
	 * 
	 * @return <tt>true</tt> if the controllable object supports the
	 *         {@link #reset()} operation, otherwise <tt>false</tt>.
	 */
	public abstract boolean supportsReset();

	/**
	 * Closes the controllable object and releases ressources collected during
	 * its life cycle (optional operation).
	 * 
	 * @throws UnsupportedOperationException if the {@link #close()} operation is
	 *         not supported by the controllable object.
	 */
	public abstract void close() throws UnsupportedOperationException;

	/**
	 * Indicates whether the controllable object supports the
	 * {@link #close()} operation or not.
	 * 
	 * @return <tt>true</tt> if the controllable object supports the
	 *         {@link #close()} operation, otherwise <tt>false</tt>.
	 */
	public abstract boolean supportsClose();

}
