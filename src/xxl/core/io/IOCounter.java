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

package xxl.core.io;

/**
 * This class provides a simple utility for counting I/Os. It contains two
 * counters for read and write operations and a sole method to reset the
 * counters.
 */
public class IOCounter {

	/**
	 * A counter for read operations.
	 */
	public int readIO = 0;

	/**
	 * A counter for write operations.
	 */
	public int writeIO = 0;

	/**
	 * Constructs a new IOCounter.
	 */
	public IOCounter () {}

	/**
	 * Resets the counters for read and write operations. In other words
	 * the counters <tt>readIO</tt> and <tt>writeIO</tt> will be equal to
	 * <tt>0</tt> after a call to this method.
	 */
	public void reset () {
		readIO = 0;
		writeIO = 0;
	}

	/** Increase readIOs.
	 */
	public void incRead(){
		readIO++;
	}

	/** Increase writeIOs.
	 */
	public void incWrite(){
		writeIO++;
	}

	/** Returns readIO.
	 * @return The number of read i/o's.
	 */
	public int getReadIO(){
		return readIO;
	}

	/** Returns writeIO.
	 * @return The number of write i/o's.
	 */
	public int getWriteIO(){
		return writeIO;
	}

	/** Returns a string containing readIOs and writeIOs as well as their sum.
	 * @return the String representation.
	 */
	public String toString () {
		return (readIO+writeIO)+"\t (read: "+readIO+"\t written: "+writeIO+")";
	}
}
