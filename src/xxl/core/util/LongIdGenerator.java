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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.TreeSet;

import xxl.core.functions.Function;
import xxl.core.io.Convertable;

/**
 * Handles identifyers which can be used for a lot of reasons.
 * This class always stores the minimum and maximum values which
 * were previously returned as identifyers.<br>
 * There are two ways to 
 * obtain an identifyer. The first and most efficient one
 * returns a new identifyer by looking at the statistics and
 * returns a new smaller or greater identifyer than the current
 * smallest/greatest value. If this method returns Long.MIN_VALUE,
 * then no identifyer could be returned with this method. Then,
 * the user has to call the second getIdentifyer method which 
 * needs a Function that returns an Iterator. This iterator 
 * has to contain all valid and used identifyers. If this method
 * also is not able to return an identifyer, it returns 
 * Long.MIN_VALUE.<br>
 * When identifyers are no longer used, then it can be useful to
 * call the removeIdentifyer method.
 */
public class LongIdGenerator implements Convertable {

	/** minimal id given so far */
	private long currentMin;
	/** maximal id given so far */
	private long currentMax;
	/** minimal id possible */
	private long absoluteMin;
	/** maximal id possible */
	private long absoluteMax;
	
	// Possibility for optimization: histograms for small intervals of identifyer

	/**
	 * Allocates a LongIdGenerator with the maximal interval possible.
	 * Both values have to be inside the interval [Long.MIN_VALUE+1, Long.MAX_VALUE-1]
	 * with absoluteMax>=absoluteMin.
	 * @param absoluteMin minimal id value possible.
	 * @param absoluteMax maximal id value possible.
	 */
	public LongIdGenerator (long absoluteMin, long absoluteMax) {
		if (absoluteMax<absoluteMin)
			throw new RuntimeException("maximum is smaller minimum!");
		if (absoluteMin<Long.MIN_VALUE+1 || absoluteMin>Long.MAX_VALUE-1)
			throw new RuntimeException("absoluteMin outside possible values!");
		if (absoluteMax<Long.MIN_VALUE+1 || absoluteMax>Long.MAX_VALUE-1)
			throw new RuntimeException("absoluteMaxoutside possible values!");
		this.absoluteMin = absoluteMin;
		this.absoluteMax = absoluteMax;
		reset();
	}

	/**
	 * Allocates a LongIdGenerator with the maximal interval possible.
	 */
	public LongIdGenerator () {
		this(Long.MIN_VALUE+1, Long.MAX_VALUE-1);
	}

	/**
	 * Resets the current IDs, so that new IDs can be generated.
	 */
	public void reset() {
		currentMin = Long.MAX_VALUE;
		currentMax = Long.MIN_VALUE;
	}

	/**
	 * If a method from the outside has knowledge of the id generator, then
	 * this external method can make a reservation on its own. Then, this
	 * method has to inform the generator by calling makeExternalReservation.
	 * So, the generator can stay in a consistent state.
	 * @param id identifyer which is reserved from the outside. The identifyer is
	 *	not checked to be unique.
	 */
	public void makeExternalReservation(long id) {
		if (currentMin==Long.MAX_VALUE) {
			currentMin = id;
			currentMax = id;
		}
		else if (id>currentMax)
			currentMax = id;
		else if (id<currentMin)
			currentMin = id;
	}

	/**
	 * Returns an identifyer only if the new identifyer can easily be
	 * generated.
	 * @return the identifyer or Long.MIN_VALUE if no identifyer can easily be
	 * 	generated. Then, call getIdentifyer.
	 */
	public long getDirectIdentifyer() {
		if (currentMin==Long.MAX_VALUE) {
			currentMin = absoluteMin;
			currentMax = currentMin;
			return absoluteMin;
		}
		else if (currentMax<absoluteMax) {
			currentMax++;
			return currentMax;
		}
		else if (currentMin>absoluteMin) {
			currentMin--;
			return currentMin;
		}
		else
			return Long.MIN_VALUE;
	}
	
	/**
	 * Determines a free numerical identifyer. This class works with 
	 * iterators of type java.lang.Number (Byte, Short, Integer, Long, ...).
	 *
	 * @param getIdIterator Function which returns an Iterator which 
	 * 	then contains all Number values of valid and used identifyers.
	 * @return a free id (or Long.MIN_VALUE if no id is availlable).
	 */
	public long getIdentifyer(Function getIdIterator) {
		long value = getDirectIdentifyer();
		
		if (value!=Long.MIN_VALUE)
			return value;
		
		Iterator it = (Iterator) getIdIterator.invoke();
		
		TreeSet ts = new TreeSet();
		currentMin = Long.MAX_VALUE;
		currentMax = Long.MIN_VALUE;
		while (it.hasNext()) {
			value = ((Number) it.next()).longValue();
			if (value<currentMin)
				currentMin = value;
			if (value>currentMax)
				currentMax = value;
			
			ts.add(new Long(value));
		}
		
		value = getDirectIdentifyer();
		
		if (value==Long.MIN_VALUE) {
			long counter=absoluteMin;
			while (counter<=absoluteMax)  {
				if (!ts.contains(new Long(counter)))
					return counter;
				counter++;
			}
		}

		// Long.MIN_VALUE if no other value has been found meanwhile
		return value;
	}

	/**
	 * Returns an identifyer to the generator.
	 * This can sometimes save some time to generate new ones 
	 * (if minimum and maximum values are returned). This method does not
	 * have to be called. So, it is not possible to say how many ids
	 * are really out there.
	 * @param id identifyer which is no longer needed.
	 */
	public void removeIdentifyer(long id) {
		if (currentMin==id)
			currentMin++;
		if (currentMax==id)
			currentMax--;
		if (currentMin>currentMax) {
			currentMin = Long.MAX_VALUE;
			currentMax = Long.MIN_VALUE;
		}
	}

	/**
	 * Reads the state from the DataInput.
	 * @param dataInput DataInput
	 * @throws IOException in case of I/O errors
	 */
	public void read(DataInput dataInput) throws IOException {
		absoluteMin = dataInput.readLong();
		absoluteMax = dataInput.readLong();
		currentMin = dataInput.readLong();
		currentMax = dataInput.readLong();
	}

	/**
	 * Writes the state to a DataOutput.
	 * @param dataOutput DataOutput
	 * @throws IOException in case of I/O errors
	 */
	public void write(DataOutput dataOutput) throws IOException {
		dataOutput.writeLong(absoluteMin);
		dataOutput.writeLong(absoluteMax);
		dataOutput.writeLong(currentMin);
		dataOutput.writeLong(currentMax);
	}
}
