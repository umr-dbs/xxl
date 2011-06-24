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

package xxl.core.util.concurrency;

/**
 * Implements a channel with n elements by using semaphores
 * and a one element channel.
 * If the channel does not contain an object, the caller thread 
 * of the take method is blocked. If the channel contains an object,
 * the caller thread of the put method has to wait.
 * <p>
 * Because of efficiency, this class is not threadsafe in the sense 
 * that multiple readers and writers can use it simultaneously. 
 * The class is designed for exaktly one reader thread and one writer 
 * thread.
 * <p>
 * The concept of channels is well described in Doug Lea: "Concurrent
 * Programming in Java", Second Edition, Sun Microsystems. 
 * Online: http://java.sun.com/Series/docs/books/cp/
 * Be aware of the trickiness of concurrent programming!
 */
public class AsynchronousNChannel implements Channel {
	/** Protects the critical section inside. */
	protected Semaphore criticalSection;

	/** Buffered objects */
	protected Object objects[];

	/** Number of objects which can be buffered by the channel. */
	protected int size;

	/** Pointer where to read the next object from the array. */
	protected int readPointer = 0;

	/** Pointer where to write the next object into the array. */
	protected int writePointer = 0;

	/** Number of elements which are currently inside the array. */
	protected int currentSize = 0;

	/** Is there a reader waiting to be awakened ? */
	protected boolean readerWaiting = false;

	/** Is there a writer waiting to be awakened ? */
	protected boolean writerWaiting = false;

	/** Channel to awaken a reader */
	protected AsynchronousChannel wakeupReader;

	/** Channel to awaken a writer */
	protected AsynchronousChannel wakeupWriter;

	/** 
	 * Constructs an asynchronous channel using the semaphores specified.
	 * Usually, the user does not use this constructor but the parameterless
	 * constructor.
	 *
	 * @param size Number of elements which can be stored inside the channel.
	 */
	public AsynchronousNChannel(int size) {
		criticalSection = new Semaphore(1,1);
		objects = new Object[size];
		wakeupReader = new AsynchronousChannel();
		wakeupWriter = new AsynchronousChannel();
		this.size = size;
	}

	/** 
	 * Constructs an asynchronous channel.
	 */
	public AsynchronousNChannel() {	
		this(1);
	}

	/**
	 * Waits for an object that is sent from a different thread 
	 * via the put method.
	 *
	 * @return object from the channel
	 */
	public Object take() {
		Object ret = null;
		
		criticalSection.acquire();
		
		do {
			if (currentSize>0) {
				ret = objects[readPointer];
				objects[readPointer] = null;
				currentSize--;
				
				readPointer++;
				if (readPointer>=size)
					readPointer=0;
				
				if (writerWaiting) {
					wakeupWriter.put(null);
					writerWaiting = false;
				}
				
				break;
			}
			else {
				readerWaiting = true; 
				criticalSection.release();
				wakeupReader.take();
				criticalSection.acquire();
			}
		}
		while (true);
		
		criticalSection.release();
		return ret;
	}

	/**
	 * Tries to read an object from the channel. 
	 * If an object is not availlable, this method returns false.
	 * The object is returned inside the first element
	 * of the object array.
	 *
	 * @param o Object array. The first element is used for
	 *	returning the desired object
	 * @return true - iff there was an object in the channel
	 */
	public boolean attemptTake(Object o[]) {
		boolean ret = false;
		
		criticalSection.acquire();
		
		if (currentSize>0) {
			o[0] = objects[readPointer];
			objects[readPointer] = null;
			currentSize--;
			
			readPointer++;
			if (readPointer>=size)
				readPointer=0;
			
			if (writerWaiting) {
				wakeupWriter.put(null);
				writerWaiting = false;
			}
			
			ret = true;
		}
		
		criticalSection.release();
		
		return ret;
	}

	/**
	 * Puts an object into the channel.
	 *
	 * @param object object that is put into the channel
	 */
	public void put(Object object) {
		criticalSection.acquire();
		
		do {
			if (currentSize<size) {
				objects[writePointer] = object;
				
				currentSize++;
				writePointer++;
				if (writePointer>=size)
					writePointer=0;
				
				if (readerWaiting) {
					wakeupReader.put(null);
					readerWaiting = false;
				}
				
				break;
			}
			else {
				writerWaiting = true;
				criticalSection.release();
				wakeupWriter.take();
				criticalSection.acquire();
			}
		}
		while (true);
		
		criticalSection.release();
	}

	/**
	 * Determines if an object is currently in the channel.
	 *
	 * @return true if the channel currently does not contain any element
	 */
	public boolean isEmpty() {
		boolean ret;
		criticalSection.acquire();
		ret = currentSize==0;
		criticalSection.release();
		return ret;
	}

	/**
	 * Determines if the channel currently is full.
	 *
	 * @return true if the channel is currently full
	 */
	public boolean isFull() {
		boolean ret;
		criticalSection.acquire();
		ret = currentSize==size;
		criticalSection.release();
		return ret;
	}
}
