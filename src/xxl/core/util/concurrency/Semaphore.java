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
 * Implements a Semaphore by using synchronized blocks.
 * The concept of Semaphores is well described in Doug Lea: "Concurrent
 * Programming in Java", Second Edition, Sun Microsystems. 
 * Online: http://java.sun.com/Series/docs/books/cp/
 */
public class Semaphore {
	
	/**
	 * The number of thread that still can enter the critical section at this
	 * moment. 
	 */
	private int count;
	
	/**
	 * The total number of threads that can enter the critical section. 
	 */
	private int initialCount;
	
	/** 
	 * Constructs a Semaphore that allows count threads to enter the critical section
	 * (section between acquire and release) at once.
	 *
	 * @param count maximal number of threads that can enter the critical section at once.
	 * @param initialValue number of threads that can enter the critical section after 
	 * 		construction (useful for several algorithms).
	 */
	public Semaphore(int count,int initialValue) {
		this.count = initialValue;
		this.initialCount = count;
		if (count<=0)
			throw new RuntimeException("Semaphore construction exception: count has to be greater than 0.");
	}
	
	/** 
	 * Constructs a Semaphore that allows only thread to be in the critical section
	 * at once.
	 */
	public Semaphore() {
		this(1,1);
	}
	
	/** 
	 * Acquires the Semaphore. This Method stops the current thread if
	 * the internal counter of the semaphore
	 */
	public synchronized void acquire() {
		while (count==0) {
			try  {
				wait();
			}
			catch (InterruptedException e) {}
		}
		count--;
	}
	
	/** 
	 * Releases the Semaphore and wakes up a waiting thread.
	 */
	public synchronized void release() {
		count++;
		if (count>initialCount)
			throw new RuntimeException("Semaphore exception: release is called too often");
		// let the next waiting thread continue execution
		notify();
	}
	
	/** 
	 * Tries to acquire the Semaphore but does not wait. If false is
	 * returned no lock has been acquired.
	 *
	 * Example:
	 * <code>
	 *		if (s.attempt()) {
	 *			... do something ...
	 *			s.release();
	 *		}
	 * </code>
	 * @return true if acquired
	 */
	public synchronized boolean attempt() {
		if (count>0) {
			count--;
			return true;
		}
		else
			return false;
	}

	/** 
	 * Tries to acquire the Semaphore and waits msecs at most. If false is
	 * returned no lock has been acquired.
	 *
	 * Example:
	 * <code>
	 *		if (s.attempt(1000)) {
	 *			... do something ...
	 *			s.release();
	 *		}
	 * </code>
	 * @param msecs maximum timeout to wait
	 * @return true if acquired
	 * 
	 */
	public boolean attempt(int msecs) {
		boolean state = attempt();
		
		if (state)
			return true;
		else {
			synchronized (this) {
				try  {
					wait(msecs);
				}
				catch (InterruptedException e) {}
				// must be in a synchronized statement because if the 
				// waiting thread receives a notification, he should 
				// be the one that gets the next lock.
				return attempt();
			}
		}
	}

	/**
	 * Writes the current state into a string (for debugging purposes). 
	 * Do not use something like that:
	 *
	 * 		if (sem.toString().equals("1") {
	 * 			critical section
	 *		}
	 * 
	 * @return returns current state as a string
	 */
	public String toString() {
		synchronized (this) {
			return String.valueOf(count);
		}
	}

}
