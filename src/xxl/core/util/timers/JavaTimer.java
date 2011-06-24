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

package xxl.core.util.timers;

/**
 * Implements the Timer interface based on java.lang.System.currentTimeMillis.
 */
public class JavaTimer implements Timer {
	
	/**
	 * The starting time of the current timer.
	 */
	protected long millis;
	
	/**
	 * Constructs a JavaTimer.
	 */
	public JavaTimer() {
	}
	
	/**
	 * Starts a JavaTimer.
	 */
	public void start() {
		millis = System.currentTimeMillis();
	}
	
	/**
	 * Returns time in ms since JavaTimer was started.
	 * @return returns time in ms since JavaTimer was started
	 */
	public long getDuration() {
		return System.currentTimeMillis()-millis;
	}
	
	/**
	 * Returns number of ticks per second (1000)
	 * @return returns number of ticks per second (1000)
	 */
	public long getTicksPerSecond() {
		return 1000;
	}
	
	/**
	 * Returns string "java.lang.System-Timer"
	 * @return returns string "java.lang.System-Timer"
	 */
	public String timerInfo() {
		return "java.lang.System-Timer";
	}
}
