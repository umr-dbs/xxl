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

import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;


/**
 * This class contains some static methods which are very useful
 * when dealing with timers. A timer can be warmed up (interesting
 * for Java hot spot compilers), the zero time can be computed 
 * (the time which is needed to start and stop the timer), and the
 * resolution of a timer can be tested.
 */
public class TimerUtils {
	/** There are no instances of this class */
	private TimerUtils() {}
	
	/**
	 * Factory method returning the best availlable (known) timer.
	 * It is tried to return a JNITimer. If the DLL is missing, a
	 * new JavaTimer is returned.
	 */
	public static Function<Object,Timer> FACTORY_METHOD =
		new AbstractFunction<Object,Timer>() {
			public Timer invoke() {
			    try {
					return new JNITimer();
				}
				catch (Throwable t) {
					return new JavaTimer();
				}
			}
		};
	
	/**
	 * Warms up the timer by calling start and getDuration 10000
	 * times. This is useful when using the java hot spot technology.
	 * @param t timer to be used
	 */
	public static void warmup(Timer t) {
		for (int i=0; i<10000; i++) {
			t.start();
			t.getDuration();
		}
	}

	/**
	 * Calculates the time that is needed to do the timer 
	 * calls without performing operations.
	 * @param t timer to be used
	 * @return returns the time calculated
	 * 
	 */
	public static long getZeroTime(Timer t) {
		long sum=0,zero;
		for (int i=0; i<1000; i++) {
			t.start();
			zero = t.getDuration();
			sum += zero;
		}
		return sum/1000;
	}
	
	/**
	 * Calculates the timer resolution by calling getDuration
	 * until the value is different from zero.
	 * @param t timer to be used
	 * @return returns the timer resolution calculated
	 * 
	 */
	public static long getTimerResolution(Timer t) {
		long time;
		long numCalls = 0;
		
		t.start();
		// busy wait until system time changes 
		// JavaTimer changes after approx. 10 ms
		do {
			time = t.getDuration();
			numCalls++;
		} while (time==0);
		
		return time;
	}

	/**
	 * Calculates the time in seconds for a given time and timer.
	 * @param timer The timer which was used.
	 * @param zeroTime The zero time.
	 * @param time The time which was meassured by the timer.
	 * @return time in seconds.
	 */
	public static double getTimeInSeconds(Timer timer, long zeroTime, long time) {
		return ((double) (time-zeroTime))/timer.getTicksPerSecond();
	}

	/**
	 * Performs a timer test and outputs the data on stdout.
	 * @param t Timer to be tested.
	 */
	public static void timerTest(Timer t) {
		System.out.println("Timer info: "+t.timerInfo());
		System.out.println("Warmup timer");
		warmup(t);
		System.out.println("Zero time: "+getZeroTime(t)+" ticks");
		System.out.println("Time resolution: "+(double) getTimerResolution(t)/t.getTicksPerSecond()*1000+" ms");
	}
}
