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

/** This class provides a simple to use stop watch. Just call start, pause or stop to get
 * the time something goes.
 */

public class StopWatch{

	/** Default stop watch; ready to use
	 */
	public static final StopWatch DEFAULT_INSTANCE = new StopWatch();

	/** Time in millis when the watch has started */
	protected long startingTime = 0;

	/** Time in millis when the watch has stopped */
	protected long stopingTime = -1;

	/** Time in millis the watch has paused */
	protected long pausedTime = -1;

	/** Constructs a new instance of a stop watch starting in paused mode
	 * @param paused indicates that watch should be started in paused mode
 	*/
	public StopWatch( boolean paused){
		if( paused){
			pausedTime = System.currentTimeMillis();
			startingTime = pausedTime;
		}
	}

	/** Constructs a new instance of a stop watch.
	*/
	public StopWatch(){
		this( false);
	}

	/** Starts the stop watch.
	 */
	public void start(){
		stopingTime = -1;
		startingTime = System.currentTimeMillis();
	}

	/** Stops the stop watch.
	 */
	public void stop(){
		stopingTime = System.currentTimeMillis();
	}

	/** Returns the passed time between start and stop resp. between start and now.
	 * @return the passed time in milli seconds.
	 */
	public long getTime(){
		if ( stopingTime == -1) return System.currentTimeMillis() - startingTime;
		return stopingTime - startingTime;
	}

	/** Returns the passed time between start and stop resp. between start and now.
	 * @return the passed time as a String
	 */
	public String getTimeString( ){
		return timeAsString( getTime());
	}

	/** Pauses the stop watch
	 */
	public void pause(){
		if ( pausedTime != -1) return;
		pausedTime = System.currentTimeMillis();
	}

	/** Resumes the paused stop watch.
	 */
	public void resume(){
		if ( pausedTime == -1) return;
		startingTime = startingTime + (System.currentTimeMillis() - pausedTime);
		pausedTime = -1;
	}

	/** Returns a String representation of this stop watch.
	 * @return a String representation of this stop watch.
	 * If the watch is already stopped recorded time will be returned.
	 */
	public String toString(){
		return (stopingTime != -1) ? getTimeString(): getTimeString() + " -> running";
	}


	/** Returns a given time in milliseconds as String.
	 * @param t in millis
	 * @return the time as String.
	 */
	public static String timeAsString( long t){
		if ( t < 0) return " --:--:--.--";
		int secs = (int) (t / 1000);
		int min = (secs / 60);
		int h = (min / 60);
		String r = "." + formatNumber( (int) ( t - (secs*1000)), 3); // millis
		r = ":" + formatNumber( ( secs-(min*60)), 2) + r; // secs
		r = ":" + formatNumber( ( min -(h*60)  ), 2) + r; // min
		return h + r;
	}

	/** Converts int to Strings with a given length (filled up with leading '0's).
	 * @param n number to convert
	 * @param digits number of digits in the String
	 * @return a String representing the given number filled up with leading 0's
	 */
	private static String formatNumber( int n, int digits){
		String r =  "" + n;
		for( int i=0; i < digits - r.length(); i++)
			r = "0" + r;
		return r;
	}
}
