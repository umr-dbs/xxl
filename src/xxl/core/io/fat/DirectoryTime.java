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

package xxl.core.io.fat;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

/**
 * This class is used to exchange time information about a directory entry.
 */
public class DirectoryTime
{	
	/**
	 * The hour of the time.
	 */
	public byte hour;
	
	/**
	 * The minute of the time.
	 */
	public byte minute;
	
	/**
	 * The second of the time.
	 */
	public byte second;
	
	
	/**
	 * Creates an instance of this object.
	 * @param hour the hour of the time.
	 * @param minute the minute of the time.
	 * @param second the second of the time.
	 */
	public DirectoryTime(byte hour, byte minute, byte second)
	{
		this.hour = hour;
		this.minute = minute;
		this.second = second;
	}	//end constructor
	
	
	/**
	 * Creates an instance of this object.
	 * @param time the new last-modified time, measured in milliseconds since
	 * the epoch (00:00:00 GMT, January 1, 1970).
	 */
	public DirectoryTime(long time)
	{
		GregorianCalendar calendar = new GregorianCalendar();
		calendar.setTime(new Date(time));
		
		hour = (byte)calendar.get(Calendar.HOUR_OF_DAY);
		minute = (byte)calendar.get(Calendar.MINUTE);
		second = (byte)calendar.get(Calendar.SECOND);
	}	//end constructor
	
	
	/**
	 * Returns a String representing the time stored at this object.
	 * The format is: hh:mm:ss
	 * @return representation of the time.
	 */
	public String toString()
	{
		String res = "";
		if (hour < 10)
			res += "0";
		res += hour+":";
		if (minute < 10)
			res += "0";
		res +=minute+":";
		if (second < 10)
			res += "0";
		res += second;
		return res;
	}	//end toString()
}	//end class DirectoryTime
