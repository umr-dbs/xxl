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

package xxl.core.io.raw;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import xxl.core.util.timers.Timer;
import xxl.core.util.timers.TimerUtils;

/**
 * This class writes a logfile for all accesses of a RawDevice.
 */
public class LoggerRawAccess implements RawAccess {
	/** Timer which is used for the entries inside the logfile. */
	Timer t;
	
	/** Close for Outputstream? */
	boolean closeFOS;
	
	/** OutputStream, where logfile will be written to. */
	OutputStream myLogger = null;
	
	/** RawAccess which is decorated */
	protected RawAccess r;

	/**
 	* Constructs RawAccessLogger 
 	* @param os name of outputstream 
 	* @param r rawaccess,to which logfile wii be written. 
 	* @exception RawAccessException a spezialized RuntimeException
 	*/
 	public LoggerRawAccess(OutputStream os, RawAccess r) throws RawAccessException{
		myLogger=os;
		this.r=r;
		t = (Timer) TimerUtils.FACTORY_METHOD.invoke();
		t.start();
	}

	/**
 	 * Constructs a new RawAccessLogger 
 	 * @param logfilename name of logfile for a given rawaccess.
 	 * @param r rawaccess ,of which logfile will be made.
 	 * @throws RawAccessException
 	 */
 	public LoggerRawAccess(String logfilename, RawAccess r)throws RawAccessException{
		super();
		
		try{
			this.r = r;
			myLogger = new FileOutputStream(logfilename);
			t= (Timer) TimerUtils.FACTORY_METHOD.invoke();
   		    t.start();
		}
		catch(IOException e){
			throw new RawAccessException("RawAccessLogger: device/file not found");
		}
   		closeFOS=true;
  	}

	/**
	* Write the result to the log file.
	*
	* @param s the log entry to be written.
	* @exception RawAccessException a spezialized RuntimeException
	*/
	private void writeLogEntry(String s) throws RawAccessException{
		try{
		   byte[] a=s.getBytes();
   		   myLogger.write(a);
   		   myLogger.write(10);
   		   myLogger.flush();
		}
		catch(IOException e){
		   throw new RawAccessException("RawAccessLogger.writeLogEntry"+e.toString());
		}
	}

	/**
	 * Opens a device or file.
	 *
	 * @param filename the name of the file.
	 * @exception RawAccessException a spezialized RuntimeException
	 */
	public void open (String filename)throws RawAccessException{
		r.open(filename);
  		writeLogEntry("open called ");
	}

	/**
	 * Closes the device or file.
	 *
	 * @exception RawAccessException a spezialized RuntimeException
	 */
	public void close(){ 
		try{
			r.close();
			if(closeFOS)
				myLogger.close(); 
  		}
  		catch(IOException e){
  			throw new RawAccessException("RawAccessLogger: " + e.toString());
		}
  	} 

	/**
	 * Writes block to file/device and a logfile.
	 *
	 * @param block array to be written
	 * @param sector number of the sector
	 * @exception RawAccessException a spezialized RuntimeException
	 */
   	public void write(byte[] block, long sector)throws RawAccessException{
		r.write(block,sector);
  		writeLogEntry( (float)t.getDuration()/t.getTicksPerSecond()*1000+
            	"\t"+0+"\t"+ sector+"\t" + 512+"\t"+ 0 );
	}     

	/**
	 * Reads block from file/device and write a logfile.
	 * 
	 * @param block byte array of 512 which will be written to the sector
	 * @param sector number of the sector
 	 * @throws RawAccessException
	 */     
	public void read(byte[] block,long sector) throws RawAccessException{
		r.read(block,sector);
  		writeLogEntry(
      		(float)t.getDuration()/t.getTicksPerSecond()*1000+ 
      		"\t"+0+"\t"+sector+"\t"+512+"\t"+1);
	}

	/**
	 *  Returns the amount of sectors in the file/device.
	 *
	 * @return amount of sectors
 	 * @throws RawAccessException
	 */
	public long getNumSectors()throws RawAccessException{
  			return r.getNumSectors();
	}

	/**
	 * Returns the size of sectors of the file/device.
	 *
	 * @return amount of sectors
 	 * @throws RawAccessException
	 */
	public int getSectorSize()throws RawAccessException{
  		return r.getSectorSize();
	}

	/**
	 * Outputs a String representation of the raw device.
	 * @return A String representation.
	 */
	public String toString()  {
		return 
			"logger raw access on: "+r;
	}
}
