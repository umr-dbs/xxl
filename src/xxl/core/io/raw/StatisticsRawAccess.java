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

import xxl.core.math.statistics.nonparametric.histograms.LogScaleHistogram;
import xxl.core.util.timers.Timer;
import xxl.core.util.timers.TimerUtils;

/**
 * Counts the calls to a (decorated) raw access and calculates
 * a statistic of seeks.
 * <p>
 * Two LogscaleHistograms are computed: <br>
 * <ol>
 * <li>seek distance histogram: average seek time for a certain seek distance</li>
 * <li>seek time histogram: number of accesses that needed a certain amount of time</li>
 * </ol>
 * To get the statistics, call the toString() method.
 */
public class StatisticsRawAccess implements RawAccess {

	/**
	 * Decorated RawAccess
	 */
	private RawAccess r;

	/**
	 * A counter for calls to the <tt>open</tt> method.
	 */
	private int openCalls;

	/**
	 * A counter for calls to the <tt>close</tt> method.
	 */
	private int closeCalls;

	/**
	 * A counter for calls to the <tt>close</tt> method.
	 */
	private int readCalls;

	/**
	 * A counter for calls to the <tt>write</tt> method.
	 */
	private int writeCalls;

	/**
	 * A counter for the number of resets.
	 */
	private int resetCount;

	/**
	 * A counter for the number of sequential access operations.
	 */
	private int sequentialAccessCount;

	/**
	 * A counter for the number of sequential access operations.
	 */
	private int sameSectorCount;

	/**
	 * A counter for the number of random access operations.
	 */
	private int randomAccessCount;

	/**
	 * The sector that is accessed at last.
	 */
	private long lastSector;

	/**
	 * A histogram storing informations about the distance between consecutive
	 * accessed sectors.
	 */
	private LogScaleHistogram histSectorDistance;

	/**
	 * A histogram storing informations about the access-time of sectors.
	 */
	private LogScaleHistogram histTime;

	/**
	 * A timer that is used for time measurement.
	 */
	private Timer timer;

	/**
	 * The time the last access operation is performed.
	 */
	private long t; // current time messured

	/**
	 * The time that is needed to do the timer calls without performing
	 * operations.
	 */
	private long tzero;

	/**
	 * The frequence of the timer.
	 */
	private long tfreq;

	/**
	 * Constructs a RawAccessStatistic.
	 *
	 * @param r Decorated RawAccess
	 * @param partitions Number of partition for both LogScaleHistograms
	 * @param sdHistFactor Partition growth factor for the seek distance histogram
	 * @param tHistFactor Partition growth factor for the seek time histogram
	 * @exception RawAccessException a specialized RuntimeException
	 */
	public StatisticsRawAccess(RawAccess r,int partitions, double sdHistFactor, double tHistFactor) throws RawAccessException {
		this.r = r;
		resetCounter();
		resetCount = 0;
		lastSector = -2; // -1 ==> 0 would be a sequential access as first operation!
		histSectorDistance = new LogScaleHistogram(0.0, r.getNumSectors(),partitions,sdHistFactor);
		histTime = new LogScaleHistogram(0.0, 0.05, partitions, tHistFactor);
		
		timer = (Timer) TimerUtils.FACTORY_METHOD.invoke();
		TimerUtils.warmup(timer);
		tzero = TimerUtils.getZeroTime(timer);
		tfreq = timer.getTicksPerSecond();
	}

	/**
	 * Constructs a RawAccessStatistic.
	 *
	 * @param r Decorated RawAccess
	 * @exception RawAccessException a specialized RuntimeException
	 */
	public StatisticsRawAccess(RawAccess r) throws RawAccessException {
		this(r,50,1.3,1.1);
	}

	/**
	 * Resets the counters to zero
	 */
	public void resetCounter() {
		openCalls = 0;
		closeCalls = 0;
		readCalls = 0;
		writeCalls = 0;
		sequentialAccessCount = 0;
		sameSectorCount = 0;
		randomAccessCount = 0;
		resetCount++;
	}

	/**
	 * Opens a device or file
	 * See super class for detailed description
	 *
	 * @param filename name of device or file
	 * @exception RawAccessException a specialized RuntimeException
	 */
	public void open(String filename) throws RawAccessException {
		openCalls++;
		r.open(filename);
	}

	/**
	 * Closes device or file
	 * See super class for detailed description
	 *
	 * @exception RawAccessException a specialized RuntimeException
	 */
	public void close() throws RawAccessException {
		closeCalls++;
		r.close();
	}

	/**
	 * Calculates if the access is sequential or random.
	 * 
	 * @param sector the sector that is accessed.
	 * @param t the time the given sector is accessed.
	 */
	private void calcAccess(long sector, long t) {
		/* System.out.print(sector+" ");
		try {
			throw new RuntimeException();
		}
		catch (Exception e) {
			StackTraceElement st[] = e.getStackTrace();
			for (int i=2; i<st.length; i++)
				System.out.print(st[i].getClassName()+"."+st[i].getMethodName()+"("+st[i].getLineNumber()+") ");
			System.out.println();
		}*/
		if (lastSector==sector)
			sameSectorCount++;
		else if (lastSector==sector-1)
			sequentialAccessCount++;
		else
			randomAccessCount++;
		double time = ((double) (t-tzero))/tfreq;
		histSectorDistance.process(Math.abs(lastSector-sector),time);
		histTime.process(time,0.0);
		lastSector = sector;
	}

	/**
	 * Writes block to file/device
	 * See super class for detailed description
	 *
	 * @param block array to be written
	 * @param sector number of the sector
	 * @exception RawAccessException a specialized RuntimeException
	 */
	public void write(byte[] block, long sector) throws RawAccessException {
		writeCalls++;
		timer.start();
		r.write(block,sector);
		t = timer.getDuration();
		calcAccess(sector,t);
	}

	/**
	 * Reads block from file/device
	 * See super class for detailed description
	 *
	 * @param block byte array of 512 which will be written to the sector
	 * @param sector number of the sector
	 */
	public void read(byte[] block, long sector) {
		readCalls++;
		timer.start();
		r.read(block,sector);
		t = timer.getDuration();
		calcAccess(sector,t);
	}

	/**
	 * Returns the amount of sectors in the file/device.
	 *
	 * @return amount of sectors
	 */
	public long getNumSectors() {
		return r.getNumSectors();
	}

	/**
	 * Returns the size of a sector of the file/device.
	 *
	 * @return size of sectors
	 */
	public int getSectorSize() {
		return r.getSectorSize();
	}

	/**
	 * Returns the number of sequential accesses.
	 * @return the number of sequential accesses.
	 */
	public int getSequentialAccessCount() {
		return sequentialAccessCount;
	}

	/**
	 * Returns the number of same sector accesses.
	 * @return the number of same sector accesses.
	 */
	public int getSameSectorAccessCount() {
		return sameSectorCount;
	}

	/**
	 * Returns the number of random accesses.
	 * @return the number of random accesses.
	 */
	public int getRandomAccessCount() {
		return randomAccessCount;
	}

	/**
	 * Outputs the statistics which were gathered.
	 * 
	 * @return the statistics as a string.
	 */
	public String toString() {
		return 
			r + 
			"\nopen: "+openCalls+" close: "+closeCalls+" read: "+readCalls+" write: "+writeCalls+
			" number of counterresets: "+resetCount+
			" sequentialAccess: "+sequentialAccessCount+" randomAccess: "+randomAccessCount+" sameAccess: "+sameSectorCount+
			"\n"+
			"Sector distance histogram:\n"+histSectorDistance+
			"Time histogram:\n"+histTime;
	}
}
