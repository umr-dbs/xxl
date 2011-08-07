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

package xxl.core.indexStructures;

import xxl.core.io.raw.NativeRawAccess;
import xxl.core.io.raw.RAFRawAccess;
import xxl.core.io.raw.RawAccess;
import xxl.core.io.raw.RawAccessUtils;
import xxl.core.io.raw.StatisticsRawAccess;
import xxl.core.util.concurrency.AsynchronousChannel;
import xxl.core.util.random.DiscreteRandomWrapper;
import xxl.core.util.reflect.TestFramework;
import xxl.core.util.timers.Timer;
import xxl.core.util.timers.TimerUtils;

/**
 * Perform some tests with raw accesses.
 */
public class RawAccessTest {	

	/** Description for variable below. */
	public static final String fn1Description = "The name of the first raw device to be tested"; 
	/** The name of the first raw device to be tested. */
	// public static String fn1 = "D:/test.txt";
	// public static String fn1 = "\\\\.\\A:";
	// public static String fn1 = "\\\\.\\D:";
	// public static String fn1 = "\\\\.\\PhysicalDrive0";
	public static String fn1 = "\\\\.\\PhysicalDrive2";
	// public static String fn1 = "\\\\.\\C:";
	// public static String fn1 = "\\\\.\\jd.txt";
	// public static String fn1 = "D:/test2.txt";
	// public static String fn1 = "\\\\.\\H:";

	/** Description for variable below. */
	public static final String fn2Description = "The name of the second raw device to be tested"; 
	/** The name of the second raw device to be tested. */
	public static String fn2 = "D:/test2.txt";

	/** Description for variable below. */
	public static final String testNrDescription = 
		"The number of the test that should be performed. "+
		"-1: No test performed, "+
		"0: Read sector 0 repeatedly, "+
		"1: Read sequentially from a RawAccess changing the sectorSize for the operations, "+
		"2: Testing seeks of a predefined length, "+
		"3: Testing seeks with random length, "+
		"4: Copying a RawAccess.";

	/** 
	 * Look inside Variable testNrDescription above! Default value must be -1,
	 * because of MainMaker!
	 */
	// public static int testNr=-1;
	public static int testNr=5;
	/** */
	public static final int testNrMin=-1;
	/** */
	public static final int testNrMax=4;

	/** Description for variable below. */
	public static final String numberOfChunksDescription = "The number of sector that should be manipulated during the test."; 
	/** The number of sector that should be manipulated during the test. */
	// public static int numberOfChunks = 80*2*18;
  	// public static int numberOfChunks = 36;
	public static int numberOfChunks = 1000;

  	/** A boolean flag determining whether the raw access should be performed via native methods (JNI and C) or a random-access file. */
	public static boolean useNative=true;
	/** For TestFramework */ public static final String useNativeDescription = "A boolean flag determining whether the raw access should be performed via native methods (JNI and C) or a random-access file."; 
	
  	/** A boolean flag deciding whether a synchronization call after each read/write operation is performed. */
	public static boolean useSync=false;
	/** For TestFramework */ public static final String useSyncDescription = "A boolean flag deciding whether a synchronization call after each read/write operation is performed.";

  	/** A boolean flag deciding whether statistical informations should be calculated during the test. */
	public static boolean makeStatistic=false;
	/** For TestFramework */ public static final String makeStatisticDescription = "A boolean flag deciding whether statistical informations should be calculated during the test.";

	/** For testNr==1: The size of a physical sector. */
	public static int physicalSectorSize = 512;
	/** For TestFramework */ public static final String physicalSectorSizeDescription = "For testNr==1: The size of a physical sector.";
	/** For testNr==1: The initial sector size for the test. */
	public static int startSectorSize = 10*physicalSectorSize;
	/** For TestFramework */ public static final String startSectorSizeDescription = "For testNr==1: The initial sector size for the test.";
  	/** For testNr==1: The final sector size for the test. */
	public static int endSectorSize   = 100*physicalSectorSize; // startSectorSize*100;
	/** For TestFramework */ public static final String endSectorSizeDescription = "For testNr==1: The final sector size for the test.";
	/** For testNr==1: The size by which the sector size is increased during the steps of the test. */
	public static int stepSectorSize  = 5*physicalSectorSize;
	/** For TestFramework */ public static final String stepSectorSizeDescription = "For testNr==1: The size by which the sector size is increased during the steps of the test.";

	/** For testNr==2: A boolean flag deciding whether the test starts seeking from position 0. */
	public static boolean seekFrom0=true;
	/** For TestFramework */ public static final String seekFrom0Description = "For testNr==2: A boolean flag deciding whether the test starts seeking from position 0.";
	/** For testNr==2: The number of sectors skipped between two seek operations. */
	public static int stepwide = 100000;
	/** For TestFramework */ public static final String stepwideDescription = "For testNr==2: The number of sectors skipped between two seek operations.";

	/** For testNr==3: A boolean flag determining whether Java's random number generator should be used or not. */ 
	public static boolean randomJava=true;
	/** For TestFramework */ public static final String randomJavaDescription = "For testNr==3: A boolean flag determining whether Java's random number generator should be used or not.";
	/** For testNr==3: The number of seek operations that should be performed during the test. */
	public static int numSeeks = 10000;
	/** For TestFramework */ public static final String numSeeksDescription = "For testNr==3: The number of seek operations that should be performed during the test.";

	/** Mode for the hard disk cache (cannot be guaranteed, depends on specific hard disk drive). Bit 0: Read Cache, Bit 1: Write Cache. */
	public static int hardDiskCacheMode=0;
	/** For TestFramework */ public static final String hardDiskCacheModeDescription = "Mode for the hard disk cache (cannot be guaranteed, depends on specific hard disk drive). Bit 0: Read Cache, Bit 1: Write Cache.";

	/** For testNr==4: A boolean flag determining whether the destination file should be created before the copy operation will be performed. */
	public static boolean construct=true;
	/** For TestFramework */ public static final String constructDescription = "For testNr==4: A boolean flag determining whether the destination file should be created before the copy operation will be performed.";

	/** For testNr==4: A boolean flag deciding whether the source and the destination file should be filled with data before the copy operation will be performed. */
	public static boolean fill=false;
	/** For TestFramework */ public static final String fillDescription = "For testNr==4: A boolean flag deciding whether the source and the destination file should be filled with data before the copy operation will be performed.";

	/** Determines if some more output should be done */
	public static boolean verbose=false;

	/**
	 * One sector
	 */
	private static byte[] temp;

	/**
	 * Used inside Test 0.
	 * @param r RawAccess which is used for the test.
	 * @param count Number of times the operation is performed.
	 */
	public static void readSektor0Repeatedly(RawAccess r, int count) {
		while (count-->0)
			r.read(temp,0);
	}

	/**
	 * Used inside Test 1.
	 * @param r RawAccess which is used for the test.
	 * @param count Number of times the operation is performed.
	 */
	public static void readSequentially(RawAccess r, int count) {
		for (int i=0; i<count; i++)
			r.read(temp,i);
	}

	/**
	 * Used inside Test 5. Does not work as expected, because
	 * Java obviously performs a synchronization before
	 * entering native code.
	 * @param r RawAccess which is used for the test.
	 * @param count Number of times the operation is performed.
	 */
	public static void readSequentiallyTwoThreads(final RawAccess r, final RawAccess r2, int count) {
		final AsynchronousChannel channel = new AsynchronousChannel();  
		final AsynchronousChannel channel2 = new AsynchronousChannel();  
		
		new Thread() {
			public void run() {
				while (true) {
					Integer i = (Integer) channel.take();
					if (i==null)
						break;
					yield();
					r2.read(temp,i.intValue());
					System.out.println(i);
				}
				channel2.put(null);
			}
		}.start();
		
	 	for (int i=0; i<count; i+=2) {
			if (i+1<count)
				channel.put(new Integer(i+1));
			r.read(temp,i);
			System.out.println(i);
		}
		
		channel.put(null);
		channel2.take();
	}

	/**
	 * Used inside Test 2.
	 * @param r RawAccess which is used for the test.
	 * @param stepwide Size of a seek.
	 */
	public static void readSeekForwardTest(RawAccess r, int stepwide) {
		long sec = r.getNumSectors();
		long position = 0;
		
		while(position<sec) {
			r.read(temp,position);
			position += stepwide;
		}
	}

	/**
	 * Used inside Test 2.
	 * @param r RawAccess which is used for the test.
	 * @param stepwide Size of a seek.
	 */
	public static void readSeekForwardFrom0Test(RawAccess r, int stepwide) {
		long sec = r.getNumSectors();
		long position = 0;
		
		while(position<sec) {
			r.read(temp,0);
			r.read(temp,position);
			position += stepwide;
		}
	}

	/**
	 * Currently not needed inside any test.
	 * @param r RawAccess which is used for the test.
	 * @param numSeeks Number of seeks.
	 */
	public static void readSeekPseudoRandomTest(RawAccess r, int numSeeks) {
		long sec = r.getNumSectors();
		long position = 0;
		long runNr=0;
		
		while( (numSeeks--) != 0 ) {
			r.read(temp,position);
			runNr++;
			position += (long) (59.2337192*(sec*runNr*runNr));
			position %= sec;
		}
	}

	/**
	 * Used inside Test 3.
	 * @param r RawAccess which is used for the test.
	 * @param numSeeks Number of seeks.
	 * @param random Random number generator which is used for the test.
	 */
	public static void readRandomSeekTest(RawAccess r, int numSeeks, DiscreteRandomWrapper random){
		long sec = r.getNumSectors();
		long position = 0;
		
		while( (numSeeks--) != 0 ) {
			r.read(temp,position);
			position += random.nextInt();
			position %= sec;
		}
	}

	/**
	 * Opens a raw access according to the parameters.
	 * @param fn Name of the RawDevice.
	 * @param sectorSize Size of each sector of the RawDevice.
	 * @param setCacheState Determines if the cache state has to be set.
	 * @return The RawDevice matching the parameters.
	 */
	public static RawAccess open(String fn, int sectorSize, boolean setCacheState) {
		RawAccess r;
		if (useNative) {
			NativeRawAccess rn = new NativeRawAccess(fn,sectorSize);
			
			if (verbose) {
				System.out.println("Construct a NativeRawAccess");
				System.out.println("Native mode: "+rn.mode);
				System.out.println("Current harddrive cache mode: "+rn.getHardDriveCacheMode());
			}
			
			if (setCacheState) {
				System.out.println("Testing cache capabilities of the hard drive");
				for (int i=0; i<4; i++) {
					System.out.print("Try to set the cache mode to "+i+": ");
					rn.setHardDriveCacheMode(i);
					int realCacheMode = rn.getHardDriveCacheMode();
					if (realCacheMode==i)
						System.out.println("ok");
					else
						System.out.println("failure. Real value is "+realCacheMode);
				}
				
				System.out.println("Try to set the cache mode to the mode for the test");
				rn.setHardDriveCacheMode(hardDiskCacheMode);
				System.out.println("Harddrive cache mode: "+rn.getHardDriveCacheMode());
			}
			r = rn;
		}
		else
			r = new RAFRawAccess(fn,useSync,sectorSize);
		if (makeStatistic)
			r = new StatisticsRawAccess(r,50,1.1,1.1);
		return r;
	}

	/**
	 * Main method
	 * @param args Command line options are ignored here.
	 */
	public static void main(String[] args) {
		if (!TestFramework.processParameters("RawAccessTest\n", RawAccessTest.class, args, System.out))
			return;
		
		RawAccess src=null,dest=null;
	  	Timer timer = null;
	  	long tzero=0;
	  	long tticks=0;
	  	
		if (testNr!=-1) {
		  	src = open(fn1,startSectorSize, true);
			temp=new byte[startSectorSize];
			System.out.println(src);
			timer = (Timer) TimerUtils.FACTORY_METHOD.invoke();
		  	TimerUtils.warmup(timer);
		  	tzero = TimerUtils.getZeroTime(timer);
		  	tticks = timer.getTicksPerSecond() / 1000; // calculation is in ms now
		  	System.out.println("Timer info: " + timer.timerInfo());
		}
		
		if (testNr==0) {
			timer.start();
			readSektor0Repeatedly(src,numberOfChunks);
			long t2 = timer.getDuration();
			System.out.println("Duration for "+numberOfChunks+" src.read(temp,0): "+(t2-tzero)/tticks+"ms");
		}
		else if (testNr==1) {
			int sectorSize = startSectorSize;
			
			do {
				timer.start();
				readSequentially(src,numberOfChunks);
				long t2 = timer.getDuration();
				double time = ((double) (t2-tzero))/tticks;
				double transferRate = ((sectorSize*numberOfChunks)/(time/1000))/1024;
				System.out.print("Sector size:\t"+sectorSize+"\t");
				System.out.print("Duration:\t"+time+"\t");
				System.out.println("Transfer rate in KB/s:\t"+transferRate);
				
				src.close();
				sectorSize += stepSectorSize;
				src = open(fn1, sectorSize, false);
				temp=new byte[sectorSize];
			} while (sectorSize<=endSectorSize);
		}
		else if (testNr==2) {
			timer.start();
			if (seekFrom0)
				readSeekForwardFrom0Test(src,stepwide);
			else
				readSeekForwardTest(src,stepwide);
			long t2 = timer.getDuration();
			System.out.println("Duration: "+(t2-tzero)/tticks+"ms");
			System.out.println("Number of seek operations: "+(int) (src.getNumSectors()/stepwide));
		}
		else if (testNr==3) {
			xxl.core.util.random.DiscreteRandomWrapper drw;
			final long sec = src.getNumSectors();
			
			if (randomJava)
				drw = new xxl.core.util.random.JavaDiscreteRandomWrapper();
			else {
				drw =  
					new xxl.core.util.random.DiscreteRandomWrapper() {
						private long runNr = 0;
						public int nextInt() {
							runNr++;
							return (int) (59.2337192*(sec*runNr*runNr));
						}
					};
			}
			
			timer.start();
			readRandomSeekTest(src,numSeeks,drw);
			long t2 = timer.getDuration();
			System.out.println("Duration: "+(t2-tzero)/tticks+"ms");
			System.out.println("Number of seek operations: "+numSeeks);
		}
		else if (testNr==4) {
			
		  	if (construct) {
		  		// System.out.println("Construct #1: "+RawAccessUtils.createFileForRaw(fn1,numberOfChunks));
	 	 		System.out.println("Construct #2: "+RawAccessUtils.createFileForRaw(fn2,numberOfChunks));
	 	 	}
		  	
			dest = open(fn2, startSectorSize, true);
			
	 	 	if (fill) {
	 	 		RawAccessUtils.fillRawAccess(src, -1, 1);
	 	 		RawAccessUtils.fillRawAccess(dest, -1, 2);
	 	 	}
	 	 	
			((StatisticsRawAccess)src).resetCounter();
			((StatisticsRawAccess)dest).resetCounter();
			
			timer.start();
			RawAccessUtils.copyRawAccess(src,dest);
			long t2 = timer.getDuration();
			System.out.println("Copy, Sektoren: "+numberOfChunks+"\t"+(t2-tzero)/tticks+"ms");
			
			dest.close();
			
			System.out.println(dest);
		}
		else if (testNr==5) {
		  	RawAccess src2 = open(fn1,startSectorSize, true);
			timer.start();
			readSequentiallyTwoThreads(src, src2, numberOfChunks);
			long t2 = timer.getDuration();
			double time = ((double) (t2-tzero))/tticks;
			double transferRate = ((startSectorSize*numberOfChunks)/(time/1000))/1024;
			System.out.print("Sector size:\t"+startSectorSize+"\t");
			System.out.print("Duration:\t"+time+"\t");
			System.out.println("Transfer rate in KB/s:\t"+transferRate);
		}

		if (testNr!=-1) {
			src.close();
			System.out.println(src);
		}
	}
}
