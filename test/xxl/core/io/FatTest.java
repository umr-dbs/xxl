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

package xxl.core.io;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.ResultSetMetaData;

import xxl.core.collections.queues.ListQueue;
import xxl.core.collections.queues.io.RandomAccessFileQueue;
import xxl.core.cursors.Cursor;
import xxl.core.cursors.Cursors;
import xxl.core.cursors.MetaDataCursor;
import xxl.core.cursors.mappers.Mapper;
import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;
import xxl.core.functions.Functions;
import xxl.core.io.FilesystemOperations;
import xxl.core.io.JavaFilesystemOperations;
import xxl.core.io.RandomAccessFileInputStream;
import xxl.core.io.fat.FAT;
import xxl.core.io.fat.FATDevice;
import xxl.core.io.fat.FATFilesystemOperations;
import xxl.core.io.raw.RAFRawAccess;
import xxl.core.io.raw.RawAccess;
import xxl.core.io.raw.RawAccessUtils;
import xxl.core.relational.cursors.InputStreamMetaDataCursor;
import xxl.core.relational.cursors.MergeSorter;
import xxl.core.relational.metaData.ResultSetMetaDatas;
import xxl.core.relational.tuples.ArrayTuple;
import xxl.core.relational.tuples.TupleConverter;
import xxl.core.relational.tuples.Tuples;
import xxl.core.util.WrappingRuntimeException;

/**
 * This class demonstrate the usage of the methods of {@link xxl.core.io.fat.FATDevice}.
 * which is a filesystem on the raw device e.g. creating, copying and deleting
 * of files.
 * In addition the usage of an external MergeSort in {@link xxl.core.relational.cursors} is shown.
 */
public class FatTest {
	/** This class uses a FATDevice fd */
	protected static FATDevice fd=null;
	
	/** Filesystem to be used for opening the InputStream for files. */
	protected static FilesystemOperations fso=null;

	/**
	 * Constant for the mode
	 */
	protected final static int MODE_INTERNAL=0;
	/**
	 * Constant for the mode
	 */
	protected final static int MODE_EXTERNAL=1;
	/**
	 * Constant for the mode
	 */
	protected final static int MODE_FAT=2;

 	/**
	 * Mode in which the application runs. Possible values: MODE_INTERNAL, MODE_EXTERNAL, MODE_FAT
	 */
	protected static int mode = MODE_FAT;

	/**
	 * Numbering for the temporary queues
	 */
	private static int countQueues=0;
	
  	/**
	 *  Creates a FATDevice.<br>
	 *  
	 * r.getNumSectors()<4086 =>: FAT.FAT12<br>
	 * 32680<r.getNumSectors()<532480=>: FAT.FAT16<br>
	 * r.getNumSectors()>4194304  =>: FAT.FAT32<br>
	 * @param r raw access to read and write from and to disk.
	 * @param devicefile name of device or file.
	 * @param out Printstream for messages.
	 * @param dumm name of dummyfile.
	 * @param fatType the type of the fat.
	 */ 
     	public static void createFATDevice(RawAccess r, String devicefile, PrintStream out, File dumm, byte fatType) {

		/** the number of sectors of rawaccess*/
		long sec_anz=r.getNumSectors();

		try{
			if(sec_anz<4084) {
	        		fd = new FATDevice(devicefile, FAT.FAT12, r, out, dumm); 
	  			System.out.println("Type: FAT12");
	   		}
	    		else if( (32680<sec_anz) && (sec_anz<4194304) && (fatType==FAT.FAT16) ) {
  				fd = new FATDevice(devicefile, FAT.FAT16, r, out, dumm); 
  				System.out.println("Type: FAT16");
   			}
   			else if ( (sec_anz>532480) ) { /* excluded: && (fatType==FAT.FAT32) */
   				fd = new FATDevice(devicefile, FAT.FAT32, r, out, dumm); 
     				System.out.println("Type: FAT132");
			}
			else { 
				fd=null;
				System.out.println("Fat device could not be constructed");
			}
		} //end of try
		catch (Exception e) {
			throw new WrappingRuntimeException(e);
		}
	}
	
	/** 
	 * The main-method contains an example, how to construct a filesystem 
	 * on a rawdevcie and how to create and delete a file, and also how to copy 
	 * one file into the virtual file system.
	 * In addition, this method shows, how to sort externally
	 * all data from a relational file (Mergesort).
	 * <p>
	 * This method can be called with 0 or 1 parameter.<br>
	 * <pre>
	 * 	xxl applications.release.io.FatTest <mode>
	 * </pre>
	 * where <mode> can be
	 * <ul>
	 * <li>0: uses main memory lists for queues.</li>
	 * <li>1: uses random access files for queues.</li>
	 * <li>2: uses random access files inside a FATDevice.</li>
	 * </ul>
	 * 
	 * @param args the arguments passed to the <tt>main</tt> method.
	 */
	public static void main(String[] args){
		System.out.println("Testing a FAT");

		String filename = "movies.tbl";
		String path="";
		
		MetaDataCursor mc;
		File dummyFile=null;
		RawAccess r=null;
		
		if (args.length==1)
			mode = Integer.parseInt(args[0]);
		
		if (mode<0 || mode>MODE_FAT) {
			System.out.println("Error in parameter");
			return;
		}
		
		if (mode==MODE_FAT) {
			System.out.println("FAT-Mode");
			//create a new raw access
			RawAccessUtils.createFileForRaw(Common.getOutPath()+"rawaccess.bin",2000);
			r = new RAFRawAccess(Common.getOutPath()+"rawaccess.bin");
	
			dummyFile = RawAccessUtils.createStdDummyFile();
	
	  		//create a FATDevice
	  		createFATDevice(r, "FatTestDevice", System.out, dummyFile, FAT.FAT16);
	
			try {
				// exchage the data of a file from the original filesystem
				// to this filesystem based on the raw-access.
				fd.copyFileToRAW(new File(Common.getRelationalDataPath()+filename),filename);
			} 
			catch(Exception e){
				throw new WrappingRuntimeException(e);
			}
			
			final String fpath = filename;
			
			fso = new FATFilesystemOperations(fd);
			try {
				mc = new InputStreamMetaDataCursor(
					new AbstractFunction<Object, RandomAccessFileInputStream>() {
						public RandomAccessFileInputStream invoke() {
							return new RandomAccessFileInputStream(fso.openFile(fpath, "rw"));
						}
					},
					new String[] {"\t"},
					new String[] {"\r\n"},
					"#",
					ArrayTuple.FACTORY_METHOD
				);
			}
			catch (IOException e) {
				throw new WrappingRuntimeException(e);
			}
		}
		else {
			if (mode==MODE_INTERNAL)
				System.out.println("Internal Mode");
			else
				System.out.println("External Mode");
				
			path = Common.getOutPath();
			fso = JavaFilesystemOperations.DEFAULT_INSTANCE;
			try {
				mc = new InputStreamMetaDataCursor(Common.getRelationalDataPath()+filename);
			}
			catch (IOException e) {
				throw new WrappingRuntimeException(e);
			}
		}

		// The TupleConverter needs ResultSetMetaData. This is
		// needed inside the newQueue-Function.
		final ResultSetMetaData rm = ResultSetMetaDatas.getResultSetMetaData(mc);
		
		// Function delivering a ListQueue in main memory
		// or a RandomAccessFileQueue on external memory
		// depending on the static class attribute 'external'
		Function newQueue = null;
		
		final String finalpath = path;
		
		switch (mode) {
		case MODE_INTERNAL:
			newQueue = new AbstractFunction() {
				public Object invoke (Object inputBufferSize, Object outputBufferSize) {
					countQueues++;
					System.out.println("Create a new temporary internal ListQueue");

					return new ListQueue();
				}
			};
			break;
		case MODE_EXTERNAL:	// works for both!
		case MODE_FAT:
			newQueue = new AbstractFunction() {
				public Object invoke (Object inputBufferSize, Object outputBufferSize) {
					countQueues++;
					String filename = finalpath+"RAF"+countQueues+".queue";
					fso.deleteFile(filename);

					System.out.println("Create a new temporary external RandomAccessFileQueue: "+filename);

					return new RandomAccessFileQueue(filename,fso,new TupleConverter(true, rm),(Function) inputBufferSize,(Function) outputBufferSize);
				}
			};
			break;
		}
		
		// sort with Mergesort
		MergeSorter sorter = new MergeSorter (
			mc,
        		Tuples.getTupleComparator(new int[] {3}),
			//objsize,memsize,finalmem 
    			512,12*4096,4*4096,
			newQueue,
			true		// verbose = true!
		);

		System.out.println("Tuples have been sorted in increasing order of the second column");

		Cursor c = sorter;
		
		/*
		c = new Mapper(
			c,
			Functions.printlnMapFunction(System.out)
		);
		*/
			
		c = new Mapper(
			Functions.comparatorTestMapFunction(Tuples.getTupleComparator(new int[] {3}),true),
			c
		);
		
		System.out.println("Consume cursor");
		System.out.println("Numer of results (should be 3027): "+Cursors.count(c));
		System.out.println("Sorted was successful (it has been tested!)");
		
		c.close();
		
		if (dummyFile!=null)
			RawAccessUtils.deleteDummyFile(dummyFile);

		// Queues are automatically deleted (because they are totally consumed)
		/* if (mode==MODE_EXTERNAL || mode==MODE_FAT)
			for (int i=1; i<=countQueues; i++)
				if (!fso.deleteFile(finalpath+"RAF"+i+".queue"))
					System.out.println("Could not delete '"+finalpath+"RAF"+i+".queue'");
		*/
		
		if (mode==MODE_FAT) {
			r.close();
			JavaFilesystemOperations.DEFAULT_INSTANCE.deleteFile(Common.getOutPath()+"rawaccess.bin");
		}
	}
}
