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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.LinkedList;
import java.util.Random;

import xxl.core.io.fat.ExtendedFile;
import xxl.core.io.fat.ExtendedRandomAccessFile;
import xxl.core.io.fat.FAT;
import xxl.core.io.fat.FATDevice;
import xxl.core.io.fat.FileSystem;
import xxl.core.io.fat.errors.DirectoryException;
import xxl.core.io.fat.errors.InitializationException;
import xxl.core.io.fat.errors.WrongFATType;
import xxl.core.io.fat.errors.WrongLength;
import xxl.core.io.raw.RAFRawAccess;
import xxl.core.io.raw.RawAccess;
import xxl.core.io.raw.RawAccessUtils;
import xxl.core.util.WrappingRuntimeException;

/**
 * Perform different test with the filesystem.
 */
public class FileSystemTest {
	
	/**
	 * The name of the device.
	 */
	private static String deviceName = null;
	
	/**
	 * The file where the information of the devices is stored.
	 */
	private static File mbr = null;
	
	/**
	 * The filesystem that is used to perform the tests.
	 */
	private static FileSystem fileSystem = null;
	
	/**
	 * The path of the directory where the output is stored, i.e., temporary
	 * files are created.
	 */
	private static String outDir = null;
	
	/**
	 * The filesystem needs a file that exists inside the OS-file system. The
	 * file is never used, just opened read only and immediately closed again.
	 */
	private static File dummyFile = null;
	
	/**
	 * An array holding the names of the devices that can be tested by this
	 * class.
	 */
	private static String deviceNames[] = new String[]{ "devFat12","devFat16","devFat32" };

	/**
	 * Compare the content of the two files.
	 * @param eraf the first file.
	 * @param raf the second file.
	 * @return true if the content (and length) of the two files are equal;
	 * false otherwise.
	 */
	public static boolean compare(ExtendedRandomAccessFile eraf, RandomAccessFile raf) {	
		boolean flag1 = true;
		boolean flag2 = true;
		long filePointer = -1;
		try {			
			if (raf.length() != eraf.length()) {
				System.out.println("File length of RandomAccessFile '"+raf+
					"' = "+raf.length()+" and ExtendedRandomAccessFile '"+eraf+
					"' = "+eraf.length()+" differ.");
				return false;
			}
			long rafLength = raf.length();
			raf.seek(0);
			eraf.seek(0);
			
			while (raf.getFilePointer() < rafLength) {
				filePointer = raf.getFilePointer();
				flag1 = false;
				byte val1 = raf.readByte();
				flag1 = true;
				
				flag2 = false;
				byte val2 = eraf.readByte();
				flag2 = true;
				if (val1 != val2) {
					System.out.println("Content of RandomAccessFile '"+raf+
						"' = "+val1+" and ExtendedRandomAccessFile '"+eraf+
						"' = "+val2+" differ at position "+raf.getFilePointer()+".");
					return false;
				}
			}
			
			raf.seek(rafLength);
			eraf.seek(rafLength);
		}
		catch (IOException ioe) {
			System.out.println(ioe);
			System.out.println("differ at position "+filePointer);
			System.out.println("flag1 "+flag1+" flag2 "+flag2);
			return false;
		}
		return true;
	}
		
	/**
	 * Write lengthInBytes number of randomly choosen bytes in both files.
	 * In case of an error false is returned; true otherwise.
	 * @param eraf the first file.
	 * @param raf the second file.
	 * @param lengthInBytes number of randomly choosen bytes.
	 * @return In case of an error false is returned; true otherwise.
	 */
	public static boolean writeContent(ExtendedRandomAccessFile eraf, RandomAccessFile raf, int lengthInBytes) {
		Random rand = new Random(255);
		byte[] content = new byte[lengthInBytes];
		rand.nextBytes(content);
		try {
			for (int i=0; i < content.length; i++) {
				raf.writeByte(content[i]);
				eraf.writeByte(content[i]);
			}
		}
		catch(IOException ioe) {
			ioe.printStackTrace();
			System.out.println(ioe);
			return false;
		}
		return true;
	}
		
	/**
	 * Create the device with the given parameters. If the device couldn't be created,
	 * a RuntimeException is thrown.
	 * @param deviceName the name of the device.
	 * @param rawAccess the raw access object.
	 * @param fatType the fat type.
	 * @param actionType the action type.
	 * @return the fat device object.
	 */
	public static FATDevice createDevice(String deviceName, RawAccess rawAccess, byte fatType, byte actionType) {					
		FATDevice device = null;
		try {
			device = fileSystem.initialize(deviceName, rawAccess, fatType, actionType);
			
			// Simple Method without FileSystem
			// device = new FATDevice(deviceName, fatType, rawAccess, System.out, dummyFile);
		}
		catch (WrongFATType et) {
			System.out.println(et);
			close();
			throw new WrappingRuntimeException(et);
		}
		catch (WrongLength wl) {
			System.out.println(wl);
			close();
			throw new WrappingRuntimeException(wl);
		}
		catch (IOException ioe) {
			System.out.println(ioe);
			close();
			throw new WrappingRuntimeException(ioe);
		}
		
		return device;
	}
		
	/**
	 * Delete all temporary data and shut down filesystem.
	 */
	public static void close() {
		//clear all temporary files and folders.
		if (fileSystem != null)
			fileSystem.shutDown();
				
		for (int i=0; i<deviceNames.length ; i++) {
			File rm = new File(outDir+deviceNames[i]);
			rm.delete();
		}

		if (mbr != null)
			mbr.delete();
	}
	
	/**
	 * Perform the test for the given device.
	 * @param device that should be tested.
	 */
	public static void performTest(FATDevice device) {		
		//create as much entries in the root directory as space is given
		
		System.out.println("Create as much directory and file entries in the root directory as space is given, but at most 500 files.");
		String dirName = "dir";
		String fileName = "file";
		long number = 0;
		LinkedList rootDirectories = new LinkedList();
		LinkedList rootFiles = new LinkedList();
		
		ExtendedFile ef;
		boolean flag;
		for (int counter = 0; counter < 1; counter++) {
			ef = device.getFile(dirName + Long.toString(number));
			flag = ef.mkdir();
			if (flag)
				rootDirectories.add(dirName + Long.toString(number));
			else
				break;
			
			ef = device.getFile(fileName + Long.toString(number));
			try{
			flag = ef.createNewFile();
			}
			catch (Exception e) {
				e.printStackTrace();
			} 
			if (flag)
				rootFiles.add(fileName + Long.toString(number));
			else
				break;
				
			++number;
		}
		
		//check if all created directories and files can be found
		System.out.println("Check now if all created entries exist.");
		for (int i=0; i < rootDirectories.size(); i++) {
			ef = device.getFile((String)rootDirectories.get(i));
			flag = ef.exists();
			if (!flag)
				System.out.println("ERROR: Couldn't find file '"+(String)rootDirectories.get(i)+"'.");
		}
		
		for (int i=0; i < rootFiles.size(); i++) {
			ef = device.getFile((String)rootFiles.get(i));
			flag = ef.exists();
			if (!flag)
				System.out.println("ERROR: Couldn't find file '"+(String)rootFiles.get(i)+"'.");
		}
		System.out.println("If you got no error message, everything is allright until now.");
		
		int numberOfLoops = 20;
		
		//choose one file and fill it with content
		//fill the same content to a file on the 'real'-device
		System.out.println("Perform "+numberOfLoops+" times a different operation"+
			" on an ExtendedRandomAccessFile and a RandomAccessFile."+
			" After each operation the content of both files will be compared."+
			" If the content is different an error will be printed out."
		);
		
		ExtendedRandomAccessFile eraf = null;
		RandomAccessFile raf = null;
		try {
			eraf = device.getRandomAccessFile((String)rootFiles.get(0), "rw");
			String javaFileName = outDir + (String)rootFiles.get(0); 
			File f = new File(outDir,(String)rootFiles.get(0));
			f.delete();
			raf = new RandomAccessFile(javaFileName,"rw");
			try { raf.setLength(0); } catch (IOException e){}
		}
		catch (FileNotFoundException fnfe) {
			System.out.println(fnfe);
			close();
			throw new WrappingRuntimeException(fnfe);
		}
		catch (DirectoryException de) {
			System.out.println(de);
			close();
			throw new WrappingRuntimeException(de);
		}
		
		boolean directOutput = true;
		//fill the files with life				
		Random rand = new Random();
		Random operationRand = new Random();
		LinkedList historie = new LinkedList();
		try {
			for (int i=0; i < numberOfLoops; i++) {
				int op = operationRand.nextInt(4);	//inclusive 0, exclusive the parameter
				if (op == 0) {
					//write some content to the files
					int length = rand.nextInt(15000);
					historie.add(new String("Write content of length="+length));
					if (directOutput)
						System.out.println("Write content of length="+length);
					writeContent(eraf, raf, length);
				}
				else if (op == 1) {
					//seek to a random position and write two bytes
					Random tmpRand = new Random();
					int tmp = (int)raf.length();
					if (tmp == 0) {
						i--;
						continue;
					}
					int newPos = tmpRand.nextInt(tmp);
					historie.add(new String("Seek to "+newPos+" and write two bytes after that."));
					if (directOutput)
						System.out.println("Seek to "+newPos+" and write two bytes after that.");
					eraf.seek(newPos);
					raf.seek(newPos);
					writeContent(eraf, raf, 2);
				}
				else if (op == 2) {
					i--;
					if (true) continue;
					//set new length
					Random tmpRand = new Random();
					int tmp = (int)raf.length();
					if (tmp == 0) {
						i--;
						continue;
					}
					int newLength = tmpRand.nextInt(tmp)*2;
					historie.add(new String("Set new length "+newLength));
					if (directOutput)
						System.out.println("Set new length "+newLength);
					
					//save old length
					long oldLength = raf.getFilePointer();					
					//set new length
					raf.setLength(newLength);
					eraf.setLength(newLength);
					/*
					//in case the new length is greater than the old length
					//we need to write the same value at each file in order to
					//adjust them. Otherwise the values between the old length
					//and the new length may differ and the compare routine will
					//cause an error
					if (newLength > oldLength)
					{
						historie.add(new String("Adjust values (newLength-oldLength="+(newLength-oldLength)+")"));
						if (directOutput)
							System.out.println("Adjust the unknown values between the old length and the new length of both files.");
						raf.seek(oldLength);
						eraf.seek(oldLength);
						for (int j=0; j < newLength - oldLength; j++)
						{
							raf.write((byte)22);
							eraf.write((byte)22);
						}
					}*/
				}
				else if (op == 3) {
					//skip n bytes
					Random tmpRand = new Random();
					int tmp = (int)raf.length();
					if (tmp == 0) {
						i--;
						continue;
					}
					int skip = tmpRand.nextInt(tmp);
					historie.add(new String("Skip "+skip+" bytes"));
					if (directOutput)
						System.out.println("Skip "+skip+" bytes");
					int val1 = raf.skipBytes(skip);
					int val2 = eraf.skipBytes(skip);
					if (val1 != val2) {
						System.out.println("ERROR: Returned skip value differ. raf.skip returned "+val1+", eraf.skip returned "+val2);
						System.out.println("Historie:");
						System.out.println(historie);
						break;
					}
				}
				
					
				//compare the content of both files
				if (!compare(eraf, raf)) {
					System.out.println("ERROR: The content differ.");
					System.out.println("Historie:");
					System.out.println(historie);
					break;
				}
			}
		}
		catch(IOException ioe) {
			System.out.println(ioe);
			System.out.println("Historie:");
			System.out.println(historie);
		}
		
		try {
			eraf.close();
			raf.close();
		}
		catch(IOException ioe) {
			System.out.println(ioe);
		}
						
		File file = new File(
			outDir+
			(String)rootFiles.get(0)
		);
		file.delete();
		
		ExtendedFile efTemp = device.getFile((String)rootFiles.get(0));
		efTemp.delete();
		
		if (deviceName != null) {	
			fileSystem.unmount(deviceName);	
			File tempfile = new File(deviceName);
			tempfile.delete();
		}
						
		System.out.println("\nThe actual test for this device finishes here.\n");
	}	//end performTest(device)
	
	/**
	 * Create a FAT12 device.
	 * @return the fat device object.
	 */
	public static FATDevice createDeviceFAT12() {
		long numOfBlocks = 2880;
		
		if (!RawAccessUtils.createFileForRaw(outDir+deviceNames[0], numOfBlocks)) {
			close();
			throw new RuntimeException("Couldn't create device '"+deviceNames[0]+"'.");
		}
		RawAccess rawAccess = new RAFRawAccess(outDir+deviceNames[0]);
		
		return createDevice(outDir+deviceNames[0], rawAccess, FAT.FAT12, FileSystem.FORMAT);
	}
	
	/**
	 * Create a FAT16 device.
	 * @return the fat device object.
	 */
	public static FATDevice createDeviceFAT16() {
		long numOfBlocks = 32680;
		
		if (!RawAccessUtils.createFileForRaw(outDir+deviceNames[1], numOfBlocks)) {
			close();
			throw new RuntimeException("Couldn't create device '"+deviceNames[1]+"'.");
		}
		RawAccess rawAccess = new RAFRawAccess(outDir+deviceNames[1]);
		return createDevice(outDir+deviceNames[1], rawAccess, FAT.FAT16, FileSystem.FORMAT);
	}
	
	/**
	 * Create a FAT32 device.
	 * @return the fat device object.
	 */
	public static FATDevice createDeviceFAT32(){
		long numOfBlocks = 532480;
		
		if (!RawAccessUtils.createFileForRaw(outDir+deviceNames[2], numOfBlocks)) {
			close();
			throw new RuntimeException("Couldn't create device '"+deviceNames[2]+"'.");
		}
		RawAccess rawAccess = new RAFRawAccess(outDir+deviceNames[2]);
		return createDevice(outDir+deviceNames[2], rawAccess, FAT.FAT32, FileSystem.FORMAT);
	}
	
	/**
	 * Main method.
     * @param args the arguments 
	 */
	public static void main(String[] args) {
		
		outDir = Common.getOutPath();
		
		try {
			// new filesystem
			mbr = new File(outDir+"mbrTest.txt");
			mbr.delete();
			mbr.createNewFile();
			
			dummyFile = new File(outDir+"dummyFile");
			dummyFile.delete();
			dummyFile.createNewFile();
			
			fileSystem = new FileSystem(outDir+"mbrTest.txt",System.out,dummyFile);
		}
		catch (InitializationException ie) {
			close();
			throw new WrappingRuntimeException(ie);
		}
		catch(java.io.IOException io) {
			close();
			throw new WrappingRuntimeException(io);
		}
		
		FATDevice device = null;
		
		//create disk-device FAT12
		System.out.println("\nCreate disk-device FAT12 as RAF-type.");
		device = createDeviceFAT12();
		System.out.println("Start test for a FAT12 filesystem\n");
		performTest(device);
		
		//create disk-device FAT16
		System.out.println("\nCreate disk-device FAT16 as RAF-type.");
		device = createDeviceFAT16();
		System.out.println("Start test for a FAT16 filesystem\n");
		performTest(device);
		
		//at this point the device exists or the program terminated
		
		//create disk-device FAT32
		System.out.println("\nCreate disk-device FAT32 as RAF-type.");
		device = createDeviceFAT32();
		System.out.println("Start test for a FAT32 filesystem\n");
		performTest(device);
	
		System.out.println("\nThe test finishes here. All error messages in case they appear are printed.");
		close();
	}
}
