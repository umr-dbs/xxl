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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.Vector;

import xxl.core.io.fat.errors.InitializationException;
import xxl.core.io.fat.errors.WrongFATType;
import xxl.core.io.fat.errors.WrongLength;
import xxl.core.io.fat.util.MyMath;
import xxl.core.io.raw.NativeRawAccess;
import xxl.core.io.raw.RAFRawAccess;
import xxl.core.io.raw.RAMRawAccess;
import xxl.core.io.raw.RawAccess;
import xxl.core.util.WrappingRuntimeException;

/**
 * This class represents a file system. The file system manages devices. Each device is a
 * raw partition, RandomAccessFile, or a RAM area. For each device there is one RawAccess
 * object which supports the low level I/O-Operations on it.
 */
public class FileSystem
{	
	/**
	 * Iff the debug flag is set you get some debug messages like bpb, fat,
	 * fsi, and directory structure.
	 */
	public static boolean debug = false;
	
	/**
	 * Indicates the type of action to do. Boot the device of the file system.
	 */
	public static final byte BOOT = 0;
	
	/**
	 * Indicates the type of action to do. Format the device of the file system.
	 */
	public static final byte FORMAT = 1;
	
	/**
	 * This file represents a master-boot-record. In that file
	 * all bootable devices and information about them are listed.
	 * Each time a new device is created, all necessary information
	 * about it will be saved at this file. The next time the file
	 * system will start, all devices stored at the bootFile
	 * will booted automatically.
	 */	
	private String bootFileName;
	
	/**
	 * List of all active devices like floppys, partitions etc.
	 */
	private List devices = new LinkedList();

	/** 
	 * The extension of a RandomAccessFile needs a file
	 * that exists inside the OS-file system. The file is never used,
	 * just opened read only and immediately closed again. This field
	 * only exists because of the inflexible implementation of RandomAccessFile.
	 */
	private File dummyFile;
	
	/**
	 * Output stream for messages.
	 */
	private PrintStream out;
	
	/**
	 * This class contains information about the device objects.
	 */
	public class DeviceInformation
	{
		/**
		 * FATDevice object.
		 */
		protected FATDevice device;
		
		/**
		 * Number of users of the device stored at this class.
		 */
		protected int numberOfUsers;
		
		/**
		 * Create an instance of this object with the given device.
		 * @param device the device object to store.
		 */
		public DeviceInformation(FATDevice device)
		{
			this.device = device;
			numberOfUsers = 1;
		}	//end constructor
		
		
		/**
		 * Add one user to the number of users that use this device.
		 */
		public void addUser()
		{
			numberOfUsers++;
		}	//end addUser()
		
		
		/**
		 * Remove one user and return number of active user.
		 * @return the number of remaining users.
		 */
		public int removeUser()
		{
			numberOfUsers--;
			return numberOfUsers;
		}	//end removeUser()
		
		
		/**
		 * Return the number of users of the device object.
		 * @return the number of users.
		 */
		public int getNumOfUsers()
		{
			return numberOfUsers;
		}	//end getNumberOfUsers()
		
		
		/**
		 * Return the stored device object.
		 * @return the stored device object.
		 */
		public FATDevice getDevice()
		{
			return device;
		}	//end getDevice()
	}	//end inner class DeviceInformation

	/**
	 * Remove the whole line where the device with deviceName is stored
	 * from the file where all bootable devices are listed.
	 * @param deviceName the name of the device.
	 * @param bootFileName Name of the file where the information of the devices is stored.
	 * @return true in case the operation was succesfull; false otherwise.
	 */
	public static boolean removeLine(String deviceName, String bootFileName)
	{
		try
		{
			RandomAccessFile raf;
			raf = new RandomAccessFile(bootFileName, "rw");
			raf.seek(0);	
			
			long fileLength = raf.length();
			long tempLength = 0;
			boolean found = false;
			
			//search the line where deviceName is stored
			while(raf.getFilePointer() < fileLength)
			{
				tempLength = raf.getFilePointer();
				String line = raf.readLine();
				StringTokenizer st = new StringTokenizer(line, "\t");
				String name = "";
				if (st.hasMoreTokens())
					name = st.nextToken();
				else
					continue;
				
				if (name.equals(deviceName))
				{
					found = true;
					break;
				}
			}
			if (!found)
				return false;
			
			//skip the line that should be deleted
			if (raf.getFilePointer() < fileLength)
				raf.readLine();
			Vector lines = new Vector();
			while(raf.getFilePointer() < fileLength)
			{
				String line = raf.readLine();
				lines.add(line);
			}
			
			//set file size to tempLength
			raf.setLength(tempLength);
			
			if (raf.length() != 0)
				raf.write(0x0A);	//lf
			
			//write the saved lines back to the file
			for (int i=0; i < lines.size(); i++)
			{
				raf.writeBytes((String)lines.get(i));
				raf.write(0x0D);	//cr
				raf.write(0x0A);	//lf
			}
			
			raf.close();
		}
		catch (IOException e)
		{
			throw new WrappingRuntimeException(e);
		}
		return true;
	}	//end removeLine(String deviceName)
	
		
	/**
	 * Return the lines of the bootFile as a string array.
	 * If the file is empty the returned string array will
	 * be empty, too.
	 * 
	 * @param bootFileName Name of the file where the information of the devices is stored.
	 * @return the lines of the bootFile or an empty string
	 * array if there is no content.
	 */
	public static String[] getBootFileContent(String bootFileName)
	{
		Vector lines = new Vector();
		try
		{
			RandomAccessFile raf;
			raf = new RandomAccessFile(bootFileName, "rw");
			raf.seek(0);	
				
			long fileLength = raf.length();
			while(raf.getFilePointer() < fileLength)
			{
				String line = raf.readLine();
				lines.add(line);
			}

			raf.close();
		}
		catch(IOException e)
		{
			throw new WrappingRuntimeException(e);
		}
		return (String[])(lines.toArray(new String[0]));
	}	//end getBootFileContent()
	
	
	/**
	 * Check if the given device name eqauls a unix device name. That is
	 * the name has as prefix "/dev/X", where X is the name of the device.
	 * @param name the name of the device.
	 * @return true in case the given name eqauls a unic device name, otherwise
	 * false is returned.
	 */
	public static boolean isUnixDeviceName(String name)
	{
		if (name.startsWith("/dev/") && name.length() > "/dev/".length())
			return true;
		return false;
	}

	/**
	 * Return a file system object. 
	 * @param bootFileName Name of the file where the information of the devices is stored.
	 * @param out Output stream for some messages of the FATDevice. You can use System.out for
	 *	example.
	 * @param dummyFile The extension of a RandomAccessFile needs a file
	 *	that exists inside the OS-file system. The file is never used,
	 *	just opened read only and immediately closed again. This parameter
	 *	only exists because of the inflexible implementation of RandomAccessFile.
	 */
	public FileSystem(String bootFileName, PrintStream out, File dummyFile)
	{
		this.bootFileName = bootFileName;
		this.out = out;
		this.dummyFile = dummyFile;
		
		bootDevices();
	}	//end constructor
	
	
	/**
	 * Boot all devices that could be found at the bootFile.
	 * All devices that couldn't be initialized or found on the hard-disk will
	 * be skipped.
	 */
	private void bootDevices() {
		try {
			RandomAccessFile bootFile;
			
			try { 
				bootFile = new RandomAccessFile(bootFileName, "rw");
			}
			catch (FileNotFoundException e) {
				throw new InitializationException("The file '"+bootFileName+"' could not be found.");
			}
						
			long bflength = bootFile.length();
			String line = "";
			do {
				line = bootFile.readLine();
				if (line == null || line.equals(""))
					return;
					
				StringTokenizer stringTokenizer = new StringTokenizer(line, "\t");
				String deviceName = stringTokenizer.nextToken();
				long sizeInByts = (new Long( stringTokenizer.nextToken() )).longValue();
				String rawType = stringTokenizer.nextToken();
				
				RawAccess rawAccess = null;
				try {
					if (rawType.equals("RAF"))
						rawAccess = new RAFRawAccess(deviceName);
					else if (rawType.equals("RAM"))
						rawAccess = new RAMRawAccess(MyMath.roundUp(sizeInByts/512.0));
					else if (rawType.equals("NATIVE"))
						rawAccess = new NativeRawAccess(deviceName);
					else {
						out.println("Wrong RawAccessType in file "+bootFileName+". It has to be: RAF, RAM, or NATIVE");
						continue;
					}
					
					if (rawAccess == null)
						throw new NullPointerException();
				}
				catch (Exception e) {
					out.println("Couldn't not open the RawAccess file "+deviceName+". Skip the boot process of the associated device.");
					continue;
				}
				
				try {
					FATDevice device = new FATDevice(deviceName, rawAccess, out, dummyFile);
					devices.add(new DeviceInformation(device));
				}
				catch (InitializationException e) {
					out.println(e);
				}
			} while(!line.equals("") || bootFile.getFilePointer() < bflength);
			
			bootFile.close();
		}
		catch(IOException e) {
			throw new RuntimeException("Couldn't boot the devices from "+bootFileName);
		}
	}	
	
	/**
	 * Return a FATDevice which supports all file system operations on the raw device
	 * specified by device name. The I/O-opeartions are supported by the RawAccess file.
	 * If the requested device already exist no new device will be created. The method
	 * returns the existing device.
	 * For a FAT12 file system the length of rawAccess must be smaller than 4084 blocks.
	 * For a FAT16 file system the length of rawAccess must be smaller than 4194304
	 * 512-byte-blocks and bigger than 32680 512-byte-blocks.
	 * For a FAT32 file system the length of rawAccess must be smaller than 0xFFFFFFFF bytes.
	 * and bigger than 532480 512-byte-blocks.
	 * @param deviceName the name of the device.
	 * @param rawAccess the raw access file which supports the I/O-Operations.
	 * @param fatType the type of FAT that should be formatted. In case you boot the
	 * device you can set fatType as UNKNOWN. The device will recognize by itself
	 * what kind of FAT it is.
	 * @param actionType indicated the action to do, that are BOOT an existing device
	 * or FORMAT the partition whith the given fat type.
	 * @return the device object.
	 * @throws WrongFATType if the given fatType is neither FAT.FAT12, nor FAT.FAT16, nor FAT.FAT32.
	 * @throws WrongLength if the number of blocks of rawAccess are to big for the given fatType.
	 * @throws InitializationException in case the device object couldn't be initialized.
	 */
	public FATDevice initialize(String deviceName, RawAccess rawAccess, byte fatType, byte actionType) 
		throws WrongFATType, WrongLength, InitializationException
	{
		if (actionType == FORMAT && fatType != FAT.FAT12 && fatType != FAT.FAT16 && fatType != FAT.FAT32)
			throw new WrongFATType(fatType);
		
		Iterator it = devices.iterator();
		DeviceInformation devInf = null;
		FATDevice device = null;
		while(it.hasNext())
		{
			devInf = (DeviceInformation)it.next();
			device = devInf.getDevice();
			if (device.getRealDeviceName().equals(deviceName))
			{
				devInf.addUser();
				return device;
			}
		}
		if (actionType == BOOT)
		{
			device = new FATDevice(deviceName, rawAccess,out, dummyFile);
			devices.add(new DeviceInformation(device));
			return device;
		}
		else if (actionType == FORMAT)
		{
			device = new FATDevice(deviceName, fatType, rawAccess, out, dummyFile);
			
			//add the new device to the bootFile
			String deviceType = null;
			long sizeInByts = rawAccess.getNumSectors() * 512;
			if (rawAccess instanceof RAFRawAccess)
				deviceType = "RAF";
			else if (rawAccess instanceof RAMRawAccess)
				deviceType = "RAM";
			else if (rawAccess instanceof NativeRawAccess)
				deviceType = "NATIVE";
			else
				throw new InitializationException("Unknown RawAccess type. Device is not written to bootFile.");
			
			if (deviceType != null && !deviceType.equals("RAM"))
			{
				RandomAccessFile bootFile;
			
				try { 
					bootFile = new RandomAccessFile(bootFileName, "rw");
					if (bootFile.length() > 0)
						bootFile.seek(bootFile.length()-1);
				
					bootFile.write(deviceName.getBytes("US-ASCII"));
					bootFile.write(0x09);	//  "\t"
					bootFile.write((new Long(sizeInByts)).toString().getBytes("US-ASCII"));
					bootFile.write(0x09);	//  "\t"
					bootFile.write(deviceType.getBytes("US-ASCII"));
					//write cr and lf
					bootFile.write(0x0D);	//cr
					bootFile.write(0x0A);	//lf
					
					bootFile.close();
				}
				catch (IOException e) {
					throw new InitializationException("Could not write to "+bootFileName);
				}
			}
			
			devices.add(new DeviceInformation(device));
			return device;
		}
				
		return null;
	}	//end initialize(String deviceName, RawAccess rawAccess, byte fatType, byte actionType)
	
		
	/**
	 * Return the device object given by deviceName.
	 * @param deviceName name of the device.
	 * @return the device object.
	 * @throws Exception in case the device could not be found.
	 */
	public FATDevice getDevice(String deviceName) throws Exception
	{
		Iterator it = devices.iterator();
		DeviceInformation devInf = null;
		while (it.hasNext())
		{
			devInf = (DeviceInformation)it.next();
			if (devInf.getDevice().getRealDeviceName().equals(deviceName))
			{
				devInf.addUser();
				return devInf.device;
			}
		}
		throw new Exception();
	}	//end getDevice(String deviceName)


	/**
	 * Remove and return the FATDevice with the given name from the
	 * devices list. If no such device exists or the device can not
	 * be removed (because an other uses uses the device) null is
	 * returned.
	 * @param deviceName name of the device.
	 * @return the FATDevice with the given name from the devices list.
	 */
	private FATDevice removeDevice(String deviceName)
	{
		Iterator it = devices.iterator();
		FATDevice device = null;
		DeviceInformation devInf = null;
		while(it.hasNext())
		{
			devInf = (DeviceInformation)it.next();
			device = devInf.getDevice();
			if (devInf.getDevice().getRealDeviceName().equals(deviceName))
			{
				devInf.removeUser();
				//check if the device can be removed
				if (devInf.getNumOfUsers() > 0)
					return null;	//device can not be removed because there is at last one active user
				
				it.remove();
				return device;
			}
		}
		return null;
	}	//end removeDevice(String deviceName)


	/**
	 * Call this method when you finished your work with the file system. If
	 * you don't call it, the next time you boot the file system checkdisk
	 * will start and you may lose some important information.
	 */	
	public void shutDown()
	{
		FATDevice device;
		for (int i=0; i < devices.size(); i++)
		{
			device = ((DeviceInformation)devices.get(i)).getDevice();
			if (device != null)
				device.unmount();
		}
		devices.clear();
	}	//end shutDown()
	
	
	/**
	 * Unmount the device given by deviceName.
	 * @param deviceName name of the device.
	 */
	public void unmount(String deviceName)
	{
		FATDevice device = removeDevice(deviceName);
		if (device != null)
			device.unmount();
	}	//end unmount(String deviceName)
	
	
	/**
	 * Unmount the given device.
	 * @param device to unmount.
	 */
	public void unmount(FATDevice device)
	{
		unmount(device.getRealDeviceName());
	}

	
	/**
	 * List the available devices. 
	 * Each device has a root directory from which all other files in that file system can be reached.
	 * This method returns a list of DeviceInformation objects which contains information about
	 * each device.
	 * @return list of DeviceInformation objects with all mounted devices.
	 * @see xxl.core.io.fat.FileSystem.DeviceInformation
	 */
	public List getAllDevices()
	{
		return devices;
	}	//end getAllDevices()
}
