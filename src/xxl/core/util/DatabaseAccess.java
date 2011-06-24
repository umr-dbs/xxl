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

package xxl.core.util;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;

/**
 * This class contains all neccessary information to connect to
 * a database. This information (name of the driver, username, password 
 * and URL of the database) can be stored and retained from a
 * property file. This class does not implement any security issues.
 * The password is stored inside a String.
 * <p>
 * Example for a property file:
 * <br><code><pre>
 *	driver=sun.jdbc.odbc.JdbcOdbcDriver
 *	url=jdbc:odbc:testdb
 *	askForPassword=false
 * </pre></code>
 */
public class DatabaseAccess {
	/**
	 * Function which asks the user for a password via the command line.
	 * The function returns the password as a String.
	 */
	public static Function<String,String> ASK_FOR_PW_CONSOLE = new AbstractFunction<String,String>() {
		public String invoke(List<? extends String> list) {
			System.out.print("No password availlable for datasource. Please enter password for user "+list.get(1)+": ");
			BufferedReader bf = new BufferedReader(new InputStreamReader(System.in));
			try {
				return bf.readLine();
			}
			catch (Exception e) {
				throw new WrappingRuntimeException(e);
			}
		}
	};

	/**
	 * The name of the class of the used JDBC-driver.
	 */
	private String classname;
	
	/**
	 * The name of the user.
	 */
	private String username;
	
	/**
	 * The password for the logon to the database.
	 */
	private String password;
	
	/**
	 * The URL of the database.
	 */
	private String dburl;
	
	/**
	 * A boolean flag that determines whether the user is asked for a password
	 * on the command line if no password is specified in the constructor call.
	 */
	private boolean askForPassword;
	
	/**
	 * A function which asks the user to enter a password for the database. The
	 * function gets the url and the username as two parameters of type String.
	 */
	private Function<String,String> askForPasswordFunction;

	/**
	 * Initialize the connection data explizitly.
	 *
	 * @param classname name of the class of the JDBC-driver
	 * @param username name of the user
	 * @param password password for the logon to the database
	 * @param dburl URL of the database
	 * @param askForPassword If no password is specified: should be asked for a 
	 *	password on the command line?
	 * @param askForPasswordFunction Function which asks the user to enter a password for
	 * 	the database. The function gets the url and the username as two parameters of type
	 * 	String.
	 */
	public DatabaseAccess(String classname, String username, String password, String dburl, boolean askForPassword, Function<String,String> askForPasswordFunction) {
		this.classname      = classname;
		this.username       = username;
		this.password       = password;
		this.dburl          = dburl;
		this.askForPassword = askForPassword;
		this.askForPasswordFunction = askForPasswordFunction;
	}

	/**
	 * Initialize the connection data explizitly.
	 *
	 * @param classname name of the class of the JDBC-driver
	 * @param username name of the user
	 * @param password password for the logon to the database
	 * @param dburl URL of the database
	 * @param askForPassword If no password is specified: should be asked for a 
	 *	password on the command line?
	 */
	public DatabaseAccess(String classname, String username, String password, String dburl, boolean askForPassword) {
		this (classname, username, password, dburl, askForPassword, ASK_FOR_PW_CONSOLE);
	}

	/**
	 * Creates a DatabaseAccess object and initializes it with the data
	 * from a property file. The names of the properties have to be
	 * "driver", "user", "password", "url" and "askForPassword".
	 *
	 * @param fileName name of the file with the property information.
	 * @param askForPasswordFunction Function which asks the user to enter a password for
	 * 	the database. The function gets the url and the username as two parameters of type
	 * 	String.
	 * @return DatabaseAccess object created
	 */
	public static DatabaseAccess loadFromPropertyFile(String fileName, Function<String,String> askForPasswordFunction) {
		Properties dbprop = new Properties();
		// The password may not be in the set of properties ==> returns null
		try {
			dbprop.load(new FileInputStream(fileName));
			
			String afp = dbprop.getProperty("askForPassword");
			return new DatabaseAccess(
				dbprop.getProperty("driver"),
				dbprop.getProperty("user"),
				dbprop.getProperty("password"),
				dbprop.getProperty("url"),
				afp==null? true: (Boolean.valueOf(dbprop.getProperty("askForPassword")).booleanValue()),
				askForPasswordFunction
			);
		}
		catch (IOException e) {
			throw new WrappingRuntimeException(e);
		}
	}

	/**
	 * Creates a DatabaseAccess object and initializes it with the data
	 * from a property file. The names of the properties have to be
	 * "driver", "user", "password", "url" and "askForPassword".
	 *
	 * @param fileName name of the file with the property information.
	 * @return DatabaseAccess object created
	 */
	public static DatabaseAccess loadFromPropertyFile(String fileName) {
		return loadFromPropertyFile(fileName, ASK_FOR_PW_CONSOLE);
	}

	/**
	 * Stores the information about the connection in a property file.
	 *
	 * @param fileName Name of the property file
	 * @param header if this argument is not null, then an ASCII #  character, the header string, 
	 *	and a line separator are first written to the output stream. Thus, this header can serve as 
	 *	an identifying comment.
	 */
	public void storePropertyFile(String fileName, String header) {
		Properties dbprop = new Properties();
		
		dbprop.setProperty("driver",classname);
		dbprop.setProperty("user",username);
		// The password may not be in the set of properties ==> returns null
		if (password!=null && !askForPassword)
			dbprop.setProperty("password",password);
		dbprop.setProperty("url",dburl);
		dbprop.setProperty("askForPassword",new Boolean(askForPassword).toString());
		
		try {
			dbprop.store(new FileOutputStream(fileName),header);
		}
		catch (IOException e) {
			throw new WrappingRuntimeException(e);
		}
	}

	/**
	 * Returns the Driver.
	 * @return a String with the driver.
	 */
	public String getDriver() { return classname; }

	/**
	 * Returns the User.
	 * @return a String with the username.
	 */
	public String getUser() { return username; }

	/**
	 * Returns the Password. If the password is null so far, the user is asked
	 * to enter a password on the command line.
	 * @return a String with the password.
	 */
	public String getPassword() { 
		if ((password==null) && askForPassword && askForPasswordFunction!=null) {
			List<String> list = new ArrayList<String>();
			list.add(dburl);
			list.add(username);
			return askForPasswordFunction.invoke(list);
		}
		else
			return password;
	}

	/**
	 * Returns the URL.
	 * @return returns the URL
	 */
	public String getURLString() { return dburl; }

	/**
	 * Returns a connection to the specified database. This Method might throw a {@link xxl.core.util.WrappingRuntimeException}.
	 * @return the connection to the database.
	 */
	public Connection getConnection() {
		// Initialize and load the specified driver
		try {
			Class.forName (getDriver());
			return DriverManager.getConnection(getURLString(),getUser(),getPassword());
		}
		catch (ClassNotFoundException e) {
			throw new WrappingRuntimeException(e);
		}
		catch (SQLException e) {
			throw new WrappingRuntimeException(e);
		}
	}

	/** 
	 * Returns the path where the database property-files are stored.
	 *
	 * @return String - the database property path
	 */
	public static String getPropsDataPath() {
		return XXLSystem.getRootPath() + System.getProperty("file.separator") + 
			"data" + System.getProperty("file.separator") +
			"databases" + System.getProperty("file.separator");
	}

	/**
	 * Converts the database access data into a String
	 * @return String containing the connection data.
	 */
	public String toString() {
		return "DatabaseAccess:\nClassname: "+classname+"\nUser: "+username+"\nPassword: "+password+"\nURL: "+dburl;
	}
}
