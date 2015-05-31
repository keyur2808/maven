package com.cloudwick.maven.MavenSql;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * Hello world!
 *
 */
public class App {
	// static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
	// static final String DB_URL = "jdbc:mysql://172.17.1.113:3306/cloudwick1";
	// static final String DB_URL = "jdbc:mysql://localhost:8990/cloudwick";

	// Database credentials
	// static final String USER = "root";
	// static final String PASS = "ankit";
	// static final String PASS = "root";

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, SQLException {

		Properties props = new Properties();
		InputStream in = new FileInputStream("target/classes/db.properties");
		props.load(in);
		in.close();

		String driver = props.getProperty("db.driver");
		if (driver != null) {
			System.out.println(driver);
			Class.forName(driver);
		}

		String url = props.getProperty("db.url");
		String username = props.getProperty("db.username");
		String password = props.getProperty("db.password");

		Connection conn = null;
		Statement stmt = null;

		System.out.println("Connecting to database...");
		conn = DriverManager.getConnection(url, username, password);

		// STEP 4: Execute a query
		System.out.println("Creating statement...");
		stmt = conn.createStatement();

		String sql = "SELECT id, ename FROM emp";
		ResultSet rs = stmt.executeQuery(sql);

		// STEP 5: Extract data from result set
		while (rs.next()) {
			// Retrieve by column name
			int id = rs.getInt("id");
			String ename = rs.getString("ename");

			// Display values
			System.out.print("Id: " + id);
			System.out.println(", Name: " + ename);
		}
		// STEP 6: Clean-up environment
		rs.close();
		stmt.close();
		conn.close();

		// Remove resources
		if (stmt != null)
			stmt.close();
		if (conn != null)
			conn.close();

		System.out.println("Goodbye!");
	}
}