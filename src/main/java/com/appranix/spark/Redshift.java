package com.appranix.spark;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class Redshift {
	
    static final String dbURL = "jdbc:redshift://spark-demo.ctwfwogrksfl.us-east-1.redshift.amazonaws.com:5439/demo"; 	
    static final String MasterUsername = "master";
    static final String MasterUserPassword = "Master123";
    
	Connection conn = null;

    
    public Redshift(String url, String username, String password)  {
    	try {
			Class.forName("com.amazon.redshift.jdbc42.Driver");
			System.out.println("Connecting to database...");
			 
			Properties props = new Properties();
			 
			props.setProperty("user", username);
			props.setProperty("password", password);
			conn = DriverManager.getConnection(url, props);  
    	}
    	catch(Exception ex) {
    		ex.printStackTrace();
    	}
    }	
	
	
	
	public void createWordCountTable() throws SQLException {
		Statement stmt = null;
        
		try {
        	stmt = conn.createStatement();
        	String createSql = "CREATE TABLE IF NOT EXISTS word_count(word_id INT NOT NULL IDENTITY(1,1) PRIMARY KEY, " +
        			  " word VARCHAR(255) NOT NULL, " +
        			  " count INT DEFAULT 0 )";
        	
        	stmt.executeUpdate(createSql);
        	stmt.close();
        } 
        catch (SQLException e) {
        	stmt.close();
			throw new SQLException(e);
		}

	}
	
	public void insertWordCount(String word, Integer count) throws SQLException {
		Statement stmt = null;
		
		try {
			stmt = conn.createStatement();
			String insertSql =  "INSERT INTO word_count (word, count) VALUES ('" + word + "'," + count + " )";
        	stmt.executeUpdate(insertSql);
        	stmt.close();			
        } 
        catch (SQLException e) {
        	stmt.close();
        	throw new SQLException(e);
		}			
	}
	
	public void truncateWordCountTable() throws SQLException {
		Statement stmt = null;
		
		try {
			stmt = conn.createStatement();
			String insertSql =  "DELETE FROM word_count";
        	stmt.executeUpdate(insertSql);
        	stmt.close();			
        } 
        catch (SQLException e) {
        	stmt.close();
        	throw new SQLException(e);
		}	
	}
	
	public void printWordCount() throws SQLException {
		Statement stmt = null;
		
		try {
			stmt = conn.createStatement();
			String sql =  "SELECT * FROM word_count";
			ResultSet rs = stmt.executeQuery(sql);

		    while(rs.next()){
		         //Retrieve by column name
		         int id  = rs.getInt("word_id");
		         String word = rs.getString("word");
		         int count = rs.getInt("count");

		         //Display values
		         System.out.println(id + " | " + word + " = " +  count);
		    }			
			
			stmt.close();			
        } 
        catch (SQLException e) {
        	stmt.close();
        	throw new SQLException(e);
		}			
	}
	
	public static void main(String args[])  {
		
		try {
			String url = "";
			String username = "";
			String password = "";
			
			if (args.length == 0) {
				url = dbURL;
				username = MasterUsername;
				password = MasterUserPassword;				
			}
			else {
				url = args[0];
				username = args[1];
				password = args[2];					
			}
			
			Redshift red = new Redshift(url, username, password);
			
			System.out.println("Printing values...");
			red.printWordCount();
			
			//System.out.println("Truncate table...");
			//red.truncateWordCountTable();		
		}
		catch(Exception ex) {
			System.out.println("Exception occured...");
			ex.printStackTrace();
		}			
	}
	
	public void test() {
		
		try {
			Redshift red = new Redshift(dbURL, MasterUsername, MasterUserPassword);
			
			System.out.println("Creatng table...");
			red.createWordCountTable();
			
			System.out.println("Inserting Values...");
			red.insertWordCount("Bharathi", 2);
			
			System.out.println("Printing values...");
			red.printWordCount();
			
			System.out.println("Truncate table...");
			red.truncateWordCountTable();
			
			System.out.println("Printing values...");
			red.printWordCount();
		}
		catch(Exception ex) {
			System.out.println("Exception occured...");
			ex.printStackTrace();
		}		
	}

}
