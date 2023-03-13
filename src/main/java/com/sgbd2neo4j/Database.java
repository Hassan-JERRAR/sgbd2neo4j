package com.sgbd2neo4j;

// Java Program to Establish Connection in JDBC

// Importing database
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class Database {

    // Connection class object
	Connection con = null;

    public Connection connDB(String url){

		

		// Try block to check for exceptions
		try {
            System.out.println("Establishing Connection");

			// Registering drivers
			Class.forName("com.mysql.jdbc.Driver");

			// Reference to connection interface
			this.con = DriverManager.getConnection(url);
            System.out.println("Connection Established");
        }
		// Catch block to handle exceptions
		catch (Exception ex) {
			// Display message when exceptions occurs
			System.err.println(ex);
            System.out.println("Connection Failed");
		}
        return this.con;
	}

    public void closeDB(Connection DB) throws SQLException{
        DB.close();
    }

    public ResultSet getTuple(Connection DB, String table ) throws SQLException{
        String query = "SELECT * FROM " + table;
        Statement stmt = DB.createStatement();
        ResultSet rs = stmt.executeQuery(query);
        return rs;
    }

    public ResultSet getMetaTable(Connection DB) throws SQLException{
        String query = "SELECT * FROM information_schema.tables";
        Statement stmt = DB.createStatement();
        ResultSet rs = stmt.executeQuery(query);
        return rs;
    }

    public ResultSet getTable(Connection DB) throws SQLException{
        String query = "show full tables where Table_type = 'BASE TABLE';";
        Statement stmt = DB.createStatement();
        ResultSet rs = stmt.executeQuery(query);
        return rs;
    }

    public void toString(ResultSet query) throws SQLException {
        ResultSetMetaData rsmd = query.getMetaData();
        int columnsNumber = rsmd.getColumnCount();
        for(int i = 1; i <= columnsNumber; i++){
            String columnName = rsmd.getColumnName(i);
            System.out.print(columnName + " ");
        }
        System.out.println();
        while(query.next()){
            for(int i = 1; i <= columnsNumber; i++){
                String columnValue = query.getString(i);
                System.out.print(columnValue + " ");
            }
            System.out.println();
        }
    }

    public ResultSet getPrimaryKey(Connection db, String table) throws SQLException{
        String query = "SELECT TABLE_NAME,COLUMN_NAME FROM information_schema.key_column_usage WHERE table_name = '" + table + "' AND constraint_name = 'PRIMARY'";
        Statement stmt = db.createStatement();
        ResultSet rs = stmt.executeQuery(query);
        return rs;
    }

    public ResultSet getForeignKey(Connection db, String table) throws SQLException{
        String query = "SELECT TABLE_NAME,COLUMN_NAME FROM information_schema.key_column_usage WHERE table_name = '" + table + "' AND constraint_name LIKE 'FK_%'";
        Statement stmt = db.createStatement();
        ResultSet rs = stmt.executeQuery(query);
        return rs;

    }

    public ResultSet getConstraint(Connection db, String table) throws SQLException{
        String query = "SELECT * FROM information_schema.table_constraints WHERE table_name = '" + table + "'";
        Statement stmt = db.createStatement();
        ResultSet rs = stmt.executeQuery(query);
        return rs;
    }

    public HashMap<String,List<String>> getListAllPK (Connection db) throws SQLException{
        HashMap<String, List<String>> pk = new HashMap<String,List<String>>();
        ResultSet table = this.getTable(db);
        while(table.next()){
            String tableName = table.getString(1);
            ResultSet primarykey = this.getPrimaryKey(db, tableName);
            while(primarykey.next()){
                if(pk.containsKey(tableName)){
                    List<String> list = pk.get(tableName);
                    list.add(primarykey.getString(2));
                    pk.put(tableName, list);
                    System.out.println(pk.get(tableName));
                }
                else{
                    List<String> list = new ArrayList<String>();
                    list.add(primarykey.getString(2));
                    pk.put(tableName, list);
                    System.out.println(pk.get(tableName));
                }
            }
        }

        return pk;
    }

    public HashMap<String,List<String>> getListAllFK (Connection db) throws SQLException{
        HashMap<String, List<String>> fk = new HashMap<String,List<String>>();
        ResultSet table = this.getTable(db);
        while(table.next()){
            String tableName = table.getString(1);
            ResultSet foreignkey = this.getForeignKey(db, tableName);
            while(foreignkey.next()){
                if(fk.containsKey(tableName)){
                    List<String> list = fk.get(tableName);
                    list.add(foreignkey.getString(2));
                    fk.put(tableName, list);
                    System.out.println(fk.get(tableName));
                }
                else{
                    List<String> list = new ArrayList<String>();
                    list.add(foreignkey.getString(2));
                    fk.put(tableName, list);
                    System.out.println(fk.get(tableName));
                }
            }
        }

        return fk;
    }


}
