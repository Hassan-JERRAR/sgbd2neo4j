package com.sgbd2neo4j;

import java.sql.*;

public class App 
{
    public static void main(String[] args) throws SQLException{
        String url = "jdbc:mysql://localhost:3306/northwind?user=root&password=Hj11062000!!";

        Database db = new Database();
        Connection DB = db.connDB(url);
        ResultSet rs = db.getTuple(DB, "customers");

        ResultSet table = db.getTable(DB);
        
        

        ResultSet primarykey = db.getPrimaryKey(DB,"customers");
        ResultSet foreignkey = db.getForeignKey(DB, "employees");
        db.toString(foreignkey);






        db.closeDB(DB);
    }
}
