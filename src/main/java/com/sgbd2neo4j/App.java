package com.sgbd2neo4j;

import java.sql.*;
import java.util.HashMap;
import java.util.List;

import org.neo4j.driver.Config;



public class App 
{
    public static void main(String[] args) throws Exception{
        String url = "jdbc:mysql://localhost:3306/northwind?user=root&password=Hj11062000!!";

        Database db = new Database();
        Connection DB = db.connDB(url);



        
        
        
        ResultSet table = db.getTable(DB);
        List <String> list = db.toList(table);

   

            
        



        // Aura queries use an encrypted connection using the "neo4j+s" protocol
        String uri = "neo4j+s://fc6a4107.databases.neo4j.io";

        String user = "neo4j";
        String password = "bWPf0QuEBJbK8wSOzUg6rq3k00rsHn1S__3HKPVwx9w";

       try (Neo4j app = new Neo4j(uri, user, password, Config.defaultConfig())) {
            Cypher cypher = new Cypher();
            
            for (String table_name : list) {
                ResultSet column = db.getColumnName(DB, table_name);
                List <String> list2 = db.toList(column);
                ResultSet rs = db.getTuple(DB,table_name);
                //System.out.println(list2);
                //System.out.println(table_name);
                if (rs != null){
                    app.createNode(rs, list2, table_name);
                }
               
            }
            
            
            
            

            
        }
       



        db.closeDB(DB);
    }
}

