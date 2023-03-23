package com.sgbd2neo4j;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Query;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.exceptions.Neo4jException;

import java.sql.ResultSet;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Neo4j implements AutoCloseable {

    private static final Logger LOGGER = Logger.getLogger(Neo4j.class.getName());
    final Driver driver;
    
    public Neo4j (String uri, String user, String password, Config config){
        driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password), config);
    }

    @Override
    public void close() throws Exception {
        driver.close();
    }  
    
    public List<Record> execute(Driver driver, String query) {
		List<Record> resultats;
		try (Session session = driver.session()) {
			List<Record> record = session.writeTransaction(tx -> {
				Result result = tx.run(query);
				return result.list();
			});
			resultats = record;
		}
		return resultats;
	}

    public void createNode(ResultSet rs,List<String> list2,String table) throws Exception{
        Cypher cypher = new Cypher();
        while(rs.next()){
            String[] test = new String[list2.size()];
            if (rs.getString(1) != null){
                for(int i = 0; i < list2.size(); i++){
                        test[i] = rs.getString(i+1);
                }}
            String query = cypher.createNode(table, list2, test);
            this.execute(this.driver, query);
     }
    }
}
