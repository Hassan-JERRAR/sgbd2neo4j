package com.sgbd2neo4j;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Logger;

public class Neo4j implements AutoCloseable {
  
    //java.sql.Types représente chaque type de données SQL par un nombre
    private static int[] VARCHAR = {1, 12, -1}; // Ensemble des types pouvant être représentés comme un String
    private static int[] INTEGER = {5, 4, -5, 6, 7, 8, 2, 3}; // Ensemble des types pouvant être représentés comme un INT

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

    public void createNode(ResultSet rs, List<String> listeColonnes, String nomTable) throws Exception{

        System.out.println("---------------------------------------------");
        System.out.println("");
        System.out.println("Debut Neo4j.createNode");
        System.out.println("");
        
        // Récupération du nombre de colonnes pour chaque type
        Integer[] nbString = nombreColonneType(VARCHAR, rs);
        Integer[] nbInt = nombreColonneType(INTEGER, rs);
        
        System.out.print("Nombre de colonnes VARCHAR : " + nbString.length);
        System.out.println(" avec index+1 : " + Arrays.toString(nbString));
        System.out.print("Nombre de colonnes INTEGER : " + nbInt.length);
        System.out.println(" avec index+1 : " + Arrays.toString(nbInt));
        System.out.println("");
        
        while(rs.next()){

            String[] valeursString = new String[nbString.length];
            Integer[] valeursInt = new Integer[nbInt.length];
            
            if (rs.getObject(1) != null){
              
              int indexString = 0;
              int indexInt = 0;
              
              //On récupère toutes la valeur de chaque colonne sous la forme d'un String ou d'un Int
              for(int i = 1 ; i <= listeColonnes.size() ; i++) {                  
                  if(rs.getObject(i) != null) {
                    if(contains(nbString, i)) {
                      valeursString[indexString] = (String) rs.getObject(i);
                      indexString++;
                    } else if(contains(nbInt, i)) {
                      valeursInt[indexInt] = (Integer) rs.getObject(i);
                      indexInt++;
                    }
                  } else {
                    System.out.println("rs.getObject(1) != null");
                  }
               }
                
             }
            
            String query = Cypher.createNode(nomTable, listeColonnes, valeursString, valeursInt);
            System.out.println(query);
            // this.execute(this.driver, query);
        }
        
        System.out.println("Fin Neo4j.createNode");
        System.out.println("");
        System.out.println("---------------------------------------------");
        System.out.println("---------------------------------------------");
        System.out.println("");
    }
    
    
    
    
    
    public void createLinks(ResultSet rs, HashMap<String, List<String>> hm, String nomAssociation) throws Exception{

      System.out.println("---------------------------------------------");
      System.out.println("");
      System.out.println("Debut Neo4j.createLink");
      System.out.println("");
      
      // Récupération du nombre de colonnes pour chaque type
      Integer[] nbString = nombreColonneType(VARCHAR, rs);
      Integer[] nbInt = nombreColonneType(INTEGER, rs);
      
      System.out.print("Nombre de colonnes VARCHAR : " + nbString.length);
      System.out.println(" avec index+1 : " + Arrays.toString(nbString));
      System.out.print("Nombre de colonnes INTEGER : " + nbInt.length);
      System.out.println(" avec index+1 : " + Arrays.toString(nbInt));
      System.out.println("");
      
      while(rs.next()){

          String[] valeursString = new String[nbString.length];
          Integer[] valeursInt = new Integer[nbInt.length];
          
          if (rs.getObject(1) != null){
            
            int indexString = 0;
            int indexInt = 0;
            
            //On récupère toutes la valeur de chaque colonne sous la forme d'un String ou d'un Int
            for(int i = 1 ; i <= listeColonnes.size() ; i++) {                  
                if(rs.getObject(i) != null) {
                  if(contains(nbString, i)) {
                    valeursString[indexString] = (String) rs.getObject(i);
                    indexString++;
                  } else if(contains(nbInt, i)) {
                    valeursInt[indexInt] = (Integer) rs.getObject(i);
                    indexInt++;
                  }
                } else {
                  System.out.println("rs.getObject(1) != null");
                }
             }
              
           }
          
          String query = Cypher.createNode(nomAssociation, listeColonnes, valeursString, valeursInt);
          this.execute(this.driver, query);
      }
      
      System.out.println("Fin Neo4j.createLink");
      System.out.println("");
      System.out.println("---------------------------------------------");
      System.out.println("---------------------------------------------");
      System.out.println("");
  }
    
    
    
    
    
    /**
     * Retourne le nombre de colonne du type renseigné
     * 
     * @param type Le type (int) dont on veut obtenir le nombre de colonne
     * @param rs Le resultSet contenant les données
     * @return ye nombre de colonne du type renseigné
     * @throws SQLException
     */
    public Integer[] nombreColonneType(int type[], ResultSet rs) throws SQLException {
      
        int nombreColonne = 0;
      
        ResultSetMetaData rsmd = rs.getMetaData();
        int nombreTotaleColonne = rsmd.getColumnCount();
        
        for(int index = 1 ; index <= nombreTotaleColonne ; index++) {
          int typeColonne = rsmd.getColumnType(index);
          for(int i = 0 ; i < type.length ; i++) {
            if(typeColonne == type[i]) {
              nombreColonne++;
            }
          }
        }
        
       Integer[] colonnes = new Integer[nombreColonne];
       int a = 0;
       for(int index = 1 ; index <= nombreTotaleColonne ; index++) {
         int typeColonne = rsmd.getColumnType(index);
         for(int i = 0 ; i < type.length ; i++) {
           if(typeColonne == type[i]) {
             colonnes[a] = index;
             a++;
           }
         }
       }
             
       return colonnes;
    }
    
    /**
     * Vérifier qu'un tableau d'entiers contient une certaine valeur
     * 
     * @param T Le tableau d'entier dont on veut étudier le contenu
     * @param val La valeur que l'on cherche dans le tableau
     * @return <true> si val est dans T
     */
    public boolean contains(Integer[] T, int val) {
      
      for(int i = 0 ; i < T.length ; i++) {
        if(T[i] == val) {
          return true;
        }
      }
      return false;
    }
}
