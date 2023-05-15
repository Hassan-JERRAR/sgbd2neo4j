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
import java.util.ArrayList;
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
                      java.math.BigDecimal bigDecimal = new java.math.BigDecimal("1234");
                      if(rs.getObject(i).getClass() == bigDecimal.getClass()) {
                        valeursInt[indexInt] = ((java.math.BigDecimal) rs.getObject(i)).intValue();
                      } else if (rs.getObject(i).getClass() == Integer.class){
                        valeursInt[indexInt] = (Integer) rs.getObject(i) ;
                      } else if (rs.getObject(i).getClass() == Float.class){
                        valeursInt[indexInt] = ((Float) rs.getObject(i)).intValue() ;
                      } else if (rs.getObject(i).getClass() == Double.class){
                        valeursInt[indexInt] = ((Double) rs.getObject(i)).intValue() ;
                      } else if (rs.getObject(i).getClass() == Long.class){
                        valeursInt[indexInt] = ((Long) rs.getObject(i)).intValue() ;
                      } else {
                        System.out.println("Type non pris en compte : " + rs.getObject(i).getClass());
                      }

                      indexInt++;
                    }
                  } else {
                    System.out.println("rs.getObject(1) != null");
                  }
               }
                
             }
            
            // On réarrange listeColonne pour avoir en premier les string et ensuite les int
            ArrayList<String> listeColonnesOrdonnees = new ArrayList<String>();
            for(int i = 0 ; i < nbString.length ; i++) {
              listeColonnesOrdonnees.add(listeColonnes.get(nbString[i] - 1));
            }
            for(int i = 0 ; i < nbInt.length ; i++) {
              listeColonnesOrdonnees.add(listeColonnes.get(nbInt[i] - 1));
            }
            
            String query = Cypher.createNode(nomTable, listeColonnesOrdonnees, valeursString, valeursInt);
            System.out.println(query);
            this.execute(this.driver, query);
        }
        
        System.out.println("Fin Neo4j.createNode");
        System.out.println("");
        System.out.println("---------------------------------------------");
        System.out.println("---------------------------------------------");
        System.out.println("");
    }
    
    
    public void createLinks(ResultSet rs, HashMap<String, List<String>> hm, String nomAssociation, HashMap<String, List<String>> hm2) throws Exception{
      
      // Déclarations des ArrayList contenant les attributs du noeud1, du noeud2 et de la relation
      ArrayList<String> attributs1 = new ArrayList<String>();
      ArrayList<String> attributs2 = new ArrayList<String>();
      ArrayList<String> attributs3 = new ArrayList<String>();
      
      
      String label1 = null;
      String label2 = null;
      
      // On récupère les tables référencées et les attributs qui les pointent 
      // à partir du hashmap
      for (String key : hm.keySet()) {
        if(label1 == null ) {
          label1 = key;
          attributs1 = (ArrayList<String>) hm.get(key);
        } else {
          label2 = key;
          attributs2 = (ArrayList<String>) hm.get(key);
        }
      }
      
      int nbPK = attributs1.size() + attributs2.size();
      //
      ArrayList<String> valeurs[] = new ArrayList[nbPK];
      for(int i = 0 ; i < nbPK ; i++) 
          valeurs[i] = new ArrayList<String>();
      
      int index = 0;
      for (String key : hm2.keySet()) {
        for(String attr : hm2.get(key)) {
          //valeurs[index] = 
          //index++;
        }
      }
      
      // On récupère et migre les données de la table d'associations 1..N 1..N
      while(rs.next()){    
        
        // Déclarations des ArrayList contenant les valeurs des attributs
        ArrayList<String> valeurs1 = new ArrayList<String>();
        ArrayList<String> valeurs2 = new ArrayList<String>();
        ArrayList<String> valeurs3 = new ArrayList<String>();
        
        
        if (rs.getObject(1) != null){
          
            //On récupère toutes les valeurs de chaque colonne sous la forme d'un String
            for(int i = 1 ; i <= nbPK ; i++) {                 
                    
             }
        }
        
        // On écrit la requête créant la relation
        String query = Cypher.createLink(label1,   attributs1, valeurs1, 
                                         label2,   attributs2, valeurs2,
                                         nomAssociation, attributs3, valeurs3);
        System.out.println(query);
        this.execute(this.driver, query);
      }
    }
    
    
    
    
    /*
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
   */
    
    
    
    
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

    public void executeCreateSingleLink(String label1, String label2, String fk1,  String fk2, String link){
      String query = Cypher.createSingleLink(label1, label2, fk1, fk2, link);
      this.execute(this.driver, query);
    }

    public void executedDeleteLabel(String label){
      String query = Cypher.deleteLabel(label);
      this.execute(this.driver, query);
    }
}
