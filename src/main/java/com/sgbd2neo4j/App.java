package com.sgbd2neo4j;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.driver.Config;

public class App 
{  
    //Aura queries use an encrypted connection using the "neo4j+s" protocol
    // final private static String url = "jdbc:mysql://localhost:3306/northwind?user=root&password=Hj11062000!!";
    // final private static String uri = "neo4j+s://43837302.databases.neo4j.io:7687";
    // final private static String user = "neo4j";
    // final private static String password = "zU5cPEHkdhQgHzlLx85OfqlbYBMbNyxv1XIDOfxmzxA";
    
    final private static String url = "jdbc:mysql://localhost:3306/northwind?user=root&password=";
    final private static String uri = "neo4j+s://02187017.databases.neo4j.io";
    final private static String user = "neo4j";
    final private static String password = "OhnI7joX34j4EPzZmMHljCE6js4UG82yHJy4j9y9K2E";
  
    static private Database db;
    static private Connection DB;
    static private ResultSet table;
    static private List <String> list;

    public static void main(String[] args) throws Exception{
      
        // Connexion au SGBD relationnel
        db = new Database();
        DB = db.connDB(url);
        
        // Récupération des tables et affichage
        table = db.getTable(DB);
        list = db.toList(table);
        afficherNomsTables();

       try (Neo4j app = new Neo4j(uri, user, password, Config.defaultConfig())) {
             
             // Pour rendre le texte plus lisible dans la console
             afficherRecuperationDonnees();
             
             // On traduit toutes les tables en noeuds puis on les migre vers Neo4J
             migrationTables(app);


             
             // On détermine les FK pour afin de définir les relations entre noeuds
             // definirRelation(app);
        }

        ResultSet test = db.getForeignKey(DB, "products");
        List <String> listColonnes = db.toList(test);

        System.out.println(listColonnes);
       
        db.closeDB(DB);
    }
    


    private static void afficherNomsTables() {
      System.out.println("");
      System.out.println("Liste des tables : ");
      System.out.println(list);
      System.out.println("");
    }
    
    
    
    private static void afficherRecuperationDonnees() {
      System.out.println("---------------------------------------------");
      System.out.println("---------------------------------------------");
      System.out.println("---------------------------------------------");
      System.out.println("");
      System.out.println("");
      System.out.println("Recuperation des donnees de chaque tables : ");
      System.out.println("");
      System.out.println("---------------------------------------------");
      System.out.println("---------------------------------------------");
      System.out.println("");
    }
    
    
    /**
     * Importation des tables avec leurs données, traduction en noeuds et migration vers Neo4J.
     * @param app
     * @throws Exception
     */
    private static void migrationTables(Neo4j app) throws Exception {
      
          for (String nomTable : list) {
            
                System.out.println("Nom de la table : " + nomTable);
               
                ResultSet rs = db.getTuple(DB, nomTable);
                List <String> listColonnes = db.listerNomsColonnes(rs);
                
                System.out.println("Colonnes de la table : ");
                System.out.println(listColonnes);
                System.out.println("");
        
                if (rs != null){
                    app.createNode(rs, listColonnes, nomTable);
                } else {
                  System.out.println("ResultSet == NULL");
                }
          }     
    }
    
    
    private static void definirRelation(Neo4j app) throws Exception {
        
        // On récupère toutes les PK et toutes les FK de la BDD dans un Hashmap
        HashMap<String,List<String>> listePK = db.getAllPK(DB, list);
        HashMap<String,List<String>> listeFK = db.getAllFK(DB, list);
        // listeFK = modifier(listeFK);
        
        afficherKey(listePK, "PK");
        afficherKey(listeFK, "FK");
        
        
        // On récupère uniquement le nom des tables d'associations (cardinalité 1..N et 1..N dans le schéma E/A)
        ArrayList<String> nomsTablesAssociation = tablesAssociations(listeFK, listePK);
        afficherTableAssociation(nomsTablesAssociation);
        
        System.exit(0);
        
        // On crée les relations entre les noeuds pour chacune des tables d'associations
        for(String nomAssociation : nomsTablesAssociation) {
            creerAssociation(nomAssociation, listeFK, app);
        }
        
    }
    
    /**
     * 
     * @param nomAssociation Le nom de la table d'association à traduire en relation entre deux noeuds
     * @param listeFK Le HashMap de toutes les FK de la table
     * @param app 
     * @throws Exception 
     */
    private static void creerAssociation(String nomAssociation, HashMap<String, List<String>> listeFK, Neo4j app) throws Exception {
        
        // On parcourt le HashMap des FK de la BDD
        for (Map.Entry key : listeFK.entrySet()) {
            if(nomAssociation.equals(key.getKey())) {
              
              // On récupère uniqement la liste des FK de la table d'association en argument
              ArrayList<String> listeFkTable = (ArrayList<String>) key.getValue();
              
              // Pour chaque FK de la table d'association, on récupère la table qu'elle référence
              // ArrayList<String> tablesPointeesParFk = db.getTablesRefParFk(DB, nomAssociation, listeFkTable);
              ArrayList<String> tablesPointeesParFk = new ArrayList<String>();
              for(int i = 0 ; i < listeFkTable.size() ; i++) {
                if(listeFkTable.get(i).equals("nomACT") || listeFkTable.get(i).equals("prenomACT") || listeFkTable.get(i).equals("nomART") || listeFkTable.get(i).equals("prenomART")) {
                  tablesPointeesParFk.add("artiste");
                } else if(listeFkTable.get(i).equals("titreFILM")) {
                  tablesPointeesParFk.add("film");
                } else if(listeFkTable.get(i).equals("pseudoINTER")) {
                  tablesPointeesParFk.add("internaute");
                }
              }
              
              // On affiche la liste des tables référencées
              afficherTableRefParFK(nomAssociation, tablesPointeesParFk);
              
              // On fait un hashmap qui récupère toutes les PK de chaque table
              HashMap<String,List<String>> listePK = new HashMap<String,List<String>>();
              listePK = db.getAllPK(DB, App.list);
              
              // Faire hashmap entre tablesPointeesParFk et listePK
              // Cle : Nom de la table (qui est referencées par l'association)
              // Valeur : Liste des PK de cette table (qui sont possiblement pointées par l'association)
              HashMap<String, List<String>> hm = new HashMap<String,List<String>>();
              for(String nomTable : tablesPointeesParFk) {
                    List<String> list = new ArrayList<String>();
                    list = listePK.get(nomTable);
                    hm.put(nomTable, list);
              }
              
              // Faire hashmap entre tablesPointeesParFk et listeFkTable
              // Cle : Nom des tables referencées par l'association
              // Valeur : Liste des attributs de l'association qui référence la table (=clé de hm2)
              HashMap<String, List<String>> hm2 = new HashMap<String,List<String>>();
              for(String nomTable : tablesPointeesParFk) {
                    List<String> list = new ArrayList<String>();
                    for(int i = 0 ; i < listeFkTable.size() ; i ++) {
                      if(tablesPointeesParFk.get(i).equals(nomTable)) {
                        list.add(listeFkTable.get(i));
                      }
                    }
                    hm2.put(nomTable, list);
              }
              
              // Afficher le HashMap
              afficherReferences(hm, nomAssociation);
              
              // Creer les relations
              ResultSet rs = db.getTuple(DB, nomAssociation);
              app.createLinks(rs, hm, nomAssociation, hm2);
            }
        }
    }

    
    
    /**
     * Affiche les tables référencées par les FK d'une autre table
     *
     * @param nomAssociation Nom de la table dont une partie des attributs sont de FK
     * @param nomsTablesAssociation Nom des tables référencées par 'nomAssociation'
     */
    private static void afficherTableRefParFK(String nomAssociation, ArrayList<String> nomsTablesAssociation) {
      
      String format = "%-50s%s%n";
      String[] arr = new String[nomsTablesAssociation.size()];
      nomsTablesAssociation.toArray(arr);
      String liste = "Liste des tables referencees par " + nomAssociation;
      System.out.format(format, liste, Arrays.toString(arr));
    }


    /**
     * Affiche le nom des tables d'associations 1..N 1..N
     *
     * @param nomsTablesAssociation
     */
    private static void afficherTableAssociation(ArrayList<String> nomsTablesAssociation) {
      String[] arr = new String[nomsTablesAssociation.size()];
      nomsTablesAssociation.toArray(arr);
      System.out.println("");
      System.out.println("------------------------------------------------------------------------------------------");
      System.out.println("------------------------------------------------------------------------------------------");
      System.out.println("");
      System.out.println("Liste des tables d'associations 1..N 1..N : " + Arrays.toString(arr));
      System.out.println("");
      System.out.println("------------------------------------------------------------------------------------------");
      System.out.println("------------------------------------------------------------------------------------------");
      System.out.println("");
    }



    /**
     * A partir de la hashmap de FK et des PK on récupère les tables d'associations 1..N 1..N.
     * 
     * Si une table a deux clés étrangères de tables différentes, ou plus, comme clé primaire, alors 
     * on la considère comme une table d'associations.
     * 
     * @param listeFK Hashmap des FK de la BDD
     * @param listePK Hashmap des PK de la BDD
     * @return nom des table d'associations 1..N 1..N
     */
    private static ArrayList<String> tablesAssociations(HashMap<String, List<String>> listeFK, HashMap<String, List<String>> listePK) {
      
      ArrayList<String> nomTableAssociations = new ArrayList<String>();
      
      // Il faut compter le nombre de PK aussi FK pour chaque table
      Integer[] nbPkAussiFk = new Integer[listePK.size()];
      for(int i = 0 ; i < nbPkAussiFk.length ; i++) {
        nbPkAussiFk[i] = 0;
      }
      
      int indexTable = 0;
      for (Map.Entry key : listePK.entrySet()) {
        
        ArrayList<String> listePkTable = (ArrayList<String>) key.getValue();
        for(int v = 0 ; v < listePkTable.size() ; v++) {
          String clePrimaire = listePkTable.get(v);
          if(listeFK.get(key.getKey()).contains(clePrimaire)) {
            nbPkAussiFk[indexTable]++;
          }
        }
        indexTable++;
      }
      
      indexTable = 0;
      for (Map.Entry key : listePK.entrySet()) {
        if(nbPkAussiFk[indexTable] >= 2) {
          nomTableAssociations.add((String) key.getKey());
        }
        indexTable++;
      }
      
      return nomTableAssociations;
    }



    /**
     * Affichage des hashmap obtenus listant les contraintes
     * @param liste hashmap à parcourir
     * @param cle Nom de la contrainte
     */
    private static void afficherKey(HashMap<String,List<String>> liste, String cle) {
      
      String format = "%-50s%s%n";
      
      System.out.println("Liste des " + cle + " : ");
      for (Map.Entry key : liste.entrySet()) {
        String typeContrainteColonne = "Colonnes " + cle + " de " + key.getKey() + " : ";
        String listeContrainte = "" + key.getValue();
        System.out.format(format, typeContrainteColonne, listeContrainte);
      }
      System.out.println("");
      
    }
    


    /**
     * Affichage des hashmap obtenus listant les attributs referencants une table
     * @param liste hashmap à parcourir contenant
     * @param nomTable Nom de la table contenant les FK
     */
    private static void afficherReferences(HashMap<String,List<String>> liste, String nomTable) {
      
      String format = "%-50s%s%n";
      
      for (Map.Entry key : liste.entrySet()) {
        String typeContrainteColonne = "Attributs de " + nomTable + " referencant " + key.getKey() + " : ";
        String listeContrainte = "" + key.getValue();
        System.out.format(format, typeContrainteColonne, listeContrainte);
      }
      System.out.println("");
      
    }
    
    
    
    /**
     * Classe à supprimer, créer pour simuler hashmap de FK
     * @param listePK
     * @return
     */
    private static HashMap<String,List<String>> modifier(HashMap<String,List<String>> listePK) {
      
      HashMap<String,List<String>> listeFK = listePK;
      
      for (String nomTable : list) {
        
        List<String> liste = listeFK.get(nomTable);
        liste = new ArrayList<String>();
        
        if(nomTable.equals("role")) {
          liste.add("nomACT");
          liste.add("prenomACT");
          liste.add("titreFILM");
        }
        
        if(nomTable.equals("notea")) {
          liste.add("pseudoINTER");
          liste.add("prenomART");
          liste.add("nomART");
        }
        
        if(nomTable.equals("notef")) {
          liste.add("titreFILM");
          liste.add("pseudoINTER");
        }
        
        listePK.put(nomTable, liste);
    } 
      
      return listeFK;
      
    }
}

