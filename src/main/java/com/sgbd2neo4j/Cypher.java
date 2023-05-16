package com.sgbd2neo4j;

import java.util.ArrayList;
import java.util.List;

public class Cypher {
    
    // Requete cypher pour créer un noeud
    public static String createNode(String label, String nomColonne, String name){
        String query = "CREATE (:" + label + " { " + nomColonne + ": '" + name + "'})";
        return query;
    }

    // Requete cypher pour créer un noeud avec une propriété
    public static String createNode(String label, String name, String property, String value){
        String query = "CREATE (:" + label + " { name: '" + name + "', " + property + ": '" + value + "'})";
        return query;
    }

    // Requete cypher pour créer un noeud avec plusieurs propriétés
    public static String createNode(String label, List<String> nomColonne, String[] value){
        if(label.contains(" ")){
            label = label.replace(" ", "_");
        }
        String query = "CREATE (:" + label + " { ";
        for(int i = 0; i < nomColonne.size(); i++){
            if( value[i] != null){
                if (value[i].contains("\"")){
                    value[i] = value[i].replace("\"", "\\\"");
                }    
            }       
            query += nomColonne.get(i) + ": \"" + value[i] + "\"";
            if(i != nomColonne.size() - 1){
                query += ", ";
            }
        }
        query += " })";
        return query;
    }
    
    /**
     * Retourne la requete de creation d'un noeud dans Neo4J
     * @param label Label du projet
     * @param nomsColonnes Liste des propriétés du noeud (d'abord les valeeurs string et ensuite les int)
     * @param valeursString La liste des valeurs String
     * @param valeursInt La liste des valeurs Int
     * @return La requête complète de création d'un noeud
     */
    public static String createNode(String label, List<String> nomsColonnes, String[] valeursString, Integer[] valeursInt){
        if(label.contains(" ")){
            label = label.replace(" ", "_");
        }
        String query = "CREATE (:" + label + " { ";
        for(int i = 0; i < nomsColonnes.size() ; i++){
            
            // On ajoute toutes les valeurs String 
            if(valeursString.length > i) {
              if( valeursString[i] != null){
                if (valeursString[i].contains("\"")){
                  valeursString[i] = valeursString[i].replace("\"", "\\\"");
                }    
              }       
              query += nomsColonnes.get(i) + ": \"" + valeursString[i] + "\"";
              if(i != nomsColonnes.size() - 1){
                  query += ", ";
              }
            } else {
              // Ensuite on ajoute les valeurs Int 
              int j = i - valeursString.length;
              query += nomsColonnes.get(i) + ": " + valeursInt[j];
              if(i != nomsColonnes.size() - 1){
                  query += ", ";
              }
            }
            
        }
        query += " })";
        return query;
    }



    /**
     * Retourne la requête pour crée un lien entre deux noeuds dont une propriété est égale
     * @param label1 Label du noeud source de la relation
     * @param label2 Label du noeud destination de la relation
     * @param fk1 Nom de la propriété du noeud source (qui est FK dans le modèle relationnel)
     * @param fk2 Nom de la propriété du noeud destinataire (qui est référencé)
     * @param link Nom de la relation
     *
     * @return La requête complète de création d'une relation dont les noeuds possèdent une valeur de propriété égales
     */
     public static String createSingleLink(String label1, String label2, String fk1,  String fk2, String link){
        String query = "MATCH (a:" + label1 + "),(b:" + label2 + ") WHERE a."+fk1+" = b."+fk2+"  CREATE (a)-[:" + link + "]->(b)";
        System.out.println(query);
        return query;
    }

    // Requete cypher pour créer un lien entre deux noeuds
    public static String createLink(String label1, String name1, String label2, String name2, String link, String string, String string2){
        String query = "MATCH (a:" + label1 + "),(b:" + label2 + ") WHERE a.name = '" + name1 + "' AND b.name = '" + name2 + "' CREATE (a)-[:" + link + "]->(b)";
        return query;
    }
    
    // Requete cypher pour créer un lien entre deux noeuds avec plusieurs propriétés
    public static String createLink(String label1, String name1, String label2, String name2, String link, String[] property, String[] value){
        String query = "MATCH (a:" + label1 + "),(b:" + label2 + ") WHERE a.name = '" + name1 + "' AND b.name = '" + name2 + "' CREATE (a)-[:" + link + " { ";
        for(int i = 0; i < property.length; i++){
            query += property[i] + ": '" + value[i] + "'";
            if(i != property.length - 1){
                query += ", ";
            }
        }
        query += " }]->(b)";
        return query;
    }
    
    
    
    /**
     * Retourne la requête pour crée un lien entre deux noeuds.
     *
     * Les noeuds sont identifiés par les valeurs de leurs propriétés.
     * La relation peut posséder un nombre quelconque de propriétés.
     * 
     * Les valeurs sont toutes des String.
     * Il faut fournir les valeurs dans le même ordre que les attributs qui
     * y correspondent.
     * 
     * 
     * @param label1 Label du noeud source
     * @param attributs1 Liste des propriétés pour identifié le noeud source
     * @param valeurs1 Liste des valeurs pour identifié le noeud source
     * @param label2 Label du noeud destinataire
     * @param attributs2 Liste des propriétés pour identifié le noeud destinataire
     * @param valeurs2 Liste des valeurs pour identifier le noeud destinataire
     * @param relation Nom de la relation
     * @param attributs3 Liste des propriétés de la relation
     * @param valeurs3 Liste des valeurs à assigner aux propriétés de la relation.
     * @return
     */
    public static String createLink(String label1,   ArrayList<String> attributs1, ArrayList<String> valeurs1, 
                                    String label2,   ArrayList<String> attributs2, ArrayList<String> valeurs2,
                                    String relation, ArrayList<String> attributs3, ArrayList<String> valeurs3){
      
        String query = "MATCH (node1:"+ label1 + " {";
        
        // On sélectionne le premier noeud selon les valeurs des propriétés
        for(int i = 0 ; i < attributs1.size() ; i ++) {
          query += attributs1.get(i) + ": \"" + valeurs1.get(i) + "\"";
          if(i <  attributs1.size() - 1) {
            query += ", ";
          }
        }
        
        // On sélectionne le deuxième noeud selon les valeurs des propriétés
        query += "}), (node2:" + label2 + "{";
        for(int i = 0 ; i < attributs2.size() ; i ++) {
          query += attributs2.get(i) + ":\"" + valeurs2.get(i) + "\"";
          if(i < attributs2.size() - 1) {
            query += ", ";
          }
        }
        
        // On crée la relation en ajoutant des propriétés si besoin
        query += "}) CREATE (node1)-[:" + relation;
        if (attributs3.size() > 0) {
          query += "{";
          for(int i = 0 ; i < attributs3.size() ; i ++) {
            query += attributs3.get(i) + ":\"" + valeurs3.get(i) + "\"";
            if(i <  attributs3.size() - 1) {
              query += ", ";
            }
          }
          query += "}";
        }
        query += "]->(node2)";
        return query;
    }
    
    // Requete cypher pour supprimer un label
    public static String deleteLabel(String label){
        String query = "MATCH (n:" + label + ") DETACH DELETE n";
        return query;
    }

    // Requete cypher pour supprimer un noeud
    public static String deleteNode(String label, String name){
        String query = "MATCH (n:" + label + " {name: '" + name + "'}) DETACH DELETE n";
        return query;
    }

    // Requete cypher pour supprimer un lien
    public static String deleteLink(String label1, String name1, String label2, String name2, String link){
        String query = "MATCH (a:" + label1 + " {name: '" + name1 + "'})-[r:" + link + "]->(b:" + label2 + " {name: '" + name2 + "'}) DELETE r";
        return query;
    }

    // Requete cypher pour supprimer un lien avec une propriété
    public static String deleteLink(String label1, String name1, String label2, String name2, String link, String property, String value){
        String query = "MATCH (a:" + label1 + " {name: '" + name1 + "'})-[r:" + link + " { " + property + ": '" + value + "' }]->(b:" + label2 + " {name: '" + name2 + "'}) DELETE r";
        return query;
    }

    // Requete cypher pour supprimer un lien avec plusieurs propriétés
    public static String deleteLink(String label1, String name1, String label2, String name2, String link, String[] property, String[] value){
        String query = "MATCH (a:" + label1 + " {name: '" + name1 + "'})-[r:" + link + " { ";
        for(int i = 0; i < property.length; i++){
            query += property[i] + ": '" + value[i] + "'";
            if(i != property.length - 1){
                query += ", ";
            }
        }
        query += " }]->(b:" + label2 + " {name: '" + name2 + "'}) DELETE r";
        return query;
    }

    // Requete cypher pour modifier un noeud
    public static String updateNode(String label, String name, String property, String value){
        String query = "MATCH (n:" + label + " {name: '" + name + "'}) SET n." + property + " = '" + value + "'";
        return query;
    }

    // Requete cypher pour modifier un noeud avec plusieurs propriétés
    public static String updateNode(String label, String name, String[] property, String[] value){
        String query = "MATCH (n:" + label + " {name: '" + name + "'}) SET ";
        for(int i = 0; i < property.length; i++){
            query += "n." + property[i] + " = '" + value[i] + "'";
            if(i != property.length - 1){
                query += ", ";
            }
        }
        return query;
    }

    // Requete cypher pour modifier un lien
    public static String updateLink(String label1, String name1, String label2, String name2, String link, String property, String value){
        String query = "MATCH (a:" + label1 + " {name: '" + name1 + "'})-[r:" + link + "]->(b:" + label2 + " {name: '" + name2 + "'}) SET r." + property + " = '" + value + "'";
        return query;
    }

    // Requete cypher pour modifier un lien avec plusieurs propriétés
    public static String updateLink(String label1, String name1, String label2, String name2, String link, String[] property, String[] value){
        String query = "MATCH (a:" + label1 + " {name: '" + name1 + "'})-[r:" + link + "]->(b:" + label2 + " {name: '" + name2 + "'}) SET ";
        for(int i = 0; i < property.length; i++){
            query += "r." + property[i] + " = '" + value[i] + "'";
            if(i != property.length - 1){
                query += ", ";
            }
        }
        return query;
    }
}
