package com.sgbd2neo4j;

// Java Program to Establish Connection in JDBC

// Importing database
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
        String query = "SELECT COLUMN_NAME FROM information_schema.key_column_usage WHERE table_name = '" + table + "' AND constraint_name = 'PRIMARY'";
        Statement stmt = db.createStatement();
        ResultSet rs = stmt.executeQuery(query);
        return rs;
    }

    public ResultSet getForeignKey(Connection db, String table) throws SQLException{
      String query = "SELECT COLUMN_NAME FROM information_schema.key_column_usage WHERE table_name = '" + table + "' and constraint_name <> 'PRIMARY'";
      Statement stmt = db.createStatement();
      ResultSet rs = stmt.executeQuery(query);
      return rs;

    }
    
    /**
     * Renvoie le nom de la table vers lequel pointe une FK
     * 
     * @param db La connection
     * @param table Le nom de la table où se trouve la FK
     * @param colonneFK Le nom de la colonne qui contient les FK
     * @return Le nomn de la table vers lequel pointe la FK
     * @throws SQLException
     */
    public ResultSet getTableFk(Connection db, String table, String colonneFK) throws SQLException{
      String query =  "SELECT REFERENCED_TABLE_NAME FROM information_schema.key_column_usage "
                    + "WHERE table_name = '" + table + "' "
                    + "AND COLUMN_NAME = '" + colonneFK + "'";
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
                    list.add(primarykey.getString(1));
                    pk.put(tableName, list);
                    //System.out.println(pk.get(tableName));
                }
                else{
                    List<String> list = new ArrayList<String>();
                    list.add(primarykey.getString(1));
                    pk.put(tableName, list);
                    //System.out.println(pk.get(tableName));
                }
            }
        }

        return pk;
    }
    
    /**
     * La fonction 'getListAllPK(Connection db)' renvoie des colonnes en trop, et parfois en double.
     * Cette fonction sert donc à supprimer les colonnes considérées abusivement comme des PK.
     *
     * @param db La connection au SGBD
     * @param list La liste du nom des tables
     * @return Une HashMap avec en clé le nom des tables et en valeurs la liste des PK associées
     * @throws SQLException
     */
    public HashMap<String,List<String>> getAllPK (Connection db, List<String> listeTables) throws SQLException {
      
        HashMap<String,List<String>> listePK = getListAllPK(db);
      
        // Toutes les PK qui ne sont pas des noms de colonnes doivent être supprimées (notamment les PK 'ID')
        for (String nomTable : listeTables) {
            
              // On récupère toutes les colonnes de la table
              ResultSet rs = this.getTuple(db, nomTable);
              List <String> listeColonnes = this.listerNomsColonnes(rs);
              
              // On compare les colonnes dans la HashMap afin de supprimer 
              // les PK qui ne sont pas des colonnes de la table
              for (Map.Entry PK : listePK.entrySet()) { // Pour toutes le PK trouvées de la table
                  if(nomTable.equals(PK.getKey())) {
                      ArrayList<String> l = (ArrayList<String>) PK.getValue();
                      for(int v = 0 ; v < l.size() ; v++) {
                          String pk = l.get(v);
                          if(!listeColonnes.contains(pk)) {
                            /* Si la clé primaire n'est pas un nom de colonne, on la supprime */
                            List<String> list = listePK.get(nomTable);
                            list.remove(pk);
                            listePK.put(nomTable, list);
                          }
                      }
                  }
              }
        } 
        
        
        // Toutes les PK en double doivent être supprimées
        for (String nomTable : listeTables) {
            
            // On récupère la liste des CP de la table
            List<String> liste = listePK.get(nomTable);
            Set<String> HsetPkSansDoublons = new HashSet<String>(liste);
            
            // Si la liste des CP possède des doublons, alors on les supprime
            if(HsetPkSansDoublons.size() < liste.size()){
              liste = new ArrayList<String>(HsetPkSansDoublons);
            }
            listePK.put(nomTable, liste);
        } 
          
        return listePK;
    }
    
    
    /**
     * La fonction 'getListAllFK(Connection db)' renvoie des colonnes en trop, et parfois en double.
     * Cette fonction sert donc à supprimer les colonnes considérées abusivement comme des FK.
     *
     * @param db La connection au SGBD
     * @param list La liste du nom des tables
     * @return Une HashMap avec en clé le nom des tables et en valeurs la liste des FK associées
     * @throws SQLException
     */
    public HashMap<String,List<String>> getAllFK (Connection db, List<String> listeTables) throws SQLException {
      
        HashMap<String,List<String>> listeFK = getListAllFK(db);
          
        return listeFK;
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
                    list.add(foreignkey.getString(1));
                    fk.put(tableName, list);
                }
                else{
                    List<String> list = new ArrayList<String>();
                    list.add(foreignkey.getString(1));
                    fk.put(tableName, list);
                }
            }
        }
        return fk;
    }

    public List<String> toList(ResultSet query) throws SQLException {
        List<String> list = new ArrayList<String>();
        while(query.next()){
            list.add(query.getString(1));
            System.out.println(query.getString(1));
        }
        return list;
    }
    
    public List<String> listerNomsColonnes(ResultSet query) throws SQLException {
      
      List<String> list = new ArrayList<String>();
    
      ResultSetMetaData rsmd = query.getMetaData();
      int nombreTotaleColonne = rsmd.getColumnCount();
      
      for(int i = 1 ; i <= nombreTotaleColonne ; i++) {
          String nom = rsmd.getColumnName(i);
          list.add(nom);
      }
      
      return list;
  }

    public ResultSet getColumnName (Connection db, String table) throws SQLException{
        String query = "SELECT COLUMN_NAME FROM information_schema.columns WHERE table_name = '" + table + "'";
        Statement stmt = db.createStatement();
        ResultSet rs = stmt.executeQuery(query);
        return rs;
    }

    public String[] toStringList(ResultSet tuple) throws SQLException{
        String[] list = new String[tuple.getMetaData().getColumnCount()];
        for(int i = 0; i < tuple.getMetaData().getColumnCount(); i++){
            if (tuple.getString(i+1).matches("'")){
                list[i] = tuple.getString(i+1).replace("'", "\\'");
            }
            else
                list[i] = tuple.getString(i+1);
        }

        return list;
    }

    /**
     * Renvoie la liste du nom des tables pointées par les FK d'une autre.
     * @param dB La connection
     * @param nomAssociation Le nom de la table ayant des FK comme attributs
     * @param listeFkTable La liste des FK de la table "nomAssociation"
     * @return Un ArrayList avec le nom des tables référencées dans l'ordre des FK
     * @throws SQLException 
     */
    public ArrayList<String> getTablesRefParFk(Connection dB, String nomAssociation, ArrayList<String> listeFkTable) throws SQLException {
      
        // On déclare la liste qui contiendra les tables référencées par les FK de 'nomAssociation'
        ArrayList<String> tablesReferencees = new ArrayList<String>();
        
        // On remplit cette liste (chaque FK référence une seule table)
        for(int v = 0 ; v < listeFkTable.size() ; v++) {
            ResultSet rs = getTableFk(dB, nomAssociation, listeFkTable.get(v));
            rs.next();
            tablesReferencees.add(rs.getString(1));
            System.out.println(rs.getString(1));
        }
        
        return tablesReferencees;
    }
}
