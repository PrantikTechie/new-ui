package com.spectra.upsert.hook;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import com.microsoft.sqlserver.jdbc.SQLServerDriver;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

public class App {
	
	private static Map<String, String> environmentMap = null;
	private static Properties props = new Properties();
	private static List<String> updateColumns = new ArrayList<>();
	
	public static void main(String[] args) {
		
		
		 
        Connection conn = null;
        String database = null;
        String schema = null;
        String table = null;
	    String tempTable = null;
	    String tempDb = null;
	    String tempSchema = null;
	    String upsertTable = null;
	    String upsertKey = null;
 
        try {
        	    File file = new File("src/main/resources/config.properties");
        	    try {
					props.load(new FileInputStream(file));
				} catch (IOException e1) {
					e1.printStackTrace();
				} 
        	    environmentMap = System.getenv();
        	    Map <String,String> inputParametersMap = new HashMap<>();
        	    getInputParam(inputParametersMap);
        	    System.out.println("#### Parameters map = "+ inputParametersMap);
        	    try {
        	       database = inputParametersMap.get("database_name");
        	       schema = inputParametersMap.get("schema_name");
        	       tempDb= inputParametersMap.get("temp_database");
        	       tempSchema = inputParametersMap.get("temp_schema");
        	       table = inputParametersMap.get("table_name");
        	       tempTable = getFullyQualifiedObjectName(tempDb, tempSchema,inputParametersMap.get("temp_table"));
        	       upsertTable = getFullyQualifiedObjectName(database,schema,inputParametersMap.get("upsert_table"));
        	       upsertKey = inputParametersMap.get("upsert_key");
        	    } catch(Exception e) {
        	    	System.out.println("Please add the required input params in flow ");
        	    	e.printStackTrace();
        	    }
        	    
                String url = "jdbc:sqlserver://"+props.getProperty("jdbc.host")+":"+props.getProperty("jdbc.port")+";database="+database;
                String user = props.getProperty("jdbc.user");
                String password = props.getProperty("jdbc.password");
                conn = DriverManager.getConnection(url, user, password);
                if (conn != null) {
                    String columnNameQuery = "SELECT name FROM sys.columns WHERE object_id = OBJECT_ID('"+database+"."+schema+"." +table+ "')";
                    try(Statement stmt3 = conn.createStatement()){
                        ResultSet rs1 = stmt3.executeQuery(columnNameQuery);
                        while (rs1.next()) {
                          String column = rs1.getString("name");
                          updateColumns.add(column);
                        }
                    } catch(SQLException e) {
                    	e.printStackTrace();
                    }
                    System.out.println(updateColumns);
                    String updateQuery = getUpdateQuery(tempTable,upsertTable,upsertKey,updateColumns);
                    String deleteQuery = getDeleteQuery(tempTable,upsertTable,upsertKey);
                    String insertQuery = getInsertQuery(tempTable,upsertTable,updateColumns);
                    System.out.println(updateQuery);
                    System.out.println(deleteQuery);
                    System.out.println(insertQuery);
                    executeUpdateQuery(conn,updateQuery);
                    executeUpdateQuery(conn,deleteQuery);
                    executeUpdateQuery(conn,insertQuery);
                }
 
            } catch (SQLException ex) {
                ex.printStackTrace();
            } finally {
                try {
                    if (conn != null && !conn.isClosed()) {
                        conn.close();
                    }
                } catch (SQLException ex) {
                    ex.printStackTrace();
                }
            }
        }
	
	private static String getFullyQualifiedObjectName(String databaseName, String schemaName, String tableName) {
		return databaseName+"."+schemaName+"."+tableName;
	}
	
	private static void getInputParam(Map <String,String> inputParametersMap) {
		if (environmentMap.containsKey("input_params")) {
			Map<String, Object> envInp = new Gson().fromJson(environmentMap.get("input_params"),
					new TypeToken<HashMap<String, Object>>() {
					}.getType());

			for (Entry<String, Object> e : envInp.entrySet()) {
				inputParametersMap.put(e.getKey(), String.valueOf(e.getValue()));
			}

		} else {
			for (Entry<String, String> e : environmentMap.entrySet()) {
				inputParametersMap.put(e.getKey(), String.valueOf(e.getValue()));
			}
		}
	}
	
	private static String getUpdateQuery(String tempTable ,String tableName,String upsertKey, List<String> updateColumns) {
		return "update t1 set " + getUpdateColumns("t1","t2",updateColumns) +" from "+ tableName + " t1 inner join " + tempTable + " t2 on t1.[" + upsertKey + "]=t2.[" + upsertKey + "]" ;
	}
	
	private static String getUpdateColumns(String aliaseOne, String aliaseTwo, List<String> updateColList) {
		StringBuilder joinCondition = new StringBuilder();
		boolean isFirstRecord = true;
		for (String column : updateColList) {
			if (!isFirstRecord) {
				joinCondition.append(", ");
			}
			joinCondition.append(aliaseOne).append(".").append(column).append(" = ").append(aliaseTwo).append(".").append(column);
			isFirstRecord = false;
		}
		return joinCondition.toString();
	}
	
	private static String getDeleteQuery(String tempTable , String tableName, String upsertKey) {
		return "delete t1 from "+tempTable+" t1 inner join "+tableName+" t2 on t1.[" + upsertKey + "]=t2.[" + upsertKey + "]";
	}
	
	private static String getInsertQuery(String tempTable ,String tableName, List<String> updateColList) {
		StringBuilder insertQuery = new StringBuilder("insert into ").append(tableName).append(" ");
		if(!updateColList.isEmpty()) {
			insertQuery.append("(").append(String.join(",", updateColList)).append(") select ")
			.append(String.join(",", updateColList)).append(" from ").append(tempTable);
		} else {
			insertQuery.append("select * from ").append(tempTable);
		}
		return insertQuery.toString();
	}
	
	private static void executeUpdateQuery(Connection conn, String query) {
		try(Statement stmt = conn.createStatement()){
            stmt.executeUpdate(query);
        } catch(SQLException e) {
        	e.printStackTrace();
        }
	}

}

