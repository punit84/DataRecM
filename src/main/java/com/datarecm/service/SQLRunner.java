package com.datarecm.service;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.datarecm.service.config.ConfigProperties;
import com.datarecm.service.config.ConfigService;

/**
 * @author Punit Jain
 *
 */
@Component
public class SQLRunner {

	@Autowired
	public ConfigService config;
	static Map<Integer, Map<String, List<Object>>> sqlResutset= new HashMap<>();

	@Autowired
	public DBConnection sourceDB;

	public Map<Integer, Map<String, List<Object>>> execuleAllRules() throws SQLException, ClassNotFoundException{
		List<String> rules = config.source().getRules();

		for (int index = 0; index < rules.size(); index++) {
			System.out.println("*******************Executing Source Query :"+ index+" *************");
			
			String updatedRule=rules.get(index);
			updatedRule = updatedRule.replace(ConfigProperties.TABLENAME, config.source().getTableName());
			updatedRule = updatedRule.replace(ConfigProperties.TABLESCHEMA, config.source().getTableSchema());
			System.out.println("QUERY NO "+ index+ " is "+updatedRule);

			Map<String, List<Object>> result = executeSQL(updatedRule);
			sqlResutset.put(index, result);
			
			System.out.println("*******************Execution successfull *************");

		}
		return sqlResutset;


	}

	public Map<String, List<Object>> executeSQL(String sqlRule) {
		PreparedStatement ruleStatement=null;

		try {
			if(null !=sourceDB && null != sourceDB.getConnection()){

				ruleStatement = sourceDB.getConnection().prepareStatement(sqlRule);
				ResultSet resultSet = ruleStatement.executeQuery();	

				return printSQLResponse(resultSet);
				//return resultSetToArrayList(resultSet);  

			}
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		finally {
			if (ruleStatement!=null) {
				try {
					ruleStatement.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}				
			}
		}
		return null;
	}
	/*
    private List<DBColumn> getSourceColumns(String tablename) throws SQLException, ClassNotFoundException {
    	 List<DBColumn> sourceTableColumns=null;
    	String selectSql = String.format("SELECT TABLE_NAME,COLUMN_NAME,DATA_TYPE,DATA_LENGTH,NVL(DATA_PRECISION,-1) DATA_PRECISION,NVL(DATA_SCALE,-1) DATA_SCALE,DATA_DEFAULT,NULLABLE FROM USER_TAB_COLS WHERE TABLE_NAME='%s'", new Object[]{tablename});
        PreparedStatement st = sourceDB.getConnection().prepareStatement(selectSql);
        if (st != null) {
            ResultSet rs = st.executeQuery();
            if (rs != null) {
                sourceTableColumns = new ArrayList<>();
                DBColumn col;
                while (rs.next()) {
                    col = new DBColumn();
                    col.setTableName(rs.getString("TABLE_NAME"));
                    col.setColumnName(rs.getString("COLUMN_NAME"));
                    col.setDataType(rs.getString("DATA_TYPE"));
                    col.setDataLength(rs.getString("DATA_LENGTH"));
                    col.setDataDefault(rs.getString("DATA_DEFAULT"));
                    col.setNullable(rs.getString("NULLABLE"));
                    col.setDataPrecision(rs.getInt("DATA_PRECISION"));
                    col.setDataScale(rs.getInt("DATA_SCALE"));
                    sourceTableColumns.add(col);
                }
            }
        }
        return sourceTableColumns;
    }*/

	public Map<String, List<Object>> printSQLResponse(ResultSet resultSet ) {

		try {
			ResultSetMetaData rsmd = resultSet.getMetaData();

			int columnsNumber = rsmd.getColumnCount();
			Map<String, List<Object>> map = new HashMap<>(columnsNumber);
			for (int i = 1; i <= columnsNumber; ++i) {
				map.put(rsmd.getColumnName(i), new ArrayList<>());
			}

			while (resultSet.next()) {
				for (int i = 1; i <= columnsNumber; i++) {
					if (i > 1) System.out.print(",  ");
					String columnValue = resultSet.getString(i);
					//System.out.println(resultSet.getArray(i));

					//System.out.print( rsmd.getColumnName(i) + ":" +columnValue);
					map.get(rsmd.getColumnName(i)).add(resultSet.getArray(i));

				}
				//System.out.println("");
			}
			return map;

		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}
}