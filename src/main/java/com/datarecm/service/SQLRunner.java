package com.datarecm.service;

import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.datarecm.service.config.AppConfig;
import com.datarecm.service.config.DBConfig;

/**
 * Class to run sql on source database
 * @author Punit Jain
 *
 */
@Component
public class SQLRunner {

	public static Log logger = LogFactory.getLog(SQLRunner.class);
	
	private DBConfig source;
	
	@Autowired
	private AppConfig appConfig;

	//public Map<Integer, Map<String, List<String>>> sqlResutset= new HashMap<>();

	@Autowired
	private DBConnection sourceDB;

//	public Map<Integer, Map<String, List<String>>> execuleAllRules() throws SQLException, ClassNotFoundException{
//		List<String> rules = appConfig.getSourceRules();
//
//		for (int index = 0; index < rules.size(); index++) {
//			System.out.println("*******************Executing Source Query :"+ index+" *************");
//
//			String updatedRule=rules.get(index);
//			Map<String, List<String>> result = executeSQL(index, updatedRule);
//			sqlResutset.put(index, result);
//
//			System.out.println("*******************Execution successfull *************");
//
//		}
//		return sqlResutset;
//
//
//	}
//	
	public Map<String, List<String>> executeSQL(int ruleIndex , String sqlRule) {
		PreparedStatement ruleStatement=null;
		try {
			ResultSet resultSet = executeSQLAtIndex(ruleStatement, ruleIndex, sqlRule);
			
			return convertSQLResponse(resultSet);

		}finally {
			if (ruleStatement!=null) {
				try {
					ruleStatement.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}				
			}
		}
	}
	
	//@Cacheable(value="cacheSQLMap", key="#sqlRule")  
	public Map<String, String> executeSQLForMd5(int ruleIndex , String sqlRule) {
		PreparedStatement ruleStatement=null;
		try {
			ResultSet resultSet =executeSQLAtIndex(ruleStatement, ruleIndex, sqlRule);
			
			return convertSQLResponseForMd5(resultSet);

		}finally {
			if (ruleStatement!=null) {
				try {
					ruleStatement.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}				
			}
		}
	}
	public ResultSet executeSQLAtIndex(PreparedStatement ruleStatement, int ruleIndex , String sqlRule) {
		sqlRule = sqlRule.replace(appConfig.TABLENAME, source.getTableName());
		sqlRule = sqlRule.replace(appConfig.TABLESCHEMA,source.getTableSchema());
		logger.info("\nQUERY NO "+ ruleIndex+ " is "+sqlRule);

		try {
			if(null !=sourceDB && null != sourceDB.getConnection(source)){

				ruleStatement = sourceDB.getConnection(source).prepareStatement(sqlRule);
				return ruleStatement.executeQuery();	
			}
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
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

	public Map<String, List<String>> convertSQLResponse(ResultSet resultSet ) {

		try {
			ResultSetMetaData rsmd = resultSet.getMetaData();

			int columnsNumber = rsmd.getColumnCount();
			Map<String, List<String>> map = new HashMap<>(columnsNumber);
			for (int i = 1; i <= columnsNumber; ++i) {
				map.put(rsmd.getColumnName(i), new ArrayList<>());
			}

			while (resultSet.next()) {
				for (int i = 1; i <= columnsNumber; i++) {
					//if (i > 1) System.out.print(",  ");
					//String columnValue = resultSet.getString(i);
					//System.out.println(resultSet.getArray(i));

					//System.out.print( rsmd.getColumnName(i) + ":" +columnValue);
					map.get(rsmd.getColumnName(i)).add(resultSet.getArray(i).toString().trim());

				}
				//System.out.println("");
			}
			return map;

		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}

	public Map<String, String> convertSQLResponseForMd5(ResultSet resultSet ) {

		try {
			ResultSetMetaData rsmd = resultSet.getMetaData();

			int columnsNumber = rsmd.getColumnCount();
			//logger.info(columnsNumber);
			Map<String, String> idVsMd5Map = new HashMap<>();
			int idIndex=1;
			int md5Index=2;
			//	map.get(rsmd.getColumnName(i)).add(resultSet.getArray(i));


			while (resultSet.next()) {
				Array id = resultSet.getArray(idIndex);
				Array md5 = resultSet.getArray(md5Index);
				idVsMd5Map.put(id.toString(), md5.toString());
				//System.out.println(test +":"+me);
				//				String[] idArray = (String[])(resultSet.getArray(idIndex).getArray());
				//				String[] md5Array = (String[])(resultSet.getArray(md5Index).getArray());
				//
				//				Map<String, String> valueMap = IntStream.range(0, idArray.length).boxed()
				//					    .collect(Collectors.toMap(i -> idArray[i], i -> md5Array[i]));
				//				idVsMd5Map.putAll(valueMap);
			}


			return idVsMd5Map;

		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public DBConfig getSource() {
		return source;
	}
	public void setSource(DBConfig source) {
		this.source = source;
	}



}
