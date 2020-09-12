package com.datarecm.service;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.datarecm.service.config.ConfigService;

@Component
public class SQLRunner {

	@Autowired
	public ConfigService config;

	@Autowired
	public DBConnection sourceDB;
	
	String RULE1="select count(*) from dms_sample.\"order\";";
	String RULE2= "select ordinal_position as \"colum_position\",column_name,\n" + 
			"    case \n" + 
			"      when data_type= 'timestamp without time zone' then 'timestamp' \n" + 
			"      when data_type= 'double precision' then 'float8' \n" + 
			"      when data_type= 'character varying' then 'varchar' \n" + 
			"      else data_type \n" + 
			"    END \n" + 
			"    FROM information_schema.columns \n" + 
			"    WHERE table_name = 'order' \n" + 
			"    ORDER BY ordinal_position;\n" + 
			";";

	public ResultSetMetaData executeSQL(String sqlRule) throws SQLException, ClassNotFoundException{
		if(null !=sourceDB && null != sourceDB.getConnection()){
			PreparedStatement ruleStatement = sourceDB.getConnection().prepareStatement(sqlRule);
			try {

				ResultSet resultSet = ruleStatement.executeQuery();			
				ResultSetMetaData rsmd = resultSet.getMetaData();
				return rsmd;  
			}

			finally {
				ruleStatement.close();
			}

		}
		return null;
	}
}
