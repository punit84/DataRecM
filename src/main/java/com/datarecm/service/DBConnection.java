/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.datarecm.service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.datarecm.service.config.AppConfig;
import com.datarecm.service.config.DBConfig;

/**
 * @author Punit Jain
 *
 */
@Component
public class DBConnection {

	private static final String ORG_POSTGRESQL_DRIVER = "org.postgresql.Driver";
	private static final String ORG_MYSQL_DRIVER = "com.mysql.jdbc.Driver";
	public static Log logger = LogFactory.getLog(DBConnection.class);

	public static Connection sourceConn=null;

	@Autowired
	private AppConfig appConfig ;

	public synchronized Connection getConnection(DBConfig source) throws SQLException, ClassNotFoundException{

		if (sourceConn == null || sourceConn.isClosed()){
			logger.info("DB connection not found");
			logger.info(appConfig.getRegion());
			String jdbcURL= createJDBCUrl(source.getHostname(), source.getPort()+"", source.getDbname(),source.getDbtype());
			sourceConn = getConnection(source.getUsername(),source.getPassword(),jdbcURL );		
		}
		return sourceConn;
	}

	public synchronized Connection getConnection(String username,String password, String  jdbcUrl) throws SQLException, ClassNotFoundException {
		try {

			sourceConn = DriverManager
					.getConnection(jdbcUrl,
							username, password);
			//logger.info(sourceConn.getClientInfo());
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println(e.getClass().getName()+": "+e.getMessage());
			return null;
		}
		logger.info("Opened database successfully");

		return sourceConn;
	}

	public String createJDBCUrl(String hostname, String port, String dbname, String dbtype)
			throws ClassNotFoundException {
		String jdbcUrl = new StringBuilder()
				.append(getDBPrefix(dbtype))
				.append(hostname)
				.append(":")
				.append(port)
				.append("/")
				.append(dbname)			        
				.toString();
		logger.info(jdbcUrl);
		return jdbcUrl;
	}

	public String getDBPrefix(String dbType) throws ClassNotFoundException
	{
		if ("mysql".equalsIgnoreCase(dbType)) {

			Class.forName(ORG_MYSQL_DRIVER);  

			return "jdbc:mysql://";
		}
		else if ("postgres".equalsIgnoreCase(dbType)) {
			Class.forName(ORG_POSTGRESQL_DRIVER);  

			return "jdbc:postgresql://";
		}    	
		else
			return null;
	}

}
