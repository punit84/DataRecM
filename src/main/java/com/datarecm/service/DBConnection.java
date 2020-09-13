/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.datarecm.service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.datarecm.service.config.ConfigService;

/**
 * @author jainpuni
 *
 */
@Component
public class DBConnection {

	private static final String ORG_POSTGRESQL_DRIVER = "org.postgresql.Driver";
	private static final String ORG_MYSQL_DRIVER = "com.mysql.jdbc.Driver";

	public static Connection sourceConn=null;
	
	@Autowired
	private ConfigService config ;
	
	public synchronized Connection getConnection() throws SQLException, ClassNotFoundException{
		
		if (sourceConn == null || sourceConn.isClosed()){
			System.out.println(config.destination().getRegion());
			sourceConn = getConnection(config.source().getUsername(),config.source().getPassword(),config.source().getHostname(),config.source().getPort()+"", config.source().getDbname(),config.source().getDbtype());		
		}
		return sourceConn;
	}
	
	public synchronized Connection getConnection(String username,String password, String hostname, String port, String dbname, String dbtype) throws SQLException, ClassNotFoundException {
		try {
			//Class.forName(ORG_POSTGRESQL_DRIVER);
			//Class.forName(ORG_MYSQL_DRIVER);  

			String jdbcUrl = new StringBuilder()
					.append(getDBPrefix(dbtype))
					.append(hostname)
					.append(":")
					.append(port)
					.append("/")
					.append(dbname)			        
					.toString();
			System.out.println(jdbcUrl);

			sourceConn = DriverManager
					.getConnection(jdbcUrl,
							username, password);
			System.out.println(sourceConn.getSchema());
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println(e.getClass().getName()+": "+e.getMessage());
			System.exit(0);
		}
		System.out.println("Opened database successfully");

		return sourceConn;
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
