package com.datarecm.service;

import java.io.IOException;
import java.sql.SQLException;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

import com.datarecm.service.config.ConfigService;

/**
 * <p>
 * This is a service class with methods for data reconciliation service.
 * <p>
 * 
 * @author Punit Jain, Amazon Web Services, Inc.
 *
 */

@SpringBootApplication
public class DataRecMApplication {

	@Autowired
	public GlueService glueService;

	@Autowired
	public AthenaService athenaService;

	@Autowired
	SQLRunner sqlRunner ;
	@Autowired
	private ConfigService config ;
	@PostConstruct
	public void runRecTest() throws Exception {
		System.out.println("************************");

		//DataRecMApplication app = new DataRecMApplication();
		//app.loadSourceConfig();
		//app.sourceDB.
		//TableCompare tc = new TableCompare();
		//tc.compareTables();
		sqlRunner.executeSQL(config.source().getRule1());
		
		athenaService.runQueries();
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException, InterruptedException {

		SpringApplication.run(DataRecMApplication.class, args);	
		System.exit(0);
	}


}
