package com.datarecm.service;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.datarecm.service.config.ConfigProperties;
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
		Map<String, List<Object>> sqlResultSet= sqlRunner.execuleAllRules();
		System.out.println("printing SQL result set");
		System.out.println(sqlResultSet.toString());
		
		Map<Integer, Map<String, List<Object>>> athenaResutset = athenaService.runQueries();
		System.out.println("printing athena result set");
		System.out.println(athenaResutset.toString());

	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException, InterruptedException {

		SpringApplication.run(DataRecMApplication.class, args);	
		System.exit(0);
	}




}
