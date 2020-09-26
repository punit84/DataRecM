package com.datarecm.service;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.amazonaws.services.athena.model.GetQueryResultsRequest;
import com.datarecm.service.config.ConfigService;

/**
 * <p>
 * This is a service class with methods for data reconciliation service.
 * <p>
 * 
 * @author Punit Jain
 *
 */

@SpringBootApplication
public class DataRecMApplication {
	public static Log logger = LogFactory.getLog(DataRecMApplication.class);




	public static void main(String[] args) {
		SpringApplication.run(DataRecMApplication.class, args);	
	}




}
