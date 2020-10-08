package com.datarecm.service;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

/**
 * <p>
 * This is a service class with methods for data reconciliation service.
 * <p>
 * 
 * @author Punit Jain
 *
 */

@SpringBootApplication
@EnableAutoConfiguration
@Configuration
@EnableConfigurationProperties
@ComponentScan
public class DataRecMApplication {
	public static Log logger = LogFactory.getLog(DataRecMApplication.class);

	public static void main(String[] args) {
		SpringApplication.run(DataRecMApplication.class, args);	
	}

}
