package com.datarecm.service.config;
import java.io.Serializable;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

/**
 * @author Punit Jain
 *
 */
@Configuration
@ConfigurationProperties()
@PropertySource( "classpath:application.properties")
//@PropertySource( "file:${home}/unicorngym/application.properties")

public class ConfigService implements Serializable{

	@Bean
	@ConfigurationProperties(prefix = "source")
	public ConfigProperties source() {
		return new ConfigProperties ();
	}

	@Bean
	@ConfigurationProperties(prefix = "target")
	public ConfigProperties target(){
		return new ConfigProperties ();
	}

}
