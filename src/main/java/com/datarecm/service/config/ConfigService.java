package com.datarecm.service.config;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

@Configuration
@ConfigurationProperties()
@PropertySource("classpath:application.properties")
public class ConfigService {
	
    @Bean
    @ConfigurationProperties(prefix = "source")
    public ConfigProperties source() {
        return new ConfigProperties ();
    }

    @Bean
    @ConfigurationProperties(prefix = "destination")
    public ConfigProperties destination(){
        return new ConfigProperties ();
    }
    
}
