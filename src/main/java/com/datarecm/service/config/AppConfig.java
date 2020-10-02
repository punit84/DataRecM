package com.datarecm.service.config;
import java.io.Serializable;
import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

/**
 * @author Punit Jain
 *
 */
@Configuration
@PropertySource( "classpath:application.properties")
@ConfigurationProperties(prefix = "app")
//@PropertySource( "file:${home}/unicorngym/application.properties")

public class AppConfig implements Serializable{


	public static final String TABLENAME="<TABLENAME>";
	public static final String TABLESCHEMA="<TABLESCHEMA>";
	public static final String MD5FILEPREFIX = "MD5Results-";

	
	
	private List<String> sourceRules;
	private List<String> targetRules;

	private List<String> ruleDesc;

	private int timeout;
	private String region;
	private String s3bucket;
	private String reportPath;
	
	
	
	private String reportFile;

	
	public String getReportPath() {
		return reportPath;
	}
	public void setReportPath(String reportPath) {
		this.reportPath = reportPath;
	}
	public String getS3bucket() {
		return s3bucket;
	}
	public void setS3bucket(String s3bucket) {
		this.s3bucket = s3bucket;
	}
	public List<String> getSourceRules() {
		return sourceRules;
	}
	public void setSourceRules(List<String> sourceRules) {
		this.sourceRules = sourceRules;
	}
	public List<String> getTargetRules() {
		return targetRules;
	}
	public void setTargetRules(List<String> targetRules) {
		this.targetRules = targetRules;
	}
	public List<String> getRuleDesc() {
		return ruleDesc;
	}
	public void setRuleDesc(List<String> ruleDesc) {
		this.ruleDesc = ruleDesc;
	}
	
	public int getTimeout() {
		return timeout;
	}
	public void setTimeout(int timeout) {
		this.timeout = timeout;
	}
	public String getRegion() {
		return region;
	}
	public void setRegion(String region) {
		this.region = region;
	}

	public String getReportFile() {
		return reportFile;
	}

	public void setReportFile(String reportFile) {
		this.reportFile = reportFile;
	}

}
