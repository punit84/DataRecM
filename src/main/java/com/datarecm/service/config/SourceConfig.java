package com.datarecm.service.config;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

@Configuration
@ConfigurationProperties(prefix = "source")
//@PropertySource("classpath:application.properties")
public class SourceConfig {
	private static final int DEFAULT_PORT = 3306;
	private static final Pattern URL_PATTERN = Pattern.compile("^([^/]*:)[^:/]+:");

	private String hostname;
	
	private int port;
	private String username;
	private String password;
	private String dbname;
	private String dbtype;

	private String url;
	private List<String> queries;
	private String rule1;

	public SourceConfig(String url) throws URISyntaxException {
		Matcher urlMatcher = URL_PATTERN.matcher(url);
		if (!urlMatcher.find()) {
			throw new URISyntaxException(url, "It doesn't contain connection protocol schema of jdbc");
		}

		String cleanUrl = url.substring(urlMatcher.group(1).length());
		URI uri = new URI(cleanUrl);

		hostname = uri.getHost();

		port = uri.getPort();
		if (port < 0) {
			port = DEFAULT_PORT;
		}
		queries = parseQueryString(uri.getQuery());
	}

	public SourceConfig() {
		// TODO Auto-generated constructor stub
	}

	private List<String> parseQueryString(String queryString) {
		ArrayList<String> queries = new ArrayList<>();

		if (queryString != null) {
			Arrays.asList(queryString.split("&")).forEach(property -> {
				queries.add(property);
			});
		}

		return queries;
	}


	public List<String> getQueries() {
		return queries;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public void setQueries(List<String> queries) {
		this.queries = queries;
	}

	public String getDbname() {
		return dbname;
	}

	public void setDbname(String dbname) {
		this.dbname = dbname;
	}

	public String getDbtype() {
		return dbtype;
	}

	public void setDbtype(String dbtype) {
		this.dbtype = dbtype;
	}

	public String getHostname() {
		return hostname;
	}

	public void setHostname(String hostname) {
		this.hostname = hostname;
	}

	public void setRule1(String rule1) {
		this.rule1 = rule1;
		
	}

	public String getRule1() {
		return rule1;
	}


}
