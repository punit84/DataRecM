package com.datarecm.service.config;

import com.amazonaws.util.StringUtils;

public class ConfigTO {
	
	DBConfig source;
	DBConfig target;
	
	public DBConfig getSource() {
		return source;
	}
	public void setSource(DBConfig source) {
		this.source = source;
	}
	public DBConfig getTarget() {
		return target;
	}
	public void setTarget(DBConfig target) {
		this.target = target;
	}
	public ConfigTO(DBConfig source, DBConfig target) {
		super();
		this.source = source;
		this.target = target;
	}
	
	public boolean validate() throws Exception {

		
		if (StringUtils.isNullOrEmpty(target.getDbname())  ) {
			throw new Exception("Target: dbname field is missing");
		}
		if (StringUtils.isNullOrEmpty(target.getDbtype())  ) {
			throw new Exception("Target;: dbtype field is missing");
		}
		if (StringUtils.isNullOrEmpty(target.getTableName())  ) {
			throw new Exception("Target tableName field is missing");
		}
		if (StringUtils.isNullOrEmpty(target.getRegion())  ) {
			throw new Exception("Target: region  field is missing");
		}
		
		if (StringUtils.isNullOrEmpty(source.getHostname())  ) {
			throw new Exception("Source: hostname field is missing");
		}

		if (StringUtils.isNullOrEmpty(source.getDbname())  ) {
			throw new Exception("Source: dbname field is missing");
		}
		if (StringUtils.isNullOrEmpty(source.getDbtype())  ) {
			throw new Exception("Source;: dbtype field is missing");
		}
		if (StringUtils.isNullOrEmpty(source.getTableName())  ) {
			throw new Exception("Source tableName field is missing");
		}
		if (StringUtils.isNullOrEmpty(source.getRegion())  ) {
			throw new Exception("Source: region  field is missing");
		}
		
		if (StringUtils.isNullOrEmpty(source.getTableSchema())  ) {
			throw new Exception("Source: tableSchema field is missing");
		}
		
		
		return true;
		
		
	}
	
}
