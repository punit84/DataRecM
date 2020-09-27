package com.datarecm.service.config;

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
	
	
}
