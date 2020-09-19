package com.datarecm.service;

import java.util.List;
import java.util.Map;

public class TableInfo {

	String COLUMN_NAME_KEY = "column_name";
	String COLUMN_DATA_TYPE = "data_type";
	String COLUMN_POSITION = "colum_position";

	List<Object> columnNameList;
	List<Object> columnTypeList;
	List<Object> columnSequenceList;
	
	Integer fieldCount;
	
	String primaryKey;
	
	String sourceRecQuery;
	
	String destRecQuery;


	public String getPrimaryKey() {
		return primaryKey;
	}
	public void setPrimaryKey(String primaryKey) {
		this.primaryKey = primaryKey;
	}
	public List<Object> getColumnNameList() {
		return columnNameList;
	}
	public void setColumnNameList(List<Object> columnNameList) {
		this.columnNameList = columnNameList;
	}
	public List<Object> getColumnTypeList() {
		return columnTypeList;
	}
	public void setColumnTypeList(List<Object> columnTypeList) {
		this.columnTypeList = columnTypeList;
	}
	public List<Object> getColumnSequenceList() {
		return columnSequenceList;
	}
	public void setColumnSequenceList(List<Object> columnSequenceList) {
		this.columnSequenceList = columnSequenceList;
	}

	
	public Integer getFieldCount() {
		return fieldCount;
	}
	public void setFieldCount(Integer fieldCount) {
		this.fieldCount = fieldCount;
	}
	
	
	
	public String getSourceRecQuery() {
		return sourceRecQuery;
	}
	public void setSourceRecQuery(String sourceRecQuery) {
		this.sourceRecQuery = sourceRecQuery;
	}
	public String getDestRecQuery() {
		return destRecQuery;
	}
	public void setDestRecQuery(String destRecQuery) {
		this.destRecQuery = destRecQuery;
	}
	public TableInfo(Map<String, List<Object>> schemaInfo) {
		super();
		if (schemaInfo.containsKey(COLUMN_NAME_KEY)) {
			columnNameList = schemaInfo.get(COLUMN_NAME_KEY);

		}
		if (schemaInfo.containsKey(COLUMN_DATA_TYPE)) {
			columnTypeList = schemaInfo.get(COLUMN_DATA_TYPE);

		}
		if (schemaInfo.containsKey(COLUMN_POSITION)) {
			columnSequenceList = schemaInfo.get(COLUMN_POSITION);
		}
		
		fieldCount= columnNameList.size();


	}
}
