package com.datarecm.service.source;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * TableInfo  
 * @author Punit Jain
 *
 */
public class TableInfo {

	String COLUMN_NAME_KEY = "column_name";
	String COLUMN_DATA_TYPE = "data_type";
	String COLUMN_POSITION = "colum_position";

	List<String> columnNameList;
	List<String> columnTypeList;
	List<String> columnSequenceList;
	
	public Integer fieldCount;
	
	String primaryKey;
	
	String query;
	
	String fetchUnmatchRecordQuery;

	public String getPrimaryKey() {
		return primaryKey;
	}
	public void setPrimaryKey(String primaryKey) {
		this.primaryKey = primaryKey;
	}
	
	
	public List<String> getColumnNameList() {
		return columnNameList;
	}
	public void setColumnNameList(List<String> columnNameList) {
		this.columnNameList = columnNameList;
	}
	public List<String> getColumnTypeList() {
		return columnTypeList;
	}
	public void setColumnTypeList(List<String> columnTypeList) {
		this.columnTypeList = columnTypeList;
	}
	public List<String> getColumnSequenceList() {
		return columnSequenceList;
	}
	public void setColumnSequenceList(List<String> columnSequenceList) {
		this.columnSequenceList = columnSequenceList;
	}

	public Integer getFieldCount() {
		return fieldCount;
	}
	public void setFieldCount(Integer fieldCount) {
		this.fieldCount = fieldCount;
	}
	
	public String getQuery() {
		return query;
	}
	public void setQuery(String query) {
		this.query = query;
	}
	public List<String> getNameWithSequence() {
		List<String> nameWithType =  new  ArrayList<String>();
		StringBuilder stringBuilder = new  StringBuilder();
		
		for (int i = 0; i < fieldCount; i++) {
			stringBuilder.append(columnNameList.get(i));
			stringBuilder.append("(");
			stringBuilder.append(columnSequenceList.get(i));
			stringBuilder.append("),");
		}
		stringBuilder.deleteCharAt(stringBuilder.length() - 1);

		nameWithType.add(stringBuilder.toString());

		return nameWithType;
	}
	
	public List<String> getNameWithType() {
		List<String> nameWithType =  new  ArrayList<String>();
		StringBuilder stringBuilder = new  StringBuilder();
		
		for (int i = 0; i < fieldCount; i++) {
			stringBuilder.append(columnNameList.get(i));
			stringBuilder.append("(");
			stringBuilder.append(columnTypeList.get(i));
			stringBuilder.append("),");
		}
		if (stringBuilder.length()>0) {
			stringBuilder.deleteCharAt(stringBuilder.length() - 1);
		}
		nameWithType.add(stringBuilder.toString());
		return nameWithType;
	}
	
	public List<String> getNameWithType(List<Integer> indexList) {
		List<String> nameWithType =  new  ArrayList<String>();
		StringBuilder stringBuilder = new  StringBuilder();
		for (Integer i : indexList) {
			stringBuilder.append(columnNameList.get(i));
			stringBuilder.append("(");
			stringBuilder.append(columnTypeList.get(i));
			stringBuilder.append("),");
		}
		if (stringBuilder.length()>0) {
			stringBuilder.deleteCharAt(stringBuilder.length() - 1);
		}
		nameWithType.add(stringBuilder.toString());
		return nameWithType;
	}
	
	public String getFetchUnmatchRecordQuery() {
		return fetchUnmatchRecordQuery;
	}
	public void setFetchUnmatchRecordQuery(String fetchUnmatchRecordQuery) {
		this.fetchUnmatchRecordQuery = fetchUnmatchRecordQuery;
	}
	public TableInfo(Map<String, List<String>> schemaInfo) {
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
