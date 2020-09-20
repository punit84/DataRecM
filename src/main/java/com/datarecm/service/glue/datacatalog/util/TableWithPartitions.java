package com.datarecm.service.glue.datacatalog.util;

import java.util.List;

import com.amazonaws.services.glue.model.Partition;
import com.amazonaws.services.glue.model.Table;

public class TableWithPartitions {

	private Table table;
	private List<Partition> partitionList;
	
	public Table getTable() {
		return table;
	}
	public void setTable(Table table) {
		this.table = table;
	}
	public List<Partition> getPartitionList() {
		return partitionList;
	}
	public void setPartitionList(List<Partition> partitionList) {
		this.partitionList = partitionList;
	}
	
	
	
}
