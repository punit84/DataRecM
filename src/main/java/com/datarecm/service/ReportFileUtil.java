package com.datarecm.service;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Date;
import java.util.List;
import java.util.Map;

import com.datarecm.service.config.DBConfig;

public class ReportFileUtil {
	Path filePath;
	
	TableInfo sourceSchema;
	TableInfo destSchema;

	public ReportFileUtil(String fileName) {
		this.filePath = Paths.get(fileName);;
	}


	public Path getFilePath() {
		return filePath;
	}


	private void writeToFile(String msg, boolean append) throws IOException {
		if (append) {
			Files.write(filePath, msg.getBytes(),StandardOpenOption.APPEND);
			
		}else {
			Files.write(filePath, msg.getBytes());

		}
		
		//Files.writeTextToFile(msg);
	//	FileUtils.writeStringToFile(file, msg, append);
	}
	
	public void printResultToFile(String type, Map<String, List<Object>> resultset ) {
		try {

			String result= "\n"+type + " execution result is :";
			writeToFile(result.toString(), true);
			writeToFile(resultset.toString(), true);

		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void printResultToFile(String type, int ruleno, Map<String, List<Object>>  resultset ) {
		try {

			//String result= type + " : Execution result for rule:" + ruleno +" is :\n";

			writeToFile(resultset.toString(), true);

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public  void writeTextToFile(String msg) {
		try {
			// 3rd parameter boolean append = true
			writeToFile(msg, true);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void createReportFile(DBConfig sourceConfig, DBConfig targetConfig) throws IOException {
		
		writeToFile("\t\t\t\tAWS - Data Reconciliation Module Report ", false);
		writeToFile("\n\t\t\t\t________________________________________\n\n", true);

		writeTextToFile("\nCurrent Date is :" +new Date());
		writeTextToFile("\nSource DBName :'" +sourceConfig.getDbname().toUpperCase()+"'");
		writeTextToFile("\nSource Table  :'" +sourceConfig.getTableName().toUpperCase()+"'");
		writeTextToFile("\nSource Type   :'" +sourceConfig.getDbtype().toUpperCase()+"'");

		writeTextToFile("\nTarget DBName :'" +targetConfig.getDbname().toUpperCase()+"'");
		writeTextToFile("\nTarget Table  :'" +targetConfig.getTableName().toUpperCase()+"'");
		writeTextToFile("\nTarget Type   :'" +targetConfig.getDbtype().toUpperCase()+"'");
 
		writeTextToFile("\nNo of Metadata rules : " +4);

		if (sourceConfig.isEvaluateDataRules()) {
			writeTextToFile("\nNo of Data validation rules : " +2);
		}
		writeTextToFile("\n");

	}
	
	public void printRuleHeader(String ruleDescription) {
		writeTextToFile("\n**********************************************************************************\n");
		writeTextToFile(ruleDescription);
		writeTextToFile("\n**********************************************************************************\n");

	}
	public void printMatchStatus(boolean match) {
		if (match) {
			writeTextToFile("Result = " + AppConstants.MATCH);
		}else {
			writeTextToFile("Result = " + AppConstants.MISMATCH);
		}
	}
	

	public void printEndSummaryOrderFields() {
		printEndSummary(("\nOrder of source fields : "+ sourceSchema.getNameWithSequence().toString()), ("\nOrder of target fields : "+ destSchema.getNameWithSequence().toString()));

	}
	
	public void printEndSummary(String source, String target) {
		writeTextToFile(source);
		writeTextToFile(target);
		writeTextToFile("\n**********************************************************************************\n");
	}
	public void printFieldTypeComparision(List<Integer> compatibleFields, List<Integer> matched,
			List<Integer> unMatched, TableInfo sourceSchema , TableInfo destSchema) {
		if (unMatched.size()==0 ) {
			if (compatibleFields.size()==0) {
				writeTextToFile("Result = " + AppConstants.MATCH);

			}else {
				writeTextToFile("Result = " + AppConstants.COMPATIBLE);

			}

		}else {
			writeTextToFile("Result = " + AppConstants.MISMATCH);
		}
		writeTextToFile("\nMATCHED("+matched.size()+")    - Source Field(Type) : "+ sourceSchema.getNameWithType(matched).toString());
		writeTextToFile("\nMATCHED("+matched.size()+")    - Target Field(Type) : "+ destSchema.getNameWithType(matched).toString());		

		writeTextToFile("\nCOMPATIBLE("+compatibleFields.size()+") - Source Field(Type) : "+ sourceSchema.getNameWithType(compatibleFields).toString());
		writeTextToFile("\nCOMPATIBLE("+compatibleFields.size()+") - Target Field(Type) : "+ destSchema.getNameWithType(compatibleFields).toString());		
		writeTextToFile("\nUNMATCHED("+unMatched.size()+")  - Source Field(Type) : "+ sourceSchema.getNameWithType(unMatched).toString());
		writeTextToFile("\nUNMATCHED("+unMatched.size()+")  - Target Field(Type) : "+ destSchema.getNameWithType(unMatched).toString());		

		writeTextToFile("\n**********************************************************************************\n");
	}
	
	public void printEndOfReport(long timetaken) {
		writeTextToFile("\n**********************************************************************************\n");
		writeTextToFile("Time Taken in seconds : " +timetaken/1000);
		writeTextToFile("\nEnd of the report!!");
		writeTextToFile("\n**********************************************************************************\n");
	}

	public void printError(Exception e, long timetaken) {
		writeTextToFile("\n************************ Error occured **********************************************************\n");
		writeTextToFile("error msg  : " +e.getLocalizedMessage());
		writeTextToFile("\nTime Taken in seconds : " +timetaken/1000);
		writeTextToFile("\nEnd of the report!!");
		writeTextToFile("\n**********************************************************************************\n");
	}

	public TableInfo getSourceSchema() {
		return sourceSchema;
	}


	public void setSourceSchema(TableInfo sourceSchema) {
		this.sourceSchema = sourceSchema;
	}


	public TableInfo getDestSchema() {
		return destSchema;
	}


	public void setDestSchema(TableInfo destSchema) {
		this.destSchema = destSchema;
	}
	
	public void buildSchemaQueries(Map<String, List<String>> sourceResult , Map<String, List<String>> destResult ) {
		sourceSchema = new TableInfo(sourceResult);
		destSchema = new TableInfo(destResult);
	}

	
}