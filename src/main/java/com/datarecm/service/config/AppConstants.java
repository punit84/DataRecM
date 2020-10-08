package com.datarecm.service.config;


/**
 * @author Punit Jain
 *
 */
public class AppConstants {

//	public static final int CLIENT_EXECUTION_TIMEOUT = 100000;
//	public static final String ATHENA_OUTPUT_BUCKET = "s3://query-result1234";
//	// This is querying a table created by the getting started tutorial in Athena
//	public static final String ATHENA_SAMPLE_QUERY = "SELECT count(*) FROM \"unicorngym\".\"unicorn_gym_2020\" ;";
//	public static final String ATHENA_DEFAULT_DATABASE = "unicorngym";

	
	public static final String FILE_TYPE_PARQUET = "parquet";
	public static final String FILE_TYPE_CSV = "csv";			
	public static final String DB_TYPE_POSTGRES = "postgres";
	public static final String DB_TYPE_MYSQL = "mysql";
	public static final String MISMATCH = "MISMATCH";
	public static final String COMPATIBLE = "MISMATCH BUT COMPATIBLE";

	public static final String MATCH = "MATCH";
	
	public static final String TABLENAME="<TABLENAME>";
	public static final String TABLESCHEMA="<TABLESCHEMA>";
	public static final String MD5FILEPREFIX = "MD5Results-";

	public static enum TargetType{CSV, PARQUET,UNKNOWN};
	public static enum SourceType{MYSQL, POSTGRES, MSSQL,UNKNOWN};


	
	
}

