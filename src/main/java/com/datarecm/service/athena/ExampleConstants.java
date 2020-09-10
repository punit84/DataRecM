//snippet-sourcedescription:[ExampleConstants.java demonstrates how to query a table created by the getting started tutorial in Athena]
//snippet-keyword:[Java]
//snippet-sourcesyntax:[java]
//snippet-keyword:[Code Sample]
//snippet-keyword:[Amazon Athena]
//snippet-service:[athena]
//snippet-sourcetype:[full-example]
//snippet-sourcedate:[2018-06-25]
//snippet-sourceauthor:[soo-aws]
package com.datarecm.service.athena;

public class ExampleConstants {

	public static final int CLIENT_EXECUTION_TIMEOUT = 100000;
	public static final String ATHENA_OUTPUT_BUCKET = "s3://query-result1234";
	// This is querying a table created by the getting started tutorial in Athena
	public static final String ATHENA_SAMPLE_QUERY = "SELECT count(*) FROM \"unicorngym\".\"unicorn_gym_2020\" ;";
	public static final long SLEEP_AMOUNT_IN_MS = 1000;
	public static final String ATHENA_DEFAULT_DATABASE = "unicorngym";

}
