//snippet-sourcedescription:[StartQueryExample.java demonstrates how to submit a query to Athena for execution, wait till results are available, and then process the results.]
//snippet-keyword:[Java]
//snippet-sourcesyntax:[java]
//snippet-keyword:[Code Sample]
//snippet-keyword:[Amazon Athena]
//snippet-service:[athena]
//snippet-sourcetype:[full-example]
//snippet-sourcedate:[2018-06-25]
//snippet-sourceauthor:[soo-aws]
package com.datarecm.service;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.athena.model.ColumnInfo;
import com.amazonaws.services.athena.model.GetQueryExecutionRequest;
import com.amazonaws.services.athena.model.GetQueryExecutionResult;
import com.amazonaws.services.athena.model.GetQueryResultsRequest;
import com.amazonaws.services.athena.model.GetQueryResultsResult;
import com.amazonaws.services.athena.model.QueryExecutionContext;
import com.amazonaws.services.athena.model.QueryExecutionState;
import com.amazonaws.services.athena.model.ResultConfiguration;
import com.amazonaws.services.athena.model.Row;
import com.amazonaws.services.athena.model.StartQueryExecutionRequest;
import com.amazonaws.services.athena.model.StartQueryExecutionResult;
import com.datarecm.service.athena.AthenaClientFactory;
import com.datarecm.service.config.ConfigService;

/**
 * AthenaService
 * -------------------------------------
 * This code shows how to submit a query to Athena for execution, wait till results
 * are available, and then process the results.
 * @author Punit Jain, Amazon Web Services, Inc.
 */
@Component
public class AthenaService
{
	AthenaClientFactory factory = new AthenaClientFactory();

	@Autowired
	private ConfigService config ;

	public void runQueries() throws InterruptedException
	{
		// Build an AmazonAthena client
		AmazonAthena athenaClient = factory.createClient(config.destination().getRegion());

		String queryExecutionId = submitAthenaQuery(athenaClient,config.destination().getRule1());

		waitForQueryToComplete(athenaClient, queryExecutionId);

		processResultRows(athenaClient, queryExecutionId);
	}

	/**
	 * Submits a sample query to Athena and returns the execution ID of the query.
	 */
	private String submitAthenaQuery(AmazonAthena athenaClient,String rule)
	{
		// The QueryExecutionContext allows us to set the Database.
		QueryExecutionContext queryExecutionContext = new QueryExecutionContext().withDatabase(config.destination().getDbname());

		// The result configuration specifies where the results of the query should go in S3 and encryption options
		ResultConfiguration resultConfiguration = new ResultConfiguration()
				// You can provide encryption options for the output that is written.
				// .withEncryptionConfiguration(encryptionConfiguration)
				.withOutputLocation(config.destination().getOutput());

		// Create the StartQueryExecutionRequest to send to Athena which will start the query.
		StartQueryExecutionRequest startQueryExecutionRequest = new StartQueryExecutionRequest()
				.withQueryString(rule)
				.withQueryExecutionContext(queryExecutionContext)
				.withResultConfiguration(resultConfiguration);

		StartQueryExecutionResult startQueryExecutionResult = athenaClient.startQueryExecution(startQueryExecutionRequest);
		return startQueryExecutionResult.getQueryExecutionId();
	}

	/**
	 * Wait for an Athena query to complete, fail or to be cancelled. This is done by polling Athena over an
	 * interval of time. If a query fails or is cancelled, then it will throw an exception.
	 */

	private static void waitForQueryToComplete(AmazonAthena athenaClient, String queryExecutionId) throws InterruptedException
	{
		GetQueryExecutionRequest getQueryExecutionRequest = new GetQueryExecutionRequest()
				.withQueryExecutionId(queryExecutionId);

		GetQueryExecutionResult getQueryExecutionResult = null;
		boolean isQueryStillRunning = true;
		while (isQueryStillRunning) {
			getQueryExecutionResult = athenaClient.getQueryExecution(getQueryExecutionRequest);
			String queryState = getQueryExecutionResult.getQueryExecution().getStatus().getState();
			if (queryState.equals(QueryExecutionState.FAILED.toString())) {
				throw new RuntimeException("Query Failed to run with Error Message: " + getQueryExecutionResult.getQueryExecution().getStatus().getStateChangeReason());
			}
			else if (queryState.equals(QueryExecutionState.CANCELLED.toString())) {
				throw new RuntimeException("Query was cancelled.");
			}
			else if (queryState.equals(QueryExecutionState.SUCCEEDED.toString())) {
				isQueryStillRunning = false;
			}
			else {
				// Sleep an amount of time before retrying again.
				Thread.sleep(AppConstants.SLEEP_AMOUNT_IN_MS);
			}
			//System.out.println("Current Status is: " + queryState);
			System.out.println(getQueryExecutionResult.toString());
		}
	}

	/**
	 * This code calls Athena and retrieves the results of a query.
	 * The query must be in a completed state before the results can be retrieved and
	 * paginated. The first row of results are the column headers.
	 */
	private void processResultRows(AmazonAthena athenaClient, String queryExecutionId)
	{
		GetQueryResultsRequest getQueryResultsRequest = new GetQueryResultsRequest()
				// Max Results can be set but if its not set,
				// it will choose the maximum page size
				// As of the writing of this code, the maximum value is 1000
				// .withMaxResults(1000)
				.withQueryExecutionId(queryExecutionId);

		GetQueryResultsResult getQueryResultsResult = athenaClient.getQueryResults(getQueryResultsRequest);
		List<ColumnInfo> columnInfoList = getQueryResultsResult.getResultSet().getResultSetMetadata().getColumnInfo();

		while (true) {
			List<Row> results = getQueryResultsResult.getResultSet().getRows();
			System.out.println(results.toString());

			for (Row row : results) {
				// Process the row. The first row of the first page holds the column names.
				processRow(row, columnInfoList);
			}
			// If nextToken is null, there are no more pages to read. Break out of the loop.
			if (getQueryResultsResult.getNextToken() == null) {
				break;
			}
			getQueryResultsResult = athenaClient.getQueryResults(
					getQueryResultsRequest.withNextToken(getQueryResultsResult.getNextToken()));

		}
	}

	private void processRow(Row row, List<ColumnInfo> columnInfoList)
	{
		for (int i = 0; i < columnInfoList.size(); ++i) {
			System.out.println(columnInfoList.get(i).getType());
			switch (columnInfoList.get(i).getType()) {
			case "varchar":
				System.out.println("varcar");
				// Convert and Process as String
				break;
			case "tinyint":
				// Convert and Process as tinyint
				break;
			case "smallint":
				// Convert and Process as smallint
				break;
			case "integer":
				// Convert and Process as integer
				break;
			case "bigint":
				// Convert and Process as bigint
				System.out.println(row.getData().get(i).getVarCharValue());
				break;
			case "double":
				// Convert and Process as double
				break;
			case "boolean":
				// Convert and Process as boolean
				break;
			case "date":
				// Convert and Process as date
				break;
			case "timestamp":
				// Convert and Process as timestamp
				break;
			default:
				throw new RuntimeException("Unexpected Type is not expected" + columnInfoList.get(i).getType());
			}
		}
	}
}
