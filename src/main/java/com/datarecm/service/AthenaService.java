package com.datarecm.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import com.datarecm.service.config.ConfigProperties;
import com.datarecm.service.config.ConfigService;

/**
 * AthenaService
 * -------------------------------------
 * This code shows how to submit a query to Athena for execution, wait till results
 * are available, and then process the results.
 * @author Punit Jain
 */
@Component
public class AthenaService
{
	public static Log logger = LogFactory.getLog(AthenaService.class);

	AthenaClientFactory factory = new AthenaClientFactory();
	public static Map<Integer, Map<String, List<Object>>> athenaResutset= new HashMap<>();
	public static  Map<Integer,String> ruleVsQueryid = new HashMap<>();

	public static final long SLEEP_AMOUNT_IN_MS = 1000;
	private AmazonAthena athenaClient = null;
	@Autowired
	private ConfigService config ;

	public Map<Integer, Map<String, List<Object>>> runQueriesSync() throws InterruptedException
	{
		// Build an AmazonAthena client

		submitAllQueriesAsync();
		List<String> rules = config.destination().getRules();

		for (int index = 0; index < rules.size(); index++) {
			Map<String, List<Object>> map = getProcessedQueriesResultSync(ruleVsQueryid.get(index));
			athenaResutset.put(index, map);
		}

		//logger.debug(athenaResutset.toString());
		return athenaResutset;

	}

	public Map<String, List<Object>> getProcessedQueriesResultSync(String queryid) throws InterruptedException
	{
		GetQueryResultsRequest getQueryResultsRequest = getQueriesResultSync(queryid);
		
		return processResultRows(athenaClient, getQueryResultsRequest);

	}

	public GetQueryResultsRequest getQueriesResultSync(String queryid) throws InterruptedException{
		try {
			waitForQueryToComplete(athenaClient, queryid);
		} catch (Exception e) {
			logger.error(e.getMessage());	
		}
		GetQueryResultsRequest getQueryResultsRequest = new GetQueryResultsRequest()
				// Max Results can be set but if its not set,
				// it will choose the maximum page size
				// As of the writing of this code, the maximum value is 1000
			    .withMaxResults(1000)
				.withQueryExecutionId(queryid);

		return getQueryResultsRequest;
	}

	public void submitAllQueriesAsync() throws InterruptedException
	{
		// Build an AmazonAthena client
		List<String> rules = config.destination().getRules();

		for (int index = 0; index < rules.size(); index++) {
			//logger.debug("*******************Executing Destination Query :"+ index+" *************");
			String updatedRule=rules.get(index);
			submitQuery(index, updatedRule);
		}

		//logger.debug(athenaResutset.toString());
	}


	public void submitQuery(int index, String updatedRule) throws InterruptedException
	{
		//logger.debug("*******************Executing Destination Query :"+ index+" *************");
		updatedRule = updatedRule.replace(ConfigProperties.TABLENAME, config.destination().getTableName());
		updatedRule = updatedRule.replace(ConfigProperties.TABLESCHEMA, config.destination().getTableSchema());
		logger.debug("QUERY NO "+ index+ " is "+updatedRule);

		String queryExecutionId = submitAthenaQuery(getAmazonAthenaClient(),updatedRule);
		ruleVsQueryid.put( index,queryExecutionId);
		logger.debug("*******************Execution successfull *************");

	}
	public synchronized AmazonAthena getAmazonAthenaClient() {
		if (athenaClient==null) {
			athenaClient = factory.createClient(config.destination().getRegion());
		}
		return athenaClient;

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
				logger.debug(getQueryExecutionResult.getQueryExecution().toString());
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
				Thread.sleep(SLEEP_AMOUNT_IN_MS);
			}
			//logger.debug("Current Status is: " + queryState);
			//athenaResutset.put(key, getQueryExecutionResult.toString())
			//logger.debug(getQueryExecutionResult.toString());
		}
	}

	/**
	 * This code calls Athena and retrieves the results of a query.
	 * The query must be in a completed state before the results can be retrieved and
	 * paginated. The first row of results are the column headers.
	 */
	private Map<String, List<Object>> processResultRows(AmazonAthena athenaClient, GetQueryResultsRequest getQueryResultsRequest)
	{		
		GetQueryResultsResult getQueryResults = athenaClient.getQueryResults(getQueryResultsRequest);

		List<ColumnInfo> columnInfoList = getQueryResults.getResultSet().getResultSetMetadata().getColumnInfo();
		int columnsNumber = columnInfoList.size();

		Map<String, List<Object>> map = new HashMap<>(columnInfoList.size());

		for (int i = 0; i < columnsNumber; ++i) {
			map.put(columnInfoList.get(i).getName(), new ArrayList<>());
		}
		while (true) {
			List<Row> results = getQueryResults.getResultSet().getRows();
			Row fistRow=results.get(0);				// Process the row. The first row of the first page holds the column names.

			for (int i = 1; i < results.size(); i++) {
				Row row=results.get(i);                // Process the row. The first row of the first page holds the column names.
				// Process the row. The first row of the first page holds the column names.
				for (int j = 0; j < fistRow.getData().size(); j++) {
					String columnName=fistRow.getData().get(j).getVarCharValue();
					List columList = map.get(columnName);
					//logger.debug(row.getData().get(j).getVarCharValue());

					String result=row.getData().get(j).getVarCharValue();
					columList.add(result);

					//logger.debug(result);

				}
			}


			// If nextToken is null, there are no more pages to read. Break out of the loop.
			if (getQueryResults.getNextToken() == null) {
				break;
			}
			getQueryResults = athenaClient.getQueryResults(getQueryResultsRequest.withNextToken(getQueryResults.getNextToken()));

		}
		return map;
	}


	//	private void processRowIntoColumList(Row row, List<ColumnInfo> columnInfoList){
	//		
	//	}
/*
	private void processRow(Row row, List<ColumnInfo> columnInfoList)
	{
		for (int i = 0; i < columnInfoList.size(); ++i) {
			logger.debug(columnInfoList.get(i).getType());
			switch (columnInfoList.get(i).getType()) {
			case "varchar":
				logger.debug("varcar");
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
				logger.debug(row.getData().get(i).getVarCharValue());
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
	*/
}
