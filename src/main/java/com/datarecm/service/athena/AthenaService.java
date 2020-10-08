package com.datarecm.service.athena;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.datarecm.service.config.AppConfig;
import com.datarecm.service.config.AppConstants;
import com.datarecm.service.config.DBConfig;

import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.AthenaException;
import software.amazon.awssdk.services.athena.model.ColumnInfo;
import software.amazon.awssdk.services.athena.model.Datum;
import software.amazon.awssdk.services.athena.model.GetQueryExecutionRequest;
import software.amazon.awssdk.services.athena.model.GetQueryExecutionResponse;
import software.amazon.awssdk.services.athena.model.GetQueryResultsRequest;
import software.amazon.awssdk.services.athena.model.GetQueryResultsResponse;
import software.amazon.awssdk.services.athena.model.QueryExecutionContext;
import software.amazon.awssdk.services.athena.model.QueryExecutionState;
import software.amazon.awssdk.services.athena.model.ResultConfiguration;
import software.amazon.awssdk.services.athena.model.Row;
import software.amazon.awssdk.services.athena.model.StartQueryExecutionRequest;
import software.amazon.awssdk.services.athena.model.StartQueryExecutionResponse;
import software.amazon.awssdk.services.athena.paginators.GetQueryResultsIterable;

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
	private Map<String, Map<String, List<String>>> athenaResutset= new HashMap<>();
	private Map<String,String> ruleVsQueryid = new HashMap<>();

	public static final long SLEEP_AMOUNT_IN_MS = 1000;
	private AthenaClient athenaClient = null;

	private AppConfig appConfig ;

	private DBConfig target;


	public AppConfig getAppConfig() {
		return appConfig;
	}

	public void setAppConfig(AppConfig appConfig) {
		this.appConfig = appConfig;
	}

	public DBConfig getTarget() {
		return target;
	}

	public void setTarget(DBConfig target) {
		this.target = target;
	}

	public Map<String, Map<String, List<String>>> runQueriesSync() throws InterruptedException
	{
		// Build an AmazonAthena client

		submitAllQueriesAsync();
		List<String> rules = appConfig.getTargetRules();

		for (int index = 0; index < rules.size(); index++) {
			Map<String, List<String>> map = getProcessedQueriesResultSync(index);
			athenaResutset.put((target.getAccessKey()+index), map);
		}

		//logger.debug(athenaResutset.toString());
		return athenaResutset;

	}

	public Map<String, List<String>> getProcessedQueriesResultSync(int queryIndex) throws InterruptedException
	{	
		GetQueryResultsRequest getQueryResultsRequest = getQueriesResultSync(queryIndex);

		return processResultRows(athenaClient, getQueryResultsRequest);

	}

	public GetQueryResultsRequest getQueriesResultSync(int queryIndex) throws InterruptedException{

		String queryid = ruleVsQueryid.get(target.getAccessKey()+queryIndex);
		logger.info("Athena ruleVsQueryid  :"+ ruleVsQueryid.toString());

		logger.info("Athena Query ID :"+ target.getAccessKey());
		try {
			waitForQueryToComplete(athenaClient, queryid);
		} catch (Exception e) {
			logger.error(e.getMessage());	
		}


		GetQueryResultsRequest getQueryResultsRequest = GetQueryResultsRequest.builder()
				// Max Results can be set but if its not set,
				// it will choose the maximum page size
				// As of the writing of this code, the maximum value is 1000
				.maxResults(1000)
				.queryExecutionId(queryid).build();

		return getQueryResultsRequest;
	}

	public void submitAllQueriesAsync() throws InterruptedException
	{
		// Build an AmazonAthena client
		List<String> rules = appConfig.getTargetRules();
		logger.info("Running Athena query ..");

		for (int index = 0; index < rules.size(); index++) {
			//logger.debug("*******************Executing Destination Query :"+ index+" *************");
			String updatedRule=rules.get(index);
			submitQuery(index, updatedRule);
		}

		logger.info("Athena query placed successfully");
	}


	public String submitQuery(int index, String updatedRule) throws InterruptedException
	{
		//logger.debug("*******************Executing Destination Query :"+ index+" *************");
		updatedRule = updatedRule.replace(AppConstants.TABLENAME, target.getTableName());
		updatedRule = updatedRule.replace(AppConstants.TABLESCHEMA, target.getDbname());
		logger.debug("QUERY NO "+ index+ " is "+updatedRule);

		logger.info("***********  Running Query *******");
		logger.info(updatedRule);
		String queryExecutionId = submitAthenaQuery(getAmazonAthenaClient(),updatedRule);
		ruleVsQueryid.put(target.getAccessKey()+index,queryExecutionId);
		logger.info("*******************Execution successfull *************");

		return queryExecutionId;

	}
	public synchronized AthenaClient getAmazonAthenaClient() {
		if (athenaClient==null) {
			athenaClient = factory.createClient(target.getRegion());
		}
		return athenaClient;

	}

	/**
	 * Submits a sample query to Athena and returns the execution ID of the query.
	 */
	public String submitAthenaQuery(AthenaClient athenaClient,String rule) {

		try {

			// The QueryExecutionContext allows us to set the Database.
			QueryExecutionContext queryExecutionContext = QueryExecutionContext.builder()
					.database(target.getDbname()).build();

			// The result configuration specifies where the results of the query should go in S3 and encryption options
			ResultConfiguration resultConfiguration = ResultConfiguration.builder()
					// You can provide encryption options for the output that is written.
					// .withEncryptionConfiguration(encryptionConfiguration)
					.outputLocation(target.getAtheneOutputDir()).build();

			// Create the StartQueryExecutionRequest to send to Athena which will start the query.
			StartQueryExecutionRequest startQueryExecutionRequest = StartQueryExecutionRequest.builder()
					.queryString(rule)
					.queryExecutionContext(queryExecutionContext)
					.resultConfiguration(resultConfiguration).build();

			StartQueryExecutionResponse startQueryExecutionResponse = athenaClient.startQueryExecution(startQueryExecutionRequest);
			return startQueryExecutionResponse.queryExecutionId();

		} catch (AthenaException e) {
			e.printStackTrace();
		}
		return "";
	}

	/**
	 * Wait for an Athena query to complete, fail or to be cancelled. This is done by polling Athena over an
	 * interval of time. If a query fails or is cancelled, then it will throw an exception.
	 */

	public static void waitForQueryToComplete(AthenaClient athenaClient, String queryExecutionId) throws InterruptedException {
		GetQueryExecutionRequest getQueryExecutionRequest = GetQueryExecutionRequest.builder()
				.queryExecutionId(queryExecutionId).build();

		GetQueryExecutionResponse getQueryExecutionResponse;
		boolean isQueryStillRunning = true;
		while (isQueryStillRunning) {
			getQueryExecutionResponse = athenaClient.getQueryExecution(getQueryExecutionRequest);
			String queryState = getQueryExecutionResponse.queryExecution().status().state().toString();
			if (queryState.equals(QueryExecutionState.FAILED.toString())) {
				throw new RuntimeException("Query Failed to run with Error Message: " + getQueryExecutionResponse
						.queryExecution().status().stateChangeReason());
			} else if (queryState.equals(QueryExecutionState.CANCELLED.toString())) {
				throw new RuntimeException("Query was cancelled.");
			} else if (queryState.equals(QueryExecutionState.SUCCEEDED.toString())) {
				isQueryStillRunning = false;
			} else {
				// Sleep an amount of time before retrying again.
				Thread.sleep(SLEEP_AMOUNT_IN_MS);
			}
			System.out.println("Current Status is: " + queryState);
		}
	}


	//	/**
	//	 * This code calls Athena and retrieves the results of a query.
	//	 * The query must be in a completed state before the results can be retrieved and
	//	 * paginated. The first row of results are the column headers.
	//	 */
	//	public Map<String, List<String>>  processResultRowss(AthenaClient athenaClient, GetQueryResultsRequest getQueryResultsRequest) {
	//
	//		try {
	//
	//
	//
	//			GetQueryResultsIterable getQueryResultsResults = athenaClient.getQueryResultsPaginator(getQueryResultsRequest);
	//
	//			for (GetQueryResultsResponse result : getQueryResultsResults) {
	//				List<ColumnInfo> columnInfoList = result.resultSet().resultSetMetadata().columnInfo();
	//				List<Row> results = result.resultSet().rows();
	//				processRow(results, columnInfoList);
	//			}
	//
	//		} catch (AthenaException e) {
	//			e.printStackTrace();
	//			System.exit(1);
	//		}
	//	}

	/**
	 * This code calls Athena and retrieves the results of a query.
	 * The query must be in a completed state before the results can be retrieved and
	 * paginated. The first row of results are the column headers.
	 */
	private Map<String, List<String>> processResultRows(AthenaClient athenaClient, GetQueryResultsRequest getQueryResultsRequest)
	{		
		GetQueryResultsResponse getQueryResults = athenaClient.getQueryResults(getQueryResultsRequest);


		List<ColumnInfo> columnInfoList = getQueryResults.resultSet().resultSetMetadata().columnInfo();
		int columnsNumber = columnInfoList.size();

		Map<String, List<String>> map = new HashMap<>(columnInfoList.size());

		for (int i = 0; i < columnsNumber; ++i) {
			map.put(columnInfoList.get(i).name(), new ArrayList<>());
		}
		logger.info(columnInfoList);
		List<Row> results = getQueryResults.resultSet().rows();
		Row fistRow=results.get(0);				// Process the row. The first row of the first page holds the column names.

		for (int i = 1; i < results.size(); i++) {
			Row row=results.get(i);  
			if (row==null) {
				continue;
			}
			// Process the row. The first row of the first page holds the column names.
			for (int j = 0; j < fistRow.data().size(); j++) {
				String columnName=fistRow.data().get(j).varCharValue();
				List<String> columList = map.get(columnName);
				//logger.debug(row.getData().get(j).getVarCharValue());
				String result=row.data().get(j).varCharValue();
				columList.add(result);

				logger.debug(result);

			}
		}
	
		return map;
	}

	private static void processRow(List<Row> row, List<ColumnInfo> columnInfoList) {

		//Write out the data
		for (Row myRow : row) {
			List<Datum> allData = myRow.data();
			for (Datum data : allData) {
				System.out.println("The value of the column is "+data.varCharValue());
			}
		}
	}


}
