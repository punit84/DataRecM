package com.datarecm.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.datarecm.service.config.ConfigService;

/**
 * <p>
 * This is a service class with methods for data reconciliation service.
 * <p>
 * 
 * @author Punit Jain
 *
 */

@SpringBootApplication
public class DataRecMApplication {
	public static Log logger = LogFactory.getLog(DataRecMApplication.class);


	@Autowired
	public GlueService glueService;

	@Autowired
	public AthenaService athenaService;

	@Autowired
	SQLRunner sqlRunner ;

	@Autowired
	private ConfigService config ;

	@Autowired
	ReportingService report;

	static TableInfo sourceSchema;
	static TableInfo destSchema;
	@PostConstruct
	public void runRecTest() throws Exception {
		logger.debug("************************");	
		int sourcerulecount=config.source().getRules().size();
		int destinationrulecount=config.destination().getRules().size();
		if (sourcerulecount!=destinationrulecount) {
			logger.error("Rule count must be equal to run the report \n");	
			System.exit(0);
		}

		//		Map<Integer, Map<String, List<Object>>> sourceResultSet= sqlRunner.execuleAllRules();
		//		logger.debug("printing SQL result set");
		//		//logger.debug(sourceResultSet.toString());
		//		
		//		
		//		Map<Integer, Map<String, List<Object>>> destinationResutset = athenaService.runQueriesSync();
		//		logger.debug("printing athena result set");
		//		//logger.debug(destinationResutset.toString());
		//		report.printResult(sourceResultSet, destinationResutset);

		//Running all Athena Queries
		Map<Integer,String> ruleVsQueryid= athenaService.submitAllQueriesAsync();
		report.createReportFile(sourcerulecount, destinationrulecount);
		List<String> rules = config.source().getRules();
		for (int ruleIndex = 0; ruleIndex < rules.size(); ruleIndex++) {
			Map<Integer, Map<String, List<Object>>> sqlResutset= new HashMap<>();

			logger.info("\n*******************Executing Source Query :"+ ruleIndex+" *************");

			String updatedSourceRule=rules.get(ruleIndex);

			Map<String, List<Object>> sourceResult = sqlRunner.executeSQL(ruleIndex , updatedSourceRule);
			//sqlResutset.put(ruleIndex, sourceResult);

			Map<String, List<Object>> destResult   = athenaService.getQueriesResultSync(ruleVsQueryid.get(ruleIndex));
			logger.info("\n*******************Execution successfull *************");

			if (ruleIndex == 1 ) {
				sourceSchema = new TableInfo(sourceResult);
				destSchema = new TableInfo(destResult);
				sourceSchema.setPrimaryKey(config.source().getPrimaryKey());
				destSchema.setPrimaryKey(config.destination().getPrimaryKey());

				QueryBuilder.createQueries(sourceSchema, destSchema , config.source().getIgnoreList());
			}
			report.printRule(ruleIndex, sourceResult, destResult);
		}

	}

	public static void main(String[] args) {
		SpringApplication.run(DataRecMApplication.class, args);	
		System.exit(0);
	}




}
