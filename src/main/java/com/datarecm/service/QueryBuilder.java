package com.datarecm.service;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.datarecm.service.AppConstants.TargetType;

/**
 * Building dynamic queries for Reconsilation using MD%
 * @author Punit Jain
 *
 */

@Component
public class QueryBuilder {
	public static Log logger = LogFactory.getLog(QueryBuilder.class);

	//source.rules[4]=select order_id, md5(CAST((order_id||customer_id||order_status||order_date||product_id||cast(product_price as numeric(30,2))||qty||order_value) AS text)) from <TABLESCHEMA>.\"<TABLENAME>\" order by order_id limit 1000;
	//destination.rules[4]=select order_id, md5(to_utf8(cast(order_id as varchar)||cast(customer_id as varchar)|| cast(order_status as varchar)|| cast(order_date as varchar)|| cast(product_id as varchar)|| cast(cast(product_price as decimal(30,2)) as varchar)|| cast(qty as varchar)|| cast(cast(order_value as decimal(30,2)) as varchar)))FROM \"<TABLESCHEMA>\".\"<TABLENAME>\" order by order_id limit 1000;

	//	select order_id, lower(to_hex(md5(to_utf8(cast(order_id as varchar)||cast(customer_id as varchar)|| cast(order_status as varchar)|| cast(order_date as varchar)||cast(substr(delivery_date,1,19) as varchar)||cast(product_id as varchar)||
	//         cast(cast(product_price as decimal(30,2)) as varchar)||cast(qty as varchar)||
	//      cast(cast(order_value as decimal(30,2)) as varchar))))) FROM default.orders order by order_id limit 100

	//	#source.rules[3]=select * from <TABLESCHEMA>.\"<TABLENAME>\" limit 1;"
	public void createFetchUnmatchedDataQueries(TableInfo sourceSchema,TableInfo destSchema, List<String> unMatchedIDs) {


		StringBuilder sourceQuery = new  StringBuilder();
		StringBuilder destQuery = new  StringBuilder();
        String primaryKey = sourceSchema.getPrimaryKey();
        
		sourceQuery.append("where ");
		destQuery.append("where ");

		sourceQuery.append(primaryKey);
		destQuery.append(primaryKey);

		sourceQuery.append(" IN ");
		destQuery.append(" IN ");

		sourceQuery.append(unMatchedIDs.toString());
		destQuery.append(unMatchedIDs.toString());

		String sourceQueryStr=sourceQuery.toString();
		String destQueryStr=destQuery.toString();

		sourceQueryStr = sourceQueryStr.replace("[", "(");
		sourceQueryStr = sourceQueryStr.replace("]", ")");

		destQueryStr = destQueryStr.replace("[", "(");
		destQueryStr = destQueryStr.replace("]", ")");

		sourceSchema.setFetchUnmatchRecordQuery(sourceSchema.getFetchUnmatchRecordQuery() + sourceQueryStr);
		destSchema.setFetchUnmatchRecordQuery(destSchema.getFetchUnmatchRecordQuery()+ destQueryStr);

		//logger.info("Source Unmatched Query is :" +sourceSchema.getFetchUnmatchRecordQuery());
		//logger.info("Dest Unmatched Query is :" +destSchema.getFetchUnmatchRecordQuery());

	}


	public void createFetchDataQueriesParquet(TableInfo sourceSchema,TableInfo destSchema, List<String> ignoreList ) {
		//If Data Source==’Postgres’ and Target Data Format==’Parquet’ then
		//cast(product_price as numeric(30,2))||qty||order_value) AS text)) from <TABLESCHEMA>.\"<TABLENAME>\" order by order_id limit 1000;
		//destination.rules[4]=select order_id, md5(to_utf8(cast(order_id as varchar)||cast(customer_id as varchar)|| cast(order_status as varchar)|| cast(order_date as varchar)|| cast(product_id as varchar)|| cast(cast(product_price as decimal(30,2)) as varchar)|| cast(qty as varchar)|| cast(cast(order_value as decimal(30,2)) as varchar)))FROM \"<TABLESCHEMA>\".\"<TABLENAME>\" order by order_id limit 1000;
		
		//SELECT order_id, customer_id,order_status,order_date,delivery_date,product_id,product_price, qty,order_value from <TABLESCHEMA>."<TABLENAME>"  where order_id IN (36407, 36408, 36409, 36400, 36401, 36402, 36403, 36404, 36405, 36406)  ;


		//SELECT cast(order_id as varchar),cast(customer_id as varchar),cast(order_status as varchar),cast(order_date as varchar),cast(substr(delivery_date,1,19) as varchar),cast(product_id as varchar),cast(product_price as varchar),cast(qty as varchar),cast(cast(order_value as decimal(30,2) ) as varchar)) from "<TABLESCHEMA>"."<TABLENAME>"


		StringBuilder sourceQuery = new  StringBuilder();
		StringBuilder destQuery = new  StringBuilder();
		
		StringBuilder sourceUnmatchQuery = new  StringBuilder();
		StringBuilder destUnmatchQuery = new  StringBuilder();
		
		
		String primaryKey = sourceSchema.getPrimaryKey();
		sourceQuery.append("SELECT ");
		sourceQuery.append(primaryKey);
		sourceQuery.append(", md5(CAST(( ");

		destQuery.append("SELECT ");
		destQuery.append(primaryKey);
		destQuery.append(", lower(to_hex( md5(to_utf8(");

		
		sourceUnmatchQuery.append("SELECT ");
		destUnmatchQuery.append("SELECT ");

	
		for (int index = 0; index < sourceSchema.getFieldCount(); index++) {
			String sourceFieldName = sourceSchema.getColumnNameList().get(index).toString();
			String sourceFieldType = sourceSchema.getColumnTypeList().get(index).toString();

			if (ignoreList.contains(sourceFieldName.toLowerCase())) {
				logger.info("ignoring " + sourceFieldName+" Type as " +sourceFieldType);
				continue;
			}
			if (index > 0) {
				sourceQuery.append("||");
				destQuery.append("||");
				sourceUnmatchQuery.append(",");
				destUnmatchQuery.append(",");
			}
			destQuery.append("cast(");
			destUnmatchQuery.append("cast(");

			switch (sourceFieldType.toLowerCase()) {

			case "numeric":
			case "numeric(12,2)":
				sourceQuery.append(sourceFieldName);
				destQuery.append("cast("+sourceFieldName+" as decimal(30,2) )");
				
				sourceUnmatchQuery.append(sourceFieldName);
				destUnmatchQuery.append("cast("+sourceFieldName+" as decimal(30,2) )");
				break;

			case "float4":
				sourceQuery.append("cast("+sourceFieldName+" ::float4::text::float8 as numeric(30,1))");
				destQuery.append("round("+sourceFieldName+" ,1)");

				sourceUnmatchQuery.append("cast("+sourceFieldName+" ::float4::text::float8 as numeric(30,1))");
				destUnmatchQuery.append("round("+sourceFieldName+" ,1)");

				break;

			case "float8":
				sourceQuery.append("cast("+sourceFieldName+" as numeric(30,2))");
				destQuery.append("cast("+sourceFieldName +" as decimal(30,2))");

				sourceUnmatchQuery.append("cast("+sourceFieldName+" as numeric(30,2))");
				destUnmatchQuery.append("cast("+sourceFieldName +" as decimal(30,2))");

				
				break;
			case "money":
				sourceQuery.append("cast("+sourceFieldName +" as numeric(30,2))");
				destQuery.append("cast("+sourceFieldName +" as decimal(30,2))");

				sourceUnmatchQuery.append("cast("+sourceFieldName +" as numeric(30,2))");
				destUnmatchQuery.append("cast("+sourceFieldName +" as decimal(30,2))");

				break;

			default:
				sourceQuery.append(sourceFieldName);
				destQuery.append(sourceFieldName);
				
				sourceUnmatchQuery.append(sourceFieldName);
				destUnmatchQuery.append(sourceFieldName);

				break;
			}

			destQuery.append(" as varchar)");
			destUnmatchQuery.append(" as varchar) as "+sourceFieldName);


		}
		sourceQuery.append(") AS text)");
		sourceQuery.append(") from <TABLESCHEMA>.\"<TABLENAME>\" ");

		//		sourceQuery.append("order by ");
		//		sourceQuery.append(primaryKey);
		sourceQuery.append( " ;");
	
		destQuery.append(")))) as md5 from \"<TABLESCHEMA>\".\"<TABLENAME>\" ");
	
		sourceUnmatchQuery.append(" from <TABLESCHEMA>.\"<TABLENAME>\" ");
		destUnmatchQuery.append(" from \"<TABLESCHEMA>\".\"<TABLENAME>\" ");

		//		sourceQuery.append("order by ");

		//		destQuery.append(primaryKey);
		destQuery.append( " ;");

		logger.info("Source Query is :" +sourceQuery);
		logger.info("Dest Query is :" +destQuery);
		
		sourceSchema.setQuery(sourceQuery.toString());
		destSchema.setQuery(destQuery.toString());

		sourceSchema.setFetchUnmatchRecordQuery(sourceUnmatchQuery.toString());
		destSchema.setFetchUnmatchRecordQuery(destUnmatchQuery.toString());

	}
	
	public void createFetchDataQueriesCSV(TableInfo sourceSchema,TableInfo destSchema, List<String> ignoreList ) {
		//If Data Source==’Postgres’ and Target Data Format==’Parquet’ then
		//cast(product_price as numeric(30,2))||qty||order_value) AS text)) from <TABLESCHEMA>.\"<TABLENAME>\" order by order_id limit 1000;
		//destination.rules[4]=select order_id, md5(to_utf8(cast(order_id as varchar)||cast(customer_id as varchar)|| cast(order_status as varchar)|| cast(order_date as varchar)|| cast(product_id as varchar)|| cast(cast(product_price as decimal(30,2)) as varchar)|| cast(qty as varchar)|| cast(cast(order_value as decimal(30,2)) as varchar)))FROM \"<TABLESCHEMA>\".\"<TABLENAME>\" order by order_id limit 1000;
	 	StringBuilder sourceQuery = new  StringBuilder();
		StringBuilder destQuery = new  StringBuilder();
		StringBuilder sourceUnmatchQuery = new  StringBuilder();
		StringBuilder destUnmatchQuery = new  StringBuilder();

		
		String primaryKey = sourceSchema.getPrimaryKey();
		sourceQuery.append("SELECT ");
		sourceQuery.append(primaryKey);
		sourceQuery.append(", md5(CAST(( ");

		destQuery.append("SELECT ");
		destQuery.append(primaryKey);
		destQuery.append(", lower(to_hex( md5(to_utf8(");
		
		sourceUnmatchQuery.append("SELECT ");
		destUnmatchQuery.append("SELECT ");

		for (int index = 0; index < sourceSchema.getFieldCount(); index++) {
			String sourceFieldName = sourceSchema.getColumnNameList().get(index).toString();
			String sourceFieldType = sourceSchema.getColumnTypeList().get(index).toString();

			if (ignoreList.contains(sourceFieldName.toLowerCase())) {
				logger.info("ignoring " + sourceFieldName+" Type as " +sourceFieldType);
				continue;
			}
			if (index > 0) {
				sourceQuery.append("||");
				destQuery.append("||");
				
				sourceUnmatchQuery.append(",");
				destUnmatchQuery.append(",");


			}
			destQuery.append("cast(");
			destUnmatchQuery.append("cast(");

			switch (sourceFieldType.toLowerCase()) {

			case "numeric":
			case "numeric(12,2)":
				sourceQuery.append(sourceFieldName);
				destQuery.append("cast("+sourceFieldName+" as decimal(30,2) )");
				
				sourceUnmatchQuery.append(sourceFieldName);
				destUnmatchQuery.append("cast("+sourceFieldName+" as decimal(30,2) )");

				break;
				
			case "money":
				sourceQuery.append("cast("+sourceFieldName +" as numeric(30,2))");
				destQuery.append(sourceFieldName);
				
				sourceUnmatchQuery.append("cast("+sourceFieldName +" as numeric(30,2))");
				destUnmatchQuery.append(sourceFieldName);

				break;

			case "timestamp":
				sourceQuery.append(sourceFieldName);
	        	destQuery.append("substr("+sourceFieldName+",1,19)");
	        	
	        	sourceUnmatchQuery.append(sourceFieldName);
	        	destUnmatchQuery.append("substr("+sourceFieldName+",1,19)");

				break;

			default:
				sourceQuery.append(sourceFieldName);
				destQuery.append(sourceFieldName);
				
				sourceUnmatchQuery.append(sourceFieldName);
				destUnmatchQuery.append(sourceFieldName);

				break;
			}

			destQuery.append(" as varchar)");

			destUnmatchQuery.append(" as varchar) as "+sourceFieldName);

		}
		sourceQuery.append(") AS text)");
		sourceQuery.append(") from <TABLESCHEMA>.\"<TABLENAME>\" ");
		//		sourceQuery.append("order by ");
		//		sourceQuery.append(primaryKey);
		sourceQuery.append( " ;");


		destQuery.append(")))) as md5 from \"<TABLESCHEMA>\".\"<TABLENAME>\" ");
		//		sourceQuery.append("order by ");

		//		destQuery.append(primaryKey);
		destQuery.append( " ;");
		
		sourceUnmatchQuery.append(" from <TABLESCHEMA>.\"<TABLENAME>\" ");
		destUnmatchQuery.append(" from \"<TABLESCHEMA>\".\"<TABLENAME>\" ");

		logger.info("Source Query is :" +sourceQuery);

		logger.info("Dest Query is :" +destQuery);
		sourceSchema.setQuery(sourceQuery.toString());
		destSchema.setQuery(destQuery.toString());
		sourceSchema.setFetchUnmatchRecordQuery(sourceUnmatchQuery.toString());
		destSchema.setFetchUnmatchRecordQuery(destUnmatchQuery.toString());

	}
	
	public void createFetchUnmatchedDataQueriesCSV(TableInfo sourceSchema,TableInfo destSchema, List<String> ignoreList ) {
		//If Data Source==’Postgres’ and Target Data Format==’Parquet’ then
		//cast(product_price as numeric(30,2))||qty||order_value) AS text)) from <TABLESCHEMA>.\"<TABLENAME>\" order by order_id limit 1000;
		//destination.rules[4]=select order_id, md5(to_utf8(cast(order_id as varchar)||cast(customer_id as varchar)|| cast(order_status as varchar)|| cast(order_date as varchar)|| cast(product_id as varchar)|| cast(cast(product_price as decimal(30,2)) as varchar)|| cast(qty as varchar)|| cast(cast(order_value as decimal(30,2)) as varchar)))FROM \"<TABLESCHEMA>\".\"<TABLENAME>\" order by order_id limit 1000;
		StringBuilder sourceQuery = new  StringBuilder();
		StringBuilder destQuery = new  StringBuilder();
		String primaryKey = sourceSchema.getPrimaryKey();
		sourceQuery.append("SELECT ");
		sourceQuery.append(primaryKey);
		sourceQuery.append(", md5(CAST(( ");

		destQuery.append("SELECT ");
		destQuery.append(primaryKey);
		destQuery.append(", lower(to_hex( md5(to_utf8(");

		for (int index = 0; index < sourceSchema.getFieldCount(); index++) {
			String sourceFieldName = sourceSchema.getColumnNameList().get(index).toString();
			String sourceFieldType = sourceSchema.getColumnTypeList().get(index).toString();

			if (ignoreList.contains(sourceFieldName.toLowerCase())) {
				logger.info("ignoring " + sourceFieldName+" Type as " +sourceFieldType);
				continue;
			}
			if (index > 0) {
				sourceQuery.append("||");
				destQuery.append("||");

			}
			destQuery.append("cast(");

			switch (sourceFieldType.toLowerCase()) {

			case "numeric":
			case "numeric(12,2)":
				sourceQuery.append(sourceFieldName);
				destQuery.append("cast("+sourceFieldName+" as decimal(30,2) )");
				break;
				
			case "money":
				sourceQuery.append("cast("+sourceFieldName +" as numeric(30,2))");
				destQuery.append(sourceFieldName);
				break;

			case "timestamp":
				sourceQuery.append(sourceFieldName);
	        	destQuery.append("substr("+sourceFieldName+",1,19)");
				break;

			default:
				sourceQuery.append(sourceFieldName);
				destQuery.append(sourceFieldName);

				break;
			}

			destQuery.append(" as varchar)");

		}
		sourceQuery.append(") AS text)");
		sourceQuery.append(") from <TABLESCHEMA>.\"<TABLENAME>\" ");
		//		sourceQuery.append("order by ");
		//		sourceQuery.append(primaryKey);
		sourceQuery.append( " ;");


		destQuery.append(")))) as md5 from \"<TABLESCHEMA>\".\"<TABLENAME>\" ");
		//		sourceQuery.append("order by ");

		//		destQuery.append(primaryKey);
		destQuery.append( " ;");

		logger.info("Source Query is :" +sourceQuery);

		logger.info("Dest Query is :" +destQuery);
		sourceSchema.setQuery(sourceQuery.toString());
		destSchema.setQuery(destQuery.toString());


	}
}
