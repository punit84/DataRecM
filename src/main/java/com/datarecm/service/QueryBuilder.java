package com.datarecm.service;

import java.util.List;

/**
 * Building dynamic queries for Reconsilation using MD%
 * @author Punit Jain
 *
 */

public class QueryBuilder {


	//source.rules[4]=select order_id, md5(CAST((order_id||customer_id||order_status||order_date||product_id||cast(product_price as numeric(30,2))||qty||order_value) AS text)) from <TABLESCHEMA>.\"<TABLENAME>\" order by order_id limit 1000;
	//destination.rules[4]=select order_id, md5(to_utf8(cast(order_id as varchar)||cast(customer_id as varchar)|| cast(order_status as varchar)|| cast(order_date as varchar)|| cast(product_id as varchar)|| cast(cast(product_price as decimal(30,2)) as varchar)|| cast(qty as varchar)|| cast(cast(order_value as decimal(30,2)) as varchar)))FROM \"<TABLESCHEMA>\".\"<TABLENAME>\" order by order_id limit 1000;

//	select order_id, lower(to_hex(md5(to_utf8(cast(order_id as varchar)||cast(customer_id as varchar)|| cast(order_status as varchar)|| cast(order_date as varchar)||cast(substr(delivery_date,1,19) as varchar)||cast(product_id as varchar)||
   //         cast(cast(product_price as decimal(30,2)) as varchar)||cast(qty as varchar)||
      //      cast(cast(order_value as decimal(30,2)) as varchar))))) FROM default.orders order by order_id limit 100

	public static void createQueries(TableInfo sourceSchema,TableInfo destSchema, List<String> ignoreList ) {
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

			if (ignoreList.contains(sourceFieldType.toLowerCase())) {
				System.out.println("ignoring " + sourceFieldName+" Type as " +sourceFieldType);
				continue;
			}
			if (index > 0) {
				sourceQuery.append("||");
				destQuery.append("||");

			}
			destQuery.append("cast(");

			switch (sourceFieldType.toLowerCase()) {

			case "numeric(12,2)":
				sourceQuery.append(sourceFieldName);
				destQuery.append("cast("+sourceFieldName+" as decimal(30,2) )");
				break;

			case "float4":
				sourceQuery.append("cast("+sourceFieldName+" ::float4::text::float8 as numeric(30,1))");
				destQuery.append("round("+sourceFieldName+" ,1)");
				break;

			case "float8":
				sourceQuery.append("cast("+sourceFieldName+" as numeric(30,2))");
				destQuery.append("cast("+sourceFieldName +" as decimal(30,2))");

				break;
			case "money":
				sourceQuery.append("cast("+sourceFieldName +" as numeric(30,2))");
				destQuery.append("cast("+sourceFieldName +" as decimal(30,2))");
				break;

			case "timestamp":
				sourceQuery.append(sourceFieldName);
				destQuery.append(sourceFieldName);
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

		System.out.println("Source Query is :" +sourceQuery);
		
		System.out.println("Dest Query is :" +destQuery);
		sourceSchema.setQuery(sourceQuery.toString());
		destSchema.setQuery(destQuery.toString());


	}
	
}
