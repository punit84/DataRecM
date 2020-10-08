//snippet-sourcedescription:[ListNamedQueryExample.java demonstrates how to obtain a list of named query IDs.]
//snippet-keyword:[Java]
//snippet-sourcesyntax:[java]
//snippet-keyword:[Code Sample]
//snippet-keyword:[Amazon Athena]
//snippet-service:[athena]
//snippet-sourcetype:[full-example]
//snippet-sourcedate:[2018-06-25]
//snippet-sourceauthor:[soo-aws]
package com.datarecm.service.athena;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.AthenaException;
import software.amazon.awssdk.services.athena.model.ListNamedQueriesRequest;
import software.amazon.awssdk.services.athena.model.ListNamedQueriesResponse;
import software.amazon.awssdk.services.athena.paginators.ListNamedQueriesIterable;

import java.util.List;

/**
 * ListNamedQueryExample
 * -------------------------------------
 * This code shows how to obtain a list of named query IDs.
 */
public class ListNamedQueryExample
{

	public static void listNamedQueries(AthenaClient athenaClient) {

		try{

			// Build the request
			ListNamedQueriesRequest listNamedQueriesRequest = ListNamedQueriesRequest.builder().build();

			// Get the list results.
			ListNamedQueriesIterable listNamedQueriesResponses = athenaClient.listNamedQueriesPaginator(listNamedQueriesRequest);

			// Process the results.
			for (ListNamedQueriesResponse listNamedQueriesResponse : listNamedQueriesResponses) {
				List<String> namedQueryIds = listNamedQueriesResponse.namedQueryIds();
				// process named query IDs
				System.out.println(namedQueryIds);
			}

		} catch (AthenaException e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
}

