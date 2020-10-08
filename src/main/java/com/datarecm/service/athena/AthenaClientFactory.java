package com.datarecm.service.athena;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.athena.AthenaClient;

/**
 * AthenaClientFactory
 * -------------------------------------
 * This code shows how to create and configure an Amazon Athena client.
 */
public class AthenaClientFactory
{
	/**
	 * AmazonAthenaClientBuilder to build Athena with the following properties:
	 * - Set the region of the client
	 * - Use the instance profile from the EC2 instance as the credentials provider
	 * - Configure the client to increase the execution timeout.
	 */
	//  private final AmazonAthenaClientBuilder builder = AmazonAthenaClientBuilder.standard()
	//          .withRegion(Regions.AP_SOUTH_1)
	//          .withCredentials(InstanceProfileCredentialsProvider.getInstance())
	//          .withClientConfiguration(new ClientConfiguration().withClientExecutionTimeout(config.des.CLIENT_EXECUTION_TIMEOUT));

	
	
	public AthenaClient createClient() {
		return AthenaClient.builder()
				.region(Region.AP_SOUTH_1).build();
	}
	public AthenaClient createClient(String region)
	{
		return AthenaClient.builder()
		.region(Region.of(region)).build();
		
	}


}
