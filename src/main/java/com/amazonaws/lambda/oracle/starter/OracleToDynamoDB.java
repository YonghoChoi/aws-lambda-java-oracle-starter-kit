// Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package com.amazonaws.lambda.oracle.starter;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.*;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Optional;

/**
 * 
 * This is a sample Lambda class with handler function. It performs the
 * following steps: 1. Connect to AWS Secrets Manager and fetch secret value
 * using a secret name 2. Parse secrets value to Database username, password and
 * other values 3. Connect to an Oracle Database - actual business logic is left
 * to the user to try.
 * 
 * Important Note 1: Do not print confidential information (e.g. database
 * credentials) to CloudWatch console
 * 
 * Important Note 2: Do not return confidential information from handler
 * function.
 * 
 * @author Ravi Itha, Amazon Web Services, Inc.
 *
 */
public class OracleToDynamoDB implements RequestHandler<Object, String> {
	final static String tableName = "NongshimSample";

	public static void main(String[] args) {
	}

	@Override
	public String handleRequest(Object input, Context context) {
		ArrayList<SampleData> sampleData = null;
		try {
			sampleData = getSampleDataFromOracle();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
		deleteDynamoDBTable();
		createDynamoDBTable();
		putDataToDynamoDB(sampleData);
		return "Successful completion of data migration lambda function with dynamodb!";
	}
	public ArrayList<SampleData> getSampleDataFromOracle() throws Exception {
		String query = "select * from employees";
		DatabaseCredentials dbCreds = getCredentialsFromSecretsManager();
		DatabaseUtil dbUtil = new DatabaseUtil();
		Connection connection = dbUtil.getConnection(dbCreds);
		if (!Optional.ofNullable(connection).isPresent()) {
			throw new Exception("fail db connect");
		}

		try {
			PreparedStatement prepStmt = connection.prepareStatement(query);
			ResultSet rs = prepStmt.executeQuery();
			ArrayList<SampleData> sampleData = new ArrayList<>();
			while (rs.next()) {
				sampleData.add(new SampleData(rs.getString(1), rs.getString(2)));
			}
			return sampleData;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}

	public DatabaseCredentials getCredentialsFromSecretsManager() throws Exception {
		String region = Optional.ofNullable(System.getenv("region")).orElse("ap-northeast-2");
		String secretName = Optional.ofNullable(System.getenv("database_secret_name")).orElse("nongshim/dev/oracle");

		SecretsManagerUtil smUtil = new SecretsManagerUtil();
		String secretString = smUtil.getSecretUsingSecretsManager(region, secretName);
		if (!Optional.ofNullable(secretString).isPresent()) {
			throw new Exception("invalid secret");
		}

		return smUtil.parseSecretString(secretString);
	}

	public void deleteDynamoDBTable()
	{
		System.out.format(
				"Deleting table \"%s\".\n",
				tableName);

		final AmazonDynamoDB ddb = AmazonDynamoDBClientBuilder.defaultClient();
		try {
			ddb.deleteTable(tableName);
			Thread.sleep(10000);
		} catch (AmazonServiceException e) {
			System.err.println(e.getErrorMessage());
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println("Done!");
	}

	public void createDynamoDBTable()
	{
		System.out.format(
				"Creating table \"%s\" with a simple primary key: \"Name\".\n",
				tableName);

		CreateTableRequest request = new CreateTableRequest()
				.withAttributeDefinitions(new AttributeDefinition("Id", ScalarAttributeType.S))
				.withKeySchema(
						new KeySchemaElement("Id", KeyType.HASH)
				)
				.withProvisionedThroughput(new ProvisionedThroughput(
						new Long(10), new Long(10)))
				.withTableName(tableName);

		final AmazonDynamoDB ddb = AmazonDynamoDBClientBuilder.defaultClient();
		try {
			ddb.createTable(request);
			Thread.sleep(10000);
		} catch (AmazonServiceException e) {
			System.err.println(e.getErrorMessage());
			System.exit(1);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println("Done!");
	}

	public void putDataToDynamoDB(ArrayList<SampleData> sampleData) {
		System.out.format(
				"Putting data into table \"%s\".\n",
				tableName);
		final AmazonDynamoDB ddb = AmazonDynamoDBClientBuilder.defaultClient();
		for(SampleData data: sampleData) {
			HashMap<String, AttributeValue> itemValues = new HashMap<>();
			itemValues.put("Id", new AttributeValue(data.getId()));
			itemValues.put("Name", new AttributeValue(data.getName()));
			try {
				ddb.putItem(tableName, itemValues);
			} catch (ResourceNotFoundException e) {
				System.err.format("Error: The table \"%s\" can't be found.\n", tableName);
				System.err.println("Be sure that it exists and that you've typed its name correctly!");
				System.exit(1);
			} catch (AmazonServiceException e) {
				System.err.println(e.getMessage());
				System.exit(1);
			}
		}

		System.out.println("Done!");
	}
}