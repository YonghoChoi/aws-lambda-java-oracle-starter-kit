// Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package com.amazonaws.lambda.oracle.starter;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
public class DynamoDBToOracle implements RequestHandler<Object, String> {
	final static String tableName = "NongshimSample";

	public static void main(String[] args) {
	}

	@Override
	public String handleRequest(Object input, Context context) {
		List<SampleData> sampleData = getDataToDynamoDB();
		try {
			insertDataToOracle(sampleData);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
		return "Successful completion of data migration lambda function with dynamodb!";
	}
	public void insertDataToOracle(List<SampleData> sampleData) throws Exception {
		System.out.println(
				"Inserting data to Oracle table customers.");
		DatabaseCredentials dbCreds = getCredentialsFromSecretsManager();
		DatabaseUtil dbUtil = new DatabaseUtil();
		Connection connection = dbUtil.getConnection(dbCreds);
		if (!Optional.ofNullable(connection).isPresent()) {
			throw new Exception("fail db connect");
		}

		for(SampleData data: sampleData) {
			String query = String.format("insert into customers(Name) values('%s')", data.getName());
			try {
				PreparedStatement prepStmt = connection.prepareStatement(query);
				prepStmt.executeQuery();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

		System.out.println("Done!");
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

	public List<SampleData> getDataToDynamoDB() {
		System.out.format(
				"Getting data from DynamoDB table \"%s\".\n",
				tableName);
		final AmazonDynamoDB ddb = AmazonDynamoDBClientBuilder.defaultClient();
		ScanRequest req = new ScanRequest(tableName);
		ScanResult result = ddb.scan(req);
		List<SampleData> sampleData = new ArrayList<>();
		for(Map<String, AttributeValue> item: result.getItems()) {
			for(String key: item.keySet()) {
				sampleData.add(new SampleData(key, item.get(key).getS()));
			}
		}
		System.out.println("Done!");

		return sampleData;
	}
}