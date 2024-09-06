/*
* Copyright (c) 2021 PSPACE, inc. KSAN Development Team ksan@pspace.co.kr
* KSAN is a suite of free software: you can redistribute it and/or modify it under the terms of
* the GNU General Public License as published by the Free Software Foundation, either version
* 3 of the License. See LICENSE for details
*
* 본 프로그램 및 관련 소스코드, 문서 등 모든 자료는 있는 그대로 제공이 됩니다.
* KSAN 프로젝트의 개발자 및 개발사는 이 프로그램을 사용한 결과에 따른 어떠한 책임도 지지 않습니다.
* KSAN 개발팀은 사전 공지, 허락, 동의 없이 KSAN 개발에 관련된 모든 결과물에 대한 LICENSE 방식을 변경 할 권리가 있습니다.
*/
package org.example.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;

import org.example.Data.MainData;
import org.junit.Test;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.ListBucketInventoryConfigurationsRequest;
import com.amazonaws.services.s3.model.inventory.InventoryConfiguration;
import com.amazonaws.services.s3.model.inventory.InventoryDestination;
import com.amazonaws.services.s3.model.inventory.InventoryFrequency;
import com.amazonaws.services.s3.model.inventory.InventoryIncludedObjectVersions;
import com.amazonaws.services.s3.model.inventory.InventoryS3BucketDestination;
import com.amazonaws.services.s3.model.inventory.InventorySchedule;

public class Inventory extends TestBase {
	@org.junit.jupiter.api.BeforeAll
	public static void beforeAll() {
		System.out.println("Inventory Start");
	}

	@org.junit.jupiter.api.AfterAll
	public static void afterAll() {
		System.out.println("Inventory End");
	}

	@Test
	@Tag("List")
	public void testListBucketInventory() {
		var client = getClient();
		var bucketName = createBucket(client);

		var response = client.listBucketInventoryConfigurations(
				new ListBucketInventoryConfigurationsRequest().withBucketName(bucketName));
		assertNull(response.getInventoryConfigurationList());
	}

	@Test
	@Tag("Put")
	public void testPutBucketInventory() {
		var client = getClient();
		var bucketName = createBucket(client);
		var targetBucketName = createBucket(client);
		var inventoryId = "my-inventory";

		var inventory = new InventoryConfiguration()
				.withId(inventoryId)
				.withDestination(new InventoryDestination().withS3BucketDestination(new InventoryS3BucketDestination()
						.withBucketArn("arn:aws:s3:::" + targetBucketName).withFormat("CSV")))
				.withEnabled(true)
				.withIncludedObjectVersions(InventoryIncludedObjectVersions.Current)
				.withSchedule(new InventorySchedule().withFrequency(InventoryFrequency.Daily));
		client.setBucketInventoryConfiguration(bucketName, inventory);
	}

	@Test
	@Tag("Check")
	public void testCheckBucketInventory() {
		var client = getClient();
		var bucketName = createBucket(client);
		var targetBucketName = createBucket(client);
		var inventoryId = "my-inventory";

		var inventory = new InventoryConfiguration()
				.withId(inventoryId)
				.withDestination(new InventoryDestination().withS3BucketDestination(new InventoryS3BucketDestination()
						.withBucketArn("arn:aws:s3:::" + targetBucketName).withFormat("CSV")))
				.withEnabled(true)
				.withIncludedObjectVersions(InventoryIncludedObjectVersions.Current)
				.withSchedule(new InventorySchedule().withFrequency(InventoryFrequency.Daily));
		client.setBucketInventoryConfiguration(bucketName, inventory);

		var response = client.listBucketInventoryConfigurations(
				new ListBucketInventoryConfigurationsRequest().withBucketName(bucketName));
		assertEquals(1, response.getInventoryConfigurationList().size());
	}

	@Test
	@Tag("Get")
	public void testGetBucketInventory() {
		var client = getClient();
		var bucketName = createBucket(client);
		var targetBucketName = createBucket(client);
		var inventoryId = "my-inventory";

		var inventory = new InventoryConfiguration()
				.withId(inventoryId)
				.withDestination(new InventoryDestination().withS3BucketDestination(new InventoryS3BucketDestination()
						.withBucketArn("arn:aws:s3:::" + targetBucketName).withFormat("CSV")))
				.withEnabled(true)
				.withIncludedObjectVersions(InventoryIncludedObjectVersions.Current)
				.withSchedule(new InventorySchedule().withFrequency(InventoryFrequency.Daily));
		client.setBucketInventoryConfiguration(bucketName, inventory);

		var response = client.getBucketInventoryConfiguration(bucketName, inventoryId);
		assertEquals(inventoryId, response.getInventoryConfiguration().getId());
	}

	@Test
	@Tag("Delete")
	public void testDeleteBucketInventory() {
		var client = getClient();
		var bucketName = createBucket(client);
		var targetBucketName = createBucket(client);
		var inventoryId = "my-inventory";

		var inventory = new InventoryConfiguration()
				.withId(inventoryId)
				.withDestination(new InventoryDestination().withS3BucketDestination(new InventoryS3BucketDestination()
						.withBucketArn("arn:aws:s3:::" + targetBucketName).withFormat("CSV")))
				.withEnabled(true)
				.withIncludedObjectVersions(InventoryIncludedObjectVersions.Current)
				.withSchedule(new InventorySchedule().withFrequency(InventoryFrequency.Daily));
		client.setBucketInventoryConfiguration(bucketName, inventory);

		client.deleteBucketInventoryConfiguration(bucketName, inventoryId);
		var response = client.listBucketInventoryConfigurations(
				new ListBucketInventoryConfigurationsRequest().withBucketName(bucketName));
		assertNull(response.getInventoryConfigurationList());
	}

	@Test
	@Tag("Error")
	public void testGetBucketInventoryNotExist() {
		var client = getClient();
		var bucketName = createBucket(client);
		var inventoryId = "my-inventory";

		var e = assertThrows(AmazonServiceException.class,
				() -> client.getBucketInventoryConfiguration(bucketName, inventoryId));

		assertEquals(404, e.getStatusCode());
		assertEquals(MainData.NO_SUCH_CONFIGURATION, e.getErrorCode());
	}

	@Test
	@Tag("Error")
	public void testDeleteBucketInventoryNotExist() {
		var client = getClient();
		var bucketName = createBucket(client);
		var inventoryId = "my-inventory";

		var e = assertThrows(AmazonServiceException.class,
				() -> client.deleteBucketInventoryConfiguration(bucketName, inventoryId));

		assertEquals(404, e.getStatusCode());
		assertEquals(MainData.NO_SUCH_CONFIGURATION, e.getErrorCode());
	}

	@Test
	@Tag("Error")
	public void testPutBucketInventoryNotExist() {
		var client = getClient();
		var bucketName = getNewBucketName();
		var targetBucketName = createBucket(client);
		var inventoryId = "my-inventory";

		var inventory = new InventoryConfiguration()
				.withId(inventoryId)
				.withDestination(new InventoryDestination().withS3BucketDestination(new InventoryS3BucketDestination()
						.withBucketArn("arn:aws:s3:::" + targetBucketName).withFormat("CSV")))
				.withEnabled(true)
				.withIncludedObjectVersions(InventoryIncludedObjectVersions.Current)
				.withSchedule(new InventorySchedule().withFrequency(InventoryFrequency.Daily));

		var e = assertThrows(AmazonServiceException.class,
				() -> client.setBucketInventoryConfiguration(bucketName, inventory));

		assertEquals(404, e.getStatusCode());
		assertEquals(MainData.NO_SUCH_BUCKET, e.getErrorCode());
	}

	@Test
	@Tag("Error")
	public void testPutBucketInventoryIdNotExist() {
		var client = getClient();
		var bucketName = createBucket(client);
		var targetBucketName = createBucket(client);
		var inventoryId = "";

		var inventory = new InventoryConfiguration()
				.withId(inventoryId)
				.withDestination(new InventoryDestination().withS3BucketDestination(new InventoryS3BucketDestination()
						.withBucketArn("arn:aws:s3:::" + targetBucketName).withFormat("CSV")))
				.withEnabled(true)
				.withIncludedObjectVersions(InventoryIncludedObjectVersions.Current)
				.withSchedule(new InventorySchedule().withFrequency(InventoryFrequency.Daily));

		var e = assertThrows(AmazonServiceException.class,
				() -> client.setBucketInventoryConfiguration(bucketName, inventory));

		assertEquals(400, e.getStatusCode());
		assertEquals(MainData.MALFORMED_XML, e.getErrorCode());
	}

	@Test
	@Tag("Error")
	public void testPutBucketInventoryIdDuplicate() {
		var client = getClient();
		var bucketName = createBucket(client);
		var targetBucketName = createBucket(client);
		var inventoryId = "my-inventory";

		var inventory = new InventoryConfiguration()
				.withId(inventoryId)
				.withDestination(new InventoryDestination().withS3BucketDestination(new InventoryS3BucketDestination()
						.withBucketArn("arn:aws:s3:::" + targetBucketName).withFormat("CSV")))
				.withEnabled(true)
				.withIncludedObjectVersions(InventoryIncludedObjectVersions.Current)
				.withSchedule(new InventorySchedule().withFrequency(InventoryFrequency.Daily));

		client.setBucketInventoryConfiguration(bucketName, inventory);
		var inventoryId2 = "my-inventory";

		var inventory2 = new InventoryConfiguration()
				.withId(inventoryId2)
				.withDestination(new InventoryDestination().withS3BucketDestination(new InventoryS3BucketDestination()
						.withBucketArn("arn:aws:s3:::" + targetBucketName).withFormat("CSV")))
				.withEnabled(true)
				.withIncludedObjectVersions(InventoryIncludedObjectVersions.Current)
				.withSchedule(new InventorySchedule().withFrequency(InventoryFrequency.Daily));

		client.setBucketInventoryConfiguration(bucketName, inventory2);

		var response = client.listBucketInventoryConfigurations(
				new ListBucketInventoryConfigurationsRequest().withBucketName(bucketName));

		assertEquals(1, response.getInventoryConfigurationList().size());
	}

	@Disabled("aws에서 타깃 버킷이 존재하는지 확인하지 않음")
	@Test
	@Tag("Error")
	public void testPutBucketInventoryTargetNotExist() {
		var client = getClient();
		var bucketName = createBucket(client);
		var targetBucketName = getNewBucketName();
		var inventoryId = "my-inventory";

		var inventory = new InventoryConfiguration()
				.withId(inventoryId)
				.withDestination(new InventoryDestination().withS3BucketDestination(new InventoryS3BucketDestination()
						.withBucketArn("arn:aws:s3:::" + targetBucketName).withFormat("CSV")))
				.withEnabled(true)
				.withIncludedObjectVersions(InventoryIncludedObjectVersions.Current)
				.withSchedule(new InventorySchedule().withFrequency(InventoryFrequency.Daily));

		var e = assertThrows(AmazonServiceException.class,
				() -> client.setBucketInventoryConfiguration(bucketName, inventory));

		assertEquals(404, e.getStatusCode());
		assertEquals(MainData.NO_SUCH_BUCKET, e.getErrorCode());
	}

	@Test
	@Tag("Error")
	public void testPutBucketInventoryInvalidFormat() {
		var client = getClient();
		var bucketName = createBucket(client);
		var targetBucketName = createBucket(client);
		var inventoryId = "my-inventory";

		var inventory = new InventoryConfiguration()
				.withId(inventoryId)
				.withDestination(new InventoryDestination().withS3BucketDestination(new InventoryS3BucketDestination()
						.withBucketArn("arn:aws:s3:::" + targetBucketName).withFormat("JSON")))
				.withEnabled(true)
				.withIncludedObjectVersions(InventoryIncludedObjectVersions.Current)
				.withSchedule(new InventorySchedule().withFrequency(InventoryFrequency.Daily));

		var e = assertThrows(AmazonServiceException.class,
				() -> client.setBucketInventoryConfiguration(bucketName, inventory));

		assertEquals(400, e.getStatusCode());
		assertEquals(MainData.MALFORMED_XML, e.getErrorCode());
	}

	@Test
	@Tag("Error")
	public void testPutBucketInventoryInvalidFrequency() {
		var client = getClient();
		var bucketName = createBucket(client);
		var targetBucketName = createBucket(client);
		var inventoryId = "my-inventory";

		var inventory = new InventoryConfiguration()
				.withId(inventoryId)
				.withDestination(new InventoryDestination().withS3BucketDestination(new InventoryS3BucketDestination()
						.withBucketArn("arn:aws:s3:::" + targetBucketName).withFormat("CSV")))
				.withEnabled(true)
				.withIncludedObjectVersions("Current")
				.withSchedule(new InventorySchedule().withFrequency("Hourly"));

		var e = assertThrows(AmazonServiceException.class,
				() -> client.setBucketInventoryConfiguration(bucketName, inventory));

		assertEquals(400, e.getStatusCode());
		assertEquals(MainData.MALFORMED_XML, e.getErrorCode());
	}

	@Test
	@Tag("Error")
	public void testPutBucketInventoryInvalidCase() {
		var client = getClient();
		var bucketName = createBucket(client);
		var targetBucketName = createBucket(client);
		var inventoryId = "my-inventory";

		var inventory = new InventoryConfiguration()
				.withId(inventoryId)
				.withDestination(new InventoryDestination().withS3BucketDestination(new InventoryS3BucketDestination()
						.withBucketArn("arn:aws:s3:::" + targetBucketName).withFormat("CSV")))
				.withEnabled(true)
				.withIncludedObjectVersions("CURRENT")
				.withSchedule(new InventorySchedule().withFrequency("Daily"));

		var e = assertThrows(AmazonServiceException.class,
				() -> client.setBucketInventoryConfiguration(bucketName, inventory));

		assertEquals(400, e.getStatusCode());
		assertEquals(MainData.MALFORMED_XML, e.getErrorCode());
	}

	@Test
	@Tag("Filter")
	public void testPutBucketInventoryPrefix() {
		var client = getClient();
		var bucketName = createBucket(client);
		var targetBucketName = createBucket(client);
		var inventoryId = "my-inventory";

		var inventoryPrefix = "a/";

		var inventory = new InventoryConfiguration()
				.withId(inventoryId)
				.withDestination(new InventoryDestination().withS3BucketDestination(new InventoryS3BucketDestination()
						.withBucketArn("arn:aws:s3:::" + targetBucketName).withFormat("CSV")
						.withPrefix(inventoryPrefix)))
				.withEnabled(true)
				.withIncludedObjectVersions(InventoryIncludedObjectVersions.Current)
				.withSchedule(new InventorySchedule().withFrequency(InventoryFrequency.Daily));

		client.setBucketInventoryConfiguration(bucketName, inventory);

		var result = client.getBucketInventoryConfiguration(bucketName, inventoryId);

		assertEquals(inventoryId, result.getInventoryConfiguration().getId());
		assertEquals(inventoryPrefix,
				result.getInventoryConfiguration().getDestination().getS3BucketDestination().getPrefix());
	}

	@Test
	@Tag("Filter")
	public void testPutBucketInventoryOptional() {
		var client = getClient();
		var bucketName = createBucket(client);
		var targetBucketName = createBucket(client);
		var inventoryId = "my-inventory";

		var inventoryPrefix = "a/";
		var inventoryOptionalFields = Arrays.asList("Size", "LastModifiedDate");

		var inventory = new InventoryConfiguration()
				.withId(inventoryId)
				.withDestination(new InventoryDestination().withS3BucketDestination(new InventoryS3BucketDestination()
						.withBucketArn("arn:aws:s3:::" + targetBucketName).withFormat("CSV")
						.withPrefix(inventoryPrefix)))
				.withEnabled(true)
				.withIncludedObjectVersions(InventoryIncludedObjectVersions.Current)
				.withOptionalFields(inventoryOptionalFields)
				.withSchedule(new InventorySchedule().withFrequency(InventoryFrequency.Daily));

		client.setBucketInventoryConfiguration(bucketName, inventory);

		var result = client.getBucketInventoryConfiguration(bucketName, inventoryId);

		assertEquals(inventoryId, result.getInventoryConfiguration().getId());
		assertEquals(inventoryPrefix,
				result.getInventoryConfiguration().getDestination().getS3BucketDestination().getPrefix());
		assertEquals(inventoryOptionalFields, result.getInventoryConfiguration().getOptionalFields());
	}

	@Test
	@Tag("Error")
	public void testPutBucketInventoryInvalidOptional() {
		var client = getClient();
		var bucketName = createBucket(client);
		var targetBucketName = createBucket(client);
		var inventoryId = "my-inventory";

		var inventoryPrefix = "a/";
		var inventoryOptionalFields = Arrays.asList("SIZE", "LastModified");

		var inventory = new InventoryConfiguration()
				.withId(inventoryId)
				.withDestination(new InventoryDestination().withS3BucketDestination(new InventoryS3BucketDestination()
						.withBucketArn("arn:aws:s3:::" + targetBucketName).withFormat("CSV")
						.withPrefix(inventoryPrefix)))
				.withEnabled(true)
				.withIncludedObjectVersions(InventoryIncludedObjectVersions.Current)
				.withOptionalFields(inventoryOptionalFields)
				.withSchedule(new InventorySchedule().withFrequency(InventoryFrequency.Daily));

		var e = assertThrows(AmazonServiceException.class,
				() -> client.setBucketInventoryConfiguration(bucketName, inventory));

		assertEquals(400, e.getStatusCode());
		assertEquals(MainData.MALFORMED_XML, e.getErrorCode());
	}
}
