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
package org.example.testV2;

import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;

import org.apache.hc.core5.http.HttpStatus;
import org.example.Data.MainData;
import org.junit.Test;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.s3.model.InventoryConfiguration;
import software.amazon.awssdk.services.s3.model.InventoryFrequency;
import software.amazon.awssdk.services.s3.model.InventoryIncludedObjectVersions;
import software.amazon.awssdk.services.s3.model.InventoryOptionalField;

public class Inventory extends TestBase {
	@org.junit.jupiter.api.BeforeAll
	public static void beforeAll() {
		System.out.println("Inventory V2 Start");
	}

	@org.junit.jupiter.api.AfterAll
	public static void afterAll() {
		System.out.println("Inventory V2 End");
	}

	@Test
	@Tag("List")
	public void testListBucketInventory() {
		var client = getClient();
		var bucketName = createBucket(client);

		var response = client.listBucketInventoryConfigurations(l -> l.bucket(bucketName));
		assertTrue(response.inventoryConfigurationList().isEmpty());
	}

	@Test
	@Tag("Put")
	public void testPutBucketInventory() {
		var client = getClient();
		var bucketName = createBucket(client);
		var targetBucketName = createBucket(client);
		var inventoryId = "my-inventory-v2";

		client.putBucketInventoryConfiguration(
				p -> p.bucket(bucketName).id(inventoryId).inventoryConfiguration(i -> i
						.id(inventoryId)
						.destination(d -> d
								.s3BucketDestination(s -> s.bucket("arn:aws:s3:::" + targetBucketName).format("CSV")))
						.isEnabled(true).includedObjectVersions(InventoryIncludedObjectVersions.CURRENT)
						.schedule(s -> s.frequency(InventoryFrequency.DAILY))));
	}

	@Test
	@Tag("Check")
	public void testCheckBucketInventory() {
		var client = getClient();
		var bucketName = createBucket(client);
		var targetBucketName = createBucket(client);
		var inventoryId = "my-inventory";

		client.putBucketInventoryConfiguration(p -> p
				.bucket(bucketName).id(inventoryId)
				.inventoryConfiguration(i -> i
						.id(inventoryId)
						.destination(d -> d
								.s3BucketDestination(s -> s
										.bucket("arn:aws:s3:::" + targetBucketName)
										.format("CSV")))
						.isEnabled(true)
						.includedObjectVersions(InventoryIncludedObjectVersions.CURRENT)
						.schedule(s -> s.frequency(InventoryFrequency.DAILY))));

		var response = client.listBucketInventoryConfigurations(l -> l.bucket(bucketName));
		assertEquals(1, response.inventoryConfigurationList().size());
	}

	@Test
	@Tag("Get")
	public void testGetBucketInventory() {
		var client = getClient();
		var bucketName = createBucket(client);
		var targetBucketName = createBucket(client);
		var inventoryId = "my-inventory";

		client.putBucketInventoryConfiguration(p -> p
				.bucket(bucketName).id(inventoryId)
				.inventoryConfiguration(i -> i
						.id(inventoryId)
						.destination(d -> d
								.s3BucketDestination(
										s -> s.bucket("arn:aws:s3:::" + targetBucketName).format("CSV").prefix("a/")))
						.isEnabled(true).includedObjectVersions(InventoryIncludedObjectVersions.CURRENT)
						.schedule(s -> s.frequency(InventoryFrequency.DAILY))));

		var response = client.getBucketInventoryConfiguration(g -> g.bucket(bucketName).id(inventoryId));
		assertEquals(inventoryId, response.inventoryConfiguration().id());
	}

	@Test
	@Tag("Delete")
	public void testDeleteBucketInventory() {
		var client = getClient();
		var bucketName = createBucket(client);
		var targetBucketName = createBucket(client);
		var inventoryId = "my-inventory";

		client.putBucketInventoryConfiguration(p -> p
				.bucket(bucketName).id(inventoryId)
				.inventoryConfiguration(i -> i
						.id(inventoryId)
						.destination(d -> d
								.s3BucketDestination(s -> s
										.bucket("arn:aws:s3:::" + targetBucketName).format("CSV")))
						.isEnabled(true)
						.includedObjectVersions(InventoryIncludedObjectVersions.CURRENT)
						.schedule(s -> s.frequency(InventoryFrequency.DAILY))));

		client.deleteBucketInventoryConfiguration(d -> d.bucket(bucketName).id(inventoryId));
		var response = client.listBucketInventoryConfigurations(l -> l.bucket(bucketName));
		assertTrue(response.inventoryConfigurationList().isEmpty());
	}

	@Test
	@Tag("Error")
	public void testGetBucketInventoryNotExist() {
		var client = getClient();
		var bucketName = createBucket(client);
		var inventoryId = "my-inventory";

		var e = assertThrows(AwsServiceException.class,
				() -> client.getBucketInventoryConfiguration(g -> g
						.bucket(bucketName)
						.id(inventoryId)));

		assertEquals(HttpStatus.SC_NOT_FOUND, e.statusCode());
		assertEquals(MainData.NO_SUCH_CONFIGURATION, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Error")
	public void testDeleteBucketInventoryNotExist() {
		var client = getClient();
		var bucketName = createBucket(client);
		var inventoryId = "my-inventory";

		var e = assertThrows(AwsServiceException.class,
				() -> client.deleteBucketInventoryConfiguration(d -> d.bucket(bucketName).id(inventoryId)));

		assertEquals(HttpStatus.SC_NOT_FOUND, e.statusCode());
		assertEquals(MainData.NO_SUCH_CONFIGURATION, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Error")
	public void testPutBucketInventoryNotExist() {
		var client = getClient();
		var bucketName = getNewBucketName();
		var targetBucketName = createBucket(client);
		var inventoryId = "my-inventory";

		var inventory = InventoryConfiguration.builder()
				.id(inventoryId)
				.destination(
						d -> d.s3BucketDestination(s3 -> s3.bucket("arn:aws:s3:::" + targetBucketName).format("CSV")))
				.isEnabled(true)
				.includedObjectVersions(InventoryIncludedObjectVersions.CURRENT)
				.schedule(s -> s.frequency(InventoryFrequency.DAILY));

		var e = assertThrows(AwsServiceException.class,
				() -> client.putBucketInventoryConfiguration(
						p -> p.bucket(bucketName).id(inventoryId).inventoryConfiguration(inventory.build())));

		assertEquals(HttpStatus.SC_NOT_FOUND, e.statusCode());
		assertEquals(MainData.NO_SUCH_BUCKET, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Error")
	public void testPutBucketInventoryIdNotExist() {
		var client = getClient();
		var bucketName = createBucket(client);
		var targetBucketName = createBucket(client);
		var inventoryId = "";

		var inventory = InventoryConfiguration.builder()
				.id(inventoryId)
				.destination(
						d -> d.s3BucketDestination(s3 -> s3.bucket("arn:aws:s3:::" + targetBucketName).format("CSV")))
				.isEnabled(true)
				.includedObjectVersions(InventoryIncludedObjectVersions.CURRENT)
				.schedule(s -> s.frequency(InventoryFrequency.DAILY));

		var e = assertThrows(AwsServiceException.class,
				() -> client.putBucketInventoryConfiguration(p -> p
						.bucket(bucketName).id(inventoryId)
						.inventoryConfiguration(inventory.build())));

		assertEquals(HttpStatus.SC_BAD_REQUEST, e.statusCode());
		assertEquals(MainData.MALFORMED_XML, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Error")
	public void testPutBucketInventoryIdDuplicate() {
		var client = getClient();
		var bucketName = createBucket(client);
		var targetBucketName = createBucket(client);
		var inventoryId = "my-inventory";

		var inventory = InventoryConfiguration.builder()
				.id(inventoryId)
				.destination(
						d -> d.s3BucketDestination(s3 -> s3.bucket("arn:aws:s3:::" + targetBucketName).format("CSV")))
				.isEnabled(true)
				.includedObjectVersions(InventoryIncludedObjectVersions.CURRENT)
				.schedule(s -> s.frequency(InventoryFrequency.DAILY));

		client.putBucketInventoryConfiguration(p -> p
				.bucket(bucketName).id(inventoryId)
				.inventoryConfiguration(inventory.build()));

		var inventory2 = InventoryConfiguration.builder()
				.id(inventoryId)
				.destination(
						d -> d.s3BucketDestination(s3 -> s3.bucket("arn:aws:s3:::" + targetBucketName).format("CSV")))
				.isEnabled(true)
				.includedObjectVersions(InventoryIncludedObjectVersions.CURRENT)
				.schedule(s -> s.frequency(InventoryFrequency.DAILY));

		client.putBucketInventoryConfiguration(p -> p
				.bucket(bucketName).id(inventoryId)
				.inventoryConfiguration(inventory2.build()));

		var response = client.listBucketInventoryConfigurations(l -> l.bucket(bucketName));

		assertEquals(1, response.inventoryConfigurationList().size());
	}

	@Disabled("aws에서 타깃 버킷이 존재하는지 확인하지 않음")
	@Test
	@Tag("Error")
	public void testPutBucketInventoryTargetNotExist() {
		var client = getClient();
		var bucketName = createBucket(client);
		var targetBucketName = getNewBucketName();
		var inventoryId = "my-inventory";

		var inventory = InventoryConfiguration.builder()
				.id(inventoryId)
				.destination(
						d -> d.s3BucketDestination(s3 -> s3.bucket("arn:aws:s3:::" + targetBucketName).format("CSV")))
				.isEnabled(true)
				.includedObjectVersions(InventoryIncludedObjectVersions.CURRENT)
				.schedule(s -> s.frequency(InventoryFrequency.DAILY));

		var e = assertThrows(AwsServiceException.class,
				() -> client.putBucketInventoryConfiguration(p -> p
						.bucket(bucketName).id(inventoryId)
						.inventoryConfiguration(inventory.build())));

		assertEquals(HttpStatus.SC_NOT_FOUND, e.statusCode());
		assertEquals(MainData.NO_SUCH_BUCKET, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Error")
	public void testPutBucketInventoryInvalidFormat() {
		var client = getClient();
		var bucketName = createBucket(client);
		var targetBucketName = createBucket(client);
		var inventoryId = "my-inventory";

		var inventory = InventoryConfiguration.builder()
				.id(inventoryId)
				.destination(
						d -> d.s3BucketDestination(s3 -> s3.bucket("arn:aws:s3:::" + targetBucketName).format("JSON")))
				.isEnabled(true)
				.includedObjectVersions(InventoryIncludedObjectVersions.CURRENT)
				.schedule(s -> s.frequency(InventoryFrequency.DAILY));

		var e = assertThrows(AwsServiceException.class,
				() -> client.putBucketInventoryConfiguration(p -> p
						.bucket(bucketName).id(inventoryId)
						.inventoryConfiguration(inventory.build())));

		assertEquals(HttpStatus.SC_BAD_REQUEST, e.statusCode());
		assertEquals(MainData.MALFORMED_XML, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Error")
	public void testPutBucketInventoryInvalidFrequency() {
		var client = getClient();
		var bucketName = createBucket(client);
		var targetBucketName = createBucket(client);
		var inventoryId = "my-inventory";

		var inventory = InventoryConfiguration.builder()
				.id(inventoryId)
				.destination(
						d -> d.s3BucketDestination(s3 -> s3.bucket("arn:aws:s3:::" + targetBucketName).format("CSV")))
				.isEnabled(true)
				.includedObjectVersions(InventoryIncludedObjectVersions.CURRENT)
				.schedule(s -> s.frequency("Hourly"));

		var e = assertThrows(AwsServiceException.class,
				() -> client.putBucketInventoryConfiguration(p -> p
						.bucket(bucketName).id(inventoryId)
						.inventoryConfiguration(inventory.build())));

		assertEquals(HttpStatus.SC_BAD_REQUEST, e.statusCode());
		assertEquals(MainData.MALFORMED_XML, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Error")
	public void testPutBucketInventoryInvalidCase() {
		var client = getClient();
		var bucketName = createBucket(client);
		var targetBucketName = createBucket(client);
		var inventoryId = "my-inventory";

		var inventory = InventoryConfiguration.builder()
				.id(inventoryId)
				.destination(
						d -> d.s3BucketDestination(s3 -> s3.bucket("arn:aws:s3:::" + targetBucketName).format("CSV")))
				.isEnabled(true)
				.includedObjectVersions("CUrrENT")
				.schedule(s -> s.frequency(InventoryFrequency.DAILY));

		var e = assertThrows(AwsServiceException.class,
				() -> client.putBucketInventoryConfiguration(p -> p
						.bucket(bucketName).id(inventoryId)
						.inventoryConfiguration(inventory.build())));

		assertEquals(HttpStatus.SC_BAD_REQUEST, e.statusCode());
		assertEquals(MainData.MALFORMED_XML, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Put")
	public void testPutBucketInventoryPrefix() {
		var client = getClient();
		var bucketName = createBucket(client);
		var targetBucketName = createBucket(client);
		var inventoryId = "my-inventory";

		var inventoryPrefix = "a/";

		var inventory = InventoryConfiguration.builder()
				.id(inventoryId)
				.destination(
						d -> d.s3BucketDestination(s3 -> s3.bucket("arn:aws:s3:::" + targetBucketName).format("CSV")
								.prefix(inventoryPrefix)))
				.isEnabled(true)
				.includedObjectVersions(InventoryIncludedObjectVersions.CURRENT)
				.schedule(s -> s.frequency(InventoryFrequency.DAILY));

		client.putBucketInventoryConfiguration(p -> p
				.bucket(bucketName).id(inventoryId)
				.inventoryConfiguration(inventory.build()));

		var result = client.getBucketInventoryConfiguration(g -> g
				.bucket(bucketName)
				.id(inventoryId));

		assertEquals(inventoryId, result.inventoryConfiguration().id());
		assertEquals(inventoryPrefix,
				result.inventoryConfiguration().destination().s3BucketDestination().prefix());
	}

	@Test
	@Tag("Put")
	public void testPutBucketInventoryOptional() {
		var client = getClient();
		var bucketName = createBucket(client);
		var targetBucketName = createBucket(client);
		var inventoryId = "my-inventory";

		var inventoryPrefix = "a/";
		var inventoryOptionalFields = Arrays.asList(
				InventoryOptionalField.SIZE,
				InventoryOptionalField.LAST_MODIFIED_DATE);

		var inventory = InventoryConfiguration.builder()
				.id(inventoryId)
				.destination(
						d -> d.s3BucketDestination(s3 -> s3.bucket("arn:aws:s3:::" + targetBucketName).format("CSV")
								.prefix(inventoryPrefix)))
				.isEnabled(true)
				.includedObjectVersions(InventoryIncludedObjectVersions.CURRENT)
				.schedule(s -> s.frequency(InventoryFrequency.DAILY))
				.optionalFields(inventoryOptionalFields);

		client.putBucketInventoryConfiguration(p -> p
				.bucket(bucketName).id(inventoryId)
				.inventoryConfiguration(inventory.build()));

		var result = client.getBucketInventoryConfiguration(g -> g
				.bucket(bucketName)
				.id(inventoryId));

		assertEquals(inventoryId, result.inventoryConfiguration().id());
		assertEquals(inventoryPrefix,
				result.inventoryConfiguration().destination().s3BucketDestination().prefix());
		assertEquals(inventoryOptionalFields, result.inventoryConfiguration().optionalFields());
	}

	@Test
	@Tag("Error")
	public void testPutBucketInventoryInvalidOptional() {
		var client = getClient();
		var bucketName = createBucket(client);
		var targetBucketName = createBucket(client);
		var inventoryId = "my-inventory";

		var inventoryPrefix = "a/";
		var inventoryOptionalFields = Arrays.asList(
				"SIZE",
				"--");

		var inventory = InventoryConfiguration.builder()
				.id(inventoryId)
				.destination(
						d -> d.s3BucketDestination(s3 -> s3.bucket("arn:aws:s3:::" + targetBucketName).format("CSV")
								.prefix(inventoryPrefix)))
				.isEnabled(true)
				.includedObjectVersions(InventoryIncludedObjectVersions.CURRENT)
				.schedule(s -> s.frequency(InventoryFrequency.DAILY))
				.optionalFieldsWithStrings(inventoryOptionalFields);

		var e = assertThrows(AwsServiceException.class,
				() -> client.putBucketInventoryConfiguration(p -> p
						.bucket(bucketName).id(inventoryId)
						.inventoryConfiguration(inventory.build())));

		assertEquals(HttpStatus.SC_BAD_REQUEST, e.statusCode());
		assertEquals(MainData.MALFORMED_XML, e.awsErrorDetails().errorCode());
	}
}
