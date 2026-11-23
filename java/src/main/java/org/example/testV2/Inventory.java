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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import org.apache.hc.core5.http.HttpStatus;
import org.example.Data.MainData;
import org.junit.Test;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.function.Executable;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.SSES3;
import software.amazon.awssdk.services.s3.model.InventoryConfiguration;
import software.amazon.awssdk.services.s3.model.InventoryFrequency;
import software.amazon.awssdk.services.s3.model.InventoryIncludedObjectVersions;
import software.amazon.awssdk.services.s3.model.InventoryOptionalField;

public class Inventory extends TestBase {
	@Test
	@Tag("List")
	public void testListBucketInventory() {
		var client = getClient();
		var bucketName = createBucket(client, 1);

		var response = client.listBucketInventoryConfigurations(l -> l.bucket(bucketName));
		assertTrue(response.inventoryConfigurationList().isEmpty());
	}

	@Test
	@Tag("Put")
	public void testPutBucketInventory() {
		var client = getClient();
		var bucketName = createBucket(client, 2);
		var targetBucketName = createBucket(client, 2);
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
		var bucketName = createBucket(client, 3);
		var targetBucketName = createBucket(client, 3);
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
		var bucketName = createBucket(client, 4);
		var targetBucketName = createBucket(client, 4);
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
		var bucketName = createBucket(client, 5);
		var targetBucketName = createBucket(client, 5);
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
		var bucketName = createBucket(client, 6);
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
		var bucketName = createBucket(client, 7);
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
		var bucketName = getNewBucketNameOnly(8);
		var targetBucketName = createBucket(client, 8);
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
		var bucketName = createBucket(client, 9);
		var targetBucketName = createBucket(client, 9);
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
		var bucketName = createBucket(client, 10);
		var targetBucketName = createBucket(client, 10);
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

	@Test
	@Tag("Put")
	public void testPutBucketInventoryTargetNotExist() {
		var client = getClient();
		var bucketName = createBucket(client, 11);
		var targetBucketName = getNewBucketName(11);
		var inventoryId = "my-inventory";

		var inventory = InventoryConfiguration.builder()
				.id(inventoryId)
				.destination(
						d -> d.s3BucketDestination(s3 -> s3.bucket("arn:aws:s3:::" + targetBucketName).format("CSV")))
				.isEnabled(true)
				.includedObjectVersions(InventoryIncludedObjectVersions.CURRENT)
				.schedule(s -> s.frequency(InventoryFrequency.DAILY));

		// AWS 는 타깃 버킷의 존재 여부를 확인하지 않고 설정을 저장한다
		client.putBucketInventoryConfiguration(p -> p
				.bucket(bucketName).id(inventoryId)
				.inventoryConfiguration(inventory.build()));

		var response = client.getBucketInventoryConfiguration(g -> g.bucket(bucketName).id(inventoryId));
		assertEquals(inventoryId, response.inventoryConfiguration().id());
		assertEquals("arn:aws:s3:::" + targetBucketName,
				response.inventoryConfiguration().destination().s3BucketDestination().bucket());
	}

	@Test
	@Tag("Error")
	public void testPutBucketInventoryInvalidFormat() {
		var client = getClient();
		var bucketName = createBucket(client, 12);
		var targetBucketName = createBucket(client, 12);
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
		var bucketName = createBucket(client, 13);
		var targetBucketName = createBucket(client, 13);
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
		var bucketName = createBucket(client, 14);
		var targetBucketName = createBucket(client, 14);
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
		var bucketName = createBucket(client, 15);
		var targetBucketName = createBucket(client, 15);
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
		var bucketName = createBucket(client, 16);
		var targetBucketName = createBucket(client, 16);
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
		var bucketName = createBucket(client, 17);
		var targetBucketName = createBucket(client, 17);
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

	/** 대상 버킷 ARN·CSV·Daily·Current 인 기본 인벤토리 설정. 필요한 항목을 덮어써서 쓴다. */
	private static InventoryConfiguration.Builder inventoryConfig(String inventoryId, String targetBucketName) {
		return inventoryConfig(inventoryId, targetBucketName, "Current", "Daily", "CSV");
	}

	private static InventoryConfiguration.Builder inventoryConfig(String inventoryId, String targetBucketName,
			String versions, String frequency, String format) {
		return InventoryConfiguration.builder()
				.id(inventoryId)
				.destination(d -> d.s3BucketDestination(s -> s.bucket("arn:aws:s3:::" + targetBucketName)
						.format(format)))
				.isEnabled(true)
				.includedObjectVersions(versions)
				.schedule(s -> s.frequency(frequency));
	}

	private static void putInventory(S3Client client, String bucketName, String inventoryId,
			InventoryConfiguration config) {
		client.putBucketInventoryConfiguration(
				p -> p.bucket(bucketName).id(inventoryId).inventoryConfiguration(config));
	}

	private static void assertError(Executable executable, int statusCode, String errorCode) {
		var e = assertThrows(AwsServiceException.class, executable);
		assertEquals(statusCode, e.statusCode());
		assertEquals(errorCode, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Max")
	public void testPutBucketInventoryIdLength() {
		var client = getClient();
		var bucketName = createBucket(client, 18);
		var targetBucketName = createBucket(client, 18);

		// ID 는 64자까지 허용한다
		var inventoryId = "a".repeat(64);
		putInventory(client, bucketName, inventoryId, inventoryConfig(inventoryId, targetBucketName).build());
		var response = client.getBucketInventoryConfiguration(g -> g.bucket(bucketName).id(inventoryId));
		assertEquals(inventoryId, response.inventoryConfiguration().id());

		var tooLong = "a".repeat(65);
		assertError(() -> putInventory(client, bucketName, tooLong,
				inventoryConfig(tooLong, targetBucketName).build()), HttpStatus.SC_BAD_REQUEST, MainData.MALFORMED_XML);
	}

	@Test
	@Tag("Error")
	public void testPutBucketInventoryInvalidId() {
		var client = getClient();
		var bucketName = createBucket(client, 19);
		var targetBucketName = createBucket(client, 19);

		// ID 에는 영숫자와 - _ . 만 쓸 수 있다
		var inventoryId = "a b/c";
		assertError(() -> putInventory(client, bucketName, inventoryId,
				inventoryConfig(inventoryId, targetBucketName).build()),
				HttpStatus.SC_BAD_REQUEST, MainData.MALFORMED_XML);
	}

	@Test
	@Tag("Error")
	public void testPutBucketInventoryIdMismatch() {
		var client = getClient();
		var bucketName = createBucket(client, 20);
		var targetBucketName = createBucket(client, 20);

		// 쿼리의 id 와 본문의 Id 가 다르면 거부된다
		assertError(() -> putInventory(client, bucketName, "my-inventory",
				inventoryConfig("other-inventory", targetBucketName).build()),
				HttpStatus.SC_BAD_REQUEST, MainData.ID_MISMATCH);
		assertTrue(client.listBucketInventoryConfigurations(l -> l.bucket(bucketName)).inventoryConfigurationList()
				.isEmpty());
	}

	@Test
	@Tag("Check")
	public void testPutBucketInventoryDisabled() {
		var client = getClient();
		var bucketName = createBucket(client, 21);
		var targetBucketName = createBucket(client, 21);
		var inventoryId = "my-inventory";

		putInventory(client, bucketName, inventoryId,
				inventoryConfig(inventoryId, targetBucketName).isEnabled(false).build());
		var response = client.getBucketInventoryConfiguration(g -> g.bucket(bucketName).id(inventoryId));
		assertEquals(false, response.inventoryConfiguration().isEnabled());
	}

	@Test
	@Tag("Error")
	public void testPutBucketInventoryInvalidVersions() {
		var client = getClient();
		var bucketName = createBucket(client, 22);
		var targetBucketName = createBucket(client, 22);
		var inventoryId = "my-inventory";

		assertError(() -> putInventory(client, bucketName, inventoryId,
				inventoryConfig(inventoryId, targetBucketName, "Invalid", "Daily", "CSV").build()),
				HttpStatus.SC_BAD_REQUEST, MainData.MALFORMED_XML);
	}

	@Test
	@Tag("Check")
	public void testPutBucketInventoryFormats() {
		var client = getClient();
		var bucketName = createBucket(client, 23);
		var targetBucketName = createBucket(client, 23);

		// 형식(CSV/ORC/Parquet), 주기(Daily/Weekly), 대상 버전(All/Current)이 그대로 저장된다
		// 순서: ID, 형식, 주기, 대상 버전
		var cases = Arrays.asList(
				Arrays.asList("inventory-csv", "CSV", "Daily", "All"),
				Arrays.asList("inventory-orc", "ORC", "Weekly", "Current"),
				Arrays.asList("inventory-parquet", "Parquet", "Weekly", "All"));
		for (var item : cases) {
			putInventory(client, bucketName, item.get(0),
					inventoryConfig(item.get(0), targetBucketName, item.get(3), item.get(2), item.get(1)).build());
		}
		for (var item : cases) {
			var config = client.getBucketInventoryConfiguration(g -> g.bucket(bucketName).id(item.get(0)))
					.inventoryConfiguration();
			assertEquals(item.get(1), config.destination().s3BucketDestination().formatAsString());
			assertEquals(item.get(2), config.schedule().frequencyAsString());
			assertEquals(item.get(3), config.includedObjectVersionsAsString());
		}
	}

	@Test
	@Tag("Check")
	public void testPutBucketInventoryDestination() {
		var client = getClient();
		var bucketName = createBucket(client, 24);
		var targetBucketName = createBucket(client, 24);
		var inventoryId = "my-inventory";

		// 대상의 접두어와 SSE-S3 암호화 설정이 그대로 저장된다
		var config = inventoryConfig(inventoryId, targetBucketName)
				.destination(d -> d.s3BucketDestination(s -> s.bucket("arn:aws:s3:::" + targetBucketName)
						.format("CSV").prefix("inventory/").encryption(e -> e.sses3(SSES3.builder().build()))))
				.build();
		putInventory(client, bucketName, inventoryId, config);

		var destination = client.getBucketInventoryConfiguration(g -> g.bucket(bucketName).id(inventoryId))
				.inventoryConfiguration().destination().s3BucketDestination();
		assertEquals("arn:aws:s3:::" + targetBucketName, destination.bucket());
		assertEquals("inventory/", destination.prefix());
		assertNotNull(destination.encryption().sses3());
	}

	@Test
	@Tag("Error")
	public void testPutBucketInventoryInvalidDestinationArn() {
		var client = getClient();
		var bucketName = createBucket(client, 25);
		var targetBucketName = createBucket(client, 25);
		var inventoryId = "my-inventory";

		// 대상 버킷은 ARN 형식이어야 한다
		var config = inventoryConfig(inventoryId, targetBucketName)
				.destination(d -> d.s3BucketDestination(s -> s.bucket(targetBucketName).format("CSV"))).build();
		assertError(() -> putInventory(client, bucketName, inventoryId, config), HttpStatus.SC_BAD_REQUEST,
				MainData.INVALID_S3_DESTINATION_BUCKET);
	}

	@Test
	@Tag("List")
	public void testListBucketInventoryPagination() {
		var client = getClient();
		var bucketName = createBucket(client, 26);
		var targetBucketName = createBucket(client, 26);
		var ids = new ArrayList<String>();
		for (int i = 0; i < 101; i++)
			ids.add(String.format("inventory-%03d", i));
		for (var inventoryId : ids)
			putInventory(client, bucketName, inventoryId, inventoryConfig(inventoryId, targetBucketName).build());

		// 한 번에 100개까지만 돌려주고 나머지는 토큰으로 이어서 받는다
		var response = client.listBucketInventoryConfigurations(l -> l.bucket(bucketName));
		assertEquals(100, response.inventoryConfigurationList().size());
		assertEquals(true, response.isTruncated());
		var listed = new ArrayList<String>();
		response.inventoryConfigurationList().forEach(c -> listed.add(c.id()));

		var token = response.nextContinuationToken();
		response = client.listBucketInventoryConfigurations(l -> l.bucket(bucketName).continuationToken(token));
		assertEquals(1, response.inventoryConfigurationList().size());
		assertEquals(false, response.isTruncated());
		response.inventoryConfigurationList().forEach(c -> listed.add(c.id()));
		Collections.sort(listed);
		assertEquals(ids, listed);
	}

	@Test
	@Tag("List")
	public void testListBucketInventoryInvalidToken() {
		var client = getClient();
		var bucketName = createBucket(client, 27);

		assertError(() -> client.listBucketInventoryConfigurations(
				l -> l.bucket(bucketName).continuationToken("invalid")),
				HttpStatus.SC_BAD_REQUEST, MainData.MALFORMED_CONTINUATION_TOKEN);
	}

	@Test
	@Tag("Error")
	public void testBucketInventoryBucketNotExist() {
		var client = getClient();
		var bucketName = getNewBucketNameOnly(28);

		assertError(() -> client.listBucketInventoryConfigurations(l -> l.bucket(bucketName)),
				HttpStatus.SC_NOT_FOUND, MainData.NO_SUCH_BUCKET);
		assertError(() -> client.getBucketInventoryConfiguration(g -> g.bucket(bucketName).id("my-inventory")),
				HttpStatus.SC_NOT_FOUND, MainData.NO_SUCH_BUCKET);
		assertError(() -> client.deleteBucketInventoryConfiguration(d -> d.bucket(bucketName).id("my-inventory")),
				HttpStatus.SC_NOT_FOUND, MainData.NO_SUCH_BUCKET);
	}
}
