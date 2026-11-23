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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import org.apache.hc.core5.http.HttpStatus;
import org.example.Data.MainData;
import org.junit.Test;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.function.Executable;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.ListBucketInventoryConfigurationsRequest;
import com.amazonaws.services.s3.model.inventory.InventoryConfiguration;
import com.amazonaws.services.s3.model.inventory.InventoryDestination;
import com.amazonaws.services.s3.model.inventory.InventoryFrequency;
import com.amazonaws.services.s3.model.inventory.InventoryIncludedObjectVersions;
import com.amazonaws.services.s3.model.inventory.InventoryS3BucketDestination;
import com.amazonaws.services.s3.model.inventory.InventorySchedule;
import com.amazonaws.services.s3.model.inventory.ServerSideEncryptionS3;

public class Inventory extends TestBase {
	@Test
	@Tag("List")
	public void testListBucketInventory() {
		var client = getClient();
		var bucketName = createBucket(client, 1);

		var response = client.listBucketInventoryConfigurations(
				new ListBucketInventoryConfigurationsRequest().withBucketName(bucketName));
		assertNull(response.getInventoryConfigurationList());
	}

	@Test
	@Tag("Put")
	public void testPutBucketInventory() {
		var client = getClient();
		var bucketName = createBucket(client, 2);
		var targetBucketName = createBucket(client, 2);
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
		var bucketName = createBucket(client, 3);
		var targetBucketName = createBucket(client, 3);
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
		var bucketName = createBucket(client, 4);
		var targetBucketName = createBucket(client, 4);
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
		var bucketName = createBucket(client, 5);
		var targetBucketName = createBucket(client, 5);
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
		var bucketName = createBucket(client, 6);
		var inventoryId = "my-inventory";

		var e = assertThrows(AmazonServiceException.class,
				() -> client.getBucketInventoryConfiguration(bucketName, inventoryId));

		assertEquals(HttpStatus.SC_NOT_FOUND, e.getStatusCode());
		assertEquals(MainData.NO_SUCH_CONFIGURATION, e.getErrorCode());
	}

	@Test
	@Tag("Error")
	public void testDeleteBucketInventoryNotExist() {
		var client = getClient();
		var bucketName = createBucket(client, 7);
		var inventoryId = "my-inventory";

		var e = assertThrows(AmazonServiceException.class,
				() -> client.deleteBucketInventoryConfiguration(bucketName, inventoryId));

		assertEquals(HttpStatus.SC_NOT_FOUND, e.getStatusCode());
		assertEquals(MainData.NO_SUCH_CONFIGURATION, e.getErrorCode());
	}

	@Test
	@Tag("Error")
	public void testPutBucketInventoryNotExist() {
		var client = getClient();
		var bucketName = getNewBucketNameOnly(8);
		var targetBucketName = createBucket(client, 8);
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

		assertEquals(HttpStatus.SC_NOT_FOUND, e.getStatusCode());
		assertEquals(MainData.NO_SUCH_BUCKET, e.getErrorCode());
	}

	@Test
	@Tag("Error")
	public void testPutBucketInventoryIdNotExist() {
		var client = getClient();
		var bucketName = createBucket(client, 9);
		var targetBucketName = createBucket(client, 9);
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

		assertEquals(HttpStatus.SC_BAD_REQUEST, e.getStatusCode());
		assertEquals(MainData.MALFORMED_XML, e.getErrorCode());
	}

	@Test
	@Tag("Error")
	public void testPutBucketInventoryIdDuplicate() {
		var client = getClient();
		var bucketName = createBucket(client, 10);
		var targetBucketName = createBucket(client, 10);
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

	@Test
	@Tag("Put")
	public void testPutBucketInventoryTargetNotExist() {
		var client = getClient();
		var bucketName = createBucket(client, 11);
		var targetBucketName = getNewBucketName(11);
		var inventoryId = "my-inventory";

		var inventory = new InventoryConfiguration()
				.withId(inventoryId)
				.withDestination(new InventoryDestination().withS3BucketDestination(new InventoryS3BucketDestination()
						.withBucketArn("arn:aws:s3:::" + targetBucketName).withFormat("CSV")))
				.withEnabled(true)
				.withIncludedObjectVersions(InventoryIncludedObjectVersions.Current)
				.withSchedule(new InventorySchedule().withFrequency(InventoryFrequency.Daily));

		// AWS 는 타깃 버킷의 존재 여부를 확인하지 않고 설정을 저장한다
		client.setBucketInventoryConfiguration(bucketName, inventory);

		var response = client.getBucketInventoryConfiguration(bucketName, inventoryId);
		assertEquals(inventoryId, response.getInventoryConfiguration().getId());
		assertEquals("arn:aws:s3:::" + targetBucketName,
				response.getInventoryConfiguration().getDestination().getS3BucketDestination().getBucketArn());
	}

	@Test
	@Tag("Error")
	public void testPutBucketInventoryInvalidFormat() {
		var client = getClient();
		var bucketName = createBucket(client, 12);
		var targetBucketName = createBucket(client, 12);
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

		assertEquals(HttpStatus.SC_BAD_REQUEST, e.getStatusCode());
		assertEquals(MainData.MALFORMED_XML, e.getErrorCode());
	}

	@Test
	@Tag("Error")
	public void testPutBucketInventoryInvalidFrequency() {
		var client = getClient();
		var bucketName = createBucket(client, 13);
		var targetBucketName = createBucket(client, 13);
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

		assertEquals(HttpStatus.SC_BAD_REQUEST, e.getStatusCode());
		assertEquals(MainData.MALFORMED_XML, e.getErrorCode());
	}

	@Test
	@Tag("Error")
	public void testPutBucketInventoryInvalidCase() {
		var client = getClient();
		var bucketName = createBucket(client, 14);
		var targetBucketName = createBucket(client, 14);
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

		assertEquals(HttpStatus.SC_BAD_REQUEST, e.getStatusCode());
		assertEquals(MainData.MALFORMED_XML, e.getErrorCode());
	}

	@Test
	@Tag("Filter")
	public void testPutBucketInventoryPrefix() {
		var client = getClient();
		var bucketName = createBucket(client, 15);
		var targetBucketName = createBucket(client, 15);
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
		var bucketName = createBucket(client, 16);
		var targetBucketName = createBucket(client, 16);
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
		var bucketName = createBucket(client, 17);
		var targetBucketName = createBucket(client, 17);
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

		assertEquals(HttpStatus.SC_BAD_REQUEST, e.getStatusCode());
		assertEquals(MainData.MALFORMED_XML, e.getErrorCode());
	}

	/** 대상 버킷 ARN·CSV·Daily·Current 인 기본 인벤토리 설정. 필요한 항목을 덮어써서 쓴다. */
	private static InventoryConfiguration inventoryConfig(String inventoryId, String targetBucketName) {
		return inventoryConfig(inventoryId, targetBucketName, "Current", "Daily", "CSV");
	}

	private static InventoryConfiguration inventoryConfig(String inventoryId, String targetBucketName,
			String versions, String frequency, String format) {
		return new InventoryConfiguration()
				.withId(inventoryId)
				.withDestination(new InventoryDestination().withS3BucketDestination(new InventoryS3BucketDestination()
						.withBucketArn("arn:aws:s3:::" + targetBucketName).withFormat(format)))
				.withEnabled(true)
				.withIncludedObjectVersions(versions)
				.withSchedule(new InventorySchedule().withFrequency(frequency));
	}

	private static void assertError(Executable executable, int statusCode, String errorCode) {
		var e = assertThrows(AmazonServiceException.class, executable);
		assertEquals(statusCode, e.getStatusCode());
		assertEquals(errorCode, e.getErrorCode());
	}

	@Test
	@Tag("Max")
	public void testPutBucketInventoryIdLength() {
		var client = getClient();
		var bucketName = createBucket(client, 18);
		var targetBucketName = createBucket(client, 18);

		// ID 는 64자까지 허용한다
		var inventoryId = "a".repeat(64);
		client.setBucketInventoryConfiguration(bucketName, inventoryConfig(inventoryId, targetBucketName));
		var response = client.getBucketInventoryConfiguration(bucketName, inventoryId);
		assertEquals(inventoryId, response.getInventoryConfiguration().getId());

		assertError(() -> client.setBucketInventoryConfiguration(bucketName,
				inventoryConfig("a".repeat(65), targetBucketName)), HttpStatus.SC_BAD_REQUEST, MainData.MALFORMED_XML);
	}

	@Test
	@Tag("Error")
	public void testPutBucketInventoryInvalidId() {
		var client = getClient();
		var bucketName = createBucket(client, 19);
		var targetBucketName = createBucket(client, 19);

		// ID 에는 영숫자와 - _ . 만 쓸 수 있다
		assertError(() -> client.setBucketInventoryConfiguration(bucketName,
				inventoryConfig("a b/c", targetBucketName)), HttpStatus.SC_BAD_REQUEST, MainData.MALFORMED_XML);
	}

	@Test
	@Tag("Check")
	public void testPutBucketInventoryDisabled() {
		var client = getClient();
		var bucketName = createBucket(client, 21);
		var targetBucketName = createBucket(client, 21);
		var inventoryId = "my-inventory";

		client.setBucketInventoryConfiguration(bucketName,
				inventoryConfig(inventoryId, targetBucketName).withEnabled(false));
		var response = client.getBucketInventoryConfiguration(bucketName, inventoryId);
		assertEquals(false, response.getInventoryConfiguration().isEnabled());
	}

	@Test
	@Tag("Error")
	public void testPutBucketInventoryInvalidVersions() {
		var client = getClient();
		var bucketName = createBucket(client, 22);
		var targetBucketName = createBucket(client, 22);

		assertError(() -> client.setBucketInventoryConfiguration(bucketName,
				inventoryConfig("my-inventory", targetBucketName, "Invalid", "Daily", "CSV")),
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
			client.setBucketInventoryConfiguration(bucketName,
					inventoryConfig(item.get(0), targetBucketName, item.get(3), item.get(2), item.get(1)));
		}
		for (var item : cases) {
			var config = client.getBucketInventoryConfiguration(bucketName, item.get(0)).getInventoryConfiguration();
			assertEquals(item.get(1), config.getDestination().getS3BucketDestination().getFormat());
			assertEquals(item.get(2), config.getSchedule().getFrequency());
			assertEquals(item.get(3), config.getIncludedObjectVersions());
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
		var config = inventoryConfig(inventoryId, targetBucketName);
		config.getDestination().getS3BucketDestination().withPrefix("inventory/")
				.withEncryption(new ServerSideEncryptionS3());
		client.setBucketInventoryConfiguration(bucketName, config);

		var destination = client.getBucketInventoryConfiguration(bucketName, inventoryId).getInventoryConfiguration()
				.getDestination().getS3BucketDestination();
		assertEquals("arn:aws:s3:::" + targetBucketName, destination.getBucketArn());
		assertEquals("inventory/", destination.getPrefix());
		assertInstanceOf(ServerSideEncryptionS3.class, destination.getEncryption());
	}

	@Test
	@Tag("Error")
	public void testPutBucketInventoryInvalidDestinationArn() {
		var client = getClient();
		var bucketName = createBucket(client, 25);
		var targetBucketName = createBucket(client, 25);

		// 대상 버킷은 ARN 형식이어야 한다
		var config = inventoryConfig("my-inventory", targetBucketName);
		config.getDestination().getS3BucketDestination().withBucketArn(targetBucketName);
		assertError(() -> client.setBucketInventoryConfiguration(bucketName, config), HttpStatus.SC_BAD_REQUEST,
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
			client.setBucketInventoryConfiguration(bucketName, inventoryConfig(inventoryId, targetBucketName));

		// 한 번에 100개까지만 돌려주고 나머지는 토큰으로 이어서 받는다
		var response = client.listBucketInventoryConfigurations(
				new ListBucketInventoryConfigurationsRequest().withBucketName(bucketName));
		assertEquals(100, response.getInventoryConfigurationList().size());
		assertEquals(true, response.isTruncated());
		var listed = new ArrayList<String>();
		response.getInventoryConfigurationList().forEach(c -> listed.add(c.getId()));

		response = client.listBucketInventoryConfigurations(new ListBucketInventoryConfigurationsRequest()
				.withBucketName(bucketName).withContinuationToken(response.getNextContinuationToken()));
		assertEquals(1, response.getInventoryConfigurationList().size());
		assertEquals(false, response.isTruncated());
		response.getInventoryConfigurationList().forEach(c -> listed.add(c.getId()));
		Collections.sort(listed);
		assertEquals(ids, listed);
	}

	@Test
	@Tag("List")
	public void testListBucketInventoryInvalidToken() {
		var client = getClient();
		var bucketName = createBucket(client, 27);

		assertError(() -> client.listBucketInventoryConfigurations(new ListBucketInventoryConfigurationsRequest()
				.withBucketName(bucketName).withContinuationToken("invalid")),
				HttpStatus.SC_BAD_REQUEST, MainData.MALFORMED_CONTINUATION_TOKEN);
	}

	@Test
	@Tag("Error")
	public void testBucketInventoryBucketNotExist() {
		var client = getClient();
		var bucketName = getNewBucketNameOnly(28);

		assertError(() -> client.listBucketInventoryConfigurations(
				new ListBucketInventoryConfigurationsRequest().withBucketName(bucketName)),
				HttpStatus.SC_NOT_FOUND, MainData.NO_SUCH_BUCKET);
		assertError(() -> client.getBucketInventoryConfiguration(bucketName, "my-inventory"),
				HttpStatus.SC_NOT_FOUND, MainData.NO_SUCH_BUCKET);
		assertError(() -> client.deleteBucketInventoryConfiguration(bucketName, "my-inventory"),
				HttpStatus.SC_NOT_FOUND, MainData.NO_SUCH_BUCKET);
	}
}
