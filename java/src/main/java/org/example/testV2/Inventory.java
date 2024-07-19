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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;

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
		System.out.println("Inventory Start");
	}

	@org.junit.jupiter.api.AfterAll
	public static void afterAll() {
		System.out.println("Inventory End");
	}

	@Test
	@Tag("List")
	// 버킷에 인벤토리를 설정하지 않은 상태에서 조회가 가능한지 확인
	public void testListBucketInventory() {
		var client = getClient();
		var bucketName = createBucket(client);

		var response = client.listBucketInventoryConfigurations(l -> l.bucket(bucketName));
		assertNull(response.inventoryConfigurationList());
	}

	@Test
	@Tag("Put")
	// 버킷에 인벤토리를 설정할 수 있는지 확인
	public void testPutBucketInventory() {
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
		client.putBucketInventoryConfiguration(p -> p.bucket(bucketName).inventoryConfiguration(inventory.build()));
	}

	@Test
	@Tag("Check")
	// 버킷에 인벤토리 설정이 되었는지 확인
	public void testCheckBucketInventory() {
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
				.bucket(bucketName)
				.inventoryConfiguration(inventory.build()));

		var response = client.listBucketInventoryConfigurations(l -> l.bucket(bucketName));
		assertEquals(1, response.inventoryConfigurationList().size());
	}

	@Test
	@Tag("Get")
	// 버킷에 설정된 인벤토리 설정을 가져올 수 있는지 확인
	public void testGetBucketInventory() {
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
				.bucket(bucketName)
				.inventoryConfiguration(inventory.build()));

		var response = client.getBucketInventoryConfiguration(g -> g.bucket(bucketName).id(inventoryId));
		assertEquals(inventoryId, response.inventoryConfiguration().id());
	}

	@Test
	@Tag("Delete")
	// 버킷에 설정된 인벤토리 설정을 삭제할 수 있는지 확인
	public void testDeleteBucketInventory() {
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
				.bucket(bucketName)
				.inventoryConfiguration(inventory.build()));

		client.deleteBucketInventoryConfiguration(d -> d.bucket(bucketName).id(inventoryId));
		var response = client.listBucketInventoryConfigurations(l -> l.bucket(bucketName));
		assertNull(response.inventoryConfigurationList());
	}

	@Test
	@Tag("Error")
	// 존재하지 않은 인벤토리를 가져오려고 할 경우 실패하는지 확인
	public void testGetBucketInventoryNotExist() {
		var client = getClient();
		var bucketName = createBucket(client);
		var inventoryId = "my-inventory";

		var e = assertThrows(AwsServiceException.class,
				() -> client.getBucketInventoryConfiguration(g -> g
						.bucket(bucketName)
						.id(inventoryId)));

		assertEquals(404, e.statusCode());
		assertEquals(MainData.NO_SUCH_CONFIGURATION, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Error")
	// 존재하지 않은 인벤토리를 삭제하려고 할 경우 실패하는지 확인
	public void testDeleteBucketInventoryNotExist() {
		var client = getClient();
		var bucketName = createBucket(client);
		var inventoryId = "my-inventory";

		var e = assertThrows(AwsServiceException.class,
				() -> client.deleteBucketInventoryConfiguration(d -> d.bucket(bucketName).id(inventoryId)));

		assertEquals(404, e.statusCode());
		assertEquals(MainData.NO_SUCH_CONFIGURATION, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Error")
	// 존재하지 않은 버킷에 인벤토리를 설정하려고 할 경우 실패하는지 확인
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
						p -> p.bucket(bucketName).inventoryConfiguration(inventory.build())));

		assertEquals(404, e.statusCode());
		assertEquals(MainData.NO_SUCH_BUCKET, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Error")
	// 인벤토리 아이디를 빈값으로 설정하려고 할 경우 실패하는지 확인
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
						.bucket(bucketName)
						.inventoryConfiguration(inventory.build())));

		assertEquals(400, e.statusCode());
		assertEquals(MainData.INVALID_CONFIGURATION_ID, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Error")
	// 인벤토리 아이디를 중복으로 설정하려고 할 경우 실패하는지 확인
	public void testPutBucketInventoryIdDuplicate() {
		var client = getClient();
		var bucketName = createBucket(client);
		var targetBucketName = createBucket(client);
		var inventoryId = "my-inventory";

		var inventory = InventoryConfiguration.builder()
				.id(inventoryId)
				.destination(d -> d.s3BucketDestination(s3 -> s3.bucket("arn:aws:s3:::" + targetBucketName).format("CSV")))
				.isEnabled(true)
				.includedObjectVersions(InventoryIncludedObjectVersions.CURRENT)
				.schedule(s -> s.frequency(InventoryFrequency.DAILY));

		client.putBucketInventoryConfiguration(p -> p
				.bucket(bucketName)
				.inventoryConfiguration(inventory.build()));

		var inventory2 = InventoryConfiguration.builder()
				.id(inventoryId)
				.destination(d -> d.s3BucketDestination(s3 -> s3.bucket("arn:aws:s3:::" + targetBucketName).format("CSV")))
				.isEnabled(true)
				.includedObjectVersions(InventoryIncludedObjectVersions.CURRENT)
				.schedule(s -> s.frequency(InventoryFrequency.DAILY));

		client.putBucketInventoryConfiguration(p -> p
				.bucket(bucketName)
				.inventoryConfiguration(inventory2.build()));

		var response = client.listBucketInventoryConfigurations(l -> l.bucket(bucketName));

		assertEquals(2, response.inventoryConfigurationList().size());
	}

	@Disabled("aws에서 타깃 버킷이 존재하는지 확인하지 않음")
	@Test
	@Tag("Error")
	// 타깃 버킷이 존재하지 않을 경우 실패하는지 확인
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
						.bucket(bucketName)
						.inventoryConfiguration(inventory.build())));

		assertEquals(404, e.statusCode());
		assertEquals(MainData.NO_SUCH_BUCKET, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Error")
	// 지원하지 않는 파일 형식의 인벤토리를 설정하려고 할 경우 실패하는지 확인
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
						.bucket(bucketName)
						.inventoryConfiguration(inventory.build())));

		assertEquals(400, e.statusCode());
		assertEquals(MainData.MALFORMED_XML, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Error")
	// 올바르지 않은 주기의 인벤토리를 설정하려고 할 경우 실패하는지 확인
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
						.bucket(bucketName)
						.inventoryConfiguration(inventory.build())));

		assertEquals(400, e.statusCode());
		assertEquals(MainData.MALFORMED_XML, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Error")
	// 대소문자를 잘못 입력하여 인벤토리를 설정하려고 할 경우 실패하는지 확인
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
						.bucket(bucketName)
						.inventoryConfiguration(inventory.build())));

		assertEquals(400, e.statusCode());
		assertEquals(MainData.MALFORMED_XML, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Put")
	// 접두어를 포함한 인벤토리 설정이 올바르게 적용되는지 확인
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
				.bucket(bucketName)
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
	// 옵션을 포함한 인벤토리 설정이 올바르게 적용되는지 확인
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
				.bucket(bucketName)
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
	// 옵션을 잘못 입력하여 인벤토리를 설정하려고 할 경우 실패하는지 확인
	public void testPutBucketInventoryInvalidOptional() {
		var client = getClient();
		var bucketName = createBucket(client);
		var targetBucketName = createBucket(client);
		var inventoryId = "my-inventory";

		var inventoryPrefix = "a/";
		var inventoryOptionalFields = Arrays.asList(
				InventoryOptionalField.SIZE,
				InventoryOptionalField.valueOf("--"));

		var inventory = InventoryConfiguration.builder()
				.id(inventoryId)
				.destination(
						d -> d.s3BucketDestination(s3 -> s3.bucket("arn:aws:s3:::" + targetBucketName).format("CSV")
								.prefix(inventoryPrefix)))
				.isEnabled(true)
				.includedObjectVersions(InventoryIncludedObjectVersions.CURRENT)
				.schedule(s -> s.frequency(InventoryFrequency.DAILY))
				.optionalFields(inventoryOptionalFields);

		var e = assertThrows(AwsServiceException.class,
				() -> client.putBucketInventoryConfiguration(p -> p
						.bucket(bucketName)
						.inventoryConfiguration(inventory.build())));

		assertEquals(400, e.statusCode());
		assertEquals(MainData.MALFORMED_XML, e.awsErrorDetails().errorCode());
	}
}
