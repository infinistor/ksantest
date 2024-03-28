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
import org.junit.Ignore;
import org.junit.Test;
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
		System.out.println("Inventory SDK V2 Start");
	}

	@org.junit.jupiter.api.AfterAll
	public static void afterAll() {
		System.out.println("Inventory SDK V2 End");
	}

	@Test
	@Tag("List")
	// 버킷에 인벤토리를 설정하지 않은 상태에서 조회가 가능한지 확인
	public void test_list_bucket_inventory() {
		var client = getClient();
		var bucketName = getNewBucket();

		var response = client.listBucketInventoryConfigurations(
				new ListBucketInventoryConfigurationsRequest().withBucketName(bucketName));
		assertNull(response.getInventoryConfigurationList());
	}

	@Test
	@Tag("Put")
	// 버킷에 인벤토리를 설정할 수 있는지 확인
	public void test_put_bucket_inventory() {
		var client = getClient();
		var bucketName = getNewBucket();
		var targetBucketName = getNewBucket();
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
	// 버킷에 인벤토리 설정이 되었는지 확인
	public void test_check_bucket_inventory() {
		var client = getClient();
		var bucketName = getNewBucket();
		var targetBucketName = getNewBucket();
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
	// 버킷에 설정된 인벤토리 설정을 가져올 수 있는지 확인
	public void test_get_bucket_inventory() {
		var client = getClient();
		var bucketName = getNewBucket();
		var targetBucketName = getNewBucket();
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
	// 버킷에 설정된 인벤토리 설정을 삭제할 수 있는지 확인
	public void test_delete_bucket_inventory() {
		var client = getClient();
		var bucketName = getNewBucket();
		var targetBucketName = getNewBucket();
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
	// 존재하지 않은 인벤토리를 가져오려고 할 경우 실패하는지 확인
	public void test_get_bucket_inventory_not_exist() {
		var client = getClient();
		var bucketName = getNewBucket();
		var inventoryId = "my-inventory";

		var e = assertThrows(AmazonServiceException.class,
				() -> client.getBucketInventoryConfiguration(bucketName, inventoryId));

		assertEquals(404, e.getStatusCode());
		assertEquals(MainData.NoSuchConfiguration, e.getErrorCode());
	}

	@Test
	@Tag("Error")
	// 존재하지 않은 인벤토리를 삭제하려고 할 경우 실패하는지 확인
	public void test_delete_bucket_inventory_not_exist() {
		var client = getClient();
		var bucketName = getNewBucket();
		var inventoryId = "my-inventory";

		var e = assertThrows(AmazonServiceException.class,
				() -> client.deleteBucketInventoryConfiguration(bucketName, inventoryId));

		assertEquals(404, e.getStatusCode());
		assertEquals(MainData.NoSuchConfiguration, e.getErrorCode());
	}

	@Test
	@Tag("Error")
	// 존재하지 않은 버킷에 인벤토리를 설정하려고 할 경우 실패하는지 확인
	public void test_put_bucket_inventory_not_exist() {
		var client = getClient();
		var bucketName = getNewBucketName();
		var targetBucketName = getNewBucket();
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
		assertEquals(MainData.NoSuchBucket, e.getErrorCode());
	}

	@Test
	@Tag("Error")
	// 인벤토리 아이디를 빈값으로 설정하려고 할 경우 실패하는지 확인
	public void test_put_bucket_inventory_id_not_exist() {
		var client = getClient();
		var bucketName = getNewBucket();
		var targetBucketName = getNewBucket();
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
		assertEquals(MainData.InvalidConfigurationId, e.getErrorCode());
	}

	@Test
	@Tag("Error")
	// 인벤토리 아이디를 중복으로 설정하려고 할 경우 실패하는지 확인
	public void test_put_bucket_inventory_id_duplicate() {
		var client = getClient();
		var bucketName = getNewBucket();
		var targetBucketName = getNewBucket();
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

		assertEquals(2, response.getInventoryConfigurationList().size());

		// var e = assertThrows(AmazonServiceException.class,
		// () -> client.setBucketInventoryConfiguration(bucketName, inventory));

		// assertEquals(409, e.getStatusCode());
		// assertEquals(MainData.DuplicateInventory, e.getErrorCode());
	}

	@Ignore("aws에서 타깃 버킷이 존재하는지 확인하지 않음")
	@Test
	@Tag("Error")
	// 타깃 버킷이 존재하지 않을 경우 실패하는지 확인
	public void test_put_bucket_inventory_target_not_exist() {
		var client = getClient();
		var bucketName = getNewBucket();
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
		assertEquals(MainData.NoSuchBucket, e.getErrorCode());
	}

	@Test
	@Tag("Error")
	// 지원하지 않는 파일 형식의 인벤토리를 설정하려고 할 경우 실패하는지 확인
	public void test_put_bucket_inventory_invalid_format() {
		var client = getClient();
		var bucketName = getNewBucket();
		var targetBucketName = getNewBucket();
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
		assertEquals(MainData.MalformedXML, e.getErrorCode());
	}

	@Test
	@Tag("Error")
	// 올바르지 않은 주기의 인벤토리를 설정하려고 할 경우 실패하는지 확인
	public void test_put_bucket_inventory_invalid_frequency() {
		var client = getClient();
		var bucketName = getNewBucket();
		var targetBucketName = getNewBucket();
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
		assertEquals(MainData.MalformedXML, e.getErrorCode());
	}

	@Test
	@Tag("Error")
	// 대소문자를 잘못 입력하여 인벤토리를 설정하려고 할 경우 실패하는지 확인
	public void test_put_bucket_inventory_invalid_case() {
		var client = getClient();
		var bucketName = getNewBucket();
		var targetBucketName = getNewBucket();
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
		assertEquals(MainData.MalformedXML, e.getErrorCode());
	}

	@Test
	@Tag("Put")
	// 접두어를 포함한 인벤토리 설정이 올바르게 적용되는지 확인
	public void test_put_bucket_inventory_prefix() {
		var client = getClient();
		var bucketName = getNewBucket();
		var targetBucketName = getNewBucket();
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
	@Tag("Put")
	// 옵션을 포함한 인벤토리 설정이 올바르게 적용되는지 확인
	public void test_put_bucket_inventory_optional() {
		var client = getClient();
		var bucketName = getNewBucket();
		var targetBucketName = getNewBucket();
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
	// 옵션을 잘못 입력하여 인벤토리를 설정하려고 할 경우 실패하는지 확인
	public void test_put_bucket_inventory_invalid_optional() {
		var client = getClient();
		var bucketName = getNewBucket();
		var targetBucketName = getNewBucket();
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
		assertEquals(MainData.MalformedXML, e.getErrorCode());
	}
}
