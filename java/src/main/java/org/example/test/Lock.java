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

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Calendar;
import java.util.TimeZone;

import org.example.Data.MainData;
import org.example.Utility.Utils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.BucketVersioningConfiguration;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.DefaultRetention;
import com.amazonaws.services.s3.model.DeleteVersionRequest;
import com.amazonaws.services.s3.model.GetObjectLegalHoldRequest;
import com.amazonaws.services.s3.model.GetObjectLockConfigurationRequest;
import com.amazonaws.services.s3.model.GetObjectRetentionRequest;
import com.amazonaws.services.s3.model.ObjectLockConfiguration;
import com.amazonaws.services.s3.model.ObjectLockEnabled;
import com.amazonaws.services.s3.model.ObjectLockLegalHold;
import com.amazonaws.services.s3.model.ObjectLockLegalHoldStatus;
import com.amazonaws.services.s3.model.ObjectLockRetention;
import com.amazonaws.services.s3.model.ObjectLockRetentionMode;
import com.amazonaws.services.s3.model.ObjectLockRule;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.SetBucketVersioningConfigurationRequest;
import com.amazonaws.services.s3.model.SetObjectLegalHoldRequest;
import com.amazonaws.services.s3.model.SetObjectLockConfigurationRequest;
import com.amazonaws.services.s3.model.SetObjectRetentionRequest;

public class Lock extends TestBase {
	@org.junit.jupiter.api.BeforeAll
	public static void beforeAll() {
		System.out.println("Lock Start");
	}

	@org.junit.jupiter.api.AfterAll
	public static void afterAll() {
		System.out.println("Lock End");
	}

	@Test
	@Tag("Check")
	// [버킷의 Lock옵션을 활성화] 오브젝트의 잠금 설정이 가능한지 확인
	public void testObjectLockPutObjLock() {
		var client = getClient();
		var bucketName = getNewBucketName();
		client.createBucket(new CreateBucketRequest(bucketName).withObjectLockEnabledForBucket(true));

		var conf = new ObjectLockConfiguration().withObjectLockEnabled(ObjectLockEnabled.ENABLED)
				.withRule(new ObjectLockRule().withDefaultRetention(
						new DefaultRetention().withMode(ObjectLockRetentionMode.COMPLIANCE).withYears(1)));
		client.setObjectLockConfiguration(
				new SetObjectLockConfigurationRequest().withBucketName(bucketName).withObjectLockConfiguration(conf));

		var versionResponse = client.getBucketVersioningConfiguration(bucketName);
		assertEquals(BucketVersioningConfiguration.ENABLED, versionResponse.getStatus());
	}

	@Test
	@Tag("ERROR")
	// 버킷을 Lock옵션을 활성화 하지않을 경우 lock 설정이 실패
	public void testObjectLockPutObjLockInvalidBucket() {
		var client = getClient();
		var bucketName = getNewBucketName();
		client.createBucket(bucketName);

		var conf = new ObjectLockConfiguration().withObjectLockEnabled(ObjectLockEnabled.ENABLED)
				.withRule(new ObjectLockRule().withDefaultRetention(
						new DefaultRetention().withMode(ObjectLockRetentionMode.GOVERNANCE).withYears(1)));

		var e = assertThrows(AmazonServiceException.class, () -> client.setObjectLockConfiguration(
				new SetObjectLockConfigurationRequest().withBucketName(bucketName).withObjectLockConfiguration(conf)));
		assertEquals(409, e.getStatusCode());
		assertEquals(MainData.INVALID_BUCKET_STATE, e.getErrorCode());
	}

	@Test
	@Tag("ERROR")
	// [버킷의 Lock옵션을 활성화] Days, Years값 모두 입력하여 Lock 설정할경우 실패
	public void testObjectLockPutObjLockWithDaysAndYears() {
		var client = getClient();
		var bucketName = getNewBucketName();
		client.createBucket(new CreateBucketRequest(bucketName).withObjectLockEnabledForBucket(true));

		var conf = new ObjectLockConfiguration().withObjectLockEnabled(ObjectLockEnabled.ENABLED)
				.withRule(new ObjectLockRule().withDefaultRetention(
						new DefaultRetention().withMode(ObjectLockRetentionMode.GOVERNANCE).withYears(1).withDays(1)));
		var e = assertThrows(AmazonServiceException.class, () -> client.setObjectLockConfiguration(
				new SetObjectLockConfigurationRequest().withBucketName(bucketName).withObjectLockConfiguration(conf)));
		assertEquals(400, e.getStatusCode());
		assertEquals(MainData.MALFORMED_XML, e.getErrorCode());
	}

	@Test
	@Tag("ERROR")
	// [버킷의 Lock옵션을 활성화] Days값을 0이하로 입력하여 Lock 설정할경우 실패
	public void testObjectLockPutObjLockInvalidDays() {
		var client = getClient();
		var bucketName = getNewBucketName();
		client.createBucket(new CreateBucketRequest(bucketName).withObjectLockEnabledForBucket(true));

		var conf = new ObjectLockConfiguration().withObjectLockEnabled(ObjectLockEnabled.ENABLED)
				.withRule(new ObjectLockRule().withDefaultRetention(
						new DefaultRetention().withMode(ObjectLockRetentionMode.GOVERNANCE).withDays(0)));

		var e = assertThrows(AmazonServiceException.class, () -> client.setObjectLockConfiguration(
				new SetObjectLockConfigurationRequest().withBucketName(bucketName).withObjectLockConfiguration(conf)));
		assertEquals(400, e.getStatusCode());
		assertEquals(MainData.INVALID_ARGUMENT, e.getErrorCode());
	}

	@Test
	@Tag("ERROR")
	// [버킷의 Lock옵션을 활성화] Years값을 0이하로 입력하여 Lock 설정할경우 실패
	public void testObjectLockPutObjLockInvalidYears() {
		var client = getClient();
		var bucketName = getNewBucketName();
		client.createBucket(new CreateBucketRequest(bucketName).withObjectLockEnabledForBucket(true));

		var conf = new ObjectLockConfiguration().withObjectLockEnabled(ObjectLockEnabled.ENABLED)
				.withRule(new ObjectLockRule().withDefaultRetention(
						new DefaultRetention().withMode(ObjectLockRetentionMode.GOVERNANCE).withYears(-1)));

		var e = assertThrows(AmazonServiceException.class, () -> client.setObjectLockConfiguration(
				new SetObjectLockConfigurationRequest().withBucketName(bucketName).withObjectLockConfiguration(conf)));
		assertEquals(400, e.getStatusCode());
		assertEquals(MainData.INVALID_ARGUMENT, e.getErrorCode());
	}

	@Test
	@Tag("ERROR")
	// [버킷의 Lock옵션을 활성화] mode값이 올바르지 않은상태에서 Lock 설정할 경우 실패
	public void testObjectLockPutObjLockInvalidMode() {
		var client = getClient();
		var bucketName = getNewBucketName();
		client.createBucket(new CreateBucketRequest(bucketName).withObjectLockEnabledForBucket(true));

		var conf = new ObjectLockConfiguration().withObjectLockEnabled(ObjectLockEnabled.ENABLED).withRule(
				new ObjectLockRule().withDefaultRetention(new DefaultRetention().withMode("invalid").withYears(1)));

		var e = assertThrows(AmazonServiceException.class, () -> client.setObjectLockConfiguration(
				new SetObjectLockConfigurationRequest().withBucketName(bucketName).withObjectLockConfiguration(conf)));
		assertEquals(400, e.getStatusCode());
		assertEquals(MainData.MALFORMED_XML, e.getErrorCode());
	}

	@Test
	@Tag("ERROR")
	// [버킷의 Lock옵션을 활성화] status값이 올바르지 않은상태에서 Lock 설정할 경우 실패
	public void testObjectLockPutObjLockInvalidStatus() {
		var client = getClient();
		var bucketName = getNewBucketName();
		client.createBucket(new CreateBucketRequest(bucketName).withObjectLockEnabledForBucket(true));

		var conf = new ObjectLockConfiguration().withObjectLockEnabled("Disabled")
				.withRule(new ObjectLockRule().withDefaultRetention(
						new DefaultRetention().withMode(ObjectLockRetentionMode.GOVERNANCE).withYears(1)));

		var e = assertThrows(AmazonServiceException.class, () -> client.setObjectLockConfiguration(
				new SetObjectLockConfigurationRequest().withBucketName(bucketName).withObjectLockConfiguration(conf)));
		assertEquals(400, e.getStatusCode());
		assertEquals(MainData.MALFORMED_XML, e.getErrorCode());
	}

	@Test
	@Tag("Version")
	// [버킷의 Lock옵션을 활성화] 버킷의 버저닝을 일시중단하려고 할경우 실패
	public void testObjectLockSuspendVersioning() {
		var client = getClient();
		var bucketName = getNewBucketName();
		client.createBucket(new CreateBucketRequest(bucketName).withObjectLockEnabledForBucket(true));

		var e = assertThrows(AmazonServiceException.class,
				() -> client.setBucketVersioningConfiguration(new SetBucketVersioningConfigurationRequest(bucketName,
						new BucketVersioningConfiguration(BucketVersioningConfiguration.SUSPENDED))));
		assertEquals(409, e.getStatusCode());
		assertEquals(MainData.INVALID_BUCKET_STATE, e.getErrorCode());
	}

	@Test
	@Tag("Check")
	// [버킷의 Lock옵션을 활성화] 버킷의 lock설정이 올바르게 되었는지 확인
	public void testObjectLockGetObjLock() {
		var client = getClient();
		var bucketName = getNewBucketName();
		client.createBucket(new CreateBucketRequest(bucketName).withObjectLockEnabledForBucket(true));

		var conf = new ObjectLockConfiguration().withObjectLockEnabled(ObjectLockEnabled.ENABLED)
				.withRule(new ObjectLockRule().withDefaultRetention(
						new DefaultRetention().withMode(ObjectLockRetentionMode.GOVERNANCE).withDays(1)));

		client.setObjectLockConfiguration(
				new SetObjectLockConfigurationRequest().withBucketName(bucketName).withObjectLockConfiguration(conf));

		var response = client
				.getObjectLockConfiguration(new GetObjectLockConfigurationRequest().withBucketName(bucketName));
		lockCompare(conf, response.getObjectLockConfiguration());
	}

	@Test
	@Tag("ERROR")
	// 버킷을 Lock옵션을 활성화 하지않을 경우 lock 설정 조회 실패
	public void testObjectLockGetObjLockInvalidBucket() {
		var client = getClient();
		var bucketName = getNewBucketName();
		client.createBucket(bucketName);

		var e = assertThrows(AmazonServiceException.class, () -> client
				.getObjectLockConfiguration(new GetObjectLockConfigurationRequest().withBucketName(bucketName)));
		assertEquals(404, e.getStatusCode());
		assertEquals(MainData.OBJECT_LOCK_CONFIGURATION_NOT_FOUND_ERROR, e.getErrorCode());
	}

	@Test
	@Tag("retention")
	// [버킷의 Lock옵션을 활성화] 오브젝트에 Lock 유지기한 설정이 가능한지 확인
	public void testObjectLockPutObjRetention() {
		var key = "testObjectLockPutObjRetention";
		var client = getClient();
		var bucketName = getNewBucketName();

		client.createBucket(new CreateBucketRequest(bucketName).withObjectLockEnabledForBucket(true));

		var putResponse = client.putObject(bucketName, key, key);
		var versionId = putResponse.getVersionId();

		var retention = new ObjectLockRetention().withMode(ObjectLockRetentionMode.GOVERNANCE)
				.withRetainUntilDate(new Calendar.Builder().setDate(2030, 1, 1)
						.setTimeZone(TimeZone.getTimeZone(("GMT"))).build().getTime());

		client.setObjectRetention(
				new SetObjectRetentionRequest().withBucketName(bucketName).withKey(key).withRetention(retention));

		client.deleteVersion(new DeleteVersionRequest(bucketName, key, versionId).withBypassGovernanceRetention(true));
	}

	@Test
	@Tag("retention")
	// 버킷을 Lock옵션을 활성화 하지않을 경우 오브젝트에 Lock 유지기한 설정 실패
	public void testObjectLockPutObjRetentionInvalidBucket() {
		var key = "testObjectLockPutObjRetentionInvalidBucket";
		var client = getClient();
		var bucketName = createBucket(client);

		client.putObject(bucketName, key, key);

		var retention = new ObjectLockRetention().withMode(ObjectLockRetentionMode.GOVERNANCE)
				.withRetainUntilDate(new Calendar.Builder().setDate(2030, 1, 1)
						.setTimeZone(TimeZone.getTimeZone(("GMT"))).build().getTime());

		var e = assertThrows(AmazonServiceException.class, () -> client.setObjectRetention(
				new SetObjectRetentionRequest().withBucketName(bucketName).withKey(key).withRetention(retention)));
		assertEquals(400, e.getStatusCode());
		assertEquals(MainData.INVALID_REQUEST, e.getErrorCode());
	}

	@Test
	@Tag("retention")
	// [버킷의 Lock옵션을 활성화] 오브젝트에 Lock 유지기한 설정할때 Mode값이 올바르지 않을 경우 설정 실패
	public void testObjectLockPutObjRetentionInvalidMode() {
		var key = "testObjectLockPutObjRetentionInvalidMode";
		var client = getClient();
		var bucketName = getNewBucketName();

		client.createBucket(new CreateBucketRequest(bucketName).withObjectLockEnabledForBucket(true));

		client.putObject(bucketName, key, key);

		var retention = new ObjectLockRetention().withMode("invalid").withRetainUntilDate(new Calendar.Builder()
				.setDate(2030, 1, 1).setTimeZone(TimeZone.getTimeZone(("GMT"))).build().getTime());

		var e = assertThrows(AmazonServiceException.class, () -> client.setObjectRetention(
				new SetObjectRetentionRequest().withBucketName(bucketName).withKey(key).withRetention(retention)));
		assertEquals(400, e.getStatusCode());
		assertEquals(MainData.MALFORMED_XML, e.getErrorCode());
	}

	@Test
	@Tag("retention")
	// [버킷의 Lock옵션을 활성화] 오브젝트에 Lock 유지기한 설정이 올바른지 확인
	public void testObjectLockGetObjRetention() {
		var key = "testObjectLockGetObjRetention";
		var client = getClient();
		var bucketName = getNewBucketName();

		client.createBucket(new CreateBucketRequest(bucketName).withObjectLockEnabledForBucket(true));

		var putResponse = client.putObject(bucketName, key, key);
		var versionId = putResponse.getVersionId();

		var retention = new ObjectLockRetention().withMode(ObjectLockRetentionMode.GOVERNANCE)
				.withRetainUntilDate(new Calendar.Builder().setDate(2030, 1, 1)
						.setTimeZone(TimeZone.getTimeZone(("GMT"))).build().getTime());

		client.setObjectRetention(
				new SetObjectRetentionRequest().withBucketName(bucketName).withKey(key).withRetention(retention));
		var response = client
				.getObjectRetention(new GetObjectRetentionRequest().withBucketName(bucketName).withKey(key));
		retentionCompare(retention, response.getRetention());
		client.deleteVersion(new DeleteVersionRequest(bucketName, key, versionId).withBypassGovernanceRetention(true));
	}

	@Test
	@Tag("retention")
	// 버킷을 Lock옵션을 활성화 하지않을 경우 오브젝트에 Lock 유지기한 조회 실패
	public void testObjectLockGetObjRetentionInvalidBucket() {
		var key = "testObjectLockGetObjRetentionInvalidBucket";
		var client = getClient();
		var bucketName = createBucket(client);

		client.putObject(bucketName, key, key);

		var e = assertThrows(AmazonServiceException.class, () -> client
				.getObjectRetention(new GetObjectRetentionRequest().withBucketName(bucketName).withKey(key)));
		assertEquals(400, e.getStatusCode());
		assertEquals(MainData.INVALID_REQUEST, e.getErrorCode());
	}

	@Test
	@Tag("retention")
	// [버킷의 Lock옵션을 활성화] 오브젝트의 특정 버전에 Lock 유지기한을 설정할 경우 올바르게 적용되었는지 확인
	public void testObjectLockPutObjRetentionVersionid() {
		var key = "testObjectLockPutObjRetentionVersionid";
		var client = getClient();
		var bucketName = getNewBucketName();

		client.createBucket(new CreateBucketRequest(bucketName).withObjectLockEnabledForBucket(true));

		client.putObject(bucketName, key, key);
		var putResponse = client.putObject(bucketName, key, key);
		var versionId = putResponse.getVersionId();

		var retention = new ObjectLockRetention().withMode(ObjectLockRetentionMode.GOVERNANCE)
				.withRetainUntilDate(new Calendar.Builder().setDate(2030, 1, 1)
						.setTimeZone(TimeZone.getTimeZone(("GMT"))).build().getTime());

		client.setObjectRetention(
				new SetObjectRetentionRequest().withBucketName(bucketName).withKey(key).withRetention(retention));
		var response = client
				.getObjectRetention(new GetObjectRetentionRequest().withBucketName(bucketName).withKey(key));
		retentionCompare(retention, response.getRetention());
		client.deleteVersion(new DeleteVersionRequest(bucketName, key, versionId).withBypassGovernanceRetention(true));
	}

	@Test
	@Tag("Priority")
	// [버킷의 Lock옵션을 활성화] 버킷에 설정한 Lock설정보다 오브젝트에 Lock설정한 값이 우선 적용됨을 확인
	public void testObjectLockPutObjRetentionOverrideDefaultRetention() {
		var key = "testObjectLockPutObjRetentionOverrideDefaultRetention";
		var client = getClient();
		var bucketName = getNewBucketName();

		client.createBucket(new CreateBucketRequest(bucketName).withObjectLockEnabledForBucket(true));

		var conf = new ObjectLockConfiguration().withObjectLockEnabled(ObjectLockEnabled.ENABLED)
				.withRule(new ObjectLockRule().withDefaultRetention(
						new DefaultRetention().withMode(ObjectLockRetentionMode.GOVERNANCE).withDays(1)));

		client.setObjectLockConfiguration(
				new SetObjectLockConfigurationRequest().withBucketName(bucketName).withObjectLockConfiguration(conf));

		var metadata = new ObjectMetadata();
		metadata.setContentMD5(Utils.getMD5(key));
		metadata.setContentType("text/plain");
		metadata.setContentLength(key.length());

		var putResponse = client.putObject(bucketName, key, createBody(key), metadata);
		var versionId = putResponse.getVersionId();

		var retention = new ObjectLockRetention().withMode(ObjectLockRetentionMode.GOVERNANCE)
				.withRetainUntilDate(new Calendar.Builder().setDate(2030, 1, 1)
						.setTimeZone(TimeZone.getTimeZone(("GMT"))).build().getTime());

		client.setObjectRetention(
				new SetObjectRetentionRequest().withBucketName(bucketName).withKey(key).withRetention(retention));
		var response = client
				.getObjectRetention(new GetObjectRetentionRequest().withBucketName(bucketName).withKey(key));
		retentionCompare(retention, response.getRetention());

		client.deleteVersion(new DeleteVersionRequest(bucketName, key, versionId).withBypassGovernanceRetention(true));
	}

	@Test
	@Tag("Overwrite")
	// [버킷의 Lock옵션을 활성화] 오브젝트의 lock 유지기한을 늘렸을때 적용되는지 확인
	public void testObjectLockPutObjRetentionIncreasePeriod() {
		var key = "testObjectLockPutObjRetentionIncreasePeriod";
		var client = getClient();
		var bucketName = getNewBucketName();

		client.createBucket(new CreateBucketRequest(bucketName).withObjectLockEnabledForBucket(true));

		client.putObject(bucketName, key, key);
		var putResponse = client.putObject(bucketName, key, key);
		var versionId = putResponse.getVersionId();

		var retention1 = new ObjectLockRetention().withMode(ObjectLockRetentionMode.GOVERNANCE)
				.withRetainUntilDate(new Calendar.Builder().setDate(2030, 1, 1)
						.setTimeZone(TimeZone.getTimeZone(("GMT"))).build().getTime());
		client.setObjectRetention(
				new SetObjectRetentionRequest().withBucketName(bucketName).withKey(key).withRetention(retention1));

		var retention2 = new ObjectLockRetention().withMode(ObjectLockRetentionMode.GOVERNANCE)
				.withRetainUntilDate(new Calendar.Builder().setDate(2030, 3, 1)
						.setTimeZone(TimeZone.getTimeZone(("GMT"))).build().getTime());
		client.setObjectRetention(
				new SetObjectRetentionRequest().withBucketName(bucketName).withKey(key).withRetention(retention2));

		var response = client
				.getObjectRetention(new GetObjectRetentionRequest().withBucketName(bucketName).withKey(key));
		retentionCompare(retention2, response.getRetention());
		client.deleteVersion(new DeleteVersionRequest(bucketName, key, versionId).withBypassGovernanceRetention(true));
	}

	@Test
	@Tag("Overwrite")
	// [버킷의 Lock옵션을 활성화] 오브젝트의 lock 유지기한을 줄였을때 실패 확인
	public void testObjectLockPutObjRetentionShortenPeriod() {
		var key = "testObjectLockPutObjRetentionShortenPeriod";
		var client = getClient();
		var bucketName = getNewBucketName();

		client.createBucket(new CreateBucketRequest(bucketName).withObjectLockEnabledForBucket(true));

		client.putObject(bucketName, key, key);
		var putResponse = client.putObject(bucketName, key, key);
		var versionId = putResponse.getVersionId();

		var retention1 = new ObjectLockRetention().withMode(ObjectLockRetentionMode.GOVERNANCE)
				.withRetainUntilDate(new Calendar.Builder().setDate(2030, 3, 1)
						.setTimeZone(TimeZone.getTimeZone(("GMT"))).build().getTime());
		client.setObjectRetention(
				new SetObjectRetentionRequest().withBucketName(bucketName).withKey(key).withRetention(retention1));

		var retention2 = new ObjectLockRetention().withMode(ObjectLockRetentionMode.GOVERNANCE)
				.withRetainUntilDate(new Calendar.Builder().setDate(2030, 1, 1)
						.setTimeZone(TimeZone.getTimeZone(("GMT"))).build().getTime());

		var e = assertThrows(AmazonServiceException.class, () -> client.setObjectRetention(
				new SetObjectRetentionRequest().withBucketName(bucketName).withKey(key).withRetention(retention2)));
		assertEquals(403, e.getStatusCode());
		assertEquals(MainData.ACCESS_DENIED, e.getErrorCode());

		client.deleteVersion(new DeleteVersionRequest(bucketName, key, versionId).withBypassGovernanceRetention(true));
	}

	@Test
	@Tag("Overwrite")
	// [버킷의 Lock옵션을 활성화] 바이패스를 True로 설정하고 오브젝트의 lock 유지기한을 줄였을때 적용되는지 확인
	public void testObjectLockPutObjRetentionShortenPeriodBypass() {
		var key = "testObjectLockPutObjRetentionShortenPeriodBypass";
		var client = getClient();
		var bucketName = getNewBucketName();

		client.createBucket(new CreateBucketRequest(bucketName).withObjectLockEnabledForBucket(true));

		client.putObject(bucketName, key, key);
		var putResponse = client.putObject(bucketName, key, key);
		var versionId = putResponse.getVersionId();

		var retention1 = new ObjectLockRetention().withMode(ObjectLockRetentionMode.GOVERNANCE)
				.withRetainUntilDate(new Calendar.Builder().setDate(2030, 3, 1)
						.setTimeZone(TimeZone.getTimeZone(("GMT"))).build().getTime());
		client.setObjectRetention(
				new SetObjectRetentionRequest().withBucketName(bucketName).withKey(key).withRetention(retention1));

		var retention2 = new ObjectLockRetention().withMode(ObjectLockRetentionMode.GOVERNANCE)
				.withRetainUntilDate(new Calendar.Builder().setDate(2030, 1, 1)
						.setTimeZone(TimeZone.getTimeZone(("GMT"))).build().getTime());
		client.setObjectRetention(new SetObjectRetentionRequest().withBucketName(bucketName).withKey(key)
				.withRetention(retention2).withBypassGovernanceRetention(true));

		var response = client
				.getObjectRetention(new GetObjectRetentionRequest().withBucketName(bucketName).withKey(key));
		retentionCompare(retention2, response.getRetention());
		client.deleteVersion(new DeleteVersionRequest(bucketName, key, versionId).withBypassGovernanceRetention(true));
	}

	@Test
	@Tag("ERROR")
	// [버킷의 Lock옵션을 활성화] 오브젝트의 lock 유지기한내에 삭제를 시도할 경우 실패 확인
	public void testObjectLockDeleteObjectWithRetention() {
		var key = "testObjectLockDeleteObjectWithRetention";
		var client = getClient();
		var bucketName = getNewBucketName();

		client.createBucket(new CreateBucketRequest(bucketName).withObjectLockEnabledForBucket(true));

		var putResponse = client.putObject(bucketName, key, key);
		var versionId = putResponse.getVersionId();

		var retention = new ObjectLockRetention().withMode(ObjectLockRetentionMode.GOVERNANCE)
				.withRetainUntilDate(new Calendar.Builder().setDate(2030, 1, 1)
						.setTimeZone(TimeZone.getTimeZone(("GMT"))).build().getTime());
		client.setObjectRetention(
				new SetObjectRetentionRequest().withBucketName(bucketName).withKey(key).withRetention(retention));

		var e = assertThrows(AmazonServiceException.class, () -> client.deleteVersion(bucketName, key, versionId));
		assertEquals(403, e.getStatusCode());
		assertEquals(MainData.ACCESS_DENIED, e.getErrorCode());

		client.deleteVersion(new DeleteVersionRequest(bucketName, key, versionId).withBypassGovernanceRetention(true));
	}

	@Test
	@Tag("LegalHold")
	// [버킷의 Lock옵션을 활성화] 오브젝트의 LegalHold를 활성화 가능한지 확인
	public void testObjectLockPutLegalHold() {
		var key = "testObjectLockPutLegalHold";
		var client = getClient();
		var bucketName = getNewBucketName();

		client.createBucket(new CreateBucketRequest(bucketName).withObjectLockEnabledForBucket(true));

		client.putObject(bucketName, key, key);

		client.setObjectLegalHold(
				new SetObjectLegalHoldRequest().withBucketName(bucketName).withKey(key)
						.withLegalHold(new ObjectLockLegalHold().withStatus(ObjectLockLegalHoldStatus.ON)));

		client.setObjectLegalHold(
				new SetObjectLegalHoldRequest().withBucketName(bucketName).withKey(key)
						.withLegalHold(new ObjectLockLegalHold().withStatus(ObjectLockLegalHoldStatus.OFF)));

	}

	@Test
	@Tag("LegalHold")
	// [버킷의 Lock옵션을 비활성화] 오브젝트의 LegalHold를 활성화 실패 확인
	public void testObjectLockPutLegalHoldInvalidBucket() {
		var key = "testObjectLockPutLegalHoldInvalidBucket";
		var client = getClient();
		var bucketName = createBucket(client);

		client.putObject(bucketName, key, key);

		var e = assertThrows(AmazonServiceException.class, () -> client.setObjectLegalHold(
				new SetObjectLegalHoldRequest().withBucketName(bucketName).withKey(key)
						.withLegalHold(new ObjectLockLegalHold().withStatus(ObjectLockLegalHoldStatus.ON))));
		assertEquals(400, e.getStatusCode());
		assertEquals(MainData.INVALID_REQUEST, e.getErrorCode());
	}

	@Test
	@Tag("LegalHold")
	// [버킷의 Lock옵션을 활성화] 오브젝트의 LegalHold에 잘못된 값을 넣을 경우 실패 확인
	public void testObjectLockPutLegalHoldInvalidStatus() {
		var key = "testObjectLockPutLegalHoldInvalidStatus";
		var client = getClient();
		var bucketName = getNewBucketName();

		client.createBucket(new CreateBucketRequest(bucketName).withObjectLockEnabledForBucket(true));

		client.putObject(bucketName, key, key);

		var e = assertThrows(AmazonServiceException.class, () -> client.setObjectLegalHold(
				new SetObjectLegalHoldRequest().withBucketName(bucketName).withKey(key)
						.withLegalHold(new ObjectLockLegalHold().withStatus("abc"))));
		assertEquals(400, e.getStatusCode());
		assertEquals(MainData.MALFORMED_XML, e.getErrorCode());
	}

	@Test
	@Tag("LegalHold")
	// [버킷의 Lock옵션을 활성화] 오브젝트의 LegalHold가 올바르게 적용되었는지 확인
	public void testObjectLockGetLegalHold() {
		var key = "testObjectLockGetLegalHold";
		var client = getClient();
		var bucketName = getNewBucketName();

		client.createBucket(new CreateBucketRequest(bucketName).withObjectLockEnabledForBucket(true));

		var putResponse = client.putObject(bucketName, key, key);

		client.setObjectLegalHold(
				new SetObjectLegalHoldRequest().withBucketName(bucketName).withKey(key)
						.withLegalHold(new ObjectLockLegalHold().withStatus(ObjectLockLegalHoldStatus.ON)));
		var response = client
				.getObjectLegalHold(new GetObjectLegalHoldRequest().withBucketName(bucketName).withKey(key));
		assertEquals(ObjectLockLegalHoldStatus.ON, response.getLegalHold().getStatus());

		client.setObjectLegalHold(
				new SetObjectLegalHoldRequest().withBucketName(bucketName).withKey(key)
						.withLegalHold(new ObjectLockLegalHold().withStatus(ObjectLockLegalHoldStatus.OFF)));
		response = client.getObjectLegalHold(new GetObjectLegalHoldRequest().withBucketName(bucketName).withKey(key));
		assertEquals(ObjectLockLegalHoldStatus.OFF, response.getLegalHold().getStatus());

		client.deleteVersion(new DeleteVersionRequest(bucketName, key, putResponse.getVersionId()));
	}

	@Test
	@Tag("LegalHold")
	// [버킷의 Lock옵션을 비활성화] 오브젝트의 LegalHold설정 조회 실패 확인
	public void testObjectLockGetLegalHoldInvalidBucket() {
		var key = "testObjectLockGetLegalHoldInvalidBucket";
		var client = getClient();
		var bucketName = createBucket(client);

		client.putObject(bucketName, key, key);

		var e = assertThrows(AmazonServiceException.class, () -> client
				.getObjectLegalHold(new GetObjectLegalHoldRequest().withBucketName(bucketName).withKey(key)));
		assertEquals(400, e.getStatusCode());
		assertEquals(MainData.INVALID_REQUEST, e.getErrorCode());
	}

	@Test
	@Tag("LegalHold")
	// [버킷의 Lock옵션을 활성화] 오브젝트의 LegalHold가 활성화되어 있을 경우 오브젝트 삭제 실패 확인
	public void testObjectLockDeleteObjectWithLegalHoldOn() {
		var client = getClient();
		var bucketName = getNewBucketName();
		client.createBucket(new CreateBucketRequest(bucketName).withObjectLockEnabledForBucket(true));

		var key = "testObjectLockDeleteObjectWithLegalHoldOn";
		var response = client.putObject(bucketName, key, key);

		client.setObjectLegalHold(
				new SetObjectLegalHoldRequest().withBucketName(bucketName).withKey(key)
						.withLegalHold(new ObjectLockLegalHold().withStatus(ObjectLockLegalHoldStatus.ON)));

		var e = assertThrows(AmazonServiceException.class,
				() -> client.deleteVersion(bucketName, key, response.getVersionId()));
		assertEquals(403, e.getStatusCode());
		assertEquals(MainData.ACCESS_DENIED, e.getErrorCode());

		client.setObjectLegalHold(
				new SetObjectLegalHoldRequest().withBucketName(bucketName).withKey(key)
						.withLegalHold(new ObjectLockLegalHold().withStatus(ObjectLockLegalHoldStatus.OFF)));
		client.deleteVersion(new DeleteVersionRequest(bucketName, key, response.getVersionId()));
	}

	@Test
	@Tag("LegalHold")
	// [버킷의 Lock옵션을 활성화] 오브젝트의 LegalHold가 비활성화되어 있을 경우 오브젝트 삭제 확인
	public void testObjectLockDeleteObjectWithLegalHoldOff() {
		var key = "testObjectLockDeleteObjectWithLegalHoldOff";
		var client = getClient();
		var bucketName = getNewBucketName();

		client.createBucket(new CreateBucketRequest(bucketName).withObjectLockEnabledForBucket(true));

		var response = client.putObject(bucketName, key, key);

		client.setObjectLegalHold(
				new SetObjectLegalHoldRequest().withBucketName(bucketName).withKey(key)
						.withLegalHold(new ObjectLockLegalHold().withStatus(ObjectLockLegalHoldStatus.OFF)));

		client.deleteVersion(bucketName, key, response.getVersionId());
	}

	@Test
	@Tag("LegalHold")
	// [버킷의 Lock옵션을 활성화] 오브젝트의 LegalHold와 Lock유지기한 설정이 모두 적용되는지 메타데이터를 통해 확인
	public void testObjectLockGetObjMetadata() {
		var key = "testObjectLockGetObjMetadata";
		var client = getClient();
		var bucketName = getNewBucketName();

		client.createBucket(new CreateBucketRequest(bucketName).withObjectLockEnabledForBucket(true));

		var putResponse = client.putObject(bucketName, key, key);

		client.setObjectLegalHold(
				new SetObjectLegalHoldRequest().withBucketName(bucketName).withKey(key)
						.withLegalHold(new ObjectLockLegalHold().withStatus(ObjectLockLegalHoldStatus.ON)));

		var retention = new ObjectLockRetention().withMode(ObjectLockRetentionMode.GOVERNANCE)
				.withRetainUntilDate(new Calendar.Builder().setDate(2030, 1, 1)
						.setTimeZone(TimeZone.getTimeZone(("GMT"))).build().getTime());
		client.setObjectRetention(
				new SetObjectRetentionRequest().withBucketName(bucketName).withKey(key).withRetention(retention));

		var response = client.getObjectMetadata(bucketName, key);
		assertEquals(retention.getMode(), response.getObjectLockMode());
		assertEquals(retention.getRetainUntilDate(), response.getObjectLockRetainUntilDate());
		assertEquals(ObjectLockLegalHoldStatus.ON, response.getObjectLockLegalHoldStatus());

		client.setObjectLegalHold(
				new SetObjectLegalHoldRequest().withBucketName(bucketName).withKey(key)
						.withLegalHold(new ObjectLockLegalHold().withStatus(ObjectLockLegalHoldStatus.OFF)));
		client.deleteVersion(new DeleteVersionRequest(bucketName, key, putResponse.getVersionId())
				.withBypassGovernanceRetention(true));
	}
}
