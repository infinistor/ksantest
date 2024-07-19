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

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Calendar;
import java.util.TimeZone;

import org.example.Data.MainData;
import org.example.Utility.Utils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.BucketVersioningStatus;
import software.amazon.awssdk.services.s3.model.ObjectLockConfiguration;
import software.amazon.awssdk.services.s3.model.ObjectLockEnabled;
import software.amazon.awssdk.services.s3.model.ObjectLockLegalHold;
import software.amazon.awssdk.services.s3.model.ObjectLockLegalHoldStatus;
import software.amazon.awssdk.services.s3.model.ObjectLockRetention;
import software.amazon.awssdk.services.s3.model.ObjectLockRetentionMode;

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
		var bucketName = getNewBucketName();
		var client = getClient();
		client.createBucket(c -> c.bucket(bucketName).objectLockEnabledForBucket(true));

		var conf = ObjectLockConfiguration.builder().objectLockEnabled(ObjectLockEnabled.ENABLED)
				.rule(r -> r.defaultRetention(retention -> retention.mode(ObjectLockRetentionMode.COMPLIANCE).years(1)))
				.build();
		client.putObjectLockConfiguration(p -> p.bucket(bucketName).objectLockConfiguration(conf));

		var versionResponse = client.getBucketVersioning(g -> g.bucket(bucketName));
		assertEquals(BucketVersioningStatus.ENABLED, versionResponse.status());
	}

	@Test
	@Tag("ERROR")
	// 버킷을 Lock옵션을 활성화 하지않을 경우 lock 설정이 실패
	public void testObjectLockPutObjLockInvalidBucket() {
		var client = getClient();
		var bucketName = createBucket(client);

		var conf = ObjectLockConfiguration.builder().objectLockEnabled(ObjectLockEnabled.ENABLED)
				.rule(r -> r.defaultRetention(retention -> retention.mode(ObjectLockRetentionMode.GOVERNANCE).years(1)))
				.build();

		var e = assertThrows(AwsServiceException.class,
				() -> client.putObjectLockConfiguration(p -> p.bucket(bucketName).objectLockConfiguration(conf)));
		assertEquals(409, e.statusCode());
		assertEquals(MainData.INVALID_BUCKET_STATE, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("ERROR")
	// [버킷의 Lock옵션을 활성화] Days, Years값 모두 입력하여 Lock 설정할경우 실패
	public void testObjectLockPutObjLockWithDaysAndYears() {
		var bucketName = getNewBucketName();
		var client = getClient();
		client.createBucket(c -> c.bucket(bucketName).objectLockEnabledForBucket(true));

		var conf = ObjectLockConfiguration.builder().objectLockEnabled(ObjectLockEnabled.ENABLED).rule(r -> r
				.defaultRetention(retention -> retention.mode(ObjectLockRetentionMode.GOVERNANCE).years(1).days(1)))
				.build();
		var e = assertThrows(AwsServiceException.class,
				() -> client.putObjectLockConfiguration(p -> p.bucket(bucketName).objectLockConfiguration(conf)));
		assertEquals(400, e.statusCode());
		assertEquals(MainData.MALFORMED_XML, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("ERROR")
	// [버킷의 Lock옵션을 활성화] Days값을 0이하로 입력하여 Lock 설정할경우 실패
	public void testObjectLockPutObjLockInvalidDays() {
		var bucketName = getNewBucketName();
		var client = getClient();
		client.createBucket(c -> c.bucket(bucketName).objectLockEnabledForBucket(true));

		var conf = ObjectLockConfiguration.builder().objectLockEnabled(ObjectLockEnabled.ENABLED)
				.rule(r -> r.defaultRetention(retention -> retention.mode(ObjectLockRetentionMode.GOVERNANCE).days(0)))
				.build();

		var e = assertThrows(AwsServiceException.class,
				() -> client.putObjectLockConfiguration(p -> p.bucket(bucketName).objectLockConfiguration(conf)));
		assertEquals(400, e.statusCode());
		assertEquals(MainData.INVALID_ARGUMENT, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("ERROR")
	// [버킷의 Lock옵션을 활성화] Years값을 0이하로 입력하여 Lock 설정할경우 실패
	public void testObjectLockPutObjLockInvalidYears() {
		var bucketName = getNewBucketName();
		var client = getClient();
		client.createBucket(c -> c.bucket(bucketName).objectLockEnabledForBucket(true));

		var conf = ObjectLockConfiguration.builder().objectLockEnabled(ObjectLockEnabled.ENABLED).rule(
				r -> r.defaultRetention(retention -> retention.mode(ObjectLockRetentionMode.GOVERNANCE).years(-1)))
				.build();

		var e = assertThrows(AwsServiceException.class,
				() -> client.putObjectLockConfiguration(p -> p.bucket(bucketName).objectLockConfiguration(conf)));
		assertEquals(400, e.statusCode());
		assertEquals(MainData.INVALID_ARGUMENT, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("ERROR")
	// [버킷의 Lock옵션을 활성화] mode값이 올바르지 않은상태에서 Lock 설정할 경우 실패
	public void testObjectLockPutObjLockInvalidMode() {
		var bucketName = getNewBucketName();
		var client = getClient();
		client.createBucket(c -> c.bucket(bucketName).objectLockEnabledForBucket(true));

		var conf = ObjectLockConfiguration.builder().objectLockEnabled(ObjectLockEnabled.ENABLED)
				.rule(r -> r.defaultRetention(retention -> retention.mode("invalid").years(1))).build();

		var e = assertThrows(AwsServiceException.class,
				() -> client.putObjectLockConfiguration(p -> p.bucket(bucketName).objectLockConfiguration(conf)));
		assertEquals(400, e.statusCode());
		assertEquals(MainData.MALFORMED_XML, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("ERROR")
	// [버킷의 Lock옵션을 활성화] status값이 올바르지 않은상태에서 Lock 설정할 경우 실패
	public void testObjectLockPutObjLockInvalidStatus() {
		var bucketName = getNewBucketName();
		var client = getClient();
		client.createBucket(c -> c.bucket(bucketName).objectLockEnabledForBucket(true));

		var conf = ObjectLockConfiguration.builder().objectLockEnabled("Disabled")
				.rule(r -> r.defaultRetention(retention -> retention.mode(ObjectLockRetentionMode.GOVERNANCE).years(1)))
				.build();

		var e = assertThrows(AwsServiceException.class,
				() -> client.putObjectLockConfiguration(p -> p.bucket(bucketName).objectLockConfiguration(conf)));
		assertEquals(400, e.statusCode());
		assertEquals(MainData.MALFORMED_XML, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Version")
	// [버킷의 Lock옵션을 활성화] 버킷의 버저닝을 일시중단하려고 할경우 실패
	public void testObjectLockSuspendVersioning() {
		var bucketName = getNewBucketName();
		var client = getClient();
		client.createBucket(c -> c.bucket(bucketName).objectLockEnabledForBucket(true));

		var e = assertThrows(AwsServiceException.class, () -> client.putBucketVersioning(
				p -> p.bucket(bucketName).versioningConfiguration(v -> v.status(BucketVersioningStatus.SUSPENDED))));
		assertEquals(409, e.statusCode());
		assertEquals(MainData.INVALID_BUCKET_STATE, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Check")
	// [버킷의 Lock옵션을 활성화] 버킷의 lock설정이 올바르게 되었는지 확인
	public void testObjectLockGetObjLock() {
		var bucketName = getNewBucketName();
		var client = getClient();
		client.createBucket(c -> c.bucket(bucketName).objectLockEnabledForBucket(true));

		var conf = ObjectLockConfiguration.builder().objectLockEnabled(ObjectLockEnabled.ENABLED)
				.rule(r -> r.defaultRetention(retention -> retention.mode(ObjectLockRetentionMode.GOVERNANCE).days(1)))
				.build();

		client.putObjectLockConfiguration(p -> p.bucket(bucketName).objectLockConfiguration(conf));

		var response = client.getObjectLockConfiguration(g -> g.bucket(bucketName));
		lockCompare(conf, response.objectLockConfiguration());
	}

	@Test
	@Tag("ERROR")
	// 버킷을 Lock옵션을 활성화 하지않을 경우 lock 설정 조회 실패
	public void testObjectLockGetObjLockInvalidBucket() {
		var bucketName = getNewBucketName();
		var client = getClient();
		client.createBucket(c -> c.bucket(bucketName));

		var e = assertThrows(AwsServiceException.class,
				() -> client.getObjectLockConfiguration(g -> g.bucket(bucketName)));
		assertEquals(404, e.statusCode());
		assertEquals(MainData.OBJECT_LOCK_CONFIGURATION_NOT_FOUND_ERROR, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("retention")
	// [버킷의 Lock옵션을 활성화] 오브젝트에 Lock 유지기한 설정이 가능한지 확인
	public void testObjectLockPutObjRetention() {
		var bucketName = getNewBucketName();
		var client = getClient();
		client.createBucket(c -> c.bucket(bucketName).objectLockEnabledForBucket(true));
		var key = "file1";

		var response = client.putObject(p -> p.bucket(bucketName).key(key).build(), RequestBody.fromString("abc"));
		var versionId = response.versionId();

		var retention = ObjectLockRetention.builder().mode(ObjectLockRetentionMode.GOVERNANCE)
				.retainUntilDate(new Calendar.Builder().setDate(2030, 1, 1)
						.setTimeZone(TimeZone.getTimeZone(("GMT"))).build().toInstant())
				.build();

		client.putObjectRetention(p -> p.bucket(bucketName).key(key).retention(retention));

		client.deleteObject(d -> d.bucket(bucketName).key(key).versionId(versionId).bypassGovernanceRetention(true));
	}

	@Test
	@Tag("retention")
	// 버킷을 Lock옵션을 활성화 하지않을 경우 오브젝트에 Lock 유지기한 설정 실패
	public void testObjectLockPutObjRetentionInvalidBucket() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "file1";

		client.putObject(p -> p.bucket(bucketName).key(key).build(), RequestBody.fromString("abc"));

		var retention = ObjectLockRetention.builder().mode(ObjectLockRetentionMode.GOVERNANCE)
				.retainUntilDate(new Calendar.Builder().setDate(2030, 1, 1)
						.setTimeZone(TimeZone.getTimeZone(("GMT"))).build().toInstant())
				.build();

		var e = assertThrows(AwsServiceException.class, () -> client
				.putObjectRetention(p -> p.bucket(bucketName).key(key).retention(retention)));
		assertEquals(400, e.statusCode());
		assertEquals(MainData.INVALID_REQUEST, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("retention")
	// [버킷의 Lock옵션을 활성화] 오브젝트에 Lock 유지기한 설정할때 Mode값이 올바르지 않을 경우 설정 실패
	public void testObjectLockPutObjRetentionInvalidMode() {
		var bucketName = getNewBucketName();
		var client = getClient();
		client.createBucket(c -> c.bucket(bucketName).objectLockEnabledForBucket(true));
		var key = "file1";

		client.putObject(p -> p.bucket(bucketName).key(key).build(), RequestBody.fromString("abc"));

		var retention = ObjectLockRetention.builder().mode("invalid").retainUntilDate(new Calendar.Builder()
				.setDate(2030, 1, 1).setTimeZone(TimeZone.getTimeZone(("GMT"))).build().toInstant()).build();

		var e = assertThrows(AwsServiceException.class, () -> client
				.putObjectRetention(p -> p.bucket(bucketName).key(key).retention(retention)));
		assertEquals(400, e.statusCode());
		assertEquals(MainData.MALFORMED_XML, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("retention")
	// [버킷의 Lock옵션을 활성화] 오브젝트에 Lock 유지기한 설정이 올바른지 확인
	public void testObjectLockGetObjRetention() {
		var bucketName = getNewBucketName();
		var client = getClient();
		client.createBucket(c -> c.bucket(bucketName).objectLockEnabledForBucket(true));
		var key = "file1";

		var putResponse = client.putObject(p -> p.bucket(bucketName).key(key).build(), RequestBody.fromString("abc"));
		var versionId = putResponse.versionId();

		var retention = ObjectLockRetention.builder().mode(ObjectLockRetentionMode.GOVERNANCE)
				.retainUntilDate(new Calendar.Builder().setDate(2030, 1, 1)
						.setTimeZone(TimeZone.getTimeZone(("GMT"))).build().toInstant())
				.build();

		client.putObjectRetention(p -> p.bucket(bucketName).key(key).retention(retention));
		var response = client.getObjectRetention(g -> g.bucket(bucketName).key(key));
		retentionCompare(retention, response.retention());
		client.deleteObject(d -> d.bucket(bucketName).key(key).versionId(versionId).bypassGovernanceRetention(true));
	}

	@Test
	@Tag("retention")
	// 버킷을 Lock옵션을 활성화 하지않을 경우 오브젝트에 Lock 유지기한 조회 실패
	public void testObjectLockGetObjRetentionInvalidBucket() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "file1";

		client.putObject(p -> p.bucket(bucketName).key(key).build(), RequestBody.fromString("abc"));

		var e = assertThrows(AwsServiceException.class, () -> client
				.getObjectRetention(g -> g.bucket(bucketName).key(key)));
		assertEquals(400, e.statusCode());
		assertEquals(MainData.INVALID_REQUEST, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("retention")
	// [버킷의 Lock옵션을 활성화] 오브젝트의 특정 버전에 Lock 유지기한을 설정할 경우 올바르게 적용되었는지 확인
	public void testObjectLockPutObjRetentionVersionid() {
		var bucketName = getNewBucketName();
		var client = getClient();
		client.createBucket(c -> c.bucket(bucketName).objectLockEnabledForBucket(true));
		var key = "file1";

		client.putObject(p -> p.bucket(bucketName).key(key).build(), RequestBody.fromString("abc"));
		var putResponse = client.putObject(p -> p.bucket(bucketName).key(key).build(), RequestBody.fromString("abc"));
		var versionId = putResponse.versionId();

		var retention = ObjectLockRetention.builder().mode(ObjectLockRetentionMode.GOVERNANCE)
				.retainUntilDate(new Calendar.Builder().setDate(2030, 1, 1)
						.setTimeZone(TimeZone.getTimeZone(("GMT"))).build().toInstant())
				.build();

		client.putObjectRetention(p -> p.bucket(bucketName).key(key).retention(retention));
		var response = client.getObjectRetention(g -> g.bucket(bucketName).key(key));
		retentionCompare(retention, response.retention());
		client.deleteObject(d -> d.bucket(bucketName).key(key).versionId(versionId).bypassGovernanceRetention(true));
	}

	@Test
	@Tag("Priority")
	// [버킷의 Lock옵션을 활성화] 버킷에 설정한 Lock설정보다 오브젝트에 Lock설정한 값이 우선 적용됨을 확인
	public void testObjectLockPutObjRetentionOverrideDefaultRetention() {
		var bucketName = getNewBucketName();
		var client = getClient();
		client.createBucket(c -> c.bucket(bucketName).objectLockEnabledForBucket(true));

		var conf = ObjectLockConfiguration.builder().objectLockEnabled(ObjectLockEnabled.ENABLED)
				.rule(r -> r.defaultRetention(retention -> retention.mode(ObjectLockRetentionMode.GOVERNANCE).days(1)))
				.build();

		client.putObjectLockConfiguration(p -> p.bucket(bucketName).objectLockConfiguration(conf));

		var key = "file1";
		var body = "abc";

		var putResponse = client.putObject(p -> p.bucket(bucketName).key(key).contentMD5(Utils.getMD5(body)).contentLength((long)body.length()),
				RequestBody.fromString(body));
		var versionId = putResponse.versionId();

		var retention = ObjectLockRetention.builder().mode(ObjectLockRetentionMode.GOVERNANCE)
				.retainUntilDate(new Calendar.Builder().setDate(2030, 1, 1)
						.setTimeZone(TimeZone.getTimeZone(("GMT"))).build().toInstant())
				.build();

		client.putObjectRetention(p -> p.bucket(bucketName).key(key).retention(retention));
		var response = client.getObjectRetention(g -> g.bucket(bucketName).key(key));
		retentionCompare(retention, response.retention());

		client.deleteObject(d -> d.bucket(bucketName).key(key).versionId(versionId).bypassGovernanceRetention(true));
	}

	@Test
	@Tag("Overwrite")
	// [버킷의 Lock옵션을 활성화] 오브젝트의 lock 유지기한을 늘렸을때 적용되는지 확인
	public void testObjectLockPutObjRetentionIncreasePeriod() {
		var bucketName = getNewBucketName();
		var client = getClient();
		client.createBucket(c -> c.bucket(bucketName).objectLockEnabledForBucket(true));
		var key = "file1";

		client.putObject(p -> p.bucket(bucketName).key(key).build(), RequestBody.fromString("abc"));
		var putResponse = client.putObject(p -> p.bucket(bucketName).key(key).build(), RequestBody.fromString("abc"));
		var versionId = putResponse.versionId();

		var retention1 = ObjectLockRetention.builder().mode(ObjectLockRetentionMode.GOVERNANCE)
				.retainUntilDate(new Calendar.Builder().setDate(2030, 1, 1)
						.setTimeZone(TimeZone.getTimeZone(("GMT"))).build().toInstant())
				.build();
		client.putObjectRetention(p -> p.bucket(bucketName).key(key).retention(retention1));

		var retention2 = ObjectLockRetention.builder().mode(ObjectLockRetentionMode.GOVERNANCE)
				.retainUntilDate(new Calendar.Builder().setDate(2030, 3, 1)
						.setTimeZone(TimeZone.getTimeZone(("GMT"))).build().toInstant())
				.build();
		client.putObjectRetention(p -> p.bucket(bucketName).key(key).retention(retention2));

		var response = client
				.getObjectRetention(g -> g.bucket(bucketName).key(key));
		retentionCompare(retention2, response.retention());
		client.deleteObject(d -> d.bucket(bucketName).key(key).versionId(versionId).bypassGovernanceRetention(true));
	}

	@Test
	@Tag("Overwrite")
	// [버킷의 Lock옵션을 활성화] 오브젝트의 lock 유지기한을 줄였을때 실패 확인
	public void testObjectLockPutObjRetentionShortenPeriod() {
		var bucketName = getNewBucketName();
		var client = getClient();
		client.createBucket(c -> c.bucket(bucketName).objectLockEnabledForBucket(true));
		var key = "file1";

		client.putObject(p -> p.bucket(bucketName).key(key).build(), RequestBody.fromString("abc"));
		var putResponse = client.putObject(p -> p.bucket(bucketName).key(key).build(), RequestBody.fromString("abc"));
		var versionId = putResponse.versionId();

		var retention1 = ObjectLockRetention.builder().mode(ObjectLockRetentionMode.GOVERNANCE)
				.retainUntilDate(new Calendar.Builder().setDate(2030, 3, 1)
						.setTimeZone(TimeZone.getTimeZone(("GMT"))).build().toInstant())
				.build();
		client.putObjectRetention(p -> p.bucket(bucketName).key(key).retention(retention1));

		var retention2 = ObjectLockRetention.builder().mode(ObjectLockRetentionMode.GOVERNANCE)
				.retainUntilDate(new Calendar.Builder().setDate(2030, 1, 1)
						.setTimeZone(TimeZone.getTimeZone(("GMT"))).build().toInstant())
				.build();

		var e = assertThrows(AwsServiceException.class,
				() -> client.putObjectRetention(p -> p.bucket(bucketName).key(key).retention(retention2)));
		assertEquals(403, e.statusCode());
		assertEquals(MainData.ACCESS_DENIED, e.awsErrorDetails().errorCode());

		client.deleteObject(d -> d.bucket(bucketName).key(key).versionId(versionId).bypassGovernanceRetention(true));
	}

	@Test
	@Tag("Overwrite")
	// [버킷의 Lock옵션을 활성화] 바이패스를 True로 설정하고 오브젝트의 lock 유지기한을 줄였을때 적용되는지 확인
	public void testObjectLockPutObjRetentionShortenPeriodBypass() {
		var bucketName = getNewBucketName();
		var client = getClient();
		client.createBucket(c -> c.bucket(bucketName).objectLockEnabledForBucket(true));
		var key = "file1";

		client.putObject(p -> p.bucket(bucketName).key(key).build(), RequestBody.fromString("abc"));
		var putResponse = client.putObject(p -> p.bucket(bucketName).key(key).build(), RequestBody.fromString("abc"));
		var versionId = putResponse.versionId();

		var retention1 = ObjectLockRetention.builder().mode(ObjectLockRetentionMode.GOVERNANCE)
				.retainUntilDate(new Calendar.Builder().setDate(2030, 3, 1)
						.setTimeZone(TimeZone.getTimeZone(("GMT"))).build().toInstant())
				.build();
		client.putObjectRetention(p -> p.bucket(bucketName).key(key).retention(retention1));

		var retention2 = ObjectLockRetention.builder().mode(ObjectLockRetentionMode.GOVERNANCE)
				.retainUntilDate(new Calendar.Builder().setDate(2030, 1, 1)
						.setTimeZone(TimeZone.getTimeZone(("GMT"))).build().toInstant())
				.build();
		client.putObjectRetention(
				p -> p.bucket(bucketName).key(key).retention(retention2).bypassGovernanceRetention(true));

		var response = client
				.getObjectRetention(g -> g.bucket(bucketName).key(key));
		retentionCompare(retention2, response.retention());
		client.deleteObject(d -> d.bucket(bucketName).key(key).versionId(versionId).bypassGovernanceRetention(true));
	}

	@Test
	@Tag("ERROR")
	// [버킷의 Lock옵션을 활성화] 오브젝트의 lock 유지기한내에 삭제를 시도할 경우 실패 확인
	public void testObjectLockDeleteObjectWithRetention() {
		var bucketName = getNewBucketName();
		var client = getClient();
		client.createBucket(c -> c.bucket(bucketName).objectLockEnabledForBucket(true));
		var key = "file1";

		var putResponse = client.putObject(p -> p.bucket(bucketName).key(key).build(), RequestBody.fromString("abc"));
		var versionId = putResponse.versionId();

		var retention = ObjectLockRetention.builder().mode(ObjectLockRetentionMode.GOVERNANCE)
				.retainUntilDate(new Calendar.Builder().setDate(2030, 1, 1)
						.setTimeZone(TimeZone.getTimeZone(("GMT"))).build().toInstant())
				.build();
		client.putObjectRetention(p -> p.bucket(bucketName).key(key).retention(retention));

		var e = assertThrows(AwsServiceException.class,
				() -> client.deleteObject(d -> d.bucket(bucketName).key(key).versionId(versionId)));
		assertEquals(403, e.statusCode());
		assertEquals(MainData.ACCESS_DENIED, e.awsErrorDetails().errorCode());

		client.deleteObject(d -> d.bucket(bucketName).key(key).versionId(versionId).bypassGovernanceRetention(true));
	}

	@Test
	@Tag("LegalHold")
	// [버킷의 Lock옵션을 활성화] 오브젝트의 LegalHold를 활성화 가능한지 확인
	public void testObjectLockPutLegalHold() {
		var bucketName = getNewBucketName();
		var client = getClient();
		client.createBucket(c -> c.bucket(bucketName).objectLockEnabledForBucket(true));

		var key = "file1";
		client.putObject(p -> p.bucket(bucketName).key(key).build(), RequestBody.fromString("abc"));

		var legalHold = ObjectLockLegalHold.builder().status(ObjectLockLegalHoldStatus.ON).build();
		client.putObjectLegalHold(p -> p.bucket(bucketName).key(key).legalHold(legalHold));

		var legalHold2 = ObjectLockLegalHold.builder().status(ObjectLockLegalHoldStatus.OFF).build();
		client.putObjectLegalHold(p -> p.bucket(bucketName).key(key).legalHold(legalHold2));

	}

	@Test
	@Tag("LegalHold")
	// [버킷의 Lock옵션을 비활성화] 오브젝트의 LegalHold를 활성화 실패 확인
	public void testObjectLockPutLegalHoldInvalidBucket() {
		var client = getClient();
		var bucketName = createBucket(client);

		var key = "file1";
		client.putObject(p -> p.bucket(bucketName).key(key).build(), RequestBody.fromString("abc"));

		var legalHold = ObjectLockLegalHold.builder().status(ObjectLockLegalHoldStatus.ON).build();
		var e = assertThrows(AwsServiceException.class,
				() -> client.putObjectLegalHold(p -> p.bucket(bucketName).key(key).legalHold(legalHold)));
		assertEquals(400, e.statusCode());
		assertEquals(MainData.INVALID_REQUEST, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("LegalHold")
	// [버킷의 Lock옵션을 활성화] 오브젝트의 LegalHold에 잘못된 값을 넣을 경우 실패 확인
	public void testObjectLockPutLegalHoldInvalidStatus() {
		var bucketName = getNewBucketName();
		var client = getClient();
		client.createBucket(c -> c.bucket(bucketName).objectLockEnabledForBucket(true));

		var key = "file1";
		client.putObject(p -> p.bucket(bucketName).key(key).build(), RequestBody.fromString("abc"));

		var legalHold = ObjectLockLegalHold.builder().status("abc").build();
		var e = assertThrows(AwsServiceException.class,
				() -> client.putObjectLegalHold(p -> p.bucket(bucketName).key(key).legalHold(legalHold)));
		assertEquals(400, e.statusCode());
		assertEquals(MainData.MALFORMED_XML, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("LegalHold")
	// [버킷의 Lock옵션을 활성화] 오브젝트의 LegalHold가 올바르게 적용되었는지 확인
	public void testObjectLockGetLegalHold() {
		var bucketName = getNewBucketName();
		var client = getClient();
		client.createBucket(c -> c.bucket(bucketName).objectLockEnabledForBucket(true));

		var key = "file1";
		client.putObject(p -> p.bucket(bucketName).key(key).build(), RequestBody.fromString("abc"));

		var legalHold = ObjectLockLegalHold.builder().status(ObjectLockLegalHoldStatus.ON).build();
		client.putObjectLegalHold(p -> p.bucket(bucketName).key(key).legalHold(legalHold));
		var response = client.getObjectLegalHold(g -> g.bucket(bucketName).key(key));
		assertEquals(legalHold.status(), response.legalHold().status());

		var legalHold2 = ObjectLockLegalHold.builder().status(ObjectLockLegalHoldStatus.OFF).build();
		client.putObjectLegalHold(p -> p.bucket(bucketName).key(key).legalHold(legalHold2));
		response = client.getObjectLegalHold(g -> g.bucket(bucketName).key(key));
		assertEquals(legalHold2.status(), response.legalHold().status());
	}

	@Test
	@Tag("LegalHold")
	// [버킷의 Lock옵션을 비활성화] 오브젝트의 LegalHold설정 조회 실패 확인
	public void testObjectLockGetLegalHoldInvalidBucket() {
		var client = getClient();
		var bucketName = createBucket(client);

		var key = "file1";
		client.putObject(p -> p.bucket(bucketName).key(key).build(), RequestBody.fromString("abc"));

		var e = assertThrows(AwsServiceException.class,
				() -> client.getObjectLegalHold(g -> g.bucket(bucketName).key(key)));
		assertEquals(400, e.statusCode());
		assertEquals(MainData.INVALID_REQUEST, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("LegalHold")
	// [버킷의 Lock옵션을 활성화] 오브젝트의 LegalHold가 활성화되어 있을 경우 오브젝트 삭제 실패 확인
	public void testObjectLockDeleteObjectWithLegalHoldOn() {
		var bucketName = getNewBucketName();
		var client = getClient();
		client.createBucket(c -> c.bucket(bucketName).objectLockEnabledForBucket(true));

		var key = "file1";
		var putResponse = client.putObject(p -> p.bucket(bucketName).key(key).build(), RequestBody.fromString("abc"));

		var legalHold = ObjectLockLegalHold.builder().status(ObjectLockLegalHoldStatus.ON).build();
		client.putObjectLegalHold(p -> p.bucket(bucketName).key(key).legalHold(legalHold));

		var e = assertThrows(AwsServiceException.class,
				() -> client.deleteObject(d -> d.bucket(bucketName).key(key).versionId(putResponse.versionId())));
		assertEquals(403, e.statusCode());
		assertEquals(MainData.ACCESS_DENIED, e.awsErrorDetails().errorCode());

		var legalHold2 = ObjectLockLegalHold.builder().status(ObjectLockLegalHoldStatus.OFF).build();
		client.putObjectLegalHold(p -> p.bucket(bucketName).key(key).legalHold(legalHold2));
	}

	@Test
	@Tag("LegalHold")
	// [버킷의 Lock옵션을 활성화] 오브젝트의 LegalHold가 비활성화되어 있을 경우 오브젝트 삭제 확인
	public void testObjectLockDeleteObjectWithLegalHoldOff() {
		var bucketName = getNewBucketName();
		var client = getClient();
		client.createBucket(c -> c.bucket(bucketName).objectLockEnabledForBucket(true));

		var key = "file1";
		var putResponse = client.putObject(p -> p.bucket(bucketName).key(key).build(), RequestBody.fromString("abc"));

		var legalHold = ObjectLockLegalHold.builder().status(ObjectLockLegalHoldStatus.OFF).build();
		client.putObjectLegalHold(p -> p.bucket(bucketName).key(key).legalHold(legalHold));

		client.deleteObject(d -> d.bucket(bucketName).key(key).versionId(putResponse.versionId()));
	}

	@Test
	@Tag("LegalHold")
	// [버킷의 Lock옵션을 활성화] 오브젝트의 LegalHold와 Lock유지기한 설정이 모두 적용되는지 메타데이터를 통해 확인
	public void testObjectLockGetObjMetadata() {
		var bucketName = getNewBucketName();
		var client = getClient();
		client.createBucket(c -> c.bucket(bucketName).objectLockEnabledForBucket(true));

		var key = "file1";
		var putResponse = client.putObject(p -> p.bucket(bucketName).key(key).build(), RequestBody.fromString("abc"));

		var legalHold = ObjectLockLegalHold.builder().status(ObjectLockLegalHoldStatus.ON).build();
		client.putObjectLegalHold(p -> p.bucket(bucketName).key(key).legalHold(legalHold));

		var retention = ObjectLockRetention.builder().mode(ObjectLockRetentionMode.GOVERNANCE)
				.retainUntilDate(new Calendar.Builder().setDate(2030, 1, 1)
						.setTimeZone(TimeZone.getTimeZone(("GMT"))).build().toInstant())
				.build();
		client.putObjectRetention(p -> p.bucket(bucketName).key(key).retention(retention));

		var response = client.headObject(h -> h.bucket(bucketName).key(key));
		assertEquals(retention.mode().toString(), response.objectLockMode().toString());
		assertEquals(retention.retainUntilDate(), response.objectLockRetainUntilDate());
		assertEquals(legalHold.status(), response.objectLockLegalHoldStatus());

		var legalHold2 = ObjectLockLegalHold.builder().status(ObjectLockLegalHoldStatus.OFF).build();
		client.putObjectLegalHold(p -> p.bucket(bucketName).key(key).legalHold(legalHold2));
		client.deleteObject(
				d -> d.bucket(bucketName).key(key).versionId(putResponse.versionId()).bypassGovernanceRetention(true));
	}
}
