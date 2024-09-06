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

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Calendar;
import java.util.TimeZone;

import org.apache.hc.core5.http.HttpStatus;
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
		System.out.println("Lock V2 Start");
	}

	@org.junit.jupiter.api.AfterAll
	public static void afterAll() {
		System.out.println("Lock V2 End");
	}

	@Test
	@Tag("Check")
	public void testObjectLockPutObjLock() {
		var client = getClient();
		var bucketName = getNewBucketName();
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
	public void testObjectLockPutObjLockInvalidBucket() {
		var client = getClient();
		var bucketName = createBucket(client);

		var conf = ObjectLockConfiguration.builder().objectLockEnabled(ObjectLockEnabled.ENABLED)
				.rule(r -> r.defaultRetention(retention -> retention.mode(ObjectLockRetentionMode.GOVERNANCE).years(1)))
				.build();

		var e = assertThrows(AwsServiceException.class,
				() -> client.putObjectLockConfiguration(p -> p.bucket(bucketName).objectLockConfiguration(conf)));
		assertEquals(HttpStatus.SC_CONFLICT, e.statusCode());
		assertEquals(MainData.INVALID_BUCKET_STATE, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("ERROR")
	public void testObjectLockPutObjLockWithDaysAndYears() {
		var client = getClient();
		var bucketName = getNewBucketName();
		client.createBucket(c -> c.bucket(bucketName).objectLockEnabledForBucket(true));

		var conf = ObjectLockConfiguration.builder().objectLockEnabled(ObjectLockEnabled.ENABLED).rule(r -> r
				.defaultRetention(retention -> retention.mode(ObjectLockRetentionMode.GOVERNANCE).years(1).days(1)))
				.build();
		var e = assertThrows(AwsServiceException.class,
				() -> client.putObjectLockConfiguration(p -> p.bucket(bucketName).objectLockConfiguration(conf)));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.statusCode());
		assertEquals(MainData.MALFORMED_XML, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("ERROR")
	public void testObjectLockPutObjLockInvalidDays() {
		var client = getClient();
		var bucketName = getNewBucketName();
		client.createBucket(c -> c.bucket(bucketName).objectLockEnabledForBucket(true));

		var conf = ObjectLockConfiguration.builder().objectLockEnabled(ObjectLockEnabled.ENABLED)
				.rule(r -> r.defaultRetention(retention -> retention.mode(ObjectLockRetentionMode.GOVERNANCE).days(0)))
				.build();

		var e = assertThrows(AwsServiceException.class,
				() -> client.putObjectLockConfiguration(p -> p.bucket(bucketName).objectLockConfiguration(conf)));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.statusCode());
		assertEquals(MainData.INVALID_ARGUMENT, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("ERROR")
	public void testObjectLockPutObjLockInvalidYears() {
		var client = getClient();
		var bucketName = getNewBucketName();
		client.createBucket(c -> c.bucket(bucketName).objectLockEnabledForBucket(true));

		var conf = ObjectLockConfiguration.builder().objectLockEnabled(ObjectLockEnabled.ENABLED).rule(
				r -> r.defaultRetention(retention -> retention.mode(ObjectLockRetentionMode.GOVERNANCE).years(-1)))
				.build();

		var e = assertThrows(AwsServiceException.class,
				() -> client.putObjectLockConfiguration(p -> p.bucket(bucketName).objectLockConfiguration(conf)));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.statusCode());
		assertEquals(MainData.INVALID_ARGUMENT, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("ERROR")
	public void testObjectLockPutObjLockInvalidMode() {
		var client = getClient();
		var bucketName = getNewBucketName();
		client.createBucket(c -> c.bucket(bucketName).objectLockEnabledForBucket(true));

		var conf = ObjectLockConfiguration.builder().objectLockEnabled(ObjectLockEnabled.ENABLED)
				.rule(r -> r.defaultRetention(retention -> retention.mode("invalid").years(1))).build();

		var e = assertThrows(AwsServiceException.class,
				() -> client.putObjectLockConfiguration(p -> p.bucket(bucketName).objectLockConfiguration(conf)));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.statusCode());
		assertEquals(MainData.MALFORMED_XML, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("ERROR")
	public void testObjectLockPutObjLockInvalidStatus() {
		var client = getClient();
		var bucketName = getNewBucketName();
		client.createBucket(c -> c.bucket(bucketName).objectLockEnabledForBucket(true));

		var conf = ObjectLockConfiguration.builder().objectLockEnabled("Disabled")
				.rule(r -> r.defaultRetention(retention -> retention.mode(ObjectLockRetentionMode.GOVERNANCE).years(1)))
				.build();

		var e = assertThrows(AwsServiceException.class,
				() -> client.putObjectLockConfiguration(p -> p.bucket(bucketName).objectLockConfiguration(conf)));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.statusCode());
		assertEquals(MainData.MALFORMED_XML, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Version")
	public void testObjectLockSuspendVersioning() {
		var client = getClient();
		var bucketName = getNewBucketName();
		client.createBucket(c -> c.bucket(bucketName).objectLockEnabledForBucket(true));

		var e = assertThrows(AwsServiceException.class, () -> client.putBucketVersioning(
				p -> p.bucket(bucketName).versioningConfiguration(v -> v.status(BucketVersioningStatus.SUSPENDED))));
		assertEquals(HttpStatus.SC_CONFLICT, e.statusCode());
		assertEquals(MainData.INVALID_BUCKET_STATE, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Check")
	public void testObjectLockGetObjLock() {
		var client = getClient();
		var bucketName = getNewBucketName();
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
	public void testObjectLockGetObjLockInvalidBucket() {
		var client = getClient();
		var bucketName = getNewBucketName();
		client.createBucket(c -> c.bucket(bucketName));

		var e = assertThrows(AwsServiceException.class,
				() -> client.getObjectLockConfiguration(g -> g.bucket(bucketName)));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.statusCode());
		assertEquals(MainData.OBJECT_LOCK_CONFIGURATION_NOT_FOUND_ERROR, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("retention")
	public void testObjectLockPutObjRetention() {
		var key = "testObjectLockPutObjRetention";
		var client = getClient();
		var bucketName = getNewBucketName();
		client.createBucket(c -> c.bucket(bucketName).objectLockEnabledForBucket(true));

		var response = client.putObject(p -> p.bucket(bucketName).key(key).build(), RequestBody.fromString(key));
		var versionId = response.versionId();

		client.putObjectRetention(p -> p.bucket(bucketName).key(key)
				.retention(r -> r.mode(ObjectLockRetentionMode.GOVERNANCE)
						.retainUntilDate(Instant.now().minus(1, ChronoUnit.DAYS))));

		client.deleteObject(d -> d.bucket(bucketName).key(key).versionId(versionId).bypassGovernanceRetention(true));
	}

	@Test
	@Tag("retention")
	public void testObjectLockPutObjRetentionInvalidBucket() {
		var key = "testObjectLockPutObjRetentionInvalidBucket";
		var client = getClient();
		var bucketName = createBucket(client);

		client.putObject(p -> p.bucket(bucketName).key(key).build(), RequestBody.fromString(key));

		var retention = ObjectLockRetention.builder().mode(ObjectLockRetentionMode.GOVERNANCE)
				.retainUntilDate(new Calendar.Builder().setDate(2030, 1, 1)
						.setTimeZone(TimeZone.getTimeZone(("GMT"))).build().toInstant())
				.build();

		var e = assertThrows(AwsServiceException.class, () -> client
				.putObjectRetention(p -> p.bucket(bucketName).key(key).retention(retention)));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.statusCode());
		assertEquals(MainData.INVALID_REQUEST, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("retention")
	public void testObjectLockPutObjRetentionInvalidMode() {
		var key = "testObjectLockPutObjRetentionInvalidMode";
		var client = getClient();
		var bucketName = getNewBucketName();

		client.createBucket(c -> c.bucket(bucketName).objectLockEnabledForBucket(true));

		client.putObject(p -> p.bucket(bucketName).key(key).build(), RequestBody.fromString(key));

		var retention = ObjectLockRetention.builder().mode("invalid").retainUntilDate(new Calendar.Builder()
				.setDate(2030, 1, 1).setTimeZone(TimeZone.getTimeZone(("GMT"))).build().toInstant()).build();

		var e = assertThrows(AwsServiceException.class, () -> client
				.putObjectRetention(p -> p.bucket(bucketName).key(key).retention(retention)));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.statusCode());
		assertEquals(MainData.MALFORMED_XML, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("retention")
	public void testObjectLockGetObjRetention() {
		var key = "testObjectLockGetObjRetention";
		var client = getClient();
		var bucketName = getNewBucketName();

		client.createBucket(c -> c.bucket(bucketName).objectLockEnabledForBucket(true));

		var putResponse = client.putObject(p -> p.bucket(bucketName).key(key).build(), RequestBody.fromString(key));
		var versionId = putResponse.versionId();

		var retention = ObjectLockRetention.builder().mode(ObjectLockRetentionMode.GOVERNANCE)
				.retainUntilDate(new Calendar.Builder().setDate(2030, 1, 1)
						.setTimeZone(TimeZone.getTimeZone(("GMT"))).build().toInstant())
				.build();

		client.putObjectRetention(p -> p.bucket(bucketName).key(key).retention(retention));
		var response = client.getObjectRetention(g -> g.bucket(bucketName).key(key));
		retentionCompare(retention, response.retention());

		client.putObjectRetention(p -> p.bucket(bucketName).key(key)
				.retention(r -> r.mode(ObjectLockRetentionMode.GOVERNANCE)
						.retainUntilDate(Instant.now().minus(1, ChronoUnit.DAYS))));
		client.deleteObject(d -> d.bucket(bucketName).key(key).versionId(versionId).bypassGovernanceRetention(true));
	}

	@Test
	@Tag("retention")
	public void testObjectLockGetObjRetentionInvalidBucket() {
		var key = "testObjectLockGetObjRetentionInvalidBucket";
		var client = getClient();
		var bucketName = createBucket(client);

		client.putObject(p -> p.bucket(bucketName).key(key).build(), RequestBody.fromString(key));

		var e = assertThrows(AwsServiceException.class, () -> client
				.getObjectRetention(g -> g.bucket(bucketName).key(key)));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.statusCode());
		assertEquals(MainData.INVALID_REQUEST, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("retention")
	public void testObjectLockPutObjRetentionVersionid() {
		var key = "testObjectLockPutObjRetentionVersionid";
		var client = getClient();
		var bucketName = getNewBucketName();

		client.createBucket(c -> c.bucket(bucketName).objectLockEnabledForBucket(true));

		client.putObject(p -> p.bucket(bucketName).key(key).build(), RequestBody.fromString(key));
		var putResponse = client.putObject(p -> p.bucket(bucketName).key(key).build(), RequestBody.fromString(key));
		var versionId = putResponse.versionId();

		var retention = ObjectLockRetention.builder().mode(ObjectLockRetentionMode.GOVERNANCE)
				.retainUntilDate(new Calendar.Builder().setDate(2030, 1, 1)
						.setTimeZone(TimeZone.getTimeZone(("GMT"))).build().toInstant())
				.build();

		client.putObjectRetention(p -> p.bucket(bucketName).key(key).retention(retention));
		var response = client.getObjectRetention(g -> g.bucket(bucketName).key(key));
		retentionCompare(retention, response.retention());

		client.putObjectRetention(p -> p.bucket(bucketName).key(key)
				.retention(r -> r.mode(ObjectLockRetentionMode.GOVERNANCE)
						.retainUntilDate(Instant.now().minus(1, ChronoUnit.DAYS))));
		client.deleteObject(d -> d.bucket(bucketName).key(key).versionId(versionId).bypassGovernanceRetention(true));
	}

	@Test
	@Tag("Priority")
	public void testObjectLockPutObjRetentionOverrideDefaultRetention() {
		var key = "testObjectLockPutObjRetentionOverrideDefaultRetention";
		var client = getClient();
		var bucketName = getNewBucketName();

		client.createBucket(c -> c.bucket(bucketName).objectLockEnabledForBucket(true));

		var conf = ObjectLockConfiguration.builder().objectLockEnabled(ObjectLockEnabled.ENABLED)
				.rule(r -> r.defaultRetention(retention -> retention.mode(ObjectLockRetentionMode.GOVERNANCE).days(1)))
				.build();

		client.putObjectLockConfiguration(p -> p.bucket(bucketName).objectLockConfiguration(conf));

		var putResponse = client.putObject(
				p -> p.bucket(bucketName).key(key).contentMD5(Utils.getMD5(key)).contentLength((long) key.length()),
				RequestBody.fromString(key));
		var versionId = putResponse.versionId();

		var retention = ObjectLockRetention.builder().mode(ObjectLockRetentionMode.GOVERNANCE)
				.retainUntilDate(new Calendar.Builder().setDate(2030, 1, 1)
						.setTimeZone(TimeZone.getTimeZone(("GMT"))).build().toInstant())
				.build();

		client.putObjectRetention(p -> p.bucket(bucketName).key(key).retention(retention));
		var response = client.getObjectRetention(g -> g.bucket(bucketName).key(key));
		retentionCompare(retention, response.retention());

		client.putObjectRetention(p -> p.bucket(bucketName).key(key)
				.retention(r -> r.mode(ObjectLockRetentionMode.GOVERNANCE)
						.retainUntilDate(Instant.now().minus(1, ChronoUnit.DAYS))));
		client.deleteObject(d -> d.bucket(bucketName).key(key).versionId(versionId).bypassGovernanceRetention(true));
	}

	@Test
	@Tag("Overwrite")
	public void testObjectLockPutObjRetentionIncreasePeriod() {
		var key = "testObjectLockPutObjRetentionIncreasePeriod";
		var client = getClient();
		var bucketName = getNewBucketName();

		client.createBucket(c -> c.bucket(bucketName).objectLockEnabledForBucket(true));

		client.putObject(p -> p.bucket(bucketName).key(key).build(), RequestBody.fromString(key));
		var putResponse = client.putObject(p -> p.bucket(bucketName).key(key).build(), RequestBody.fromString(key));
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

		var response = client.getObjectRetention(g -> g.bucket(bucketName).key(key));
		retentionCompare(retention2, response.retention());

		client.putObjectRetention(p -> p.bucket(bucketName).key(key)
				.retention(r -> r.mode(ObjectLockRetentionMode.GOVERNANCE)
						.retainUntilDate(Instant.now().minus(1, ChronoUnit.DAYS))));
		client.deleteObject(d -> d.bucket(bucketName).key(key).versionId(versionId).bypassGovernanceRetention(true));
	}

	@Test
	@Tag("Overwrite")
	public void testObjectLockPutObjRetentionShortenPeriod() {
		var key = "testObjectLockPutObjRetentionShortenPeriod";
		var client = getClient();
		var bucketName = getNewBucketName();

		client.createBucket(c -> c.bucket(bucketName).objectLockEnabledForBucket(true));

		client.putObject(p -> p.bucket(bucketName).key(key).build(), RequestBody.fromString(key));
		var putResponse = client.putObject(p -> p.bucket(bucketName).key(key).build(), RequestBody.fromString(key));
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
		assertEquals(HttpStatus.SC_FORBIDDEN, e.statusCode());
		assertEquals(MainData.ACCESS_DENIED, e.awsErrorDetails().errorCode());

		client.putObjectRetention(p -> p.bucket(bucketName).key(key)
				.retention(r -> r.mode(ObjectLockRetentionMode.GOVERNANCE)
						.retainUntilDate(Instant.now().minus(1, ChronoUnit.DAYS))));
		client.deleteObject(d -> d.bucket(bucketName).key(key).versionId(versionId).bypassGovernanceRetention(true));
	}

	@Test
	@Tag("Overwrite")
	public void testObjectLockPutObjRetentionShortenPeriodBypass() {
		var key = "testObjectLockPutObjRetentionShortenPeriodBypass";
		var client = getClient();
		var bucketName = getNewBucketName();

		client.createBucket(c -> c.bucket(bucketName).objectLockEnabledForBucket(true));

		client.putObject(p -> p.bucket(bucketName).key(key).build(), RequestBody.fromString(key));
		var putResponse = client.putObject(p -> p.bucket(bucketName).key(key).build(), RequestBody.fromString(key));
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

		client.putObjectRetention(p -> p.bucket(bucketName).key(key)
				.retention(r -> r.mode(ObjectLockRetentionMode.GOVERNANCE)
						.retainUntilDate(Instant.now().minus(1, ChronoUnit.DAYS))));
		client.deleteObject(d -> d.bucket(bucketName).key(key).versionId(versionId).bypassGovernanceRetention(true));
	}

	@Test
	@Tag("ERROR")
	public void testObjectLockDeleteObjectWithRetention() {
		var key = "testObjectLockDeleteObjectWithRetention";
		var client = getClient();
		var bucketName = getNewBucketName();

		client.createBucket(c -> c.bucket(bucketName).objectLockEnabledForBucket(true));

		var putResponse = client.putObject(p -> p.bucket(bucketName).key(key).build(), RequestBody.fromString(key));
		var versionId = putResponse.versionId();

		var retention = ObjectLockRetention.builder().mode(ObjectLockRetentionMode.GOVERNANCE)
				.retainUntilDate(new Calendar.Builder().setDate(2030, 1, 1)
						.setTimeZone(TimeZone.getTimeZone(("GMT"))).build().toInstant())
				.build();
		client.putObjectRetention(p -> p.bucket(bucketName).key(key).retention(retention));

		var e = assertThrows(AwsServiceException.class,
				() -> client.deleteObject(d -> d.bucket(bucketName).key(key).versionId(versionId)));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.statusCode());
		assertEquals(MainData.ACCESS_DENIED, e.awsErrorDetails().errorCode());

		client.putObjectRetention(p -> p.bucket(bucketName).key(key)
				.retention(r -> r.mode(ObjectLockRetentionMode.GOVERNANCE)
						.retainUntilDate(Instant.now().minus(1, ChronoUnit.DAYS))));
		client.deleteObject(d -> d.bucket(bucketName).key(key).versionId(versionId).bypassGovernanceRetention(true));
	}

	@Test
	@Tag("LegalHold")
	public void testObjectLockPutLegalHold() {
		var key = "testObjectLockPutLegalHold";
		var client = getClient();
		var bucketName = getNewBucketName();

		client.createBucket(c -> c.bucket(bucketName).objectLockEnabledForBucket(true));

		client.putObject(p -> p.bucket(bucketName).key(key).build(), RequestBody.fromString(key));

		client.putObjectLegalHold(p -> p.bucket(bucketName).key(key).legalHold(l -> l.status(ObjectLockLegalHoldStatus.ON)));

		client.putObjectLegalHold(p -> p.bucket(bucketName).key(key).legalHold(l -> l.status(ObjectLockLegalHoldStatus.OFF)));

	}

	@Test
	@Tag("LegalHold")
	public void testObjectLockPutLegalHoldInvalidBucket() {
		var key = "testObjectLockPutLegalHoldInvalidBucket";
		var client = getClient();
		var bucketName = createBucket(client);

		client.putObject(p -> p.bucket(bucketName).key(key).build(), RequestBody.fromString(key));

		var e = assertThrows(AwsServiceException.class,
				() -> client.putObjectLegalHold(
						p -> p.bucket(bucketName).key(key).legalHold(l -> l.status(ObjectLockLegalHoldStatus.ON))));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.statusCode());
		assertEquals(MainData.INVALID_REQUEST, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("LegalHold")
	public void testObjectLockPutLegalHoldInvalidStatus() {
		var key = "testObjectLockPutLegalHoldInvalidStatus";
		var client = getClient();
		var bucketName = getNewBucketName();
		client.createBucket(c -> c.bucket(bucketName).objectLockEnabledForBucket(true));

		client.putObject(p -> p.bucket(bucketName).key(key).build(), RequestBody.fromString(key));

		var legalHold = ObjectLockLegalHold.builder().status("abc").build();
		var e = assertThrows(AwsServiceException.class,
				() -> client.putObjectLegalHold(p -> p.bucket(bucketName).key(key).legalHold(legalHold)));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.statusCode());
		assertEquals(MainData.MALFORMED_XML, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("LegalHold")
	public void testObjectLockGetLegalHold() {
		var key = "testObjectLockGetLegalHold";
		var client = getClient();
		var bucketName = getNewBucketName();
		client.createBucket(c -> c.bucket(bucketName).objectLockEnabledForBucket(true));

		client.putObject(p -> p.bucket(bucketName).key(key).build(), RequestBody.fromString(key));

		client.putObjectLegalHold(
				p -> p.bucket(bucketName).key(key).legalHold(l -> l.status(ObjectLockLegalHoldStatus.ON)));
		var response = client.getObjectLegalHold(g -> g.bucket(bucketName).key(key));
		assertEquals(ObjectLockLegalHoldStatus.ON, response.legalHold().status());

		client.putObjectLegalHold(
				p -> p.bucket(bucketName).key(key).legalHold(l -> l.status(ObjectLockLegalHoldStatus.OFF)));
		response = client.getObjectLegalHold(g -> g.bucket(bucketName).key(key));
		assertEquals(ObjectLockLegalHoldStatus.OFF, response.legalHold().status());
	}

	@Test
	@Tag("LegalHold")
	public void testObjectLockGetLegalHoldInvalidBucket() {
		var key = "testObjectLockGetLegalHoldInvalidBucket";
		var client = getClient();
		var bucketName = createBucket(client);

		client.putObject(p -> p.bucket(bucketName).key(key).build(), RequestBody.fromString(key));

		var e = assertThrows(AwsServiceException.class,
				() -> client.getObjectLegalHold(g -> g.bucket(bucketName).key(key)));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.statusCode());
		assertEquals(MainData.INVALID_REQUEST, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("LegalHold")
	public void testObjectLockDeleteObjectWithLegalHoldOn() {
		var key = "testObjectLockDeleteObjectWithLegalHoldOn";
		var client = getClient();
		var bucketName = getNewBucketName();
		client.createBucket(c -> c.bucket(bucketName).objectLockEnabledForBucket(true));

		var putResponse = client.putObject(p -> p.bucket(bucketName).key(key).build(), RequestBody.fromString(key));

		client.putObjectLegalHold(
				p -> p.bucket(bucketName).key(key).legalHold(l -> l.status(ObjectLockLegalHoldStatus.ON)));

		var e = assertThrows(AwsServiceException.class,
				() -> client.deleteObject(d -> d.bucket(bucketName).key(key).versionId(putResponse.versionId())));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.statusCode());
		assertEquals(MainData.ACCESS_DENIED, e.awsErrorDetails().errorCode());

		client.putObjectLegalHold(
				p -> p.bucket(bucketName).key(key).legalHold(l -> l.status(ObjectLockLegalHoldStatus.OFF)));
	}

	@Test
	@Tag("LegalHold")
	public void testObjectLockDeleteObjectWithLegalHoldOff() {
		var key = "testObjectLockDeleteObjectWithLegalHoldOff";
		var client = getClient();
		var bucketName = getNewBucketName();
		client.createBucket(c -> c.bucket(bucketName).objectLockEnabledForBucket(true));

		var putResponse = client.putObject(p -> p.bucket(bucketName).key(key).build(), RequestBody.fromString(key));

		client.putObjectLegalHold(
				p -> p.bucket(bucketName).key(key).legalHold(l -> l.status(ObjectLockLegalHoldStatus.OFF)));

		client.deleteObject(d -> d.bucket(bucketName).key(key).versionId(putResponse.versionId()));
	}

	@Test
	@Tag("LegalHold")
	public void testObjectLockGetObjMetadata() {
		var key = "testObjectLockGetObjMetadata";
		var client = getClient();
		var bucketName = getNewBucketName();

		client.createBucket(c -> c.bucket(bucketName).objectLockEnabledForBucket(true));

		var putResponse = client.putObject(p -> p.bucket(bucketName).key(key).build(), RequestBody.fromString(key));

		client.putObjectLegalHold(
				p -> p.bucket(bucketName).key(key).legalHold(l -> l.status(ObjectLockLegalHoldStatus.ON)));

		var retention = ObjectLockRetention.builder().mode(ObjectLockRetentionMode.GOVERNANCE)
				.retainUntilDate(new Calendar.Builder().setDate(2030, 1, 1)
						.setTimeZone(TimeZone.getTimeZone(("GMT"))).build().toInstant())
				.build();
		client.putObjectRetention(p -> p.bucket(bucketName).key(key).retention(retention));

		var response = client.headObject(h -> h.bucket(bucketName).key(key));
		assertEquals(retention.mode().toString(), response.objectLockMode().toString());
		assertEquals(retention.retainUntilDate(), response.objectLockRetainUntilDate());
		assertEquals(ObjectLockLegalHoldStatus.ON, response.objectLockLegalHoldStatus());

		client.putObjectLegalHold(
				p -> p.bucket(bucketName).key(key).legalHold(l -> l.status(ObjectLockLegalHoldStatus.OFF)));

		client.putObjectRetention(p -> p.bucket(bucketName).key(key)
				.retention(r -> r.mode(ObjectLockRetentionMode.GOVERNANCE)
						.retainUntilDate(Instant.now().minus(1, ChronoUnit.DAYS))));
		client.deleteObject(
				d -> d.bucket(bucketName).key(key).versionId(putResponse.versionId()).bypassGovernanceRetention(true));
	}
}
