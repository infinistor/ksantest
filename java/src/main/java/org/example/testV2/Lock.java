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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.TimeZone;

import org.apache.hc.core5.http.HttpStatus;
import org.example.Data.MainData;
import org.example.Utility.Utils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketVersioningStatus;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.ObjectLockConfiguration;
import software.amazon.awssdk.services.s3.model.ObjectLockEnabled;
import software.amazon.awssdk.services.s3.model.ObjectLockLegalHold;
import software.amazon.awssdk.services.s3.model.ObjectLockLegalHoldStatus;
import software.amazon.awssdk.services.s3.model.ObjectLockMode;
import software.amazon.awssdk.services.s3.model.ObjectLockRetention;
import software.amazon.awssdk.services.s3.model.ObjectLockRetentionMode;

public class Lock extends TestBase {
	@Test
	@Tag("Put")
	public void testCreatedBucketEnableObjectLock() {
		var client = getClient();
		var bucketName = createBucket(client, 1);

		client.putBucketVersioning(
				p -> p.bucket(bucketName).versioningConfiguration(v -> v.status(BucketVersioningStatus.ENABLED)));
		client.putObjectLockConfiguration(p -> p.bucket(bucketName).objectLockConfiguration(
				ObjectLockConfiguration.builder().objectLockEnabled(ObjectLockEnabled.ENABLED).build()));
	}

	@Test
	@Tag("Check")
	public void testObjectLockPutObjLock() {
		var client = getClient();
		var bucketName = getNewBucketName(2);
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
		var bucketName = createBucket(client, 3);

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
		var bucketName = getNewBucketName(4);
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
		var bucketName = getNewBucketName(5);
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
		var bucketName = getNewBucketName(6);
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
		var bucketName = getNewBucketName(7);
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
		var bucketName = getNewBucketName(8);
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
		var bucketName = getNewBucketName(9);
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
		var bucketName = getNewBucketName(10);
		client.createBucket(c -> c.bucket(bucketName).objectLockEnabledForBucket(true));

		var conf = ObjectLockConfiguration.builder().objectLockEnabled(ObjectLockEnabled.ENABLED)
				.rule(r -> r.defaultRetention(retention -> retention.mode(ObjectLockRetentionMode.GOVERNANCE).days(1)))
				.build();

		client.putObjectLockConfiguration(p -> p.bucket(bucketName).objectLockConfiguration(conf));

		var response = client.getObjectLockConfiguration(g -> g.bucket(bucketName));
		lockCompare(conf, response.objectLockConfiguration());
	}

	@Test
	@Tag("Check")
	public void testObjectLockPutObject() {
		var client = getClient();
		var bucketName = getNewBucketName(11);
		var key = "testObjectLockPutObject";

		client.createBucket(c -> c.bucket(bucketName).objectLockEnabledForBucket(true));

		var conf = ObjectLockConfiguration.builder().objectLockEnabled(ObjectLockEnabled.ENABLED)
				.rule(r -> r.defaultRetention(retention -> retention.mode(ObjectLockRetentionMode.GOVERNANCE).days(1)))
				.build();

		client.putObjectLockConfiguration(p -> p.bucket(bucketName).objectLockConfiguration(conf));

		// Put Object
		client.putObject(p -> p.bucket(bucketName).key(key).contentMD5(Utils.getMD5(key)).build(),
				RequestBody.fromString(key));

		var response = client.headObject(h -> h.bucket(bucketName).key(key));
		assertEquals(ObjectLockRetentionMode.GOVERNANCE.toString(), response.objectLockMode().toString());
		assertNotNull(response.objectLockRetainUntilDate());

		var e = assertThrows(AwsServiceException.class,
				() -> client.deleteObject(d -> d.bucket(bucketName).key(key).versionId(response.versionId())));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.statusCode());
		assertEquals(MainData.ACCESS_DENIED, e.awsErrorDetails().errorCode());

		client.deleteObject(
				d -> d.bucket(bucketName).key(key).versionId(response.versionId()).bypassGovernanceRetention(true));
	}

	@Test
	@Tag("Check")
	public void testObjectLockCopyObject() {
		var client = getClient();
		var bucketName = getNewBucketName(12);
		var bucketName2 = getNewBucketName(12);
		var key = "testObjectLockCopyObject-lock";
		var keyCopy = key + "-copy";
		var key2 = "testObjectLockCopyObject";
		var key2Copy = key2 + "-copy";

		client.createBucket(c -> c.bucket(bucketName).objectLockEnabledForBucket(true));
		client.createBucket(c -> c.bucket(bucketName2).objectLockEnabledForBucket(true));

		var conf = ObjectLockConfiguration.builder().objectLockEnabled(ObjectLockEnabled.ENABLED)
				.rule(r -> r.defaultRetention(retention -> retention.mode(ObjectLockRetentionMode.GOVERNANCE).days(1)))
				.build();

		client.putObjectLockConfiguration(p -> p.bucket(bucketName).objectLockConfiguration(conf));

		client.putObject(p -> p.bucket(bucketName).key(key).contentMD5(Utils.getMD5(key)).build(),
				RequestBody.fromString(key));
		client.putObject(p -> p.bucket(bucketName2).key(key2).build(), RequestBody.fromString(key2));

		client.copyObject(
				c -> c.sourceBucket(bucketName).sourceKey(key).destinationBucket(bucketName2).destinationKey(keyCopy));
		client.copyObject(c -> c.sourceBucket(bucketName2).sourceKey(key2).destinationBucket(bucketName)
				.destinationKey(key2Copy));

		var response = client.headObject(h -> h.bucket(bucketName).key(key));
		assertEquals(ObjectLockRetentionMode.GOVERNANCE.toString(), response.objectLockMode().toString());
		assertNotNull(response.objectLockRetainUntilDate());

		var response2 = client.headObject(h -> h.bucket(bucketName2).key(keyCopy));
		assertNull(response2.objectLockMode());
		assertNull(response2.objectLockRetainUntilDate());

		var response3 = client.headObject(h -> h.bucket(bucketName2).key(key2));
		assertNull(response3.objectLockMode());
		assertNull(response3.objectLockRetainUntilDate());

		var response4 = client.headObject(h -> h.bucket(bucketName).key(key2Copy));
		assertEquals(ObjectLockRetentionMode.GOVERNANCE.toString(), response4.objectLockMode().toString());
		assertNotNull(response4.objectLockRetainUntilDate());

		var e = assertThrows(AwsServiceException.class,
				() -> client.deleteObject(d -> d.bucket(bucketName).key(key).versionId(response.versionId())));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.statusCode());
		assertEquals(MainData.ACCESS_DENIED, e.awsErrorDetails().errorCode());

		var e2 = assertThrows(AwsServiceException.class,
				() -> client.deleteObject(d -> d.bucket(bucketName).key(key2Copy).versionId(response4.versionId())));
		assertEquals(HttpStatus.SC_FORBIDDEN, e2.statusCode());
		assertEquals(MainData.ACCESS_DENIED, e2.awsErrorDetails().errorCode());

		client.deleteObject(d -> d.bucket(bucketName).key(key).versionId(response.versionId())
				.bypassGovernanceRetention(true));
		client.deleteObject(d -> d.bucket(bucketName2).key(keyCopy).versionId(response2.versionId())
				.bypassGovernanceRetention(true));
		client.deleteObject(d -> d.bucket(bucketName2).key(key2).versionId(response3.versionId())
				.bypassGovernanceRetention(true));
		client.deleteObject(d -> d.bucket(bucketName).key(key2Copy).versionId(response4.versionId())
				.bypassGovernanceRetention(true));
	}

	@Test
	@Tag("Check")
	public void testObjectLockMultipart() {
		var client = getClient();
		var bucketName = getNewBucketName(13);
		var key = "testObjectLockMultipart";

		client.createBucket(c -> c.bucket(bucketName).objectLockEnabledForBucket(true));

		var conf = ObjectLockConfiguration.builder().objectLockEnabled(ObjectLockEnabled.ENABLED)
				.rule(r -> r.defaultRetention(retention -> retention.mode(ObjectLockRetentionMode.GOVERNANCE).days(1)))
				.build();

		client.putObjectLockConfiguration(p -> p.bucket(bucketName).objectLockConfiguration(conf));

		// Multipart Upload
		var uploadData = setupMultipartUpload(client, bucketName, key, 1 * MainData.MB);
		client.completeMultipartUpload(c -> c.bucket(bucketName).key(key).uploadId(uploadData.uploadId)
				.multipartUpload(p -> p.parts(uploadData.parts)));

		var response = client.headObject(h -> h.bucket(bucketName).key(key));
		assertEquals(ObjectLockRetentionMode.GOVERNANCE.toString(), response.objectLockMode().toString());
		assertNotNull(response.objectLockRetainUntilDate());

		var e = assertThrows(AwsServiceException.class,
				() -> client.deleteObject(d -> d.bucket(bucketName).key(key).versionId(response.versionId())));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.statusCode());
		assertEquals(MainData.ACCESS_DENIED, e.awsErrorDetails().errorCode());

		client.deleteObject(
				d -> d.bucket(bucketName).key(key).versionId(response.versionId()).bypassGovernanceRetention(true));
	}

	@Test
	@Tag("ERROR")
	public void testObjectLockMD5() {
		var client = getClient();
		var bucketName = getNewBucketName(14);
		var key = "testObjectLockMD5";
		var content = Utils.randomTextToLong(1 * MainData.MB);

		client.createBucket(c -> c.bucket(bucketName).objectLockEnabledForBucket(true));

		var conf = ObjectLockConfiguration.builder().objectLockEnabled(ObjectLockEnabled.ENABLED)
				.rule(r -> r.defaultRetention(retention -> retention.mode(ObjectLockRetentionMode.GOVERNANCE).days(1)))
				.build();

		client.putObjectLockConfiguration(p -> p.bucket(bucketName).objectLockConfiguration(conf));

		var e = assertThrows(AwsServiceException.class,
				() -> client.putObject(p -> p.bucket(bucketName).key(key).build(),
						RequestBody.fromString(content)));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.statusCode());
		assertEquals(MainData.INVALID_REQUEST, e.awsErrorDetails().errorCode());

		var initResponse = client.createMultipartUpload(p -> p.bucket(bucketName).key(key));
		var uploadId = initResponse.uploadId();

		var e2 = assertThrows(AwsServiceException.class,
				() -> client.uploadPart(p -> p.bucket(bucketName).key(key).uploadId(uploadId).partNumber(1).build(),
						RequestBody.fromString(content)));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e2.statusCode());
		assertEquals(MainData.INVALID_REQUEST, e2.awsErrorDetails().errorCode());

		client.abortMultipartUpload(a -> a.bucket(bucketName).key(key).uploadId(uploadId));
	}

	@Test
	@Tag("ERROR")
	public void testObjectLockGetObjLockInvalidBucket() {
		var client = getClient();
		var bucketName = getNewBucketName(15);
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
		var bucketName = getNewBucketName(16);
		client.createBucket(c -> c.bucket(bucketName).objectLockEnabledForBucket(true));

		var response = client.putObject(p -> p.bucket(bucketName).key(key).build(), RequestBody.fromString(key));
		var versionId = response.versionId();
		client.putObjectRetention(p -> p.bucket(bucketName).key(key)
				.retention(r -> r.mode(ObjectLockRetentionMode.GOVERNANCE)
						.retainUntilDate(new Calendar.Builder().setDate(2030, 1, 1)
								.setTimeZone(TimeZone.getTimeZone(("GMT"))).build().toInstant())));

		client.deleteObject(d -> d.bucket(bucketName).key(key).versionId(versionId).bypassGovernanceRetention(true));
	}

	@Test
	@Tag("retention")
	public void testObjectLockPutObjRetentionInvalidBucket() {
		var key = "testObjectLockPutObjRetentionInvalidBucket";
		var client = getClient();
		var bucketName = createBucket(client, 17);

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
		var bucketName = getNewBucketName(18);

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
		var bucketName = getNewBucketName(19);

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

		client.deleteObject(d -> d.bucket(bucketName).key(key).versionId(versionId).bypassGovernanceRetention(true));
	}

	@Test
	@Tag("retention")
	public void testObjectLockGetObjRetentionInvalidBucket() {
		var key = "testObjectLockGetObjRetentionInvalidBucket";
		var client = getClient();
		var bucketName = createBucket(client, 20);

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
		var bucketName = getNewBucketName(21);

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

		client.deleteObject(d -> d.bucket(bucketName).key(key).versionId(versionId).bypassGovernanceRetention(true));
	}

	@Test
	@Tag("Priority")
	public void testObjectLockPutObjRetentionOverrideDefaultRetention() {
		var key = "testObjectLockPutObjRetentionOverrideDefaultRetention";
		var client = getClient();
		var bucketName = getNewBucketName(22);

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

		client.deleteObject(d -> d.bucket(bucketName).key(key).versionId(versionId).bypassGovernanceRetention(true));
	}

	@Test
	@Tag("Overwrite")
	public void testObjectLockPutObjRetentionIncreasePeriod() {
		var key = "testObjectLockPutObjRetentionIncreasePeriod";
		var client = getClient();
		var bucketName = getNewBucketName(23);

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

		client.deleteObject(d -> d.bucket(bucketName).key(key).versionId(versionId).bypassGovernanceRetention(true));
	}

	@Test
	@Tag("Overwrite")
	public void testObjectLockPutObjRetentionShortenPeriod() {
		var key = "testObjectLockPutObjRetentionShortenPeriod";
		var client = getClient();
		var bucketName = getNewBucketName(24);

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

		client.deleteObject(d -> d.bucket(bucketName).key(key).versionId(versionId).bypassGovernanceRetention(true));
	}

	@Test
	@Tag("Overwrite")
	public void testObjectLockPutObjRetentionShortenPeriodBypass() {
		var key = "testObjectLockPutObjRetentionShortenPeriodBypass";
		var client = getClient();
		var bucketName = getNewBucketName(25);

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

		client.deleteObject(d -> d.bucket(bucketName).key(key).versionId(versionId).bypassGovernanceRetention(true));
	}

	@Test
	@Tag("ERROR")
	public void testObjectLockDeleteObjectWithRetention() {
		var key = "testObjectLockDeleteObjectWithRetention";
		var client = getClient();
		var bucketName = getNewBucketName(26);

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

		client.deleteObject(d -> d.bucket(bucketName).key(key).versionId(versionId).bypassGovernanceRetention(true));
	}

	@Test
	@Tag("Retention")
	public void testObjectLockDeleteObjectWithRetentionBypass() {
		var key = "testObjectLockDeleteObjectWithRetentionBypass";
		var client = getClient();
		var bucketName = getNewBucketName(27);

		client.createBucket(c -> c.bucket(bucketName).objectLockEnabledForBucket(true));

		var putResponse = client.putObject(p -> p.bucket(bucketName).key(key).build(), RequestBody.fromString(key));
		var versionId = putResponse.versionId();

		var retention = ObjectLockRetention.builder().mode(ObjectLockRetentionMode.GOVERNANCE)
				.retainUntilDate(new Calendar.Builder().setDate(2030, 1, 1)
						.setTimeZone(TimeZone.getTimeZone(("GMT"))).build().toInstant())
				.build();
		client.putObjectRetention(p -> p.bucket(bucketName).key(key).retention(retention));

		client.deleteObject(d -> d.bucket(bucketName).key(key).versionId(versionId).bypassGovernanceRetention(true));
	}

	@Test
	@Tag("Retention")
	public void testObjectLockDeleteObjectsWithRetentionBypass() {
		var client = getClient();
		var bucketName = getNewBucketName(28);
		var keyVersions = new ArrayList<ObjectIdentifier>();

		client.createBucket(c -> c.bucket(bucketName).objectLockEnabledForBucket(true));

		for (int i = 0; i < 10; i++) {
			var key = String.format("testObjectLockDeleteObjectsWithRetentionBypass-%03d", i);
			var putResponse = client.putObject(p -> p.bucket(bucketName).key(key).build(), RequestBody.fromString(key));
			var versionId = putResponse.versionId();

			var retention = ObjectLockRetention.builder().mode(ObjectLockRetentionMode.GOVERNANCE)
					.retainUntilDate(new Calendar.Builder().setDate(2030, 1, 1)
							.setTimeZone(TimeZone.getTimeZone(("GMT"))).build().toInstant())
					.build();
			client.putObjectRetention(p -> p.bucket(bucketName).key(key).retention(retention).versionId(versionId));

			keyVersions.add(ObjectIdentifier.builder().key(key).versionId(versionId).build());
		}

		client.deleteObjects(d -> d
				.bucket(bucketName)
				.delete(o -> o.objects(keyVersions))
				.bypassGovernanceRetention(true));
	}

	@Test
	@Tag("LegalHold")
	public void testObjectLockPutLegalHold() {
		var key = "testObjectLockPutLegalHold";
		var client = getClient();
		var bucketName = getNewBucketName(29);

		client.createBucket(c -> c.bucket(bucketName).objectLockEnabledForBucket(true));

		client.putObject(p -> p.bucket(bucketName).key(key).build(), RequestBody.fromString(key));

		client.putObjectLegalHold(
				p -> p.bucket(bucketName).key(key).legalHold(l -> l.status(ObjectLockLegalHoldStatus.ON)));

		client.putObjectLegalHold(
				p -> p.bucket(bucketName).key(key).legalHold(l -> l.status(ObjectLockLegalHoldStatus.OFF)));

	}

	@Test
	@Tag("LegalHold")
	public void testObjectLockPutLegalHoldInvalidBucket() {
		var key = "testObjectLockPutLegalHoldInvalidBucket";
		var client = getClient();
		var bucketName = createBucket(client, 30);

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
		var bucketName = getNewBucketName(31);
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
		var bucketName = getNewBucketName(32);
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
		var bucketName = createBucket(client, 33);

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
		var bucketName = getNewBucketName(34);
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
		var bucketName = getNewBucketName(35);
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
		var bucketName = getNewBucketName(36);

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

		client.deleteObject(
				d -> d.bucket(bucketName).key(key).versionId(putResponse.versionId()).bypassGovernanceRetention(true));
	}


	@Test
	@Tag("Mode")
	public void testObjectLockChangeModeGovernanceToCompliance() {
		retentionChangeTest(37, "testObjectLockChangeModeGovernanceToCompliance", ObjectLockRetentionMode.GOVERNANCE, ObjectLockRetentionMode.COMPLIANCE, 0, false, false);
	}

	@Test
	@Tag("Mode")
	public void testObjectLockChangeModeGovernanceToComplianceBypass() {
		retentionChangeTest(38, "testObjectLockChangeModeGovernanceToComplianceBypass", ObjectLockRetentionMode.GOVERNANCE, ObjectLockRetentionMode.COMPLIANCE, 0, true, true);
	}

	@Test
	@Tag("Mode")
	public void testObjectLockChangeModeGovernanceToComplianceExtend() {
		retentionChangeTest(39, "testObjectLockChangeModeGovernanceToComplianceExtend", ObjectLockRetentionMode.GOVERNANCE, ObjectLockRetentionMode.COMPLIANCE, RETENTION_DELTA_SECONDS, false, false);
	}

	@Test
	@Tag("Mode")
	public void testObjectLockChangeModeGovernanceToComplianceExtendBypass() {
		retentionChangeTest(40, "testObjectLockChangeModeGovernanceToComplianceExtendBypass", ObjectLockRetentionMode.GOVERNANCE, ObjectLockRetentionMode.COMPLIANCE, RETENTION_DELTA_SECONDS, true, true);
	}

	@Test
	@Tag("Mode")
	public void testObjectLockChangeModeGovernanceToComplianceShorten() {
		retentionChangeTest(41, "testObjectLockChangeModeGovernanceToComplianceShorten", ObjectLockRetentionMode.GOVERNANCE, ObjectLockRetentionMode.COMPLIANCE, -RETENTION_DELTA_SECONDS, false, false);
	}

	@Test
	@Tag("Mode")
	public void testObjectLockChangeModeGovernanceToComplianceShortenBypass() {
		retentionChangeTest(42, "testObjectLockChangeModeGovernanceToComplianceShortenBypass", ObjectLockRetentionMode.GOVERNANCE, ObjectLockRetentionMode.COMPLIANCE, -RETENTION_DELTA_SECONDS, true, true);
	}

	@Test
	@Tag("Mode")
	public void testObjectLockPutObjRetentionIncreasePeriodBypass() {
		retentionChangeTest(43, "testObjectLockPutObjRetentionIncreasePeriodBypass", ObjectLockRetentionMode.GOVERNANCE, ObjectLockRetentionMode.GOVERNANCE, RETENTION_DELTA_SECONDS, true, true);
	}

	@Test
	@Tag("Mode")
	public void testObjectLockChangeModeComplianceToGovernance() {
		retentionChangeTest(44, "testObjectLockChangeModeComplianceToGovernance", ObjectLockRetentionMode.COMPLIANCE, ObjectLockRetentionMode.GOVERNANCE, 0, false, false);
	}

	@Test
	@Tag("Mode")
	public void testObjectLockChangeModeComplianceToGovernanceBypass() {
		retentionChangeTest(45, "testObjectLockChangeModeComplianceToGovernanceBypass", ObjectLockRetentionMode.COMPLIANCE, ObjectLockRetentionMode.GOVERNANCE, 0, true, false);
	}

	@Test
	@Tag("Mode")
	public void testObjectLockChangeModeComplianceToGovernanceExtend() {
		retentionChangeTest(46, "testObjectLockChangeModeComplianceToGovernanceExtend", ObjectLockRetentionMode.COMPLIANCE, ObjectLockRetentionMode.GOVERNANCE, RETENTION_DELTA_SECONDS, false, false);
	}

	@Test
	@Tag("Mode")
	public void testObjectLockChangeModeComplianceToGovernanceExtendBypass() {
		retentionChangeTest(47, "testObjectLockChangeModeComplianceToGovernanceExtendBypass", ObjectLockRetentionMode.COMPLIANCE, ObjectLockRetentionMode.GOVERNANCE, RETENTION_DELTA_SECONDS, true, false);
	}

	@Test
	@Tag("Mode")
	public void testObjectLockChangeModeComplianceToGovernanceShorten() {
		retentionChangeTest(48, "testObjectLockChangeModeComplianceToGovernanceShorten", ObjectLockRetentionMode.COMPLIANCE, ObjectLockRetentionMode.GOVERNANCE, -RETENTION_DELTA_SECONDS, false, false);
	}

	@Test
	@Tag("Mode")
	public void testObjectLockChangeModeComplianceToGovernanceShortenBypass() {
		retentionChangeTest(49, "testObjectLockChangeModeComplianceToGovernanceShortenBypass", ObjectLockRetentionMode.COMPLIANCE, ObjectLockRetentionMode.GOVERNANCE, -RETENTION_DELTA_SECONDS, true, false);
	}

	@Test
	@Tag("Mode")
	public void testObjectLockComplianceSamePeriod() {
		retentionChangeTest(50, "testObjectLockComplianceSamePeriod", ObjectLockRetentionMode.COMPLIANCE, ObjectLockRetentionMode.COMPLIANCE, 0, false, true);
	}

	@Test
	@Tag("Mode")
	public void testObjectLockComplianceSamePeriodBypass() {
		retentionChangeTest(51, "testObjectLockComplianceSamePeriodBypass", ObjectLockRetentionMode.COMPLIANCE, ObjectLockRetentionMode.COMPLIANCE, 0, true, true);
	}

	@Test
	@Tag("Mode")
	public void testObjectLockComplianceIncreasePeriod() {
		retentionChangeTest(52, "testObjectLockComplianceIncreasePeriod", ObjectLockRetentionMode.COMPLIANCE, ObjectLockRetentionMode.COMPLIANCE, RETENTION_DELTA_SECONDS, false, true);
	}

	@Test
	@Tag("Mode")
	public void testObjectLockComplianceIncreasePeriodBypass() {
		retentionChangeTest(53, "testObjectLockComplianceIncreasePeriodBypass", ObjectLockRetentionMode.COMPLIANCE, ObjectLockRetentionMode.COMPLIANCE, RETENTION_DELTA_SECONDS, true, true);
	}

	@Test
	@Tag("Mode")
	public void testObjectLockComplianceShortenPeriod() {
		retentionChangeTest(54, "testObjectLockComplianceShortenPeriod", ObjectLockRetentionMode.COMPLIANCE, ObjectLockRetentionMode.COMPLIANCE, -RETENTION_DELTA_SECONDS, false, false);
	}

	@Test
	@Tag("Mode")
	public void testObjectLockComplianceShortenPeriodBypass() {
		retentionChangeTest(55, "testObjectLockComplianceShortenPeriodBypass", ObjectLockRetentionMode.COMPLIANCE, ObjectLockRetentionMode.COMPLIANCE, -RETENTION_DELTA_SECONDS, true, false);
	}

	@Test
	@Tag("Mode")
	public void testObjectLockRemoveRetentionGovernance() {
		retentionRemoveTest(56, "testObjectLockRemoveRetentionGovernance", ObjectLockRetentionMode.GOVERNANCE, false, false);
	}

	@Test
	@Tag("Mode")
	public void testObjectLockRemoveRetentionGovernanceBypass() {
		retentionRemoveTest(57, "testObjectLockRemoveRetentionGovernanceBypass", ObjectLockRetentionMode.GOVERNANCE, true, true);
	}

	@Test
	@Tag("Mode")
	public void testObjectLockRemoveRetentionCompliance() {
		retentionRemoveTest(58, "testObjectLockRemoveRetentionCompliance", ObjectLockRetentionMode.COMPLIANCE, false, false);
	}

	@Test
	@Tag("Mode")
	public void testObjectLockRemoveRetentionComplianceBypass() {
		retentionRemoveTest(59, "testObjectLockRemoveRetentionComplianceBypass", ObjectLockRetentionMode.COMPLIANCE, true, false);
	}

	@Test
	@Tag("Mode")
	public void testObjectLockDeleteObjectCompliance() {
		complianceDeleteTest(60, "testObjectLockDeleteObjectCompliance", false);
	}

	@Test
	@Tag("Mode")
	public void testObjectLockDeleteObjectComplianceBypass() {
		complianceDeleteTest(61, "testObjectLockDeleteObjectComplianceBypass", true);
	}

	@Test
	@Tag("Mode")
	public void testObjectLockDeleteObjectsCompliance() {
		complianceDeleteObjectsTest(62, "testObjectLockDeleteObjectsCompliance", false);
	}

	@Test
	@Tag("Mode")
	public void testObjectLockDeleteObjectsComplianceBypass() {
		complianceDeleteObjectsTest(63, "testObjectLockDeleteObjectsComplianceBypass", true);
	}

	@Test
	@Tag("Mode")
	public void testObjectLockChangeModeHeaderComplianceToGovernance() {
		complianceHeaderToGovernanceTest(64, "testObjectLockChangeModeHeaderComplianceToGovernance", false);
	}

	@Test
	@Tag("Mode")
	public void testObjectLockChangeModeHeaderComplianceToGovernanceBypass() {
		complianceHeaderToGovernanceTest(65, "testObjectLockChangeModeHeaderComplianceToGovernanceBypass", true);
	}

	// region 보존 모드/기한 변경 (AWS 실측 기준)

	/** 보존 기간. COMPLIANCE 는 만료 전에는 지울 수 없으므로 짧게 잡는다. */
	static final int RETENTION_SECONDS = 15;
	/** 기한 연장/단축 폭. 단축해도 미래여야 한다. */
	static final int RETENTION_DELTA_SECONDS = 5;

	Instant retentionBase() {
		return Instant.now().plusSeconds(RETENTION_SECONDS).truncatedTo(ChronoUnit.SECONDS);
	}

	ObjectLockRetention retentionOf(ObjectLockRetentionMode mode, Instant until) {
		return ObjectLockRetention.builder().mode(mode).retainUntilDate(until).build();
	}

	/** false 일 때는 헤더 자체를 보내지 않는다. */
	Boolean bypassHeader(boolean bypass) {
		return bypass ? Boolean.TRUE : null;
	}

	void assertAccessDenied(AwsServiceException e) {
		assertEquals(HttpStatus.SC_FORBIDDEN, e.statusCode());
		assertEquals(MainData.ACCESS_DENIED, e.awsErrorDetails().errorCode());
	}

	/** COMPLIANCE 버전은 bypass 를 줘도 지워지면 안 된다(WORM). */
	void assertComplianceProtected(S3Client client, String bucketName, String key, String versionId) {
		var e = assertThrows(AwsServiceException.class, () -> client.deleteObject(
				d -> d.bucket(bucketName).key(key).versionId(versionId).bypassGovernanceRetention(true)));
		assertAccessDenied(e);
	}

	/**
	 * 보존 설정 후 다른 모드/기한으로 PutObjectRetention 을 보낸다. 허용이면 바뀐 값, 거부면 403 과 원래 값을 확인한다.
	 * 마지막 모드가 COMPLIANCE 면 bypass 삭제가 막히는지도 확인한다.
	 */
	void retentionChangeTest(int testId, String key, ObjectLockRetentionMode from, ObjectLockRetentionMode to,
			int deltaSeconds, boolean bypass, boolean allowed) {
		var client = getClient();
		var bucketName = getNewBucketName(testId);
		client.createBucket(c -> c.bucket(bucketName).objectLockEnabledForBucket(true));
		var versionId = client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(key)).versionId();

		var base = retentionBase();
		var initial = retentionOf(from, base);
		var changed = retentionOf(to, base.plusSeconds(deltaSeconds));
		try {
			client.putObjectRetention(p -> p.bucket(bucketName).key(key).versionId(versionId).retention(initial));

			if (allowed) {
				client.putObjectRetention(p -> p.bucket(bucketName).key(key).versionId(versionId).retention(changed)
						.bypassGovernanceRetention(bypassHeader(bypass)));
			} else {
				var e = assertThrows(AwsServiceException.class,
						() -> client.putObjectRetention(p -> p.bucket(bucketName).key(key).versionId(versionId)
								.retention(changed).bypassGovernanceRetention(bypassHeader(bypass))));
				assertAccessDenied(e);
			}

			var expected = allowed ? changed : initial;
			var response = client.getObjectRetention(g -> g.bucket(bucketName).key(key).versionId(versionId));
			retentionCompare(expected, response.retention());

			if (expected.mode() == ObjectLockRetentionMode.COMPLIANCE)
				assertComplianceProtected(client, bucketName, key, versionId);
		} finally {
			deleteAfterRetentionExpires(client, bucketName, key, versionId,
					base.plusSeconds(Math.abs(deltaSeconds)));
		}
	}

	/** 빈 Retention 으로 보존 제거를 시도한다. */
	void retentionRemoveTest(int testId, String key, ObjectLockRetentionMode mode, boolean bypass, boolean allowed) {
		var client = getClient();
		var bucketName = getNewBucketName(testId);
		client.createBucket(c -> c.bucket(bucketName).objectLockEnabledForBucket(true));
		var versionId = client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(key)).versionId();

		var base = retentionBase();
		var initial = retentionOf(mode, base);
		var empty = ObjectLockRetention.builder().build();
		try {
			client.putObjectRetention(p -> p.bucket(bucketName).key(key).versionId(versionId).retention(initial));

			if (allowed) {
				client.putObjectRetention(p -> p.bucket(bucketName).key(key).versionId(versionId).retention(empty)
						.bypassGovernanceRetention(bypassHeader(bypass)));
				var e = assertThrows(AwsServiceException.class,
						() -> client.getObjectRetention(g -> g.bucket(bucketName).key(key).versionId(versionId)));
				assertEquals(MainData.NO_SUCH_OBJECT_LOCK_CONFIGURATION, e.awsErrorDetails().errorCode());
			} else {
				var e = assertThrows(AwsServiceException.class,
						() -> client.putObjectRetention(p -> p.bucket(bucketName).key(key).versionId(versionId)
								.retention(empty).bypassGovernanceRetention(bypassHeader(bypass))));
				assertAccessDenied(e);
				var response = client.getObjectRetention(g -> g.bucket(bucketName).key(key).versionId(versionId));
				retentionCompare(initial, response.retention());
			}
		} finally {
			deleteAfterRetentionExpires(client, bucketName, key, versionId, base);
		}
	}

	/** COMPLIANCE 버전을 DeleteObject 로 지우면 bypass 여부와 상관없이 거부된다. */
	void complianceDeleteTest(int testId, String key, boolean bypass) {
		var client = getClient();
		var bucketName = getNewBucketName(testId);
		client.createBucket(c -> c.bucket(bucketName).objectLockEnabledForBucket(true));
		var versionId = client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(key)).versionId();

		var base = retentionBase();
		var retention = retentionOf(ObjectLockRetentionMode.COMPLIANCE, base);
		try {
			client.putObjectRetention(p -> p.bucket(bucketName).key(key).versionId(versionId).retention(retention));

			var e = assertThrows(AwsServiceException.class, () -> client.deleteObject(
					d -> d.bucket(bucketName).key(key).versionId(versionId).bypassGovernanceRetention(bypassHeader(bypass))));
			assertAccessDenied(e);

			var response = client.getObjectRetention(g -> g.bucket(bucketName).key(key).versionId(versionId));
			retentionCompare(retention, response.retention());
		} finally {
			deleteAfterRetentionExpires(client, bucketName, key, versionId, base);
		}
	}

	/** COMPLIANCE 버전을 DeleteObjects 로 지우면 요청은 200 이고 키별로 AccessDenied 가 돌아온다. */
	void complianceDeleteObjectsTest(int testId, String key, boolean bypass) {
		var client = getClient();
		var bucketName = getNewBucketName(testId);
		client.createBucket(c -> c.bucket(bucketName).objectLockEnabledForBucket(true));
		var versionId = client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(key)).versionId();

		var base = retentionBase();
		var retention = retentionOf(ObjectLockRetentionMode.COMPLIANCE, base);
		try {
			client.putObjectRetention(p -> p.bucket(bucketName).key(key).versionId(versionId).retention(retention));

			var response = client.deleteObjects(d -> d.bucket(bucketName)
					.delete(o -> o.objects(ObjectIdentifier.builder().key(key).versionId(versionId).build()))
					.bypassGovernanceRetention(bypassHeader(bypass)));
			assertEquals(0, response.deleted().size());
			assertEquals(1, response.errors().size());
			assertEquals(key, response.errors().get(0).key());
			assertEquals(MainData.ACCESS_DENIED, response.errors().get(0).code());

			var get = client.getObjectRetention(g -> g.bucket(bucketName).key(key).versionId(versionId));
			retentionCompare(retention, get.retention());
		} finally {
			deleteAfterRetentionExpires(client, bucketName, key, versionId, base);
		}
	}

	/** PutObject 헤더로 지정한 COMPLIANCE 도 PutObjectRetention 으로 건 것과 똑같이 GOVERNANCE 로 바꿀 수 없다. */
	void complianceHeaderToGovernanceTest(int testId, String key, boolean bypass) {
		var client = getClient();
		var bucketName = getNewBucketName(testId);
		client.createBucket(c -> c.bucket(bucketName).objectLockEnabledForBucket(true));

		var base = retentionBase();
		var compliance = retentionOf(ObjectLockRetentionMode.COMPLIANCE, base);
		var governance = retentionOf(ObjectLockRetentionMode.GOVERNANCE, base);
		var versionId = client.putObject(p -> p.bucket(bucketName).key(key).contentMD5(Utils.getMD5(key))
				.objectLockMode(ObjectLockMode.COMPLIANCE).objectLockRetainUntilDate(base),
				RequestBody.fromString(key)).versionId();
		try {
			var head = client.headObject(h -> h.bucket(bucketName).key(key).versionId(versionId));
			assertEquals(ObjectLockMode.COMPLIANCE, head.objectLockMode());
			assertEquals(base, head.objectLockRetainUntilDate());

			var e = assertThrows(AwsServiceException.class,
					() -> client.putObjectRetention(p -> p.bucket(bucketName).key(key).versionId(versionId)
							.retention(governance).bypassGovernanceRetention(bypassHeader(bypass))));
			assertAccessDenied(e);

			var response = client.getObjectRetention(g -> g.bucket(bucketName).key(key).versionId(versionId));
			retentionCompare(compliance, response.retention());
			assertComplianceProtected(client, bucketName, key, versionId);
		} finally {
			deleteAfterRetentionExpires(client, bucketName, key, versionId, base);
		}
	}

	/**
	 * COMPLIANCE 로 잠긴 버전은 만료 전에는 삭제할 수 없어 버킷 정리가 실패한다. 만료될 때까지 재시도하며 삭제한다.
	 * GOVERNANCE 는 bypass 로 바로 지워진다. 서버와 클라이언트의 시계 차이를 고려해 넉넉히 기다린다.
	 */
	void deleteAfterRetentionExpires(S3Client client, String bucketName, String key, String versionId,
			Instant retainUntil) {
		var deadline = retainUntil.plusSeconds(60);
		while (true) {
			try {
				client.deleteObject(
						d -> d.bucket(bucketName).key(key).versionId(versionId).bypassGovernanceRetention(true));
				return;
			} catch (AwsServiceException e) {
				if (e.statusCode() != HttpStatus.SC_FORBIDDEN || Instant.now().isAfter(deadline))
					throw e;
				delay(1000);
			}
		}
	}
	// endregion
}
