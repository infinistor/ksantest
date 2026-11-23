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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.TimeZone;

import org.apache.hc.core5.http.HttpStatus;
import org.example.Data.MainData;
import org.example.Utility.Utils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.BucketVersioningConfiguration;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.DefaultRetention;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest.KeyVersion;
import com.amazonaws.services.s3.model.DeleteVersionRequest;
import com.amazonaws.services.s3.model.GetObjectLegalHoldRequest;
import com.amazonaws.services.s3.model.GetObjectLockConfigurationRequest;
import com.amazonaws.services.s3.model.GetObjectRetentionRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
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
import com.amazonaws.services.s3.model.UploadPartRequest;
import java.time.Instant;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.MultiObjectDeleteException;
import com.amazonaws.services.s3.model.ObjectLockMode;
import com.amazonaws.services.s3.model.PutObjectRequest;

public class Lock extends TestBase {
	@Test
	@Tag("Put")
	public void testCreatedBucketEnableObjectLock() {
		var client = getClient();
		var bucketName = createBucket(client, 1);

		client.setBucketVersioningConfiguration(new SetBucketVersioningConfigurationRequest(bucketName,
				new BucketVersioningConfiguration(BucketVersioningConfiguration.ENABLED)));

		client.setObjectLockConfiguration(new SetObjectLockConfigurationRequest().withBucketName(bucketName)
				.withObjectLockConfiguration(
						new ObjectLockConfiguration().withObjectLockEnabled(ObjectLockEnabled.ENABLED)));
	}

	@Test
	@Tag("Check")
	public void testObjectLockPutObjLock() {
		var client = getClient();
		var bucketName = getNewBucketName(2);
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
	public void testObjectLockPutObjLockInvalidBucket() {
		var client = getClient();
		var bucketName = getNewBucketName(3);
		client.createBucket(bucketName);

		var conf = new ObjectLockConfiguration().withObjectLockEnabled(ObjectLockEnabled.ENABLED)
				.withRule(new ObjectLockRule().withDefaultRetention(
						new DefaultRetention().withMode(ObjectLockRetentionMode.GOVERNANCE).withYears(1)));

		var e = assertThrows(AmazonServiceException.class, () -> client.setObjectLockConfiguration(
				new SetObjectLockConfigurationRequest().withBucketName(bucketName).withObjectLockConfiguration(conf)));
		assertEquals(HttpStatus.SC_CONFLICT, e.getStatusCode());
		assertEquals(MainData.INVALID_BUCKET_STATE, e.getErrorCode());
	}

	@Test
	@Tag("ERROR")
	public void testObjectLockPutObjLockWithDaysAndYears() {
		var client = getClient();
		var bucketName = getNewBucketName(4);
		client.createBucket(new CreateBucketRequest(bucketName).withObjectLockEnabledForBucket(true));

		var conf = new ObjectLockConfiguration().withObjectLockEnabled(ObjectLockEnabled.ENABLED)
				.withRule(new ObjectLockRule().withDefaultRetention(
						new DefaultRetention().withMode(ObjectLockRetentionMode.GOVERNANCE).withYears(1).withDays(1)));
		var e = assertThrows(AmazonServiceException.class, () -> client.setObjectLockConfiguration(
				new SetObjectLockConfigurationRequest().withBucketName(bucketName).withObjectLockConfiguration(conf)));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.getStatusCode());
		assertEquals(MainData.MALFORMED_XML, e.getErrorCode());
	}

	@Test
	@Tag("ERROR")
	public void testObjectLockPutObjLockInvalidDays() {
		var client = getClient();
		var bucketName = getNewBucketName(5);
		client.createBucket(new CreateBucketRequest(bucketName).withObjectLockEnabledForBucket(true));

		var conf = new ObjectLockConfiguration().withObjectLockEnabled(ObjectLockEnabled.ENABLED)
				.withRule(new ObjectLockRule().withDefaultRetention(
						new DefaultRetention().withMode(ObjectLockRetentionMode.GOVERNANCE).withDays(0)));

		var e = assertThrows(AmazonServiceException.class, () -> client.setObjectLockConfiguration(
				new SetObjectLockConfigurationRequest().withBucketName(bucketName).withObjectLockConfiguration(conf)));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.getStatusCode());
		assertEquals(MainData.INVALID_ARGUMENT, e.getErrorCode());
	}

	@Test
	@Tag("ERROR")
	public void testObjectLockPutObjLockInvalidYears() {
		var client = getClient();
		var bucketName = getNewBucketName(6);
		client.createBucket(new CreateBucketRequest(bucketName).withObjectLockEnabledForBucket(true));

		var conf = new ObjectLockConfiguration().withObjectLockEnabled(ObjectLockEnabled.ENABLED)
				.withRule(new ObjectLockRule().withDefaultRetention(
						new DefaultRetention().withMode(ObjectLockRetentionMode.GOVERNANCE).withYears(-1)));

		var e = assertThrows(AmazonServiceException.class, () -> client.setObjectLockConfiguration(
				new SetObjectLockConfigurationRequest().withBucketName(bucketName).withObjectLockConfiguration(conf)));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.getStatusCode());
		assertEquals(MainData.INVALID_ARGUMENT, e.getErrorCode());
	}

	@Test
	@Tag("ERROR")
	public void testObjectLockPutObjLockInvalidMode() {
		var client = getClient();
		var bucketName = getNewBucketName(7);
		client.createBucket(new CreateBucketRequest(bucketName).withObjectLockEnabledForBucket(true));

		var conf = new ObjectLockConfiguration().withObjectLockEnabled(ObjectLockEnabled.ENABLED).withRule(
				new ObjectLockRule().withDefaultRetention(new DefaultRetention().withMode("invalid").withYears(1)));

		var e = assertThrows(AmazonServiceException.class, () -> client.setObjectLockConfiguration(
				new SetObjectLockConfigurationRequest().withBucketName(bucketName).withObjectLockConfiguration(conf)));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.getStatusCode());
		assertEquals(MainData.MALFORMED_XML, e.getErrorCode());
	}

	@Test
	@Tag("ERROR")
	public void testObjectLockPutObjLockInvalidStatus() {
		var client = getClient();
		var bucketName = getNewBucketName(8);
		client.createBucket(new CreateBucketRequest(bucketName).withObjectLockEnabledForBucket(true));

		var conf = new ObjectLockConfiguration().withObjectLockEnabled("Disabled")
				.withRule(new ObjectLockRule().withDefaultRetention(
						new DefaultRetention().withMode(ObjectLockRetentionMode.GOVERNANCE).withYears(1)));

		var e = assertThrows(AmazonServiceException.class, () -> client.setObjectLockConfiguration(
				new SetObjectLockConfigurationRequest().withBucketName(bucketName).withObjectLockConfiguration(conf)));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.getStatusCode());
		assertEquals(MainData.MALFORMED_XML, e.getErrorCode());
	}

	@Test
	@Tag("Version")
	public void testObjectLockSuspendVersioning() {
		var client = getClient();
		var bucketName = getNewBucketName(9);
		client.createBucket(new CreateBucketRequest(bucketName).withObjectLockEnabledForBucket(true));

		var e = assertThrows(AmazonServiceException.class,
				() -> client.setBucketVersioningConfiguration(new SetBucketVersioningConfigurationRequest(bucketName,
						new BucketVersioningConfiguration(BucketVersioningConfiguration.SUSPENDED))));
		assertEquals(HttpStatus.SC_CONFLICT, e.getStatusCode());
		assertEquals(MainData.INVALID_BUCKET_STATE, e.getErrorCode());
	}

	@Test
	@Tag("Check")
	public void testObjectLockGetObjLock() {
		var client = getClient();
		var bucketName = getNewBucketName(10);
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
	@Tag("Check")
	public void testObjectLockPutObject() {
		var client = getClient();
		var bucketName = getNewBucketName(11);
		var key = "testObjectLockPutObject";

		client.createBucket(new CreateBucketRequest(bucketName).withObjectLockEnabledForBucket(true));

		var conf = new ObjectLockConfiguration().withObjectLockEnabled(ObjectLockEnabled.ENABLED)
				.withRule(new ObjectLockRule().withDefaultRetention(
						new DefaultRetention().withMode(ObjectLockRetentionMode.GOVERNANCE).withDays(1)));
		client.setObjectLockConfiguration(
				new SetObjectLockConfigurationRequest().withBucketName(bucketName).withObjectLockConfiguration(conf));

		// Put Object
		var metadata = new ObjectMetadata();
		metadata.setContentMD5(Utils.getMD5(key));
		metadata.setContentType("text/plain");
		metadata.setContentLength(key.length());

		client.putObject(bucketName, key, createBody(key), metadata);

		var response = client.getObjectMetadata(bucketName, key);
		assertEquals(ObjectLockRetentionMode.GOVERNANCE.toString(), response.getObjectLockMode());
		assertNotNull(response.getObjectLockRetainUntilDate());

		var e = assertThrows(AmazonServiceException.class,
				() -> client.deleteVersion(bucketName, key, response.getVersionId()));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.getStatusCode());
		assertEquals(MainData.ACCESS_DENIED, e.getErrorCode());

		client.deleteVersion(new DeleteVersionRequest(bucketName, key, response.getVersionId())
				.withBypassGovernanceRetention(true));
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

		client.createBucket(new CreateBucketRequest(bucketName).withObjectLockEnabledForBucket(true));
		client.createBucket(new CreateBucketRequest(bucketName2).withObjectLockEnabledForBucket(true));

		var conf = new ObjectLockConfiguration().withObjectLockEnabled(ObjectLockEnabled.ENABLED)
				.withRule(new ObjectLockRule().withDefaultRetention(
						new DefaultRetention().withMode(ObjectLockRetentionMode.GOVERNANCE).withDays(1)));
		client.setObjectLockConfiguration(
				new SetObjectLockConfigurationRequest().withBucketName(bucketName).withObjectLockConfiguration(conf));

		// Put Object
		var metadata = new ObjectMetadata();
		metadata.setContentMD5(Utils.getMD5(key));
		metadata.setContentType("text/plain");
		metadata.setContentLength(key.length());

		client.putObject(bucketName, key, createBody(key), metadata);
		client.putObject(bucketName2, key2, key2);

		client.copyObject(bucketName, key, bucketName2, keyCopy);
		client.copyObject(bucketName2, key2, bucketName, key2Copy);

		var response = client.getObjectMetadata(bucketName, key);
		assertEquals(ObjectLockRetentionMode.GOVERNANCE.toString(), response.getObjectLockMode());
		assertNotNull(response.getObjectLockRetainUntilDate());

		var response2 = client.getObjectMetadata(bucketName2, keyCopy);
		assertNull(response2.getObjectLockMode());
		assertNull(response2.getObjectLockRetainUntilDate());

		var response3 = client.getObjectMetadata(bucketName2, key2);
		assertNull(response3.getObjectLockMode());
		assertNull(response3.getObjectLockRetainUntilDate());

		var response4 = client.getObjectMetadata(bucketName, key2Copy);
		assertEquals(ObjectLockRetentionMode.GOVERNANCE.toString(), response4.getObjectLockMode());
		assertNotNull(response4.getObjectLockRetainUntilDate());

		var e = assertThrows(AmazonServiceException.class,
				() -> client.deleteVersion(bucketName, key, response.getVersionId()));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.getStatusCode());
		assertEquals(MainData.ACCESS_DENIED, e.getErrorCode());

		var e2 = assertThrows(AmazonServiceException.class,
				() -> client.deleteVersion(bucketName, key2Copy, response4.getVersionId()));
		assertEquals(HttpStatus.SC_FORBIDDEN, e2.getStatusCode());
		assertEquals(MainData.ACCESS_DENIED, e2.getErrorCode());

		client.deleteVersion(new DeleteVersionRequest(bucketName, key, response.getVersionId())
				.withBypassGovernanceRetention(true));
		client.deleteVersion(new DeleteVersionRequest(bucketName2, keyCopy, response2.getVersionId())
				.withBypassGovernanceRetention(true));
		client.deleteVersion(new DeleteVersionRequest(bucketName2, key2, response3.getVersionId())
				.withBypassGovernanceRetention(true));
		client.deleteVersion(new DeleteVersionRequest(bucketName, key2Copy, response4.getVersionId())
				.withBypassGovernanceRetention(true));
	}

	@Test
	@Tag("Check")
	public void testObjectLockMultipart() {

		var client = getClient();
		var bucketName = getNewBucketName(13);
		var key = "testObjectLockMultipart";

		client.createBucket(new CreateBucketRequest(bucketName).withObjectLockEnabledForBucket(true));

		var conf = new ObjectLockConfiguration().withObjectLockEnabled(ObjectLockEnabled.ENABLED)
				.withRule(new ObjectLockRule().withDefaultRetention(
						new DefaultRetention().withMode(ObjectLockRetentionMode.GOVERNANCE).withDays(1)));
		client.setObjectLockConfiguration(
				new SetObjectLockConfigurationRequest().withBucketName(bucketName).withObjectLockConfiguration(conf));

		// Multipart Upload
		var uploadData = setupMultipartUploadLock(client, bucketName, key, 1 * MainData.MB);
		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(bucketName, key, uploadData.uploadId, uploadData.parts));

		var response = client.getObjectMetadata(bucketName, key);
		assertEquals(ObjectLockRetentionMode.GOVERNANCE.toString(), response.getObjectLockMode());
		assertNotNull(response.getObjectLockRetainUntilDate());

		var e = assertThrows(AmazonServiceException.class,
				() -> client.deleteVersion(bucketName, key, response.getVersionId()));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.getStatusCode());
		assertEquals(MainData.ACCESS_DENIED, e.getErrorCode());

		client.deleteVersion(new DeleteVersionRequest(bucketName, key, response.getVersionId())
				.withBypassGovernanceRetention(true));
	}

	@Test
	@Tag("ERROR")
	public void testObjectLockMD5() {
		var client = getClient();
		var bucketName = getNewBucketName(14);
		var key = "testObjectLockMD5";
		var content = Utils.randomTextToLong(1 * MainData.MB);

		client.createBucket(new CreateBucketRequest(bucketName).withObjectLockEnabledForBucket(true));

		var conf = new ObjectLockConfiguration().withObjectLockEnabled(ObjectLockEnabled.ENABLED)
				.withRule(new ObjectLockRule().withDefaultRetention(
						new DefaultRetention().withMode(ObjectLockRetentionMode.GOVERNANCE).withDays(1)));
		client.setObjectLockConfiguration(
				new SetObjectLockConfigurationRequest().withBucketName(bucketName).withObjectLockConfiguration(conf));

		var e = assertThrows(AmazonServiceException.class, () -> client.putObject(bucketName, key, key));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.getStatusCode());
		assertEquals(MainData.INVALID_REQUEST, e.getErrorCode());

		var initResponse = client.initiateMultipartUpload(new InitiateMultipartUploadRequest(bucketName, key));
		var uploadId = initResponse.getUploadId();

		var e2 = assertThrows(AmazonServiceException.class,
				() -> client.uploadPart(new UploadPartRequest().withBucketName(bucketName).withKey(key)
						.withUploadId(uploadId).withInputStream(createBody(content)).withPartNumber(1)
						.withPartSize(content.length())));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e2.getStatusCode());
		assertEquals(MainData.INVALID_REQUEST, e2.getErrorCode());

		client.abortMultipartUpload(new AbortMultipartUploadRequest(bucketName, key, uploadId));
	}

	@Test
	@Tag("ERROR")
	public void testObjectLockGetObjLockInvalidBucket() {
		var client = getClient();
		var bucketName = getNewBucketName(15);
		client.createBucket(bucketName);

		var e = assertThrows(AmazonServiceException.class, () -> client
				.getObjectLockConfiguration(new GetObjectLockConfigurationRequest().withBucketName(bucketName)));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.getStatusCode());
		assertEquals(MainData.OBJECT_LOCK_CONFIGURATION_NOT_FOUND_ERROR, e.getErrorCode());
	}

	@Test
	@Tag("retention")
	public void testObjectLockPutObjRetention() {
		var key = "testObjectLockPutObjRetention";
		var client = getClient();
		var bucketName = getNewBucketName(16);

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
	public void testObjectLockPutObjRetentionInvalidBucket() {
		var key = "testObjectLockPutObjRetentionInvalidBucket";
		var client = getClient();
		var bucketName = createBucket(client, 17);

		client.putObject(bucketName, key, key);

		var retention = new ObjectLockRetention().withMode(ObjectLockRetentionMode.GOVERNANCE)
				.withRetainUntilDate(new Calendar.Builder().setDate(2030, 1, 1)
						.setTimeZone(TimeZone.getTimeZone(("GMT"))).build().getTime());

		var e = assertThrows(AmazonServiceException.class, () -> client.setObjectRetention(
				new SetObjectRetentionRequest().withBucketName(bucketName).withKey(key).withRetention(retention)));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.getStatusCode());
		assertEquals(MainData.INVALID_REQUEST, e.getErrorCode());
	}

	@Test
	@Tag("retention")
	public void testObjectLockPutObjRetentionInvalidMode() {
		var key = "testObjectLockPutObjRetentionInvalidMode";
		var client = getClient();
		var bucketName = getNewBucketName(18);

		client.createBucket(new CreateBucketRequest(bucketName).withObjectLockEnabledForBucket(true));

		client.putObject(bucketName, key, key);

		var retention = new ObjectLockRetention().withMode("invalid").withRetainUntilDate(new Calendar.Builder()
				.setDate(2030, 1, 1).setTimeZone(TimeZone.getTimeZone(("GMT"))).build().getTime());

		var e = assertThrows(AmazonServiceException.class, () -> client.setObjectRetention(
				new SetObjectRetentionRequest().withBucketName(bucketName).withKey(key).withRetention(retention)));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.getStatusCode());
		assertEquals(MainData.MALFORMED_XML, e.getErrorCode());
	}

	@Test
	@Tag("retention")
	public void testObjectLockGetObjRetention() {
		var key = "testObjectLockGetObjRetention";
		var client = getClient();
		var bucketName = getNewBucketName(19);

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
	public void testObjectLockGetObjRetentionInvalidBucket() {
		var key = "testObjectLockGetObjRetentionInvalidBucket";
		var client = getClient();
		var bucketName = createBucket(client, 20);

		client.putObject(bucketName, key, key);

		var e = assertThrows(AmazonServiceException.class, () -> client
				.getObjectRetention(new GetObjectRetentionRequest().withBucketName(bucketName).withKey(key)));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.getStatusCode());
		assertEquals(MainData.INVALID_REQUEST, e.getErrorCode());
	}

	@Test
	@Tag("retention")
	public void testObjectLockPutObjRetentionVersionid() {
		var key = "testObjectLockPutObjRetentionVersionid";
		var client = getClient();
		var bucketName = getNewBucketName(21);

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
	public void testObjectLockPutObjRetentionOverrideDefaultRetention() {
		var key = "testObjectLockPutObjRetentionOverrideDefaultRetention";
		var client = getClient();
		var bucketName = getNewBucketName(22);

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

		var response = client
				.getObjectRetention(new GetObjectRetentionRequest().withBucketName(bucketName).withKey(key));
		assertEquals(ObjectLockRetentionMode.GOVERNANCE.toString(), response.getRetention().getMode());
		assertNotNull(response.getRetention().getRetainUntilDate());

		var retention = new ObjectLockRetention().withMode(ObjectLockRetentionMode.GOVERNANCE)
				.withRetainUntilDate(new Calendar.Builder().setDate(2030, 1, 1)
						.setTimeZone(TimeZone.getTimeZone(("GMT"))).build().getTime());

		client.setObjectRetention(
				new SetObjectRetentionRequest().withBucketName(bucketName).withKey(key).withRetention(retention));
		response = client
				.getObjectRetention(new GetObjectRetentionRequest().withBucketName(bucketName).withKey(key));
		retentionCompare(retention, response.getRetention());

		client.deleteVersion(new DeleteVersionRequest(bucketName, key, versionId).withBypassGovernanceRetention(true));
	}

	@Test
	@Tag("Overwrite")
	public void testObjectLockPutObjRetentionIncreasePeriod() {
		var key = "testObjectLockPutObjRetentionIncreasePeriod";
		var client = getClient();
		var bucketName = getNewBucketName(23);

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
	public void testObjectLockPutObjRetentionShortenPeriod() {
		var key = "testObjectLockPutObjRetentionShortenPeriod";
		var client = getClient();
		var bucketName = getNewBucketName(24);

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
		assertEquals(HttpStatus.SC_FORBIDDEN, e.getStatusCode());
		assertEquals(MainData.ACCESS_DENIED, e.getErrorCode());

		client.deleteVersion(new DeleteVersionRequest(bucketName, key, versionId).withBypassGovernanceRetention(true));
	}

	@Test
	@Tag("Overwrite")
	public void testObjectLockPutObjRetentionShortenPeriodBypass() {
		var key = "testObjectLockPutObjRetentionShortenPeriodBypass";
		var client = getClient();
		var bucketName = getNewBucketName(25);

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
	public void testObjectLockDeleteObjectWithRetention() {
		var key = "testObjectLockDeleteObjectWithRetention";
		var client = getClient();
		var bucketName = getNewBucketName(26);

		client.createBucket(new CreateBucketRequest(bucketName).withObjectLockEnabledForBucket(true));

		var putResponse = client.putObject(bucketName, key, key);
		var versionId = putResponse.getVersionId();

		var retention = new ObjectLockRetention().withMode(ObjectLockRetentionMode.GOVERNANCE)
				.withRetainUntilDate(new Calendar.Builder().setDate(2030, 1, 1)
						.setTimeZone(TimeZone.getTimeZone(("GMT"))).build().getTime());
		client.setObjectRetention(
				new SetObjectRetentionRequest().withBucketName(bucketName).withKey(key).withRetention(retention));

		var e = assertThrows(AmazonServiceException.class, () -> client.deleteVersion(bucketName, key, versionId));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.getStatusCode());
		assertEquals(MainData.ACCESS_DENIED, e.getErrorCode());

		client.deleteVersion(new DeleteVersionRequest(bucketName, key, versionId).withBypassGovernanceRetention(true));
	}

	@Test
	@Tag("Retention")
	public void testObjectLockDeleteObjectWithRetentionBypass() {
		var key = "testObjectLockDeleteObjectWithRetentionBypass";
		var client = getClient();
		var bucketName = getNewBucketName(27);

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
	@Tag("Retention")
	public void testObjectLockDeleteObjectsWithRetentionBypass() {
		var client = getClient();
		var bucketName = getNewBucketName(28);
		var keyVersions = new ArrayList<KeyVersion>();

		var retention = new ObjectLockRetention().withMode(ObjectLockRetentionMode.GOVERNANCE)
				.withRetainUntilDate(new Calendar.Builder().setDate(2030, 1, 1)
						.setTimeZone(TimeZone.getTimeZone(("GMT"))).build().getTime());

		client.createBucket(new CreateBucketRequest(bucketName).withObjectLockEnabledForBucket(true));
		for (int i = 0; i < 10; i++) {
			var key = String.format("testObjectLockDeleteObjectsWithRetentionBypass-%03d", i);
			var putResponse = client.putObject(bucketName, key, key);
			var versionId = putResponse.getVersionId();
			keyVersions.add(new KeyVersion(key, versionId));
			client.setObjectRetention(new SetObjectRetentionRequest()
					.withBucketName(bucketName)
					.withKey(key)
					.withVersionId(versionId)
					.withRetention(retention));
		}

		client.deleteObjects(new DeleteObjectsRequest(bucketName).withKeys(keyVersions)
				.withBypassGovernanceRetention(true));
	}

	@Test
	@Tag("LegalHold")
	public void testObjectLockPutLegalHold() {
		var key = "testObjectLockPutLegalHold";
		var client = getClient();
		var bucketName = getNewBucketName(29);

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
	public void testObjectLockPutLegalHoldInvalidBucket() {
		var key = "testObjectLockPutLegalHoldInvalidBucket";
		var client = getClient();
		var bucketName = createBucket(client, 30);

		client.putObject(bucketName, key, key);

		var e = assertThrows(AmazonServiceException.class, () -> client.setObjectLegalHold(
				new SetObjectLegalHoldRequest().withBucketName(bucketName).withKey(key)
						.withLegalHold(new ObjectLockLegalHold().withStatus(ObjectLockLegalHoldStatus.ON))));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.getStatusCode());
		assertEquals(MainData.INVALID_REQUEST, e.getErrorCode());
	}

	@Test
	@Tag("LegalHold")
	public void testObjectLockPutLegalHoldInvalidStatus() {
		var key = "testObjectLockPutLegalHoldInvalidStatus";
		var client = getClient();
		var bucketName = getNewBucketName(31);

		client.createBucket(new CreateBucketRequest(bucketName).withObjectLockEnabledForBucket(true));

		client.putObject(bucketName, key, key);

		var e = assertThrows(AmazonServiceException.class, () -> client.setObjectLegalHold(
				new SetObjectLegalHoldRequest().withBucketName(bucketName).withKey(key)
						.withLegalHold(new ObjectLockLegalHold().withStatus("abc"))));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.getStatusCode());
		assertEquals(MainData.MALFORMED_XML, e.getErrorCode());
	}

	@Test
	@Tag("LegalHold")
	public void testObjectLockGetLegalHold() {
		var key = "testObjectLockGetLegalHold";
		var client = getClient();
		var bucketName = getNewBucketName(32);

		client.createBucket(new CreateBucketRequest(bucketName).withObjectLockEnabledForBucket(true));

		var putResponse = client.putObject(bucketName, key, key);

		client.setObjectLegalHold(
				new SetObjectLegalHoldRequest().withBucketName(bucketName).withKey(key)
						.withLegalHold(new ObjectLockLegalHold().withStatus(ObjectLockLegalHoldStatus.ON)));
		var response = client
				.getObjectLegalHold(new GetObjectLegalHoldRequest().withBucketName(bucketName).withKey(key));
		assertEquals(ObjectLockLegalHoldStatus.ON.toString(), response.getLegalHold().getStatus());

		client.setObjectLegalHold(
				new SetObjectLegalHoldRequest().withBucketName(bucketName).withKey(key)
						.withLegalHold(new ObjectLockLegalHold().withStatus(ObjectLockLegalHoldStatus.OFF)));
		response = client.getObjectLegalHold(new GetObjectLegalHoldRequest().withBucketName(bucketName).withKey(key));
		assertEquals(ObjectLockLegalHoldStatus.OFF.toString(), response.getLegalHold().getStatus());

		client.deleteVersion(new DeleteVersionRequest(bucketName, key, putResponse.getVersionId()));
	}

	@Test
	@Tag("LegalHold")
	public void testObjectLockGetLegalHoldInvalidBucket() {
		var key = "testObjectLockGetLegalHoldInvalidBucket";
		var client = getClient();
		var bucketName = createBucket(client, 33);

		client.putObject(bucketName, key, key);

		var e = assertThrows(AmazonServiceException.class, () -> client
				.getObjectLegalHold(new GetObjectLegalHoldRequest().withBucketName(bucketName).withKey(key)));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.getStatusCode());
		assertEquals(MainData.INVALID_REQUEST, e.getErrorCode());
	}

	@Test
	@Tag("LegalHold")
	public void testObjectLockDeleteObjectWithLegalHoldOn() {
		var client = getClient();
		var bucketName = getNewBucketName(34);
		client.createBucket(new CreateBucketRequest(bucketName).withObjectLockEnabledForBucket(true));

		var key = "testObjectLockDeleteObjectWithLegalHoldOn";
		var response = client.putObject(bucketName, key, key);

		client.setObjectLegalHold(
				new SetObjectLegalHoldRequest().withBucketName(bucketName).withKey(key)
						.withLegalHold(new ObjectLockLegalHold().withStatus(ObjectLockLegalHoldStatus.ON)));

		var e = assertThrows(AmazonServiceException.class,
				() -> client.deleteVersion(bucketName, key, response.getVersionId()));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.getStatusCode());
		assertEquals(MainData.ACCESS_DENIED, e.getErrorCode());

		client.setObjectLegalHold(
				new SetObjectLegalHoldRequest().withBucketName(bucketName).withKey(key)
						.withLegalHold(new ObjectLockLegalHold().withStatus(ObjectLockLegalHoldStatus.OFF)));
		client.deleteVersion(new DeleteVersionRequest(bucketName, key, response.getVersionId()));
	}

	@Test
	@Tag("LegalHold")
	public void testObjectLockDeleteObjectWithLegalHoldOff() {
		var key = "testObjectLockDeleteObjectWithLegalHoldOff";
		var client = getClient();
		var bucketName = getNewBucketName(35);

		client.createBucket(new CreateBucketRequest(bucketName).withObjectLockEnabledForBucket(true));

		var response = client.putObject(bucketName, key, key);

		client.setObjectLegalHold(
				new SetObjectLegalHoldRequest().withBucketName(bucketName).withKey(key)
						.withLegalHold(new ObjectLockLegalHold().withStatus(ObjectLockLegalHoldStatus.OFF)));

		client.deleteVersion(bucketName, key, response.getVersionId());
	}

	@Test
	@Tag("LegalHold")
	public void testObjectLockGetObjMetadata() {
		var key = "testObjectLockGetObjMetadata";
		var client = getClient();
		var bucketName = getNewBucketName(36);

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
		assertEquals(ObjectLockLegalHoldStatus.ON.toString(), response.getObjectLockLegalHoldStatus());

		client.setObjectLegalHold(
				new SetObjectLegalHoldRequest().withBucketName(bucketName).withKey(key)
						.withLegalHold(new ObjectLockLegalHold().withStatus(ObjectLockLegalHoldStatus.OFF)));
		client.deleteVersion(new DeleteVersionRequest(bucketName, key, putResponse.getVersionId())
				.withBypassGovernanceRetention(true));
	}

	// region 오브젝트 잠금 API 검증
	static final Date RETENTION_DATE = Date.from(Instant.parse("2030-02-01T00:00:00Z"));
	static final Date PAST_DATE = Date.from(Instant.parse("2020-01-01T00:00:00Z"));

	String createBucketObjectLock(AmazonS3 client, int testId) {
		var bucketName = getNewBucketName(testId);
		client.createBucket(new CreateBucketRequest(bucketName).withObjectLockEnabledForBucket(true));
		return bucketName;
	}

	/** 기본 보존 규칙이 있는 잠금 설정. null 인 항목은 보내지 않는다. */
	static ObjectLockConfiguration lockRule(ObjectLockRetentionMode mode, Integer days, Integer years) {
		var retention = new DefaultRetention();
		if (mode != null)
			retention.setMode(mode);
		retention.setDays(days);
		retention.setYears(years);
		return new ObjectLockConfiguration().withObjectLockEnabled(ObjectLockEnabled.ENABLED)
				.withRule(new ObjectLockRule().withDefaultRetention(retention));
	}

	static PutObjectRequest putContentRequest(String bucketName, String key, String content) {
		var metadata = new ObjectMetadata();
		metadata.setContentLength(content.length());
		return new PutObjectRequest(bucketName, key, createBody(content), metadata);
	}

	@Test
	@Tag("retention")
	public void testObjectLockGetNotConfigured() {
		var key = "testObjectLockGetNotConfigured";
		var client = getClient();
		var bucketName = createBucketObjectLock(client, 37);
		client.putObject(bucketName, key, "content");

		// 보존 기한·법적 보존을 설정한 적이 없는 오브젝트
		assertS3Error(HttpStatus.SC_NOT_FOUND, MainData.NO_SUCH_OBJECT_LOCK_CONFIGURATION, () -> client
				.getObjectRetention(new GetObjectRetentionRequest().withBucketName(bucketName).withKey(key)));
		assertS3Error(HttpStatus.SC_NOT_FOUND, MainData.NO_SUCH_OBJECT_LOCK_CONFIGURATION, () -> client
				.getObjectLegalHold(new GetObjectLegalHoldRequest().withBucketName(bucketName).withKey(key)));
	}

	@Test
	@Tag("retention")
	public void testObjectLockPutObjRetentionPastDate() {
		var key = "testObjectLockPutObjRetentionPastDate";
		var client = getClient();
		var bucketName = createBucketObjectLock(client, 38);
		client.putObject(bucketName, key, "content");

		var retention = new ObjectLockRetention().withMode(ObjectLockRetentionMode.GOVERNANCE)
				.withRetainUntilDate(PAST_DATE);
		assertS3Error(HttpStatus.SC_BAD_REQUEST, MainData.INVALID_ARGUMENT, () -> client.setObjectRetention(
				new SetObjectRetentionRequest().withBucketName(bucketName).withKey(key).withRetention(retention)));
	}

	@Test
	@Tag("ERROR")
	public void testObjectLockKeyNotExist() {
		var key = "testObjectLockKeyNotExist";
		var client = getClient();
		var bucketName = createBucketObjectLock(client, 39);

		var retention = new ObjectLockRetention().withMode(ObjectLockRetentionMode.GOVERNANCE)
				.withRetainUntilDate(RETENTION_DATE);
		assertS3Error(HttpStatus.SC_NOT_FOUND, MainData.NO_SUCH_KEY, () -> client.setObjectRetention(
				new SetObjectRetentionRequest().withBucketName(bucketName).withKey(key).withRetention(retention)));
		assertS3Error(HttpStatus.SC_NOT_FOUND, MainData.NO_SUCH_KEY, () -> client
				.getObjectRetention(new GetObjectRetentionRequest().withBucketName(bucketName).withKey(key)));
		assertS3Error(HttpStatus.SC_NOT_FOUND, MainData.NO_SUCH_KEY,
				() -> client.setObjectLegalHold(new SetObjectLegalHoldRequest().withBucketName(bucketName).withKey(key)
						.withLegalHold(new ObjectLockLegalHold().withStatus(ObjectLockLegalHoldStatus.ON))));
		assertS3Error(HttpStatus.SC_NOT_FOUND, MainData.NO_SUCH_KEY, () -> client
				.getObjectLegalHold(new GetObjectLegalHoldRequest().withBucketName(bucketName).withKey(key)));
	}

	@Test
	@Tag("ERROR")
	public void testObjectLockPutObjectInvalidHeaders() {
		var key = "testObjectLockPutObjectInvalidHeaders";
		var client = getClient();
		var bucketName = createBucketObjectLock(client, 40);

		var invalidHeaders = List.<UnaryOperator<PutObjectRequest>>of(
				p -> p.withObjectLockMode(ObjectLockMode.GOVERNANCE), // 모드만
				p -> p.withObjectLockRetainUntilDate(RETENTION_DATE), // 기한만
				p -> p.withObjectLockMode(ObjectLockMode.GOVERNANCE).withObjectLockRetainUntilDate(PAST_DATE), // 지난 기한
				p -> p.withObjectLockMode("INVALID").withObjectLockRetainUntilDate(RETENTION_DATE),
				p -> p.withObjectLockLegalHoldStatus("INVALID"));
		for (var headers : invalidHeaders)
			assertS3Error(HttpStatus.SC_BAD_REQUEST, MainData.INVALID_ARGUMENT,
					() -> client.putObject(headers.apply(putContentRequest(bucketName, key, "content"))));
		assertS3Error(HttpStatus.SC_NOT_FOUND, null, () -> client.getObjectMetadata(bucketName, key));
	}

	@Test
	@Tag("ERROR")
	public void testObjectLockPutObjectInvalidBucket() {
		var key = "testObjectLockPutObjectInvalidBucket";
		var client = getClient();
		var bucketName = createBucket(client, 41);

		// Object Lock 을 켜지 않은 버킷에는 잠금 헤더를 쓸 수 없다
		var lockHeaders = List.<UnaryOperator<PutObjectRequest>>of(
				p -> p.withObjectLockMode(ObjectLockMode.GOVERNANCE).withObjectLockRetainUntilDate(RETENTION_DATE),
				p -> p.withObjectLockLegalHoldStatus(ObjectLockLegalHoldStatus.ON));
		for (var headers : lockHeaders)
			assertS3Error(HttpStatus.SC_BAD_REQUEST, MainData.INVALID_REQUEST,
					() -> client.putObject(headers.apply(putContentRequest(bucketName, key, "content"))));
	}

	@Test
	@Tag("legalhold")
	public void testObjectLockLegalHoldOnPutObject() {
		var key = "testObjectLockLegalHoldOnPutObject";
		var content = "content";
		var client = getClient();
		var bucketName = createBucketObjectLock(client, 42);

		// 잠금 헤더가 있는 PutObject 는 Content-MD5 나 체크섬이 있어야 한다
		var request = putContentRequest(bucketName, key, content)
				.withObjectLockLegalHoldStatus(ObjectLockLegalHoldStatus.ON);
		request.getMetadata().setContentMD5(Utils.getMD5(content));
		var versionId = client.putObject(request).getVersionId();
		try {
			var head = client.getObjectMetadata(bucketName, key);
			assertEquals(ObjectLockLegalHoldStatus.ON.toString(), head.getObjectLockLegalHoldStatus());
			assertNull(head.getObjectLockMode());

			// 버전을 지정하지 않은 삭제(삭제 마커)와 덮어쓰기는 막지 않는다
			client.deleteObject(bucketName, key);
			assertTrue(client.listVersions(bucketName, key).getVersionSummaries().get(0).isDeleteMarker());
			assertNotEquals(versionId, client.putObject(bucketName, key, "new").getVersionId());

			// 법적 보존 중인 버전은 bypass 를 줘도 지울 수 없다
			assertS3Error(HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED, () -> client.deleteVersion(
					new DeleteVersionRequest(bucketName, key, versionId).withBypassGovernanceRetention(true)));
			// SDK V1 은 키별 오류가 있으면 MultiObjectDeleteException 을 던진다
			var e = assertThrows(MultiObjectDeleteException.class, () -> client.deleteObjects(
					new DeleteObjectsRequest(bucketName).withKeys(List.of(new KeyVersion(key, versionId)))));
			assertEquals(0, e.getDeletedObjects().size());
			assertEquals(MainData.ACCESS_DENIED, e.getErrors().get(0).getCode());
			assertEquals(content,
					getBody(client.getObject(new GetObjectRequest(bucketName, key, versionId)).getObjectContent()));
		} finally {
			// 법적 보존을 풀어야 버킷을 정리할 수 있다
			client.setObjectLegalHold(new SetObjectLegalHoldRequest().withBucketName(bucketName).withKey(key)
					.withVersionId(versionId)
					.withLegalHold(new ObjectLockLegalHold().withStatus(ObjectLockLegalHoldStatus.OFF)));
		}
	}

	@Test
	@Tag("ERROR")
	public void testObjectLockDeleteMarker() {
		var key = "testObjectLockDeleteMarker";
		var client = getClient();
		var bucketName = createBucketObjectLock(client, 43);
		client.putObject(bucketName, key, "content");
		client.deleteObject(bucketName, key);
		var markerId = client.listVersions(bucketName, key).getVersionSummaries().stream()
				.filter(v -> v.isDeleteMarker()).findFirst().orElseThrow().getVersionId();
		var retention = new ObjectLockRetention().withMode(ObjectLockRetentionMode.GOVERNANCE)
				.withRetainUntilDate(RETENTION_DATE);

		// 삭제 마커에는 보존 기한·법적 보존을 걸 수 없다(버전을 지정하든 않든. versionId 가 null 이면 지정하지 않은 요청)
		for (var target : Arrays.asList(markerId, null)) {
			assertS3Error(HttpStatus.SC_METHOD_NOT_ALLOWED, MainData.METHOD_NOT_ALLOWED,
					() -> client.setObjectRetention(new SetObjectRetentionRequest().withBucketName(bucketName)
							.withKey(key).withVersionId(target).withRetention(retention)));
			assertS3Error(HttpStatus.SC_METHOD_NOT_ALLOWED, MainData.METHOD_NOT_ALLOWED,
					() -> client.getObjectRetention(new GetObjectRetentionRequest().withBucketName(bucketName)
							.withKey(key).withVersionId(target)));
			assertS3Error(HttpStatus.SC_METHOD_NOT_ALLOWED, MainData.METHOD_NOT_ALLOWED,
					() -> client.setObjectLegalHold(new SetObjectLegalHoldRequest().withBucketName(bucketName)
							.withKey(key).withVersionId(target)
							.withLegalHold(new ObjectLockLegalHold().withStatus(ObjectLockLegalHoldStatus.ON))));
		}
	}

	@Test
	@Tag("ERROR")
	public void testObjectLockPutObjLockInvalidRule() {
		var client = getClient();
		var bucketName = createBucketObjectLock(client, 44);
		Consumer<ObjectLockConfiguration> put = conf -> client.setObjectLockConfiguration(
				new SetObjectLockConfigurationRequest().withBucketName(bucketName).withObjectLockConfiguration(conf));

		// ObjectLockEnabled 누락, 모드 누락
		var noEnabled = new ObjectLockConfiguration().withRule(new ObjectLockRule().withDefaultRetention(
				new DefaultRetention().withMode(ObjectLockRetentionMode.GOVERNANCE).withDays(1)));
		assertS3Error(HttpStatus.SC_BAD_REQUEST, MainData.MALFORMED_XML, () -> put.accept(noEnabled));
		assertS3Error(HttpStatus.SC_BAD_REQUEST, MainData.MALFORMED_XML,
				() -> put.accept(lockRule(null, 1, null)));
		// 기간 누락
		assertS3Error(HttpStatus.SC_BAD_REQUEST, MainData.INVALID_REQUEST,
				() -> put.accept(lockRule(ObjectLockRetentionMode.GOVERNANCE, null, null)));
		// 기본 보존 기간은 36500일(100년)까지
		assertS3Error(HttpStatus.SC_BAD_REQUEST, MainData.INVALID_ARGUMENT,
				() -> put.accept(lockRule(ObjectLockRetentionMode.GOVERNANCE, 36501, null)));
		assertS3Error(HttpStatus.SC_BAD_REQUEST, MainData.INVALID_ARGUMENT,
				() -> put.accept(lockRule(ObjectLockRetentionMode.GOVERNANCE, null, 101)));

		put.accept(lockRule(ObjectLockRetentionMode.GOVERNANCE, 36500, null));
		var retention = client
				.getObjectLockConfiguration(new GetObjectLockConfigurationRequest().withBucketName(bucketName))
				.getObjectLockConfiguration().getRule().getDefaultRetention();
		assertEquals(ObjectLockRetentionMode.GOVERNANCE.toString(), retention.getMode());
		assertEquals(36500, retention.getDays());
		assertNull(retention.getYears());
	}

	@Test
	@Tag("Check")
	public void testObjectLockPutObjLockRemoveRule() {
		var client = getClient();
		var bucketName = createBucketObjectLock(client, 45);
		client.setObjectLockConfiguration(new SetObjectLockConfigurationRequest().withBucketName(bucketName)
				.withObjectLockConfiguration(lockRule(ObjectLockRetentionMode.GOVERNANCE, 1, null)));

		// Rule 없이 다시 넣으면 기본 보존 규칙이 지워진다
		client.setObjectLockConfiguration(new SetObjectLockConfigurationRequest().withBucketName(bucketName)
				.withObjectLockConfiguration(
						new ObjectLockConfiguration().withObjectLockEnabled(ObjectLockEnabled.ENABLED)));
		var conf = client
				.getObjectLockConfiguration(new GetObjectLockConfigurationRequest().withBucketName(bucketName))
				.getObjectLockConfiguration();
		assertEquals(ObjectLockEnabled.ENABLED.toString(), conf.getObjectLockEnabled());
		assertNull(conf.getRule());

		var key = "testObjectLockPutObjLockRemoveRule";
		client.putObject(bucketName, key, "content");
		assertNull(client.getObjectMetadata(bucketName, key).getObjectLockMode());
	}

	@Test
	@Tag("Access")
	public void testObjectLockGetObjLockAccessDenied() {
		var client = getClient();
		var bucketName = createBucketObjectLock(client, 46);

		assertS3Error(HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED, () -> getAltClient()
				.getObjectLockConfiguration(new GetObjectLockConfigurationRequest().withBucketName(bucketName)));
		var notExist = getNewBucketNameOnly(46);
		assertS3Error(HttpStatus.SC_NOT_FOUND, MainData.NO_SUCH_BUCKET, () -> client
				.getObjectLockConfiguration(new GetObjectLockConfigurationRequest().withBucketName(notExist)));
	}
	// endregion
}
