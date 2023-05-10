/*
* Copyright (c) 2021 PSPACE, inc. KSAN Development Team ksan@pspace.co.kr
* KSAN is a suite of free software: you can redistribute it and/or modify it under the terms of
* the GNU General Public License as published by the Free Software Foundation, either version
* 3 of the License.  See LICENSE for details
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

public class Lock extends TestBase
{
	@org.junit.jupiter.api.BeforeAll
	static public void BeforeAll()
	{
		System.out.println("Lock Start");
	}

	@org.junit.jupiter.api.AfterAll
	static public void AfterAll()
	{
		System.out.println("Lock End");
	}

	@Test
	@Tag("Check")
	//[버킷의 Lock옵션을 활성화] 오브젝트의 잠금 설정이 가능한지 확인
	public void test_object_lock_put_obj_lock() {
		var bucketName = getNewBucketName();
		var client = getClient();
		client.createBucket(new CreateBucketRequest(bucketName).withObjectLockEnabledForBucket(true));

		var Conf = new ObjectLockConfiguration().withObjectLockEnabled(ObjectLockEnabled.ENABLED)
				.withRule(new ObjectLockRule().withDefaultRetention(
						new DefaultRetention().withMode(ObjectLockRetentionMode.COMPLIANCE).withYears(1)));
		client.setObjectLockConfiguration(
				new SetObjectLockConfigurationRequest().withBucketName(bucketName).withObjectLockConfiguration(Conf));

		var VersionResponse = client.getBucketVersioningConfiguration(bucketName);
		assertEquals(BucketVersioningConfiguration.ENABLED, VersionResponse.getStatus());
	}

	@Test
	@Tag("ERROR")
	//버킷을 Lock옵션을 활성화 하지않을 경우 lock 설정이 실패
	public void test_object_lock_put_obj_lock_invalid_bucket() {
		var bucketName = getNewBucketName();
		var client = getClient();
		client.createBucket(bucketName);

		var Conf = new ObjectLockConfiguration().withObjectLockEnabled(ObjectLockEnabled.ENABLED)
				.withRule(new ObjectLockRule().withDefaultRetention(
						new DefaultRetention().withMode(ObjectLockRetentionMode.GOVERNANCE).withYears(1)));

		var e = assertThrows(AmazonServiceException.class, () -> client.setObjectLockConfiguration(
				new SetObjectLockConfigurationRequest().withBucketName(bucketName).withObjectLockConfiguration(Conf)));
		var StatusCode = e.getStatusCode();
		var ErrorCode = e.getErrorCode();
		assertEquals(409, StatusCode);
		assertEquals(MainData.InvalidBucketState, ErrorCode);
	}

	@Test
	@Tag("ERROR")
	//[버킷의 Lock옵션을 활성화] Days, Years값 모두 입력하여 Lock 설정할경우 실패
	public void test_object_lock_put_obj_lock_with_days_and_years() {
		var bucketName = getNewBucketName();
		var client = getClient();
		client.createBucket(new CreateBucketRequest(bucketName).withObjectLockEnabledForBucket(true));

		var Conf = new ObjectLockConfiguration().withObjectLockEnabled(ObjectLockEnabled.ENABLED)
				.withRule(new ObjectLockRule().withDefaultRetention(
						new DefaultRetention().withMode(ObjectLockRetentionMode.GOVERNANCE).withYears(1).withDays(1)));
		var e = assertThrows(AmazonServiceException.class, () -> client.setObjectLockConfiguration(
				new SetObjectLockConfigurationRequest().withBucketName(bucketName).withObjectLockConfiguration(Conf)));
		var StatusCode = e.getStatusCode();
		var ErrorCode = e.getErrorCode();
		assertEquals(400, StatusCode);
		assertEquals(MainData.MalformedXML, ErrorCode);
	}

	@Test
	@Tag("ERROR")
	//[버킷의 Lock옵션을 활성화] Days값을 0이하로 입력하여 Lock 설정할경우 실패
	public void test_object_lock_put_obj_lock_invalid_days() {
		var bucketName = getNewBucketName();
		var client = getClient();
		client.createBucket(new CreateBucketRequest(bucketName).withObjectLockEnabledForBucket(true));

		var Conf = new ObjectLockConfiguration().withObjectLockEnabled(ObjectLockEnabled.ENABLED)
				.withRule(new ObjectLockRule().withDefaultRetention(
						new DefaultRetention().withMode(ObjectLockRetentionMode.GOVERNANCE).withDays(0)));

		var e = assertThrows(AmazonServiceException.class, () -> client.setObjectLockConfiguration(
				new SetObjectLockConfigurationRequest().withBucketName(bucketName).withObjectLockConfiguration(Conf)));
		var StatusCode = e.getStatusCode();
		var ErrorCode = e.getErrorCode();
		assertEquals(400, StatusCode);
		assertEquals(MainData.InvalidArgument, ErrorCode);
	}

	@Test
	@Tag("ERROR")
	//[버킷의 Lock옵션을 활성화] Years값을 0이하로 입력하여 Lock 설정할경우 실패
	public void test_object_lock_put_obj_lock_invalid_years() {
		var bucketName = getNewBucketName();
		var client = getClient();
		client.createBucket(new CreateBucketRequest(bucketName).withObjectLockEnabledForBucket(true));

		var Conf = new ObjectLockConfiguration().withObjectLockEnabled(ObjectLockEnabled.ENABLED)
				.withRule(new ObjectLockRule().withDefaultRetention(
						new DefaultRetention().withMode(ObjectLockRetentionMode.GOVERNANCE).withYears(-1)));

		var e = assertThrows(AmazonServiceException.class, () -> client.setObjectLockConfiguration(
				new SetObjectLockConfigurationRequest().withBucketName(bucketName).withObjectLockConfiguration(Conf)));
		var StatusCode = e.getStatusCode();
		var ErrorCode = e.getErrorCode();
		assertEquals(400, StatusCode);
		assertEquals(MainData.InvalidArgument, ErrorCode);
	}

	@Test
	@Tag("ERROR")
	//[버킷의 Lock옵션을 활성화] mode값이 올바르지 않은상태에서 Lock 설정할 경우 실패
	public void test_object_lock_put_obj_lock_invalid_mode() {
		var bucketName = getNewBucketName();
		var client = getClient();
		client.createBucket(new CreateBucketRequest(bucketName).withObjectLockEnabledForBucket(true));

		var Conf = new ObjectLockConfiguration().withObjectLockEnabled(ObjectLockEnabled.ENABLED).withRule(
				new ObjectLockRule().withDefaultRetention(new DefaultRetention().withMode("invalid").withYears(1)));

		var e = assertThrows(AmazonServiceException.class, () -> client.setObjectLockConfiguration(
				new SetObjectLockConfigurationRequest().withBucketName(bucketName).withObjectLockConfiguration(Conf)));
		var StatusCode = e.getStatusCode();
		var ErrorCode = e.getErrorCode();
		assertEquals(400, StatusCode);
		assertEquals(MainData.MalformedXML, ErrorCode);
	}

	@Test
	@Tag("ERROR")
	//[버킷의 Lock옵션을 활성화] status값이 올바르지 않은상태에서 Lock 설정할 경우 실패
	public void test_object_lock_put_obj_lock_invalid_status() {
		var bucketName = getNewBucketName();
		var client = getClient();
		client.createBucket(new CreateBucketRequest(bucketName).withObjectLockEnabledForBucket(true));

		var Conf = new ObjectLockConfiguration().withObjectLockEnabled("Disabled")
				.withRule(new ObjectLockRule().withDefaultRetention(
						new DefaultRetention().withMode(ObjectLockRetentionMode.GOVERNANCE).withYears(1)));

		var e = assertThrows(AmazonServiceException.class, () -> client.setObjectLockConfiguration(
				new SetObjectLockConfigurationRequest().withBucketName(bucketName).withObjectLockConfiguration(Conf)));
		var StatusCode = e.getStatusCode();
		var ErrorCode = e.getErrorCode();
		assertEquals(400, StatusCode);
		assertEquals(MainData.MalformedXML, ErrorCode);
	}

	@Test
	@Tag("Version")
	//[버킷의 Lock옵션을 활성화] 버킷의 버저닝을 일시중단하려고 할경우 실패
	public void test_object_lock_suspend_versioning() {
		var bucketName = getNewBucketName();
		var client = getClient();
		client.createBucket(new CreateBucketRequest(bucketName).withObjectLockEnabledForBucket(true));

		var e = assertThrows(AmazonServiceException.class,
				() -> client.setBucketVersioningConfiguration(new SetBucketVersioningConfigurationRequest(bucketName,
						new BucketVersioningConfiguration(BucketVersioningConfiguration.SUSPENDED))));
		var StatusCode = e.getStatusCode();
		var ErrorCode = e.getErrorCode();
		assertEquals(409, StatusCode);
		assertEquals(MainData.InvalidBucketState, ErrorCode);
	}

	@Test
	@Tag("Check")
	//[버킷의 Lock옵션을 활성화] 버킷의 lock설정이 올바르게 되었는지 확인
	public void test_object_lock_get_obj_lock() {
		var bucketName = getNewBucketName();
		var client = getClient();
		client.createBucket(new CreateBucketRequest(bucketName).withObjectLockEnabledForBucket(true));

		var Conf = new ObjectLockConfiguration().withObjectLockEnabled(ObjectLockEnabled.ENABLED)
				.withRule(new ObjectLockRule().withDefaultRetention(
						new DefaultRetention().withMode(ObjectLockRetentionMode.GOVERNANCE).withDays(1)));

		client.setObjectLockConfiguration(
				new SetObjectLockConfigurationRequest().withBucketName(bucketName).withObjectLockConfiguration(Conf));

		var Response = client
				.getObjectLockConfiguration(new GetObjectLockConfigurationRequest().withBucketName(bucketName));
		LockCompare(Conf, Response.getObjectLockConfiguration());
	}

	@Test
	@Tag("ERROR")
	//버킷을 Lock옵션을 활성화 하지않을 경우 lock 설정 조회 실패
	public void test_object_lock_get_obj_lock_invalid_bucket() {
		var bucketName = getNewBucketName();
		var client = getClient();
		client.createBucket(bucketName);

		var e = assertThrows(AmazonServiceException.class, () -> client
				.getObjectLockConfiguration(new GetObjectLockConfigurationRequest().withBucketName(bucketName)));
		var StatusCode = e.getStatusCode();
		var ErrorCode = e.getErrorCode();
		assertEquals(404, StatusCode);
		assertEquals(MainData.ObjectLockConfigurationNotFoundError, ErrorCode);
	}

	@Test
	@Tag("Retention")
	//[버킷의 Lock옵션을 활성화] 오브젝트에 Lock 유지기한 설정이 가능한지 확인
	public void test_object_lock_put_obj_retention() {
		var bucketName = getNewBucketName();
		var client = getClient();
		client.createBucket(new CreateBucketRequest(bucketName).withObjectLockEnabledForBucket(true));
		var Key = "file1";

		var PutResponse = client.putObject(bucketName, Key, "abc");
		var VersionID = PutResponse.getVersionId();

		var Retention = new ObjectLockRetention().withMode(ObjectLockRetentionMode.GOVERNANCE)
				.withRetainUntilDate(new Calendar.Builder().setDate(2030, 1, 1)
						.setTimeZone(TimeZone.getTimeZone(("GMT"))).build().getTime());

		client.setObjectRetention(
				new SetObjectRetentionRequest().withBucketName(bucketName).withKey(Key).withRetention(Retention));

		client.deleteVersion(new DeleteVersionRequest(bucketName, Key, VersionID).withBypassGovernanceRetention(true));
	}

	@Test
	@Tag("Retention")
	//버킷을 Lock옵션을 활성화 하지않을 경우 오브젝트에 Lock 유지기한 설정 실패
	public void test_object_lock_put_obj_retention_invalid_bucket() {
		var bucketName = getNewBucketName();
		var client = getClient();
		client.createBucket(bucketName);
		var Key = "file1";

		client.putObject(bucketName, Key, "abc");

		var Retention = new ObjectLockRetention().withMode(ObjectLockRetentionMode.GOVERNANCE)
				.withRetainUntilDate(new Calendar.Builder().setDate(2030, 1, 1)
						.setTimeZone(TimeZone.getTimeZone(("GMT"))).build().getTime());

		var e = assertThrows(AmazonServiceException.class, () -> client.setObjectRetention(
				new SetObjectRetentionRequest().withBucketName(bucketName).withKey(Key).withRetention(Retention)));
		var StatusCode = e.getStatusCode();
		var ErrorCode = e.getErrorCode();
		assertEquals(400, StatusCode);
		assertEquals(MainData.InvalidRequest, ErrorCode);
	}

	@Test
	@Tag("Retention")
	//[버킷의 Lock옵션을 활성화] 오브젝트에 Lock 유지기한 설정할때 Mode값이 올바르지 않을 경우 설정 실패
	public void test_object_lock_put_obj_retention_invalid_mode() {
		var bucketName = getNewBucketName();
		var client = getClient();
		client.createBucket(new CreateBucketRequest(bucketName).withObjectLockEnabledForBucket(true));
		var Key = "file1";

		client.putObject(bucketName, Key, "abc");

		var Retention = new ObjectLockRetention().withMode("invalid").withRetainUntilDate(new Calendar.Builder()
				.setDate(2030, 1, 1).setTimeZone(TimeZone.getTimeZone(("GMT"))).build().getTime());

		var e = assertThrows(AmazonServiceException.class, () -> client.setObjectRetention(
				new SetObjectRetentionRequest().withBucketName(bucketName).withKey(Key).withRetention(Retention)));
		var StatusCode = e.getStatusCode();
		var ErrorCode = e.getErrorCode();
		assertEquals(400, StatusCode);
		assertEquals(MainData.MalformedXML, ErrorCode);
	}

	@Test
	@Tag("Retention")
	//[버킷의 Lock옵션을 활성화] 오브젝트에 Lock 유지기한 설정이 올바른지 확인
	public void test_object_lock_get_obj_retention() {
		var bucketName = getNewBucketName();
		var client = getClient();
		client.createBucket(new CreateBucketRequest(bucketName).withObjectLockEnabledForBucket(true));
		var Key = "file1";

		var PutResponse = client.putObject(bucketName, Key, "abc");
		var VersionID = PutResponse.getVersionId();

		var Retention = new ObjectLockRetention().withMode(ObjectLockRetentionMode.GOVERNANCE)
				.withRetainUntilDate(new Calendar.Builder().setDate(2030, 1, 1)
						.setTimeZone(TimeZone.getTimeZone(("GMT"))).build().getTime());

		client.setObjectRetention(
				new SetObjectRetentionRequest().withBucketName(bucketName).withKey(Key).withRetention(Retention));
		var Response = client
				.getObjectRetention(new GetObjectRetentionRequest().withBucketName(bucketName).withKey(Key));
		RetentionCompare(Retention, Response.getRetention());
		client.deleteVersion(new DeleteVersionRequest(bucketName, Key, VersionID).withBypassGovernanceRetention(true));
	}

	@Test
	@Tag("Retention")
	//버킷을 Lock옵션을 활성화 하지않을 경우 오브젝트에 Lock 유지기한 조회 실패
	public void test_object_lock_get_obj_retention_invalid_bucket() {
		var bucketName = getNewBucketName();
		var client = getClient();
		client.createBucket(bucketName);
		var Key = "file1";

		client.putObject(bucketName, Key, "abc");

		var e = assertThrows(AmazonServiceException.class, () -> client
				.getObjectRetention(new GetObjectRetentionRequest().withBucketName(bucketName).withKey(Key)));
		var StatusCode = e.getStatusCode();
		var ErrorCode = e.getErrorCode();
		assertEquals(400, StatusCode);
		assertEquals(MainData.InvalidRequest, ErrorCode);
	}

	@Test
	@Tag("Retention")
	//[버킷의 Lock옵션을 활성화] 오브젝트의 특정 버전에 Lock 유지기한을 설정할 경우 올바르게 적용되었는지 확인
	public void test_object_lock_put_obj_retention_versionid() {
		var bucketName = getNewBucketName();
		var client = getClient();
		client.createBucket(new CreateBucketRequest(bucketName).withObjectLockEnabledForBucket(true));
		var Key = "file1";

		client.putObject(bucketName, Key, "abc");
		var PutResponse = client.putObject(bucketName, Key, "abc");
		var VersionID = PutResponse.getVersionId();

		var Retention = new ObjectLockRetention().withMode(ObjectLockRetentionMode.GOVERNANCE)
				.withRetainUntilDate(new Calendar.Builder().setDate(2030, 1, 1)
						.setTimeZone(TimeZone.getTimeZone(("GMT"))).build().getTime());

		client.setObjectRetention(
				new SetObjectRetentionRequest().withBucketName(bucketName).withKey(Key).withRetention(Retention));
		var Response = client
				.getObjectRetention(new GetObjectRetentionRequest().withBucketName(bucketName).withKey(Key));
		RetentionCompare(Retention, Response.getRetention());
		client.deleteVersion(new DeleteVersionRequest(bucketName, Key, VersionID).withBypassGovernanceRetention(true));
	}

	@Test
	@Tag("Priority")
	//[버킷의 Lock옵션을 활성화] 버킷에 설정한 Lock설정보다 오브젝트에 Lock설정한 값이 우선 적용됨을 확인
	public void test_object_lock_put_obj_retention_override_default_retention() {
		var bucketName = getNewBucketName();
		var client = getClient();
		client.createBucket(new CreateBucketRequest(bucketName).withObjectLockEnabledForBucket(true));

		var Conf = new ObjectLockConfiguration().withObjectLockEnabled(ObjectLockEnabled.ENABLED)
				.withRule(new ObjectLockRule().withDefaultRetention(
						new DefaultRetention().withMode(ObjectLockRetentionMode.GOVERNANCE).withDays(1)));

		client.setObjectLockConfiguration(
				new SetObjectLockConfigurationRequest().withBucketName(bucketName).withObjectLockConfiguration(Conf));

		var Key = "file1";
		var Body = "abc";
		var Metadata = new ObjectMetadata();
		Metadata.setContentMD5(Utils.getMD5(Body));
		Metadata.setContentType("text/plain");
		Metadata.setContentLength(Body.length());

		var PutResponse = client.putObject(bucketName, Key, createBody(Body), Metadata);
		var VersionID = PutResponse.getVersionId();

		var Retention = new ObjectLockRetention().withMode(ObjectLockRetentionMode.GOVERNANCE)
				.withRetainUntilDate(new Calendar.Builder().setDate(2030, 1, 1)
						.setTimeZone(TimeZone.getTimeZone(("GMT"))).build().getTime());

		client.setObjectRetention(
				new SetObjectRetentionRequest().withBucketName(bucketName).withKey(Key).withRetention(Retention));
		var Response = client
				.getObjectRetention(new GetObjectRetentionRequest().withBucketName(bucketName).withKey(Key));
		RetentionCompare(Retention, Response.getRetention());

		client.deleteVersion(new DeleteVersionRequest(bucketName, Key, VersionID).withBypassGovernanceRetention(true));
	}

	@Test
	@Tag("Overwrite")
	//[버킷의 Lock옵션을 활성화] 오브젝트의 lock 유지기한을 늘렸을때 적용되는지 확인
	public void test_object_lock_put_obj_retention_increase_period() {
		var bucketName = getNewBucketName();
		var client = getClient();
		client.createBucket(new CreateBucketRequest(bucketName).withObjectLockEnabledForBucket(true));
		var Key = "file1";

		client.putObject(bucketName, Key, "abc");
		var PutResponse = client.putObject(bucketName, Key, "abc");
		var VersionID = PutResponse.getVersionId();

		var Retention1 = new ObjectLockRetention().withMode(ObjectLockRetentionMode.GOVERNANCE)
				.withRetainUntilDate(new Calendar.Builder().setDate(2030, 1, 1)
						.setTimeZone(TimeZone.getTimeZone(("GMT"))).build().getTime());
		client.setObjectRetention(
				new SetObjectRetentionRequest().withBucketName(bucketName).withKey(Key).withRetention(Retention1));

		var Retention2 = new ObjectLockRetention().withMode(ObjectLockRetentionMode.GOVERNANCE)
				.withRetainUntilDate(new Calendar.Builder().setDate(2030, 3, 1)
						.setTimeZone(TimeZone.getTimeZone(("GMT"))).build().getTime());
		client.setObjectRetention(
				new SetObjectRetentionRequest().withBucketName(bucketName).withKey(Key).withRetention(Retention2));

		var Response = client
				.getObjectRetention(new GetObjectRetentionRequest().withBucketName(bucketName).withKey(Key));
		RetentionCompare(Retention2, Response.getRetention());
		client.deleteVersion(new DeleteVersionRequest(bucketName, Key, VersionID).withBypassGovernanceRetention(true));
	}

	@Test
	@Tag("Overwrite")
	//[버킷의 Lock옵션을 활성화] 오브젝트의 lock 유지기한을 줄였을때 실패 확인
	public void test_object_lock_put_obj_retention_shorten_period() {
		var bucketName = getNewBucketName();
		var client = getClient();
		client.createBucket(new CreateBucketRequest(bucketName).withObjectLockEnabledForBucket(true));
		var Key = "file1";

		client.putObject(bucketName, Key, "abc");
		var PutResponse = client.putObject(bucketName, Key, "abc");
		var VersionID = PutResponse.getVersionId();

		var Retention1 = new ObjectLockRetention().withMode(ObjectLockRetentionMode.GOVERNANCE)
				.withRetainUntilDate(new Calendar.Builder().setDate(2030, 3, 1)
						.setTimeZone(TimeZone.getTimeZone(("GMT"))).build().getTime());
		client.setObjectRetention(
				new SetObjectRetentionRequest().withBucketName(bucketName).withKey(Key).withRetention(Retention1));

		var Retention2 = new ObjectLockRetention().withMode(ObjectLockRetentionMode.GOVERNANCE)
				.withRetainUntilDate(new Calendar.Builder().setDate(2030, 1, 1)
						.setTimeZone(TimeZone.getTimeZone(("GMT"))).build().getTime());

		var e = assertThrows(AmazonServiceException.class, () -> client.setObjectRetention(
				new SetObjectRetentionRequest().withBucketName(bucketName).withKey(Key).withRetention(Retention2)));
		var StatusCode = e.getStatusCode();
		var ErrorCode = e.getErrorCode();
		assertEquals(403, StatusCode);
		assertEquals(MainData.AccessDenied, ErrorCode);

		client.deleteVersion(new DeleteVersionRequest(bucketName, Key, VersionID).withBypassGovernanceRetention(true));
	}

	@Test
	@Tag("Overwrite")
	//[버킷의 Lock옵션을 활성화] 바이패스를 True로 설정하고 오브젝트의 lock 유지기한을 줄였을때 적용되는지 확인
	public void test_object_lock_put_obj_retention_shorten_period_bypass() {
		var bucketName = getNewBucketName();
		var client = getClient();
		client.createBucket(new CreateBucketRequest(bucketName).withObjectLockEnabledForBucket(true));
		var Key = "file1";

		client.putObject(bucketName, Key, "abc");
		var PutResponse = client.putObject(bucketName, Key, "abc");
		var VersionID = PutResponse.getVersionId();

		var Retention1 = new ObjectLockRetention().withMode(ObjectLockRetentionMode.GOVERNANCE)
				.withRetainUntilDate(new Calendar.Builder().setDate(2030, 3, 1)
						.setTimeZone(TimeZone.getTimeZone(("GMT"))).build().getTime());
		client.setObjectRetention(
				new SetObjectRetentionRequest().withBucketName(bucketName).withKey(Key).withRetention(Retention1));

		var Retention2 = new ObjectLockRetention().withMode(ObjectLockRetentionMode.GOVERNANCE)
				.withRetainUntilDate(new Calendar.Builder().setDate(2030, 1, 1)
						.setTimeZone(TimeZone.getTimeZone(("GMT"))).build().getTime());
		client.setObjectRetention(new SetObjectRetentionRequest().withBucketName(bucketName).withKey(Key)
				.withRetention(Retention2).withBypassGovernanceRetention(true));

		var Response = client
				.getObjectRetention(new GetObjectRetentionRequest().withBucketName(bucketName).withKey(Key));
		RetentionCompare(Retention2, Response.getRetention());
		client.deleteVersion(new DeleteVersionRequest(bucketName, Key, VersionID).withBypassGovernanceRetention(true));
	}

	@Test
	@Tag("ERROR")
	//[버킷의 Lock옵션을 활성화] 오브젝트의 lock 유지기한내에 삭제를 시도할 경우 실패 확인
	public void test_object_lock_delete_object_with_retention() {
		var bucketName = getNewBucketName();
		var client = getClient();
		client.createBucket(new CreateBucketRequest(bucketName).withObjectLockEnabledForBucket(true));
		var Key = "file1";

		var PutResponse = client.putObject(bucketName, Key, "abc");
		var VersionID = PutResponse.getVersionId();

		var Retention = new ObjectLockRetention().withMode(ObjectLockRetentionMode.GOVERNANCE)
				.withRetainUntilDate(new Calendar.Builder().setDate(2030, 1, 1)
						.setTimeZone(TimeZone.getTimeZone(("GMT"))).build().getTime());
		client.setObjectRetention(
				new SetObjectRetentionRequest().withBucketName(bucketName).withKey(Key).withRetention(Retention));

		var e = assertThrows(AmazonServiceException.class, () -> client.deleteVersion(bucketName, Key, VersionID));
		var StatusCode = e.getStatusCode();
		var ErrorCode = e.getErrorCode();
		assertEquals(403, StatusCode);
		assertEquals(MainData.AccessDenied, ErrorCode);

		client.deleteVersion(new DeleteVersionRequest(bucketName, Key, VersionID).withBypassGovernanceRetention(true));
	}

	@Test
	@Tag("LegalHold")
	//[버킷의 Lock옵션을 활성화] 오브젝트의 LegalHold를 활성화 가능한지 확인
	public void test_object_lock_put_legal_hold() {
		var bucketName = getNewBucketName();
		var client = getClient();
		client.createBucket(new CreateBucketRequest(bucketName).withObjectLockEnabledForBucket(true));

		var Key = "file1";
		client.putObject(bucketName, Key, "abc");

		var LegalHold = new ObjectLockLegalHold().withStatus(ObjectLockLegalHoldStatus.ON);
		client.setObjectLegalHold(
				new SetObjectLegalHoldRequest().withBucketName(bucketName).withKey(Key).withLegalHold(LegalHold));

		LegalHold = new ObjectLockLegalHold().withStatus(ObjectLockLegalHoldStatus.OFF);
		client.setObjectLegalHold(
				new SetObjectLegalHoldRequest().withBucketName(bucketName).withKey(Key).withLegalHold(LegalHold));

	}

	@Test
	@Tag("LegalHold")
	//[버킷의 Lock옵션을 비활성화] 오브젝트의 LegalHold를 활성화 실패 확인
	public void test_object_lock_put_legal_hold_invalid_bucket() {
		var bucketName = getNewBucketName();
		var client = getClient();
		client.createBucket(bucketName);

		var Key = "file1";
		client.putObject(bucketName, Key, "abc");

		var LegalHold = new ObjectLockLegalHold().withStatus(ObjectLockLegalHoldStatus.ON);
		var e = assertThrows(AmazonServiceException.class, () -> client.setObjectLegalHold(
				new SetObjectLegalHoldRequest().withBucketName(bucketName).withKey(Key).withLegalHold(LegalHold)));
		var StatusCode = e.getStatusCode();
		var ErrorCode = e.getErrorCode();
		assertEquals(400, StatusCode);
		assertEquals(MainData.InvalidRequest, ErrorCode);
	}

	@Test
	@Tag("LegalHold")
	//[버킷의 Lock옵션을 활성화] 오브젝트의 LegalHold에 잘못된 값을 넣을 경우 실패 확인
	public void test_object_lock_put_legal_hold_invalid_status() {
		var bucketName = getNewBucketName();
		var client = getClient();
		client.createBucket(new CreateBucketRequest(bucketName).withObjectLockEnabledForBucket(true));

		var Key = "file1";
		client.putObject(bucketName, Key, "abc");

		var LegalHold = new ObjectLockLegalHold().withStatus("abc");
		var e = assertThrows(AmazonServiceException.class, () -> client.setObjectLegalHold(
				new SetObjectLegalHoldRequest().withBucketName(bucketName).withKey(Key).withLegalHold(LegalHold)));
		var StatusCode = e.getStatusCode();
		var ErrorCode = e.getErrorCode();
		assertEquals(400, StatusCode);
		assertEquals(MainData.MalformedXML, ErrorCode);
	}

	@Test
	@Tag("LegalHold")
	//[버킷의 Lock옵션을 활성화] 오브젝트의 LegalHold가 올바르게 적용되었는지 확인
	public void test_object_lock_get_legal_hold() {
		var bucketName = getNewBucketName();
		var client = getClient();
		client.createBucket(new CreateBucketRequest(bucketName).withObjectLockEnabledForBucket(true));

		var Key = "file1";
		client.putObject(bucketName, Key, "abc");

		var LegalHold = new ObjectLockLegalHold().withStatus(ObjectLockLegalHoldStatus.ON);
		client.setObjectLegalHold(
				new SetObjectLegalHoldRequest().withBucketName(bucketName).withKey(Key).withLegalHold(LegalHold));
		var Response = client
				.getObjectLegalHold(new GetObjectLegalHoldRequest().withBucketName(bucketName).withKey(Key));
		assertEquals(LegalHold.getStatus(), Response.getLegalHold().getStatus());

		LegalHold = new ObjectLockLegalHold().withStatus(ObjectLockLegalHoldStatus.OFF);
		client.setObjectLegalHold(
				new SetObjectLegalHoldRequest().withBucketName(bucketName).withKey(Key).withLegalHold(LegalHold));
		Response = client.getObjectLegalHold(new GetObjectLegalHoldRequest().withBucketName(bucketName).withKey(Key));
		assertEquals(LegalHold.getStatus(), Response.getLegalHold().getStatus());
	}

	@Test
	@Tag("LegalHold")
	//[버킷의 Lock옵션을 비활성화] 오브젝트의 LegalHold설정 조회 실패 확인
	public void test_object_lock_get_legal_hold_invalid_bucket() {
		var bucketName = getNewBucketName();
		var client = getClient();
		client.createBucket(bucketName);

		var Key = "file1";
		client.putObject(bucketName, Key, "abc");

		var e = assertThrows(AmazonServiceException.class, () -> client
				.getObjectLegalHold(new GetObjectLegalHoldRequest().withBucketName(bucketName).withKey(Key)));
		var StatusCode = e.getStatusCode();
		var ErrorCode = e.getErrorCode();
		assertEquals(400, StatusCode);
		assertEquals(MainData.InvalidRequest, ErrorCode);
	}

	@Test
	@Tag("LegalHold")
	//[버킷의 Lock옵션을 활성화] 오브젝트의 LegalHold가 활성화되어 있을 경우 오브젝트 삭제 실패 확인
	public void test_object_lock_delete_object_with_legal_hold_on() {
		var bucketName = getNewBucketName();
		var client = getClient();
		client.createBucket(new CreateBucketRequest(bucketName).withObjectLockEnabledForBucket(true));

		var Key = "file1";
		var PutResponse = client.putObject(bucketName, Key, "abc");

		var LegalHold = new ObjectLockLegalHold().withStatus(ObjectLockLegalHoldStatus.ON);
		client.setObjectLegalHold(
				new SetObjectLegalHoldRequest().withBucketName(bucketName).withKey(Key).withLegalHold(LegalHold));

		var e = assertThrows(AmazonServiceException.class,
				() -> client.deleteVersion(bucketName, Key, PutResponse.getVersionId()));
		var StatusCode = e.getStatusCode();
		var ErrorCode = e.getErrorCode();
		assertEquals(403, StatusCode);
		assertEquals(MainData.AccessDenied, ErrorCode);

		LegalHold = new ObjectLockLegalHold().withStatus(ObjectLockLegalHoldStatus.OFF);
		client.setObjectLegalHold(
				new SetObjectLegalHoldRequest().withBucketName(bucketName).withKey(Key).withLegalHold(LegalHold));
	}

	@Test
	@Tag("LegalHold")
	//[버킷의 Lock옵션을 활성화] 오브젝트의 LegalHold가 비활성화되어 있을 경우 오브젝트 삭제 확인
	public void test_object_lock_delete_object_with_legal_hold_off() {
		var bucketName = getNewBucketName();
		var client = getClient();
		client.createBucket(new CreateBucketRequest(bucketName).withObjectLockEnabledForBucket(true));

		var Key = "file1";
		var PutResponse = client.putObject(bucketName, Key, "abc");

		var LegalHold = new ObjectLockLegalHold().withStatus(ObjectLockLegalHoldStatus.OFF);
		client.setObjectLegalHold(
				new SetObjectLegalHoldRequest().withBucketName(bucketName).withKey(Key).withLegalHold(LegalHold));

		client.deleteVersion(bucketName, Key, PutResponse.getVersionId());
	}

	@Test
	@Tag("LegalHold")
	//[버킷의 Lock옵션을 활성화] 오브젝트의 LegalHold와 Lock유지기한 설정이 모두 적용되는지 메타데이터를 통해 확인
	public void test_object_lock_get_obj_metadata() {
		var bucketName = getNewBucketName();
		var client = getClient();
		client.createBucket(new CreateBucketRequest(bucketName).withObjectLockEnabledForBucket(true));

		var Key = "file1";
		var PutResponse = client.putObject(bucketName, Key, "abc");

		var LegalHold = new ObjectLockLegalHold().withStatus(ObjectLockLegalHoldStatus.ON);
		client.setObjectLegalHold(
				new SetObjectLegalHoldRequest().withBucketName(bucketName).withKey(Key).withLegalHold(LegalHold));

		var Retention = new ObjectLockRetention().withMode(ObjectLockRetentionMode.GOVERNANCE)
				.withRetainUntilDate(new Calendar.Builder().setDate(2030, 1, 1)
						.setTimeZone(TimeZone.getTimeZone(("GMT"))).build().getTime());
		client.setObjectRetention(
				new SetObjectRetentionRequest().withBucketName(bucketName).withKey(Key).withRetention(Retention));

		var Response = client.getObjectMetadata(bucketName, Key);
		assertEquals(Retention.getMode(), Response.getObjectLockMode());
		assertEquals(Retention.getRetainUntilDate(), Response.getObjectLockRetainUntilDate());
		assertEquals(LegalHold.getStatus(), Response.getObjectLockLegalHoldStatus());

		LegalHold = new ObjectLockLegalHold().withStatus(ObjectLockLegalHoldStatus.OFF);
		client.setObjectLegalHold(
				new SetObjectLegalHoldRequest().withBucketName(bucketName).withKey(Key).withLegalHold(LegalHold));
		client.deleteVersion(new DeleteVersionRequest(bucketName, Key, PutResponse.getVersionId())
				.withBypassGovernanceRetention(true));
	}
}
