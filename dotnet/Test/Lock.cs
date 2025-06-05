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
using Amazon.S3;
using Amazon.S3.Model;
using System;
using System.Net;
using Xunit;

namespace s3tests
{
	public class Lock : TestBase
	{
		public Lock(Xunit.Abstractions.ITestOutputHelper Output) => this.Output = Output;

		[Fact]
		[Trait(MainData.Major, "Lock")]
		[Trait(MainData.Minor, "Check")]
		[Trait(MainData.Explanation, "[버킷의 Lock옵션을 활성화] 오브젝트의 잠금 설정이 가능한지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestObjectLockPutObjLock()
		{
			var bucketName = GetNewBucketName(false);
			var Client = GetClient();
			Client.PutBucket(bucketName, objectLockEnabledForBucket: true);

			var Conf = new ObjectLockConfiguration()
			{
				ObjectLockEnabled = ObjectLockEnabled.Enabled,
				Rule = new ObjectLockRule()
				{
					DefaultRetention = new DefaultRetention()
					{
						Mode = ObjectLockRetentionMode.Compliance,
						Years = 1,
					}
				}
			};
			var Response = Client.PutObjectLockConfiguration(bucketName, Conf);
			Assert.Equal(HttpStatusCode.OK, Response.HttpStatusCode);

			var VersionResponse = Client.GetBucketVersioning(bucketName);
			Assert.Equal(VersionStatus.Enabled, VersionResponse.VersioningConfig.Status);
		}

		[Fact]
		[Trait(MainData.Major, "Lock")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "버킷을 Lock옵션을 활성화 하지않을 경우 lock 설정이 실패")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestObjectLockPutObjLockInvalidBucket()
		{
			var bucketName = GetNewBucketName();
			var Client = GetClient();
			Client.PutBucket(bucketName);

			var Conf = new ObjectLockConfiguration()
			{
				ObjectLockEnabled = ObjectLockEnabled.Enabled,
				Rule = new ObjectLockRule()
				{
					DefaultRetention = new DefaultRetention()
					{
						Mode = ObjectLockRetentionMode.Governance,
						Years = 1,
					}
				}
			};
			var e = Assert.Throws<AggregateException>(() => Client.PutObjectLockConfiguration(bucketName, Conf));
			Assert.Equal(HttpStatusCode.Conflict, GetStatus(e));
			Assert.Equal(MainData.INVALID_BUCKET_STATE, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "Lock")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "[버킷의 Lock옵션을 활성화] Days, Years값 모두 입력하여 Lock 설정할경우 실패")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_object_lock_put_obj_lock_with_days_and_years()
		{
			var bucketName = GetNewBucketName();
			var Client = GetClient();
			Client.PutBucket(bucketName, objectLockEnabledForBucket: true);

			var Conf = new ObjectLockConfiguration()
			{
				ObjectLockEnabled = ObjectLockEnabled.Enabled,
				Rule = new ObjectLockRule()
				{
					DefaultRetention = new DefaultRetention()
					{
						Mode = ObjectLockRetentionMode.Governance,
						Years = 1,
						Days = 1
					}
				}
			};
			var e = Assert.Throws<AggregateException>(() => Client.PutObjectLockConfiguration(bucketName, Conf));
			Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
			Assert.Equal(MainData.MALFORMED_XML, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "Lock")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "[버킷의 Lock옵션을 활성화] Days값을 0이하로 입력하여 Lock 설정할경우 실패")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_object_lock_put_obj_lock_invalid_days()
		{
			var bucketName = GetNewBucketName();
			var Client = GetClient();
			Client.PutBucket(bucketName, objectLockEnabledForBucket: true);

			var Conf = new ObjectLockConfiguration()
			{
				ObjectLockEnabled = ObjectLockEnabled.Enabled,
				Rule = new ObjectLockRule()
				{
					DefaultRetention = new DefaultRetention()
					{
						Mode = ObjectLockRetentionMode.Governance,
						Days = 0,
					}
				}
			};
			var e = Assert.Throws<AggregateException>(() => Client.PutObjectLockConfiguration(bucketName, Conf));
			Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
			Assert.Equal(MainData.INVALID_ARGUMENT, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "Lock")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "[버킷의 Lock옵션을 활성화] Years값을 0이하로 입력하여 Lock 설정할경우 실패")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_object_lock_put_obj_lock_invalid_years()
		{
			var bucketName = GetNewBucketName();
			var Client = GetClient();
			Client.PutBucket(bucketName, objectLockEnabledForBucket: true);

			var Conf = new ObjectLockConfiguration()
			{
				ObjectLockEnabled = ObjectLockEnabled.Enabled,
				Rule = new ObjectLockRule()
				{
					DefaultRetention = new DefaultRetention()
					{
						Mode = ObjectLockRetentionMode.Governance,
						Years = -1,
					}
				}
			};
			var e = Assert.Throws<AggregateException>(() => Client.PutObjectLockConfiguration(bucketName, Conf));
			Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
			Assert.Equal(MainData.INVALID_ARGUMENT, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "Lock")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "[버킷의 Lock옵션을 활성화] mode값이 올바르지 않은상태에서 Lock 설정할 경우 실패")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_object_lock_put_obj_lock_invalid_mode()
		{
			var bucketName = GetNewBucketName();
			var Client = GetClient();
			Client.PutBucket(bucketName, objectLockEnabledForBucket: true);

			var Conf = new ObjectLockConfiguration()
			{
				ObjectLockEnabled = ObjectLockEnabled.Enabled,
				Rule = new ObjectLockRule()
				{
					DefaultRetention = new DefaultRetention()
					{
						Mode = new ObjectLockRetentionMode("abc"),
						Years = 1,
					}
				}
			};
			var e = Assert.Throws<AggregateException>(() => Client.PutObjectLockConfiguration(bucketName, Conf));
			Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
			Assert.Equal(MainData.MALFORMED_XML, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "Lock")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "[버킷의 Lock옵션을 활성화] status값이 올바르지 않은상태에서 Lock 설정할 경우 실패")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_object_lock_put_obj_lock_invalid_status()
		{
			var bucketName = GetNewBucketName();
			var Client = GetClient();
			Client.PutBucket(bucketName, objectLockEnabledForBucket: true);

			var Conf = new ObjectLockConfiguration()
			{
				ObjectLockEnabled = new ObjectLockEnabled("Disabled"),
				Rule = new ObjectLockRule()
				{
					DefaultRetention = new DefaultRetention()
					{
						Mode = ObjectLockRetentionMode.Governance,
						Years = 1,
					}
				}
			};
			var e = Assert.Throws<AggregateException>(() => Client.PutObjectLockConfiguration(bucketName, Conf));
			Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
			Assert.Equal(MainData.MALFORMED_XML, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "Lock")]
		[Trait(MainData.Minor, "Version")]
		[Trait(MainData.Explanation, "[버킷의 Lock옵션을 활성화] 버킷의 버저닝을 일시중단하려고 할경우 실패")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_object_lock_suspend_versioning()
		{
			var bucketName = GetNewBucketName();
			var Client = GetClient();
			Client.PutBucket(bucketName, objectLockEnabledForBucket: true);

			var e = Assert.Throws<AggregateException>(() => Client.PutBucketVersioning(bucketName, status: VersionStatus.Suspended));
			Assert.Equal(HttpStatusCode.Conflict, GetStatus(e));
			Assert.Equal(MainData.INVALID_BUCKET_STATE, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "Lock")]
		[Trait(MainData.Minor, "Check")]
		[Trait(MainData.Explanation, "[버킷의 Lock옵션을 활성화] 버킷의 lock설정이 올바르게 되었는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_object_lock_get_obj_lock()
		{
			var bucketName = GetNewBucketName();
			var Client = GetClient();
			Client.PutBucket(bucketName, objectLockEnabledForBucket: true);

			var Conf = new ObjectLockConfiguration()
			{
				ObjectLockEnabled = ObjectLockEnabled.Enabled,
				Rule = new ObjectLockRule()
				{
					DefaultRetention = new DefaultRetention()
					{
						Mode = ObjectLockRetentionMode.Governance,
						Days = 1,
					}
				}
			};
			Client.PutObjectLockConfiguration(bucketName, Conf);

			var Response = Client.GetObjectLockConfiguration(bucketName);
			LockCompare(Conf, Response.ObjectLockConfiguration);
		}

		[Fact]
		[Trait(MainData.Major, "Lock")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "버킷을 Lock옵션을 활성화 하지않을 경우 lock 설정 조회 실패")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_object_lock_get_obj_lock_invalid_bucket()
		{
			var bucketName = GetNewBucketName();
			var Client = GetClient();
			Client.PutBucket(bucketName);

			var e = Assert.Throws<AggregateException>(() => Client.GetObjectLockConfiguration(bucketName));
			Assert.Equal(HttpStatusCode.NotFound, GetStatus(e));
			Assert.Equal(MainData.OBJECT_LOCK_CONFIGURATION_NOT_FOUND_ERROR, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "Lock")]
		[Trait(MainData.Minor, "Retention")]
		[Trait(MainData.Explanation, "[버킷의 Lock옵션을 활성화] 오브젝트에 Lock 유지기한 설정이 가능한지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_object_lock_put_obj_retention()
		{
			var bucketName = GetNewBucketName();
			var Client = GetClient();
			Client.PutBucket(bucketName, objectLockEnabledForBucket: true);
			var Key = "file1";

			var PutResponse = Client.PutObject(bucketName, Key, body: "abc");
			var VersionId = PutResponse.VersionId;

			var Retention = new ObjectLockRetention()
			{
				Mode = ObjectLockRetentionMode.Governance,
				RetainUntilDate = new DateTime(2030, 1, 1, 0, 0, 0, DateTimeKind.Utc),
			};

			var Response = Client.PutObjectRetention(bucketName, Key, Retention);
			Assert.Equal(HttpStatusCode.OK, Response.HttpStatusCode);
			Client.DeleteObject(bucketName, Key, versionId: VersionId, bypassGovernanceRetention: true);
		}

		[Fact]
		[Trait(MainData.Major, "Lock")]
		[Trait(MainData.Minor, "Retention")]
		[Trait(MainData.Explanation, "버킷을 Lock옵션을 활성화 하지않을 경우 오브젝트에 Lock 유지기한 설정 실패")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_object_lock_put_obj_retention_invalid_bucket()
		{
			var bucketName = GetNewBucketName();
			var Client = GetClient();
			Client.PutBucket(bucketName);
			var Key = "file1";

			var PutResponse = Client.PutObject(bucketName, Key, body: "abc");

			var Retention = new ObjectLockRetention()
			{
				Mode = ObjectLockRetentionMode.Governance,
				RetainUntilDate = new DateTime(2030, 1, 1, 0, 0, 0, DateTimeKind.Utc),
			};

			var e = Assert.Throws<AggregateException>(() => Client.PutObjectRetention(bucketName, Key, Retention));
			Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
			Assert.Equal(MainData.INVALID_REQUEST, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "Lock")]
		[Trait(MainData.Minor, "Retention")]
		[Trait(MainData.Explanation, "[버킷의 Lock옵션을 활성화] 오브젝트에 Lock 유지기한 설정할때 Mode값이 올바르지 않을 경우 설정 실패")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_object_lock_put_obj_retention_invalid_mode()
		{
			var bucketName = GetNewBucketName();
			var Client = GetClient();
			Client.PutBucket(bucketName, objectLockEnabledForBucket: true);
			var Key = "file1";

			var PutResponse = Client.PutObject(bucketName, Key, body: "abc");

			var Retention = new ObjectLockRetention()
			{
				Mode = new ObjectLockRetentionMode("abc"),
				RetainUntilDate = new DateTime(2030, 1, 1, 0, 0, 0, DateTimeKind.Utc),
			};

			var e = Assert.Throws<AggregateException>(() => Client.PutObjectRetention(bucketName, Key, Retention));
			Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
			Assert.Equal(MainData.MALFORMED_XML, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "Lock")]
		[Trait(MainData.Minor, "Retention")]
		[Trait(MainData.Explanation, "[버킷의 Lock옵션을 활성화] 오브젝트에 Lock 유지기한 설정이 올바른지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_object_lock_get_obj_retention()
		{
			var bucketName = GetNewBucketName();
			var Client = GetClient();
			Client.PutBucket(bucketName, objectLockEnabledForBucket: true);
			var Key = "file1";

			var PutResponse = Client.PutObject(bucketName, Key, body: "abc");
			var VersionId = PutResponse.VersionId;

			var Retention = new ObjectLockRetention()
			{
				Mode = ObjectLockRetentionMode.Governance,
				RetainUntilDate = new DateTime(2030, 1, 1, 0, 0, 0, DateTimeKind.Utc),
			};

			Client.PutObjectRetention(bucketName, Key, Retention);
			var Response = Client.GetObjectRetention(bucketName, Key);
			RetentionCompare(Retention, Response.Retention);
			Client.DeleteObject(bucketName, Key, versionId: VersionId, bypassGovernanceRetention: true);
		}

		[Fact]
		[Trait(MainData.Major, "Lock")]
		[Trait(MainData.Minor, "Retention")]
		[Trait(MainData.Explanation, "버킷을 Lock옵션을 활성화 하지않을 경우 오브젝트에 Lock 유지기한 조회 실패")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_object_lock_get_obj_retention_invalid_bucket()
		{
			var bucketName = GetNewBucketName();
			var Client = GetClient();
			Client.PutBucket(bucketName);
			var Key = "file1";

			var PutResponse = Client.PutObject(bucketName, Key, body: "abc");

			var e = Assert.Throws<AggregateException>(() => Client.GetObjectRetention(bucketName, Key));
			Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
			Assert.Equal(MainData.INVALID_REQUEST, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "Lock")]
		[Trait(MainData.Minor, "Retention")]
		[Trait(MainData.Explanation, "[버킷의 Lock옵션을 활성화] " +
									 "오브젝트의 특정 버전에 Lock 유지기한을 설정할 경우 올바르게 적용되었는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_object_lock_put_obj_retention_versionid()
		{
			var bucketName = GetNewBucketName();
			var Client = GetClient();
			Client.PutBucket(bucketName, objectLockEnabledForBucket: true);
			var Key = "file1";

			Client.PutObject(bucketName, Key, body: "abc");
			var PutResponse = Client.PutObject(bucketName, Key, body: "abc");
			var VersionId = PutResponse.VersionId;

			var Retention = new ObjectLockRetention()
			{
				Mode = ObjectLockRetentionMode.Governance,
				RetainUntilDate = new DateTime(2030, 1, 1, 0, 0, 0, DateTimeKind.Utc),
			};

			Client.PutObjectRetention(bucketName, Key, Retention);
			var Response = Client.GetObjectRetention(bucketName, Key);
			RetentionCompare(Retention, Response.Retention);
			Client.DeleteObject(bucketName, Key, versionId: VersionId, bypassGovernanceRetention: true);
		}

		[Fact]
		[Trait(MainData.Major, "Lock")]
		[Trait(MainData.Minor, "Priority")]
		[Trait(MainData.Explanation, "[버킷의 Lock옵션을 활성화] 버킷에 설정한 Lock설정보다 오브젝트에 Lock설정한 값이 우선 적용됨을 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_object_lock_put_obj_retention_override_default_retention()
		{
			var bucketName = GetNewBucketName();
			var Client = GetClient();
			Client.PutBucket(bucketName, objectLockEnabledForBucket: true);

			var Conf = new ObjectLockConfiguration()
			{
				ObjectLockEnabled = ObjectLockEnabled.Enabled,
				Rule = new ObjectLockRule()
				{
					DefaultRetention = new DefaultRetention()
					{
						Mode = ObjectLockRetentionMode.Governance,
						Days = 1,
					}
				}
			};
			Client.PutObjectLockConfiguration(bucketName, Conf);

			var Key = "file1";
			var body = "abc";
			var MD5 = GetMD5(body);
			var PutResponse = Client.PutObject(bucketName, Key, body: body, md5Digest: MD5);
			var VersionId = PutResponse.VersionId;

			var Retention = new ObjectLockRetention()
			{
				Mode = ObjectLockRetentionMode.Governance,
				RetainUntilDate = new DateTime(2030, 1, 1, 0, 0, 0, DateTimeKind.Utc),
			};

			Client.PutObjectRetention(bucketName, Key, Retention);
			var Response = Client.GetObjectRetention(bucketName, Key);
			RetentionCompare(Retention, Response.Retention);

			Client.DeleteObject(bucketName, Key, versionId: VersionId, bypassGovernanceRetention: true);
		}

		[Fact]
		[Trait(MainData.Major, "Lock")]
		[Trait(MainData.Minor, "Overwrite")]
		[Trait(MainData.Explanation, "[버킷의 Lock옵션을 활성화] 오브젝트의 lock 유지기한을 늘렸을때 적용되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_object_lock_put_obj_retention_increase_period()
		{
			var bucketName = GetNewBucketName();
			var Client = GetClient();
			Client.PutBucket(bucketName, objectLockEnabledForBucket: true);
			var Key = "file1";

			Client.PutObject(bucketName, Key, body: "abc");
			var PutResponse = Client.PutObject(bucketName, Key, body: "abc");
			var VersionId = PutResponse.VersionId;

			var Retention1 = new ObjectLockRetention()
			{
				Mode = ObjectLockRetentionMode.Governance,
				RetainUntilDate = new DateTime(2030, 1, 1, 0, 0, 0, DateTimeKind.Utc),
			};
			Client.PutObjectRetention(bucketName, Key, Retention1);

			var Retention2 = new ObjectLockRetention()
			{
				Mode = ObjectLockRetentionMode.Governance,
				RetainUntilDate = new DateTime(2030, 1, 3, 0, 0, 0, DateTimeKind.Utc),
			};
			Client.PutObjectRetention(bucketName, Key, Retention2);

			var Response = Client.GetObjectRetention(bucketName, Key);
			RetentionCompare(Retention2, Response.Retention);
			Client.DeleteObject(bucketName, Key, versionId: VersionId, bypassGovernanceRetention: true);
		}

		[Fact]
		[Trait(MainData.Major, "Lock")]
		[Trait(MainData.Minor, "Overwrite")]
		[Trait(MainData.Explanation, "[버킷의 Lock옵션을 활성화] 오브젝트의 lock 유지기한을 줄였을때 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_object_lock_put_obj_retention_shorten_period()
		{
			var bucketName = GetNewBucketName();
			var Client = GetClient();
			Client.PutBucket(bucketName, objectLockEnabledForBucket: true);
			var Key = "file1";

			Client.PutObject(bucketName, Key, body: "abc");
			var PutResponse = Client.PutObject(bucketName, Key, body: "abc");
			var VersionId = PutResponse.VersionId;

			var Retention1 = new ObjectLockRetention()
			{
				Mode = ObjectLockRetentionMode.Governance,
				RetainUntilDate = new DateTime(2030, 1, 3, 0, 0, 0, DateTimeKind.Utc),
			};
			Client.PutObjectRetention(bucketName, Key, Retention1);

			var Retention2 = new ObjectLockRetention()
			{
				Mode = ObjectLockRetentionMode.Governance,
				RetainUntilDate = new DateTime(2030, 1, 1, 0, 0, 0, DateTimeKind.Utc),
			};
			var e = Assert.Throws<AggregateException>(() => Client.PutObjectRetention(bucketName, Key, Retention2));
			Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));
			Assert.Equal(MainData.ACCESS_DENIED, GetErrorCode(e));

			Client.DeleteObject(bucketName, Key, versionId: VersionId, bypassGovernanceRetention: true);
		}

		[Fact]
		[Trait(MainData.Major, "Lock")]
		[Trait(MainData.Minor, "Overwrite")]
		[Trait(MainData.Explanation, "[버킷의 Lock옵션을 활성화] " +
									 "바이패스를 True로 설정하고 오브젝트의 lock 유지기한을 줄였을때 적용되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_object_lock_put_obj_retention_shorten_period_bypass()
		{
			var bucketName = GetNewBucketName();
			var Client = GetClient();
			Client.PutBucket(bucketName, objectLockEnabledForBucket: true);
			var Key = "file1";

			Client.PutObject(bucketName, Key, body: "abc");
			var PutResponse = Client.PutObject(bucketName, Key, body: "abc");
			var VersionId = PutResponse.VersionId;

			var Retention1 = new ObjectLockRetention()
			{
				Mode = ObjectLockRetentionMode.Governance,
				RetainUntilDate = new DateTime(2030, 1, 3, 0, 0, 0, DateTimeKind.Utc),
			};
			Client.PutObjectRetention(bucketName, Key, Retention1);

			var Retention2 = new ObjectLockRetention()
			{
				Mode = ObjectLockRetentionMode.Governance,
				RetainUntilDate = new DateTime(2030, 1, 1, 0, 0, 0, DateTimeKind.Utc),
			};
			Client.PutObjectRetention(bucketName, Key, Retention2, bypassGovernanceRetention: true);

			var Response = Client.GetObjectRetention(bucketName, Key);
			RetentionCompare(Retention2, Response.Retention);
			Client.DeleteObject(bucketName, Key, versionId: VersionId, bypassGovernanceRetention: true);
		}

		[Fact]
		[Trait(MainData.Major, "Lock")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "[버킷의 Lock옵션을 활성화] 오브젝트의 lock 유지기한내에 삭제를 시도할 경우 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_object_lock_delete_object_with_retention()
		{
			var bucketName = GetNewBucketName();
			var Client = GetClient();
			Client.PutBucket(bucketName, objectLockEnabledForBucket: true);
			var Key = "file1";

			var PutResponse = Client.PutObject(bucketName, Key, body: "abc");
			var VersionId = PutResponse.VersionId;

			var Retention = new ObjectLockRetention()
			{
				Mode = ObjectLockRetentionMode.Governance,
				RetainUntilDate = new DateTime(2030, 1, 1, 0, 0, 0, DateTimeKind.Utc),
			};
			Client.PutObjectRetention(bucketName, Key, Retention);

			var e = Assert.Throws<AggregateException>(() => Client.DeleteObject(bucketName, Key, versionId: VersionId));
			Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));
			Assert.Equal(MainData.ACCESS_DENIED, GetErrorCode(e));

			Client.DeleteObject(bucketName, Key, versionId: VersionId, bypassGovernanceRetention: true);
		}

		[Fact]
		[Trait(MainData.Major, "Lock")]
		[Trait(MainData.Minor, "LegalHold")]
		[Trait(MainData.Explanation, "[버킷의 Lock옵션을 활성화] 오브젝트의 LegalHold를 활성화 가능한지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_object_lock_put_legal_hold()
		{
			var bucketName = GetNewBucketName();
			var Client = GetClient();
			Client.PutBucket(bucketName, objectLockEnabledForBucket: true);

			var Key = "file1";
			Client.PutObject(bucketName, Key, body: "abc");

			var LegalHold = new ObjectLockLegalHold() { Status = ObjectLockLegalHoldStatus.On };
			var Response = Client.PutObjectLegalHold(bucketName, Key, LegalHold);
			Assert.Equal(HttpStatusCode.OK, Response.HttpStatusCode);

			LegalHold = new ObjectLockLegalHold() { Status = ObjectLockLegalHoldStatus.Off };
			Response = Client.PutObjectLegalHold(bucketName, Key, LegalHold);
			Assert.Equal(HttpStatusCode.OK, Response.HttpStatusCode);
		}

		[Fact]
		[Trait(MainData.Major, "Lock")]
		[Trait(MainData.Minor, "LegalHold")]
		[Trait(MainData.Explanation, "[버킷의 Lock옵션을 비활성화] 오브젝트의 LegalHold를 활성화 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_object_lock_put_legal_hold_invalid_bucket()
		{
			var bucketName = GetNewBucketName();
			var Client = GetClient();
			Client.PutBucket(bucketName);

			var Key = "file1";
			Client.PutObject(bucketName, Key, body: "abc");

			var LegalHold = new ObjectLockLegalHold() { Status = ObjectLockLegalHoldStatus.On };
			var e = Assert.Throws<AggregateException>(() => Client.PutObjectLegalHold(bucketName, Key, LegalHold));
			Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
			Assert.Equal(MainData.INVALID_REQUEST, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "Lock")]
		[Trait(MainData.Minor, "LegalHold")]
		[Trait(MainData.Explanation, "[버킷의 Lock옵션을 비활성화] 오브젝트의 LegalHold를 활성화 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_object_lock_put_legal_hold_invalid_status()
		{
			var bucketName = GetNewBucketName();
			var Client = GetClient();
			Client.PutBucket(bucketName, objectLockEnabledForBucket: true);

			var Key = "file1";
			Client.PutObject(bucketName, Key, body: "abc");

			var LegalHold = new ObjectLockLegalHold() { Status = new ObjectLockLegalHoldStatus("abc") };
			var e = Assert.Throws<AggregateException>(() => Client.PutObjectLegalHold(bucketName, Key, LegalHold));
			Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
			Assert.Equal(MainData.MALFORMED_XML, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "Lock")]
		[Trait(MainData.Minor, "LegalHold")]
		[Trait(MainData.Explanation, "[버킷의 Lock옵션을 활성화] 오브젝트의 LegalHold가 올바르게 적용되었는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_object_lock_get_legal_hold()
		{
			var bucketName = GetNewBucketName();
			var Client = GetClient();
			Client.PutBucket(bucketName, objectLockEnabledForBucket: true);

			var Key = "file1";
			Client.PutObject(bucketName, Key, body: "abc");

			var LegalHold = new ObjectLockLegalHold() { Status = ObjectLockLegalHoldStatus.On };
			Client.PutObjectLegalHold(bucketName, Key, LegalHold);
			var Response = Client.GetObjectLegalHold(bucketName, Key);
			Assert.Equal(LegalHold.Status, Response.LegalHold.Status);

			LegalHold = new ObjectLockLegalHold() { Status = ObjectLockLegalHoldStatus.Off };
			Client.PutObjectLegalHold(bucketName, Key, LegalHold);
			Response = Client.GetObjectLegalHold(bucketName, Key);
			Assert.Equal(LegalHold.Status, Response.LegalHold.Status);
		}

		[Fact]
		[Trait(MainData.Major, "Lock")]
		[Trait(MainData.Minor, "LegalHold")]
		[Trait(MainData.Explanation, "[버킷의 Lock옵션을 활성화] 오브젝트의 LegalHold설정 조회 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_object_lock_get_legal_hold_invalid_bucket()
		{
			var bucketName = GetNewBucketName();
			var Client = GetClient();
			Client.PutBucket(bucketName);

			var Key = "file1";
			Client.PutObject(bucketName, Key, body: "abc");

			var e = Assert.Throws<AggregateException>(() => Client.GetObjectLegalHold(bucketName, Key));
			Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
			Assert.Equal(MainData.INVALID_REQUEST, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "Lock")]
		[Trait(MainData.Minor, "LegalHold")]
		[Trait(MainData.Explanation, "[버킷의 Lock옵션을 활성화] 오브젝트의 LegalHold가 활성화되어 있을 경우 오브젝트 삭제 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_object_lock_delete_object_with_legal_hold_on()
		{
			var bucketName = GetNewBucketName();
			var Client = GetClient();
			Client.PutBucket(bucketName, objectLockEnabledForBucket: true);

			var Key = "file1";
			var PutResponse = Client.PutObject(bucketName, Key, body: "abc");

			var LegalHold = new ObjectLockLegalHold() { Status = ObjectLockLegalHoldStatus.On };
			Client.PutObjectLegalHold(bucketName, Key, LegalHold);

			var e = Assert.Throws<AggregateException>(() => Client.DeleteObject(bucketName, Key, versionId: PutResponse.VersionId));
			Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));
			Assert.Equal(MainData.ACCESS_DENIED, GetErrorCode(e));

			LegalHold = new ObjectLockLegalHold() { Status = ObjectLockLegalHoldStatus.Off };
			Client.PutObjectLegalHold(bucketName, Key, LegalHold);
		}

		[Fact]
		[Trait(MainData.Major, "Lock")]
		[Trait(MainData.Minor, "LegalHold")]
		[Trait(MainData.Explanation, "[버킷의 Lock옵션을 활성화] 오브젝트의 LegalHold가 비활성화되어 있을 경우 오브젝트 삭제 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestObjectLockDeleteObjectWithLegalHoldOff()
		{
			var bucketName = GetNewBucketName();
			var Client = GetClient();
			Client.PutBucket(bucketName, objectLockEnabledForBucket: true);

			var Key = "file1";
			var PutResponse = Client.PutObject(bucketName, Key, body: "abc");

			var LegalHold = new ObjectLockLegalHold() { Status = ObjectLockLegalHoldStatus.Off };
			Client.PutObjectLegalHold(bucketName, Key, LegalHold);

			var Response = Client.DeleteObject(bucketName, Key, versionId: PutResponse.VersionId);
			Assert.Equal(HttpStatusCode.NoContent, Response.HttpStatusCode);
		}

		[Fact]
		[Trait(MainData.Major, "Lock")]
		[Trait(MainData.Minor, "LegalHold")]
		[Trait(MainData.Explanation, "[버킷의 Lock옵션을 활성화] " +
									 "오브젝트의 LegalHold와 Lock유지기한 설정이 모두 적용되는지 메타데이터를 통해 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestObjectLockGetObjMetadata()
		{
			var bucketName = GetNewBucketName();
			var Client = GetClient();
			Client.PutBucket(bucketName, objectLockEnabledForBucket: true);

			var Key = "file1";
			var PutResponse = Client.PutObject(bucketName, Key, body: "abc");

			var LegalHold = new ObjectLockLegalHold() { Status = ObjectLockLegalHoldStatus.On };
			Client.PutObjectLegalHold(bucketName, Key, LegalHold);


			var Retention = new ObjectLockRetention()
			{
				Mode = ObjectLockRetentionMode.Governance,
				RetainUntilDate = new DateTime(2030, 1, 1, 0, 0, 0, DateTimeKind.Utc),
			};
			Client.PutObjectRetention(bucketName, Key, Retention);

			var Response = Client.GetObjectMetadata(bucketName, Key);
			Assert.Equal(Retention.Mode, Response.ObjectLockMode);
			Assert.Equal(Retention.RetainUntilDate, Response.ObjectLockRetainUntilDate.ToUniversalTime());
			Assert.Equal(LegalHold.Status, Response.ObjectLockLegalHoldStatus);

			LegalHold = new ObjectLockLegalHold() { Status = ObjectLockLegalHoldStatus.Off };
			Client.PutObjectLegalHold(bucketName, Key, LegalHold);
			Client.DeleteObject(bucketName, Key, versionId: PutResponse.VersionId, bypassGovernanceRetention: true);
		}
	}
}
