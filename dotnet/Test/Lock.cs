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
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using s3tests.Utils;
using System;
using System.Net;
using Xunit;

namespace s3tests.Test
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
			TestId = 2;
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
			TestId = 3;
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
		public void TestObjectLockPutObjLockWithDaysAndYears()
		{
			TestId = 4;
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
		public void TestObjectLockPutObjLockInvalidDays()
		{
			TestId = 5;
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
		public void TestObjectLockPutObjLockInvalidYears()
		{
			TestId = 6;
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
		public void TestObjectLockPutObjLockInvalidMode()
		{
			TestId = 7;
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
		public void TestObjectLockPutObjLockInvalidStatus()
		{
			TestId = 8;
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
		public void TestObjectLockSuspendVersioning()
		{
			TestId = 9;
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
		public void TestObjectLockGetObjLock()
		{
			TestId = 10;
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
		public void TestObjectLockGetObjLockInvalidBucket()
		{
			TestId = 15;
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
		public void TestObjectLockPutObjRetention()
		{
			TestId = 16;
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
		public void TestObjectLockPutObjRetentionInvalidBucket()
		{
			TestId = 17;
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
		public void TestObjectLockPutObjRetentionInvalidMode()
		{
			TestId = 18;
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
		public void TestObjectLockGetObjRetention()
		{
			TestId = 19;
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
		public void TestObjectLockGetObjRetentionInvalidBucket()
		{
			TestId = 20;
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
		public void TestObjectLockPutObjRetentionVersionid()
		{
			TestId = 21;
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
		public void TestObjectLockPutObjRetentionOverrideDefaultRetention()
		{
			TestId = 22;
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
			var MD5 = S3Utils.GetMD5(body);
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
		public void TestObjectLockPutObjRetentionIncreasePeriod()
		{
			TestId = 23;
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
		public void TestObjectLockPutObjRetentionShortenPeriod()
		{
			TestId = 24;
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
		public void TestObjectLockPutObjRetentionShortenPeriodBypass()
		{
			TestId = 25;
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
		public void TestObjectLockDeleteObjectWithRetention()
		{
			TestId = 26;
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
		public void TestObjectLockPutLegalHold()
		{
			TestId = 29;
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
		public void TestObjectLockPutLegalHoldInvalidBucket()
		{
			TestId = 30;
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
		public void TestObjectLockPutLegalHoldInvalidStatus()
		{
			TestId = 31;
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
		public void TestObjectLockGetLegalHold()
		{
			TestId = 32;
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
		public void TestObjectLockGetLegalHoldInvalidBucket()
		{
			TestId = 33;
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
		public void TestObjectLockDeleteObjectWithLegalHoldOn()
		{
			TestId = 34;
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
			TestId = 35;
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
			TestId = 36;
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
			Assert.Equal(Retention.RetainUntilDate, Response.ObjectLockRetainUntilDate.Value);
			Assert.Equal(LegalHold.Status, Response.ObjectLockLegalHoldStatus);

			LegalHold = new ObjectLockLegalHold() { Status = ObjectLockLegalHoldStatus.Off };
			Client.PutObjectLegalHold(bucketName, Key, LegalHold);
			Client.DeleteObject(bucketName, Key, versionId: PutResponse.VersionId, bypassGovernanceRetention: true);
		}

		private static ObjectLockConfiguration GovernanceLockConfig(int days = 1) => new()
		{
			ObjectLockEnabled = ObjectLockEnabled.Enabled,
			Rule = new ObjectLockRule() { DefaultRetention = new DefaultRetention() { Mode = ObjectLockRetentionMode.Governance, Days = days } }
		};

		private static ObjectLockRetention GovernanceRetention(DateTime date) => new()
		{
			Mode = ObjectLockRetentionMode.Governance,
			RetainUntilDate = date,
		};

		[Fact]
		[Trait(MainData.Major, "Lock")]
		[Trait(MainData.Minor, "Put")]
		[Trait(MainData.Explanation, "버킷 생성 후 버저닝을 활성화하고 오브젝트 잠금을 설정할 수 있는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestCreatedBucketEnableObjectLock()
		{
			TestId = 1;
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			client.PutBucketVersioning(bucketName, status: VersionStatus.Enabled);
			var response = client.PutObjectLockConfiguration(bucketName,
				new ObjectLockConfiguration() { ObjectLockEnabled = ObjectLockEnabled.Enabled });
			Assert.Equal(HttpStatusCode.OK, response.HttpStatusCode);
		}

		[Fact]
		[Trait(MainData.Major, "Lock")]
		[Trait(MainData.Minor, "Check")]
		[Trait(MainData.Explanation, "[버킷의 Lock옵션을 활성화] 잠금이 설정된 오브젝트가 유지기한 내에 삭제되지 않는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestObjectLockPutObject()
		{
			TestId = 11;
			var client = GetClient();
			var bucketName = GetNewBucketName(false);
			var key = "testObjectLockPutObject";
			client.PutBucket(bucketName, objectLockEnabledForBucket: true);

			client.PutObjectLockConfiguration(bucketName, GovernanceLockConfig());

			client.PutObject(bucketName, key, body: key, md5Digest: S3Utils.GetMD5(key));

			var response = client.GetObjectMetadata(bucketName, key);
			Assert.Equal(ObjectLockMode.Governance, response.ObjectLockMode);
			Assert.NotNull(response.ObjectLockRetainUntilDate);

			var e = Assert.Throws<AggregateException>(() => client.DeleteObject(bucketName, key, versionId: response.VersionId));
			Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));
			Assert.Equal(MainData.ACCESS_DENIED, GetErrorCode(e));

			client.DeleteObject(bucketName, key, versionId: response.VersionId, bypassGovernanceRetention: true);
		}

		[Fact]
		[Trait(MainData.Major, "Lock")]
		[Trait(MainData.Minor, "Check")]
		[Trait(MainData.Explanation, "[버킷의 Lock옵션을 활성화] 잠금이 설정된 오브젝트를 복사할 때 잠금 정보가 올바르게 처리되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestObjectLockCopyObject()
		{
			TestId = 12;
			var client = GetClient();
			var bucketName = GetNewBucketName(false);
			var bucketName2 = GetNewBucketName(false);
			var key = "testObjectLockCopyObject-lock";
			var keyCopy = key + "-copy";
			var key2 = "testObjectLockCopyObject";
			var key2Copy = key2 + "-copy";

			client.PutBucket(bucketName, objectLockEnabledForBucket: true);
			client.PutBucket(bucketName2, objectLockEnabledForBucket: true);

			client.PutObjectLockConfiguration(bucketName, GovernanceLockConfig());

			client.PutObject(bucketName, key, body: key, md5Digest: S3Utils.GetMD5(key));
			client.PutObject(bucketName2, key2, body: key2);

			client.CopyObject(bucketName, key, bucketName2, keyCopy);
			client.CopyObject(bucketName2, key2, bucketName, key2Copy);

			var response = client.GetObjectMetadata(bucketName, key);
			Assert.Equal(ObjectLockMode.Governance, response.ObjectLockMode);
			Assert.NotNull(response.ObjectLockRetainUntilDate);

			var response2 = client.GetObjectMetadata(bucketName2, keyCopy);
			Assert.Null(response2.ObjectLockMode);
			Assert.Null(response2.ObjectLockRetainUntilDate);

			var response3 = client.GetObjectMetadata(bucketName2, key2);
			Assert.Null(response3.ObjectLockMode);
			Assert.Null(response3.ObjectLockRetainUntilDate);

			var response4 = client.GetObjectMetadata(bucketName, key2Copy);
			Assert.Equal(ObjectLockMode.Governance, response4.ObjectLockMode);
			Assert.NotNull(response4.ObjectLockRetainUntilDate);

			var e = Assert.Throws<AggregateException>(() => client.DeleteObject(bucketName, key, versionId: response.VersionId));
			Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));
			Assert.Equal(MainData.ACCESS_DENIED, GetErrorCode(e));

			var e2 = Assert.Throws<AggregateException>(() => client.DeleteObject(bucketName, key2Copy, versionId: response4.VersionId));
			Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e2));
			Assert.Equal(MainData.ACCESS_DENIED, GetErrorCode(e2));

			client.DeleteObject(bucketName, key, versionId: response.VersionId, bypassGovernanceRetention: true);
			client.DeleteObject(bucketName2, keyCopy, versionId: response2.VersionId, bypassGovernanceRetention: true);
			client.DeleteObject(bucketName2, key2, versionId: response3.VersionId, bypassGovernanceRetention: true);
			client.DeleteObject(bucketName, key2Copy, versionId: response4.VersionId, bypassGovernanceRetention: true);
		}

		[Fact]
		[Trait(MainData.Major, "Lock")]
		[Trait(MainData.Minor, "Check")]
		[Trait(MainData.Explanation, "[버킷의 Lock옵션을 활성화] 멀티파트로 업로드한 오브젝트에 잠금이 적용되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestObjectLockMultipart()
		{
			TestId = 13;
			// Object Lock 버킷의 UploadPart는 Content-MD5 또는 x-amz-checksum-* 헤더가 필수다.
			// 기본 클라이언트(WHEN_REQUIRED)는 이를 붙이지 않으므로 SDK가 체크섬을 넣도록 WHEN_SUPPORTED를 쓴다.
			var client = GetClient(RequestChecksumCalculation.WHEN_SUPPORTED, ResponseChecksumValidation.WHEN_REQUIRED);
			var bucketName = GetNewBucketName(false);
			var key = "testObjectLockMultipart";
			client.PutBucket(bucketName, objectLockEnabledForBucket: true);

			client.PutObjectLockConfiguration(bucketName, GovernanceLockConfig());

			var uploadData = S3Utils.SetupMultipartUpload(client, bucketName, key, 1 * MainData.MB);
			client.CompleteMultipartUpload(bucketName, key, uploadData.UploadId, uploadData.Parts);

			var response = client.GetObjectMetadata(bucketName, key);
			Assert.Equal(ObjectLockMode.Governance, response.ObjectLockMode);
			Assert.NotNull(response.ObjectLockRetainUntilDate);

			var e = Assert.Throws<AggregateException>(() => client.DeleteObject(bucketName, key, versionId: response.VersionId));
			Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));
			Assert.Equal(MainData.ACCESS_DENIED, GetErrorCode(e));

			client.DeleteObject(bucketName, key, versionId: response.VersionId, bypassGovernanceRetention: true);
		}

		[Fact]
		[Trait(MainData.Major, "Lock")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "[버킷의 Lock옵션을 활성화] Content-MD5 없이 오브젝트/파트 업로드 시 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestObjectLockMD5()
		{
			TestId = 14;
			var client = GetClient();
			var bucketName = GetNewBucketName(false);
			var key = "testObjectLockMD5";
			var content = S3Utils.RandomTextToLong(1 * MainData.MB);
			client.PutBucket(bucketName, objectLockEnabledForBucket: true);

			client.PutObjectLockConfiguration(bucketName, GovernanceLockConfig());

			var e = Assert.Throws<AggregateException>(() => client.PutObject(bucketName, key, body: content));
			Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
			Assert.Equal(MainData.INVALID_REQUEST, GetErrorCode(e));

			var initResponse = client.InitiateMultipartUpload(bucketName, key);
			var uploadId = initResponse.UploadId;

			var e2 = Assert.Throws<AggregateException>(() => client.UploadPart(bucketName, key, uploadId, content, 1));
			Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e2));
			Assert.Equal(MainData.INVALID_REQUEST, GetErrorCode(e2));

			client.AbortMultipartUpload(bucketName, key, uploadId);
		}

		[Fact]
		[Trait(MainData.Major, "Lock")]
		[Trait(MainData.Minor, "Retention")]
		[Trait(MainData.Explanation, "[버킷의 Lock옵션을 활성화] 유지기한이 설정된 오브젝트를 bypass 옵션으로 삭제 가능한지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestObjectLockDeleteObjectWithRetentionBypass()
		{
			TestId = 27;
			var key = "file1";
			var client = GetClient();
			var bucketName = GetNewBucketName(false);
			client.PutBucket(bucketName, objectLockEnabledForBucket: true);

			var putResponse = client.PutObject(bucketName, key, body: key);
			var versionId = putResponse.VersionId;

			client.PutObjectRetention(bucketName, key, GovernanceRetention(new DateTime(2030, 1, 1, 0, 0, 0, DateTimeKind.Utc)));

			client.DeleteObject(bucketName, key, versionId: versionId, bypassGovernanceRetention: true);
		}

		[Fact]
		[Trait(MainData.Major, "Lock")]
		[Trait(MainData.Minor, "Retention")]
		[Trait(MainData.Explanation, "[버킷의 Lock옵션을 활성화] 유지기한이 설정된 여러 오브젝트를 bypass 옵션으로 일괄 삭제 가능한지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestObjectLockDeleteObjectsWithRetentionBypass()
		{
			TestId = 28;
			var client = GetClient();
			var bucketName = GetNewBucketName(false);
			var keyVersions = new System.Collections.Generic.List<KeyVersion>();
			client.PutBucket(bucketName, objectLockEnabledForBucket: true);

			for (int i = 0; i < 10; i++)
			{
				var key = string.Format("testObjectLockDeleteObjectsWithRetentionBypass-{0:D3}", i);
				var putResponse = client.PutObject(bucketName, key, body: key);
				var versionId = putResponse.VersionId;

				client.PutObjectRetention(bucketName, key, GovernanceRetention(new DateTime(2030, 1, 1, 0, 0, 0, DateTimeKind.Utc)), versionId: versionId);

				keyVersions.Add(new KeyVersion() { Key = key, VersionId = versionId });
			}

			client.DeleteObjects(bucketName, keyVersions, bypassGovernanceRetention: true);
		}
	}
}
