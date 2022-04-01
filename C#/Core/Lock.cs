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

        [Fact(DisplayName = "test_object_lock_put_obj_lock")]
        [Trait(MainData.Major, "Lock")]
        [Trait(MainData.Minor, "Check")]
        [Trait(MainData.Explanation, "[버킷의 Lock옵션을 활성화] 오브젝트의 잠금 설정이 가능한지 확인")]
        [Trait(MainData.Result, MainData.ResultSuccess)]
        public void test_object_lock_put_obj_lock()
        {
            var BucketName = GetNewBucketName();
            var Client = GetClient();
            Client.PutBucket(BucketName, ObjectLockEnabledForBucket: true);

            var Conf = new ObjectLockConfiguration()
            {
                ObjectLockEnabled = ObjectLockEnabled.Enabled,
                Rule = new ObjectLockRule() {
                    DefaultRetention = new DefaultRetention()
                    {
                        Mode = ObjectLockRetentionMode.Compliance,
                        Years = 1,
                    }
                }
            };
            var Response = Client.PutObjectLockConfiguration(BucketName, Conf);
            Assert.Equal(HttpStatusCode.OK, Response.HttpStatusCode);

            var VersionResponse = Client.GetBucketVersioning(BucketName);
            Assert.Equal(VersionStatus.Enabled, VersionResponse.VersioningConfig.Status);
        }

        [Fact(DisplayName = "test_object_lock_put_obj_lock_invalid_bucket")]
        [Trait(MainData.Major, "Lock")]
        [Trait(MainData.Minor, "ERROR")]
        [Trait(MainData.Explanation, "버킷을 Lock옵션을 활성화 하지않을 경우 lock 설정이 실패")]
        [Trait(MainData.Result, MainData.ResultFailure)]
        public void test_object_lock_put_obj_lock_invalid_bucket()
        {
            var BucketName = GetNewBucketName();
            var Client = GetClient();
            Client.PutBucket(BucketName);

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
            var e = Assert.Throws<AggregateException>(() => Client.PutObjectLockConfiguration(BucketName, Conf));
            Assert.Equal(HttpStatusCode.Conflict, GetStatus(e));
            Assert.Equal(MainData.InvalidBucketState, GetErrorCode(e));
        }

        [Fact(DisplayName = "test_object_lock_put_obj_lock_with_days_and_years")]
        [Trait(MainData.Major, "Lock")]
        [Trait(MainData.Minor, "ERROR")]
        [Trait(MainData.Explanation, "[버킷의 Lock옵션을 활성화] Days, Years값 모두 입력하여 Lock 설정할경우 실패")]
        [Trait(MainData.Result, MainData.ResultFailure)]
        public void test_object_lock_put_obj_lock_with_days_and_years()
        {
            var BucketName = GetNewBucketName();
            var Client = GetClient();
            Client.PutBucket(BucketName, ObjectLockEnabledForBucket: true);

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
            var e = Assert.Throws<AggregateException>(() => Client.PutObjectLockConfiguration(BucketName, Conf));
            Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
            Assert.Equal(MainData.MalformedXML, GetErrorCode(e));
        }

        [Fact(DisplayName = "test_object_lock_put_obj_lock_invalid_days")]
        [Trait(MainData.Major, "Lock")]
        [Trait(MainData.Minor, "ERROR")]
        [Trait(MainData.Explanation, "[버킷의 Lock옵션을 활성화] Days값을 0이하로 입력하여 Lock 설정할경우 실패")]
        [Trait(MainData.Result, MainData.ResultFailure)]
        public void test_object_lock_put_obj_lock_invalid_days()
        {
            var BucketName = GetNewBucketName();
            var Client = GetClient();
            Client.PutBucket(BucketName, ObjectLockEnabledForBucket: true);

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
            var e = Assert.Throws<AggregateException>(() => Client.PutObjectLockConfiguration(BucketName, Conf));
            Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
            Assert.Equal(MainData.InvalidArgument, GetErrorCode(e));
        }

        [Fact(DisplayName = "test_object_lock_put_obj_lock_invalid_years")]
        [Trait(MainData.Major, "Lock")]
        [Trait(MainData.Minor, "ERROR")]
        [Trait(MainData.Explanation, "[버킷의 Lock옵션을 활성화] Years값을 0이하로 입력하여 Lock 설정할경우 실패")]
        [Trait(MainData.Result, MainData.ResultFailure)]
        public void test_object_lock_put_obj_lock_invalid_years()
        {
            var BucketName = GetNewBucketName();
            var Client = GetClient();
            Client.PutBucket(BucketName, ObjectLockEnabledForBucket: true);

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
            var e = Assert.Throws<AggregateException>(() => Client.PutObjectLockConfiguration(BucketName, Conf));
            Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
            Assert.Equal(MainData.InvalidArgument, GetErrorCode(e));
        }

        [Fact(DisplayName = "test_object_lock_put_obj_lock_invalid_mode")]
        [Trait(MainData.Major, "Lock")]
        [Trait(MainData.Minor, "ERROR")]
        [Trait(MainData.Explanation, "[버킷의 Lock옵션을 활성화] mode값이 올바르지 않은상태에서 Lock 설정할 경우 실패")]
        [Trait(MainData.Result, MainData.ResultFailure)]
        public void test_object_lock_put_obj_lock_invalid_mode()
        {
            var BucketName = GetNewBucketName();
            var Client = GetClient();
            Client.PutBucket(BucketName, ObjectLockEnabledForBucket: true);

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
            var e = Assert.Throws<AggregateException>(() => Client.PutObjectLockConfiguration(BucketName, Conf));
            Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
            Assert.Equal(MainData.MalformedXML, GetErrorCode(e));
        }

        [Fact(DisplayName = "test_object_lock_put_obj_lock_invalid_status")]
        [Trait(MainData.Major, "Lock")]
        [Trait(MainData.Minor, "ERROR")]
        [Trait(MainData.Explanation, "[버킷의 Lock옵션을 활성화] status값이 올바르지 않은상태에서 Lock 설정할 경우 실패")]
        [Trait(MainData.Result, MainData.ResultFailure)]
        public void test_object_lock_put_obj_lock_invalid_status()
        {
            var BucketName = GetNewBucketName();
            var Client = GetClient();
            Client.PutBucket(BucketName, ObjectLockEnabledForBucket: true);

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
            var e = Assert.Throws<AggregateException>(() => Client.PutObjectLockConfiguration(BucketName, Conf));
            Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
            Assert.Equal(MainData.MalformedXML, GetErrorCode(e));
        }

        [Fact(DisplayName = "test_object_lock_suspend_versioning")]
        [Trait(MainData.Major, "Lock")]
        [Trait(MainData.Minor, "Version")]
        [Trait(MainData.Explanation, "[버킷의 Lock옵션을 활성화] 버킷의 버저닝을 일시중단하려고 할경우 실패")]
        [Trait(MainData.Result, MainData.ResultFailure)]
        public void test_object_lock_suspend_versioning()
        {
            var BucketName = GetNewBucketName();
            var Client = GetClient();
            Client.PutBucket(BucketName, ObjectLockEnabledForBucket: true);

            var e = Assert.Throws<AggregateException>(() => Client.PutBucketVersioning(BucketName, Status: VersionStatus.Suspended));
            Assert.Equal(HttpStatusCode.Conflict, GetStatus(e));
            Assert.Equal(MainData.InvalidBucketState, GetErrorCode(e));
        }

        [Fact(DisplayName = "test_object_lock_get_obj_lock")]
        [Trait(MainData.Major, "Lock")]
        [Trait(MainData.Minor, "Check")]
        [Trait(MainData.Explanation, "[버킷의 Lock옵션을 활성화] 버킷의 lock설정이 올바르게 되었는지 확인")]
        [Trait(MainData.Result, MainData.ResultSuccess)]
        public void test_object_lock_get_obj_lock()
        {
            var BucketName = GetNewBucketName();
            var Client = GetClient();
            Client.PutBucket(BucketName, ObjectLockEnabledForBucket: true);

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
            Client.PutObjectLockConfiguration(BucketName, Conf);

            var Response = Client.GetObjectLockConfiguration(BucketName);
            LockCompare(Conf, Response.ObjectLockConfiguration);
        }

        [Fact(DisplayName = "test_object_lock_get_obj_lock_invalid_bucket")]
        [Trait(MainData.Major, "Lock")]
        [Trait(MainData.Minor, "ERROR")]
        [Trait(MainData.Explanation, "버킷을 Lock옵션을 활성화 하지않을 경우 lock 설정 조회 실패")]
        [Trait(MainData.Result, MainData.ResultFailure)]
        public void test_object_lock_get_obj_lock_invalid_bucket()
        {
            var BucketName = GetNewBucketName();
            var Client = GetClient();
            Client.PutBucket(BucketName);

            var e = Assert.Throws<AggregateException>(() => Client.GetObjectLockConfiguration(BucketName));
            Assert.Equal(HttpStatusCode.NotFound, GetStatus(e));
            Assert.Equal(MainData.ObjectLockConfigurationNotFoundError, GetErrorCode(e));
        }

        [Fact(DisplayName = "test_object_lock_put_obj_retention")]
        [Trait(MainData.Major, "Lock")]
        [Trait(MainData.Minor, "Retention")]
        [Trait(MainData.Explanation, "[버킷의 Lock옵션을 활성화] 오브젝트에 Lock 유지기한 설정이 가능한지 확인")]
        [Trait(MainData.Result, MainData.ResultSuccess)]
        public void test_object_lock_put_obj_retention()
        {
            var BucketName = GetNewBucketName();
            var Client = GetClient();
            Client.PutBucket(BucketName, ObjectLockEnabledForBucket: true);
            var Key = "file1";

            var PutResponse = Client.PutObject(BucketName, Key, Body: "abc");
            var VersionId = PutResponse.VersionId;

            var Retention = new ObjectLockRetention()
            {
                Mode = ObjectLockRetentionMode.Governance,
                RetainUntilDate = new DateTime(2030, 1, 1, 0, 0, 0, DateTimeKind.Utc),
            };

            var Response = Client.PutObjectRetention(BucketName, Key, Retention);
            Assert.Equal(HttpStatusCode.OK, Response.HttpStatusCode);
            Client.DeleteObject(BucketName, Key, VersionId: VersionId, BypassGovernanceRetention: true);
        }

        [Fact(DisplayName = "test_object_lock_put_obj_retention_invalid_bucket")]
        [Trait(MainData.Major, "Lock")]
        [Trait(MainData.Minor, "Retention")]
        [Trait(MainData.Explanation, "버킷을 Lock옵션을 활성화 하지않을 경우 오브젝트에 Lock 유지기한 설정 실패")]
        [Trait(MainData.Result, MainData.ResultFailure)]
        public void test_object_lock_put_obj_retention_invalid_bucket()
        {
            var BucketName = GetNewBucketName();
            var Client = GetClient();
            Client.PutBucket(BucketName);
            var Key = "file1";

            var PutResponse = Client.PutObject(BucketName, Key, Body: "abc");

            var Retention = new ObjectLockRetention()
            {
                Mode = ObjectLockRetentionMode.Governance,
                RetainUntilDate = new DateTime(2030, 1, 1, 0, 0, 0, DateTimeKind.Utc),
            };

            var e = Assert.Throws<AggregateException>(() => Client.PutObjectRetention(BucketName, Key, Retention));
            Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
            Assert.Equal(MainData.InvalidRequest, GetErrorCode(e));
        }

        [Fact(DisplayName = "test_object_lock_put_obj_retention_invalid_mode")]
        [Trait(MainData.Major, "Lock")]
        [Trait(MainData.Minor, "Retention")]
        [Trait(MainData.Explanation, "[버킷의 Lock옵션을 활성화] 오브젝트에 Lock 유지기한 설정할때 Mode값이 올바르지 않을 경우 설정 실패")]
        [Trait(MainData.Result, MainData.ResultFailure)]
        public void test_object_lock_put_obj_retention_invalid_mode()
        {
            var BucketName = GetNewBucketName();
            var Client = GetClient();
            Client.PutBucket(BucketName, ObjectLockEnabledForBucket: true);
            var Key = "file1";

            var PutResponse = Client.PutObject(BucketName, Key, Body: "abc");

            var Retention = new ObjectLockRetention()
            {
                Mode = new ObjectLockRetentionMode("abc"),
                RetainUntilDate = new DateTime(2030, 1, 1, 0, 0, 0, DateTimeKind.Utc),
            };

            var e = Assert.Throws<AggregateException>(() => Client.PutObjectRetention(BucketName, Key, Retention));
            Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
            Assert.Equal(MainData.MalformedXML, GetErrorCode(e));
        }

        [Fact(DisplayName = "test_object_lock_get_obj_retention")]
        [Trait(MainData.Major, "Lock")]
        [Trait(MainData.Minor, "Retention")]
        [Trait(MainData.Explanation, "[버킷의 Lock옵션을 활성화] 오브젝트에 Lock 유지기한 설정이 올바른지 확인")]
        [Trait(MainData.Result, MainData.ResultSuccess)]
        public void test_object_lock_get_obj_retention()
        {
            var BucketName = GetNewBucketName();
            var Client = GetClient();
            Client.PutBucket(BucketName, ObjectLockEnabledForBucket: true);
            var Key = "file1";

            var PutResponse = Client.PutObject(BucketName, Key, Body: "abc");
            var VersionId = PutResponse.VersionId;

            var Retention = new ObjectLockRetention()
            {
                Mode = ObjectLockRetentionMode.Governance,
                RetainUntilDate = new DateTime(2030, 1, 1, 0, 0, 0, DateTimeKind.Utc),
            };

            Client.PutObjectRetention(BucketName, Key, Retention);
            var Response = Client.GetObjectRetention(BucketName, Key);
            RetentionCompare(Retention, Response.Retention);
            Client.DeleteObject(BucketName, Key, VersionId: VersionId, BypassGovernanceRetention: true);
        }

        [Fact(DisplayName = "test_object_lock_get_obj_retention_invalid_bucket")]
        [Trait(MainData.Major, "Lock")]
        [Trait(MainData.Minor, "Retention")]
        [Trait(MainData.Explanation, "버킷을 Lock옵션을 활성화 하지않을 경우 오브젝트에 Lock 유지기한 조회 실패")]
        [Trait(MainData.Result, MainData.ResultFailure)]
        public void test_object_lock_get_obj_retention_invalid_bucket()
        {
            var BucketName = GetNewBucketName();
            var Client = GetClient();
            Client.PutBucket(BucketName);
            var Key = "file1";

            var PutResponse = Client.PutObject(BucketName, Key, Body: "abc");

            var e = Assert.Throws<AggregateException>(() => Client.GetObjectRetention(BucketName, Key));
            Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
            Assert.Equal(MainData.InvalidRequest, GetErrorCode(e));
        }

        [Fact(DisplayName = "test_object_lock_put_obj_retention_versionid")]
        [Trait(MainData.Major, "Lock")]
        [Trait(MainData.Minor, "Retention")]
        [Trait(MainData.Explanation, "[버킷의 Lock옵션을 활성화] " +
                                     "오브젝트의 특정 버전에 Lock 유지기한을 설정할 경우 올바르게 적용되었는지 확인")]
        [Trait(MainData.Result, MainData.ResultSuccess)]
        public void test_object_lock_put_obj_retention_versionid()
        {
            var BucketName = GetNewBucketName();
            var Client = GetClient();
            Client.PutBucket(BucketName, ObjectLockEnabledForBucket: true);
            var Key = "file1";

            Client.PutObject(BucketName, Key, Body: "abc");
            var PutResponse = Client.PutObject(BucketName, Key, Body: "abc");
            var VersionId = PutResponse.VersionId;

            var Retention = new ObjectLockRetention()
            {
                Mode = ObjectLockRetentionMode.Governance,
                RetainUntilDate = new DateTime(2030, 1, 1, 0, 0, 0, DateTimeKind.Utc),
            };

            Client.PutObjectRetention(BucketName, Key, Retention);
            var Response = Client.GetObjectRetention(BucketName, Key);
            RetentionCompare(Retention, Response.Retention);
            Client.DeleteObject(BucketName, Key, VersionId: VersionId, BypassGovernanceRetention: true);
        }

        [Fact(DisplayName = "test_object_lock_put_obj_retention_override_default_retention")]
        [Trait(MainData.Major, "Lock")]
        [Trait(MainData.Minor, "Priority")]
        [Trait(MainData.Explanation, "[버킷의 Lock옵션을 활성화] 버킷에 설정한 Lock설정보다 오브젝트에 Lock설정한 값이 우선 적용됨을 확인")]
        [Trait(MainData.Result, MainData.ResultSuccess)]
        public void test_object_lock_put_obj_retention_override_default_retention()
        {
            var BucketName = GetNewBucketName();
            var Client = GetClient();
            Client.PutBucket(BucketName, ObjectLockEnabledForBucket: true);

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
            Client.PutObjectLockConfiguration(BucketName, Conf);

            var Key = "file1";
            var Body = "abc";
            var MD5 = GetMD5(Body);
            var PutResponse = Client.PutObject(BucketName, Key, Body: Body, MD5Digest: MD5);
            var VersionId = PutResponse.VersionId;

            var Retention = new ObjectLockRetention()
            {
                Mode = ObjectLockRetentionMode.Governance,
                RetainUntilDate = new DateTime(2030, 1, 1, 0, 0, 0, DateTimeKind.Utc),
            };

            Client.PutObjectRetention(BucketName, Key, Retention);
            var Response = Client.GetObjectRetention(BucketName, Key);
            RetentionCompare(Retention, Response.Retention);

            Client.DeleteObject(BucketName, Key, VersionId: VersionId, BypassGovernanceRetention: true);
        }

        [Fact(DisplayName = "test_object_lock_put_obj_retention_increase_period")]
        [Trait(MainData.Major, "Lock")]
        [Trait(MainData.Minor, "Overwrite")]
        [Trait(MainData.Explanation, "[버킷의 Lock옵션을 활성화] 오브젝트의 lock 유지기한을 늘렸을때 적용되는지 확인")]
        [Trait(MainData.Result, MainData.ResultSuccess)]
        public void test_object_lock_put_obj_retention_increase_period()
        {
            var BucketName = GetNewBucketName();
            var Client = GetClient();
            Client.PutBucket(BucketName, ObjectLockEnabledForBucket: true);
            var Key = "file1";

            Client.PutObject(BucketName, Key, Body: "abc");
            var PutResponse = Client.PutObject(BucketName, Key, Body: "abc");
            var VersionId = PutResponse.VersionId;

            var Retention1 = new ObjectLockRetention()
            {
                Mode = ObjectLockRetentionMode.Governance,
                RetainUntilDate = new DateTime(2030, 1, 1, 0, 0, 0, DateTimeKind.Utc),
            };
            Client.PutObjectRetention(BucketName, Key, Retention1);

            var Retention2 = new ObjectLockRetention()
            {
                Mode = ObjectLockRetentionMode.Governance,
                RetainUntilDate = new DateTime(2030, 1, 3, 0, 0, 0, DateTimeKind.Utc),
            };
            Client.PutObjectRetention(BucketName, Key, Retention2);

            var Response = Client.GetObjectRetention(BucketName, Key);
            RetentionCompare(Retention2, Response.Retention);
            Client.DeleteObject(BucketName, Key, VersionId: VersionId, BypassGovernanceRetention: true);
        }

        [Fact(DisplayName = "test_object_lock_put_obj_retention_shorten_period")]
        [Trait(MainData.Major, "Lock")]
        [Trait(MainData.Minor, "Overwrite")]
        [Trait(MainData.Explanation, "[버킷의 Lock옵션을 활성화] 오브젝트의 lock 유지기한을 줄였을때 실패 확인")]
        [Trait(MainData.Result, MainData.ResultFailure)]
        public void test_object_lock_put_obj_retention_shorten_period()
        {
            var BucketName = GetNewBucketName();
            var Client = GetClient();
            Client.PutBucket(BucketName, ObjectLockEnabledForBucket: true);
            var Key = "file1";

            Client.PutObject(BucketName, Key, Body: "abc");
            var PutResponse = Client.PutObject(BucketName, Key, Body: "abc");
            var VersionId = PutResponse.VersionId;

            var Retention1 = new ObjectLockRetention()
            {
                Mode = ObjectLockRetentionMode.Governance,
                RetainUntilDate = new DateTime(2030, 1, 3, 0, 0, 0, DateTimeKind.Utc),
            };
            Client.PutObjectRetention(BucketName, Key, Retention1);

            var Retention2 = new ObjectLockRetention()
            {
                Mode = ObjectLockRetentionMode.Governance,
                RetainUntilDate = new DateTime(2030, 1, 1, 0, 0, 0, DateTimeKind.Utc),
            };
            var e = Assert.Throws<AggregateException>(() => Client.PutObjectRetention(BucketName, Key, Retention2));
            Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));
            Assert.Equal(MainData.AccessDenied, GetErrorCode(e));

            Client.DeleteObject(BucketName, Key, VersionId: VersionId, BypassGovernanceRetention: true);
        }

        [Fact(DisplayName = "test_object_lock_put_obj_retention_shorten_period_bypass")]
        [Trait(MainData.Major, "Lock")]
        [Trait(MainData.Minor, "Overwrite")]
        [Trait(MainData.Explanation, "[버킷의 Lock옵션을 활성화] " +
                                     "바이패스를 True로 설정하고 오브젝트의 lock 유지기한을 줄였을때 적용되는지 확인")]
        [Trait(MainData.Result, MainData.ResultSuccess)]
        public void test_object_lock_put_obj_retention_shorten_period_bypass()
        {
            var BucketName = GetNewBucketName();
            var Client = GetClient();
            Client.PutBucket(BucketName, ObjectLockEnabledForBucket: true);
            var Key = "file1";

            Client.PutObject(BucketName, Key, Body: "abc");
            var PutResponse = Client.PutObject(BucketName, Key, Body: "abc");
            var VersionId = PutResponse.VersionId;

            var Retention1 = new ObjectLockRetention()
            {
                Mode = ObjectLockRetentionMode.Governance,
                RetainUntilDate = new DateTime(2030, 1, 3, 0, 0, 0, DateTimeKind.Utc),
            };
            Client.PutObjectRetention(BucketName, Key, Retention1);

            var Retention2 = new ObjectLockRetention()
            {
                Mode = ObjectLockRetentionMode.Governance,
                RetainUntilDate = new DateTime(2030, 1, 1, 0, 0, 0, DateTimeKind.Utc),
            };
            Client.PutObjectRetention(BucketName, Key, Retention2, BypassGovernanceRetention: true);

            var Response = Client.GetObjectRetention(BucketName, Key);
            RetentionCompare(Retention2, Response.Retention);
            Client.DeleteObject(BucketName, Key, VersionId: VersionId, BypassGovernanceRetention: true);
        }

        [Fact(DisplayName = "test_object_lock_delete_object_with_retention")]
        [Trait(MainData.Major, "Lock")]
        [Trait(MainData.Minor, "ERROR")]
        [Trait(MainData.Explanation, "[버킷의 Lock옵션을 활성화] 오브젝트의 lock 유지기한내에 삭제를 시도할 경우 실패 확인")]
        [Trait(MainData.Result, MainData.ResultFailure)]
        public void test_object_lock_delete_object_with_retention()
        {
            var BucketName = GetNewBucketName();
            var Client = GetClient();
            Client.PutBucket(BucketName, ObjectLockEnabledForBucket: true);
            var Key = "file1";

            var PutResponse = Client.PutObject(BucketName, Key, Body: "abc");
            var VersionId = PutResponse.VersionId;

            var Retention = new ObjectLockRetention()
            {
                Mode = ObjectLockRetentionMode.Governance,
                RetainUntilDate = new DateTime(2030, 1, 1, 0, 0, 0, DateTimeKind.Utc),
            };
            Client.PutObjectRetention(BucketName, Key, Retention);

            var e = Assert.Throws<AggregateException>(() => Client.DeleteObject(BucketName, Key, VersionId:VersionId));
            Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));
            Assert.Equal(MainData.AccessDenied, GetErrorCode(e));

            Client.DeleteObject(BucketName, Key, VersionId: VersionId, BypassGovernanceRetention: true);
        }

        [Fact(DisplayName = "test_object_lock_put_legal_hold")]
        [Trait(MainData.Major, "Lock")]
        [Trait(MainData.Minor, "LegalHold")]
        [Trait(MainData.Explanation, "[버킷의 Lock옵션을 활성화] 오브젝트의 LegalHold를 활성화 가능한지 확인")]
        [Trait(MainData.Result, MainData.ResultSuccess)]
        public void test_object_lock_put_legal_hold()
        {
            var BucketName = GetNewBucketName();
            var Client = GetClient();
            Client.PutBucket(BucketName, ObjectLockEnabledForBucket: true);
            
            var Key = "file1";
            Client.PutObject(BucketName, Key, Body: "abc");

            var LegalHold = new ObjectLockLegalHold() { Status = ObjectLockLegalHoldStatus.On };
            var Response = Client.PutObjectLegalHold(BucketName, Key, LegalHold);
            Assert.Equal(HttpStatusCode.OK, Response.HttpStatusCode);

            LegalHold = new ObjectLockLegalHold() { Status = ObjectLockLegalHoldStatus.Off };
            Response = Client.PutObjectLegalHold(BucketName, Key, LegalHold);
            Assert.Equal(HttpStatusCode.OK, Response.HttpStatusCode);
        }

        [Fact(DisplayName = "test_object_lock_put_legal_hold_invalid_bucket")]
        [Trait(MainData.Major, "Lock")]
        [Trait(MainData.Minor, "LegalHold")]
        [Trait(MainData.Explanation, "[버킷의 Lock옵션을 비활성화] 오브젝트의 LegalHold를 활성화 실패 확인")]
        [Trait(MainData.Result, MainData.ResultFailure)]
        public void test_object_lock_put_legal_hold_invalid_bucket()
        {
            var BucketName = GetNewBucketName();
            var Client = GetClient();
            Client.PutBucket(BucketName);

            var Key = "file1";
            Client.PutObject(BucketName, Key, Body: "abc");

            var LegalHold = new ObjectLockLegalHold() { Status = ObjectLockLegalHoldStatus.On };
            var e = Assert.Throws<AggregateException>(() => Client.PutObjectLegalHold(BucketName, Key, LegalHold));
            Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
            Assert.Equal(MainData.InvalidRequest, GetErrorCode(e));
        }

        [Fact(DisplayName = "test_object_lock_put_legal_hold_invalid_status")]
        [Trait(MainData.Major, "Lock")]
        [Trait(MainData.Minor, "LegalHold")]
        [Trait(MainData.Explanation, "[버킷의 Lock옵션을 비활성화] 오브젝트의 LegalHold를 활성화 실패 확인")]
        [Trait(MainData.Result, MainData.ResultFailure)]
        public void test_object_lock_put_legal_hold_invalid_status()
        {
            var BucketName = GetNewBucketName();
            var Client = GetClient();
            Client.PutBucket(BucketName, ObjectLockEnabledForBucket: true);

            var Key = "file1";
            Client.PutObject(BucketName, Key, Body: "abc");

            var LegalHold = new ObjectLockLegalHold() { Status = new ObjectLockLegalHoldStatus("abc") };
            var e = Assert.Throws<AggregateException>(() => Client.PutObjectLegalHold(BucketName, Key, LegalHold));
            Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
            Assert.Equal(MainData.MalformedXML, GetErrorCode(e));
        }

        [Fact(DisplayName = "test_object_lock_get_legal_hold")]
        [Trait(MainData.Major, "Lock")]
        [Trait(MainData.Minor, "LegalHold")]
        [Trait(MainData.Explanation, "[버킷의 Lock옵션을 활성화] 오브젝트의 LegalHold가 올바르게 적용되었는지 확인")]
        [Trait(MainData.Result, MainData.ResultSuccess)]
        public void test_object_lock_get_legal_hold()
        {
            var BucketName = GetNewBucketName();
            var Client = GetClient();
            Client.PutBucket(BucketName, ObjectLockEnabledForBucket: true);

            var Key = "file1";
            Client.PutObject(BucketName, Key, Body: "abc");

            var LegalHold = new ObjectLockLegalHold() { Status = ObjectLockLegalHoldStatus.On };
            Client.PutObjectLegalHold(BucketName, Key, LegalHold);
            var Response = Client.GetObjectLegalHold(BucketName, Key);
            Assert.Equal(LegalHold.Status, Response.LegalHold.Status);

            LegalHold = new ObjectLockLegalHold() { Status = ObjectLockLegalHoldStatus.Off };
            Client.PutObjectLegalHold(BucketName, Key, LegalHold);
            Response = Client.GetObjectLegalHold(BucketName, Key);
            Assert.Equal(LegalHold.Status, Response.LegalHold.Status);
        }

        [Fact(DisplayName = "test_object_lock_get_legal_hold_invalid_bucket")]
        [Trait(MainData.Major, "Lock")]
        [Trait(MainData.Minor, "LegalHold")]
        [Trait(MainData.Explanation, "[버킷의 Lock옵션을 활성화] 오브젝트의 LegalHold설정 조회 실패 확인")]
        [Trait(MainData.Result, MainData.ResultFailure)]
        public void test_object_lock_get_legal_hold_invalid_bucket()
        {
            var BucketName = GetNewBucketName();
            var Client = GetClient();
            Client.PutBucket(BucketName);

            var Key = "file1";
            Client.PutObject(BucketName, Key, Body: "abc");

            var e = Assert.Throws<AggregateException>(() => Client.GetObjectLegalHold(BucketName, Key));
            Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
            Assert.Equal(MainData.InvalidRequest, GetErrorCode(e));
        }

        [Fact(DisplayName = "test_object_lock_delete_object_with_legal_hold_on")]
        [Trait(MainData.Major, "Lock")]
        [Trait(MainData.Minor, "LegalHold")]
        [Trait(MainData.Explanation, "[버킷의 Lock옵션을 활성화] 오브젝트의 LegalHold가 활성화되어 있을 경우 오브젝트 삭제 실패 확인")]
        [Trait(MainData.Result, MainData.ResultFailure)]
        public void test_object_lock_delete_object_with_legal_hold_on()
        {
            var BucketName = GetNewBucketName();
            var Client = GetClient();
            Client.PutBucket(BucketName, ObjectLockEnabledForBucket: true);

            var Key = "file1";
            var PutResponse = Client.PutObject(BucketName, Key, Body: "abc");

            var LegalHold = new ObjectLockLegalHold() { Status = ObjectLockLegalHoldStatus.On };
            Client.PutObjectLegalHold(BucketName, Key, LegalHold);

            var e = Assert.Throws<AggregateException>(() => Client.DeleteObject(BucketName, Key, VersionId: PutResponse.VersionId));
            Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));
            Assert.Equal(MainData.AccessDenied, GetErrorCode(e));

            LegalHold = new ObjectLockLegalHold() { Status = ObjectLockLegalHoldStatus.Off };
            Client.PutObjectLegalHold(BucketName, Key, LegalHold);
        }

        [Fact(DisplayName = "test_object_lock_delete_object_with_legal_hold_off")]
        [Trait(MainData.Major, "Lock")]
        [Trait(MainData.Minor, "LegalHold")]
        [Trait(MainData.Explanation, "[버킷의 Lock옵션을 활성화] 오브젝트의 LegalHold가 비활성화되어 있을 경우 오브젝트 삭제 확인")]
        [Trait(MainData.Result, MainData.ResultSuccess)]
        public void test_object_lock_delete_object_with_legal_hold_off()
        {
            var BucketName = GetNewBucketName();
            var Client = GetClient();
            Client.PutBucket(BucketName, ObjectLockEnabledForBucket: true);

            var Key = "file1";
            var PutResponse = Client.PutObject(BucketName, Key, Body: "abc");

            var LegalHold = new ObjectLockLegalHold() { Status = ObjectLockLegalHoldStatus.Off };
            Client.PutObjectLegalHold(BucketName, Key, LegalHold);

            var Response = Client.DeleteObject(BucketName, Key, VersionId: PutResponse.VersionId);
            Assert.Equal(HttpStatusCode.NoContent, Response.HttpStatusCode);
        }

        [Fact(DisplayName = "test_object_lock_get_obj_metadata")]
        [Trait(MainData.Major, "Lock")]
        [Trait(MainData.Minor, "LegalHold")]
        [Trait(MainData.Explanation, "[버킷의 Lock옵션을 활성화] " +
                                     "오브젝트의 LegalHold와 Lock유지기한 설정이 모두 적용되는지 메타데이터를 통해 확인")]
        [Trait(MainData.Result, MainData.ResultSuccess)]
        public void test_object_lock_get_obj_metadata()
        {
            var BucketName = GetNewBucketName();
            var Client = GetClient();
            Client.PutBucket(BucketName, ObjectLockEnabledForBucket: true);

            var Key = "file1";
            var PutResponse = Client.PutObject(BucketName, Key, Body: "abc");

            var LegalHold = new ObjectLockLegalHold() { Status = ObjectLockLegalHoldStatus.On };
            Client.PutObjectLegalHold(BucketName, Key, LegalHold);


            var Retention = new ObjectLockRetention()
            {
                Mode = ObjectLockRetentionMode.Governance,
                RetainUntilDate = new DateTime(2030, 1, 1, 0, 0, 0, DateTimeKind.Utc),
            };
            Client.PutObjectRetention(BucketName, Key, Retention);

            var Response = Client.GetObjectMetadata(BucketName, Key);
            Assert.Equal(Retention.Mode, Response.ObjectLockMode);
            Assert.Equal(Retention.RetainUntilDate, Response.ObjectLockRetainUntilDate.ToUniversalTime());
            Assert.Equal(LegalHold.Status, Response.ObjectLockLegalHoldStatus);

            LegalHold = new ObjectLockLegalHold() { Status = ObjectLockLegalHoldStatus.Off };
            Client.PutObjectLegalHold(BucketName, Key, LegalHold);
            Client.DeleteObject(BucketName, Key, VersionId: PutResponse.VersionId, BypassGovernanceRetention: true);
        }
    }
}
