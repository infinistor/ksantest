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
using System.Collections.Generic;
using System.Net;
using Xunit;

namespace s3tests
{
	public class Grants : TestBase
	{
		public Grants(Xunit.Abstractions.ITestOutputHelper output) => Output = output;

		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "Bucket")]
		[Trait(MainData.Explanation, "[bucket_acl : default] " +
									 "권한을 설정하지 않고 생성한 버킷의 default acl정보가 올바른지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketAclDefault()
		{
			var bucketName = GetNewBucket();

			var client = GetClient();
			var response = client.GetBucketACL(bucketName);

			var displayName = Config.MainUser.DisplayName;
			var userId = Config.MainUser.UserId;

			if (!Config.S3.IsAWS) Assert.Equal(displayName, response.AccessControlList.Owner.DisplayName);
			Assert.Equal(userId, response.AccessControlList.Owner.Id);

			var GetGrants = response.AccessControlList.Grants;
			CheckGrants(new()
			{
				new()
				{
					Permission = S3Permission.FULL_CONTROL,
					Grantee = new()
					{
						CanonicalUser = userId,
						DisplayName = displayName,
					}
				}
			},
			GetGrants);
		}

		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "Bucket")]
		[Trait(MainData.Explanation, "[bucket_acl : public-read] " +
									 "권한을 public-read로 생성한 버킷의 acl정보가 올바른지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_bucket_acl_canned_during_create()
		{
			var bucketName = GetNewBucketName();

			var client = GetClient();
			client.PutBucket(bucketName, acl: S3CannedACL.PublicRead);
			var response = client.GetBucketACL(bucketName);

			var displayName = Config.MainUser.DisplayName;
			var userId = Config.MainUser.UserId;

			var getGrants = response.AccessControlList.Grants;
			CheckGrants(new()
			{
				new()
				{
					Permission = S3Permission.FULL_CONTROL,
					Grantee = new()
					{
						CanonicalUser = userId,
						DisplayName = displayName,
					}
				},
				new()
				{
					Permission = S3Permission.READ,
					Grantee = new()
					{
						URI = "http://acs.amazonaws.com/groups/global/AllUsers",
					}
				},
			},
			getGrants);
		}

		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "Bucket")]
		[Trait(MainData.Explanation, "[bucket_acl : public-read => bucket_acl : private] " +
									 "권한을 public-read로 생성한 버킷을 private로 변경할경우 올바르게 적용되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_bucket_acl_canned()
		{
			var bucketName = GetNewBucketName();

			var client = GetClient();
			client.PutBucket(bucketName, acl: S3CannedACL.PublicRead);
			var response = client.GetBucketACL(bucketName);

			var displayName = Config.MainUser.DisplayName;
			var userId = Config.MainUser.UserId;

			var GetGrants = response.AccessControlList.Grants;
			CheckGrants(new List<S3Grant>()
			{
				new()
				{
					Permission = S3Permission.FULL_CONTROL,
					Grantee = new()
					{
						CanonicalUser = userId,
						DisplayName = displayName,
					}
				},
				new()
				{
					Permission = S3Permission.READ,
					Grantee = new()
					{
						URI = "http://acs.amazonaws.com/groups/global/AllUsers",
					}
				},
			},
			GetGrants);

			client.PutBucketACL(bucketName, acl: S3CannedACL.Private);
			response = client.GetBucketACL(bucketName);
			GetGrants = response.AccessControlList.Grants;

			CheckGrants(new List<S3Grant>()
			{
				new()
				{
					Permission = S3Permission.FULL_CONTROL,
					Grantee = new()
					{
						CanonicalUser = userId,
						DisplayName = displayName,
					}
				}
			},
			GetGrants);
		}

		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "Bucket")]
		[Trait(MainData.Explanation, "[bucket_acl : public-read-write] " +
									 "권한을 public-read-write로 생성한 버킷의 acl정보가 올바른지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_bucket_acl_canned_publicreadwrite()
		{
			var bucketName = GetNewBucketName();

			var client = GetClient();
			client.PutBucket(bucketName, acl: S3CannedACL.PublicReadWrite);
			var response = client.GetBucketACL(bucketName);

			var displayName = Config.MainUser.DisplayName;
			var userId = Config.MainUser.UserId;

			var GetGrants = response.AccessControlList.Grants;
			CheckGrants(new List<S3Grant>()
			{
				new()
				{
					Permission = S3Permission.FULL_CONTROL,
					Grantee = new()
					{
						CanonicalUser = userId,
						DisplayName = displayName,
					}
				},
				new()
				{
					Permission = S3Permission.READ,
					Grantee = new()
					{
						URI = "http://acs.amazonaws.com/groups/global/AllUsers",
					}
				},
				new()
				{
					Permission = S3Permission.WRITE,
					Grantee = new()
					{
						URI = "http://acs.amazonaws.com/groups/global/AllUsers",
					}
				},
			},
			GetGrants);
		}

		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "Bucket")]
		[Trait(MainData.Explanation, "[bucket_acl : authenticated-read] " +
									 "권한을 authenticated-read로 생성한 버킷의 acl정보가 올바른지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_bucket_acl_canned_authenticatedread()
		{
			var bucketName = GetNewBucketName();

			var client = GetClient();
			client.PutBucket(bucketName, acl: S3CannedACL.AuthenticatedRead);
			var response = client.GetBucketACL(bucketName);

			var displayName = Config.MainUser.DisplayName;
			var userId = Config.MainUser.UserId;

			var GetGrants = response.AccessControlList.Grants;
			CheckGrants(new List<S3Grant>()
			{
				new()
				{
					Permission = S3Permission.FULL_CONTROL,
					Grantee = new()
					{
						CanonicalUser = userId,
						DisplayName = displayName,
					}
				},
				new()
				{
					Permission = S3Permission.READ,
					Grantee = new()
					{
						URI = "http://acs.amazonaws.com/groups/global/AuthenticatedUsers",
					}
				},
			},
			GetGrants);
		}

		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "Object")]
		[Trait(MainData.Explanation, "[object_acl : default] " +
									 "권한을 설정하지 않고 생성한 오브젝트의 default acl정보가 올바른지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_object_acl_default()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			var key = "foo";
			client.PutObject(bucketName, key, body: "bar");
			var response = client.GetObjectACL(bucketName, key);


			var displayName = Config.MainUser.DisplayName;
			var userId = Config.MainUser.UserId;

			var GetGrants = response.AccessControlList.Grants;
			CheckGrants(new List<S3Grant>()
			{
				new()
				{
					Permission = S3Permission.FULL_CONTROL,
					Grantee = new()
					{
						CanonicalUser = userId,
						DisplayName = displayName,
					}
				}
			},
			GetGrants);
		}

		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "Object")]
		[Trait(MainData.Explanation, "[object_acl : public-read] " +
									 "권한을 public-read로 생성한 오브젝트의 acl정보가 올바른지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_object_acl_canned_during_create()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			var key = "foo";
			client.PutObject(bucketName, key, body: "bar", acl: S3CannedACL.PublicRead);
			var response = client.GetObjectACL(bucketName, key);


			var displayName = Config.MainUser.DisplayName;
			var userId = Config.MainUser.UserId;

			var GetGrants = response.AccessControlList.Grants;
			CheckGrants(new List<S3Grant>()
			{
				new()
				{
					Permission = S3Permission.FULL_CONTROL,
					Grantee = new()
					{
						CanonicalUser = userId,
						DisplayName = displayName,
					}
				},
				new()
				{
					Permission = S3Permission.READ,
					Grantee = new()
					{
						URI = "http://acs.amazonaws.com/groups/global/AllUsers",
					}
				},
			},
			GetGrants);
		}

		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "Object")]
		[Trait(MainData.Explanation, "[object_acl : public-read => object_acl : private] " +
									 "권한을 public-read로 생성한 오브젝트를 private로 변경할경우 올바르게 적용되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_object_acl_canned()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			var key = "foo";
			client.PutObject(bucketName, key, body: "bar", acl: S3CannedACL.PublicRead);
			var response = client.GetObjectACL(bucketName, key);


			var displayName = Config.MainUser.DisplayName;
			var userId = Config.MainUser.UserId;

			var GetGrants = response.AccessControlList.Grants;
			CheckGrants(new List<S3Grant>()
			{
				new()
				{
					Permission = S3Permission.FULL_CONTROL,
					Grantee = new()
					{
						CanonicalUser = userId,
						DisplayName = displayName,
					}
				},
				new()
				{
					Permission = S3Permission.READ,
					Grantee = new()
					{
						URI = "http://acs.amazonaws.com/groups/global/AllUsers",
					}
				},
			},
			GetGrants);

			client.PutObjectACL(bucketName, key, acl: S3CannedACL.Private);
			response = client.GetObjectACL(bucketName, key);

			GetGrants = response.AccessControlList.Grants;
			CheckGrants(new List<S3Grant>()
			{
				new()
				{
					Permission = S3Permission.FULL_CONTROL,
					Grantee = new()
					{
						CanonicalUser = userId,
						DisplayName = displayName,
					}
				},
			},
			GetGrants);
		}


		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "Object")]
		[Trait(MainData.Explanation, "[object_acl : public-read-write] " +
									 "권한을 public-read-write로 생성한 오브젝트의 acl정보가 올바른지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_object_acl_canned_publicreadwrite()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			var key = "foo";
			client.PutObject(bucketName, key, body: "bar", acl: S3CannedACL.PublicReadWrite);
			var response = client.GetObjectACL(bucketName, key);


			var displayName = Config.MainUser.DisplayName;
			var userId = Config.MainUser.UserId;

			var GetGrants = response.AccessControlList.Grants;

			CheckGrants(new List<S3Grant>()
			{
				new()
				{
					Permission = S3Permission.FULL_CONTROL,
					Grantee = new()
					{
						CanonicalUser = userId,
						DisplayName = displayName,
					}
				},
				new()
				{
					Permission = S3Permission.READ,
					Grantee = new()
					{
						URI = "http://acs.amazonaws.com/groups/global/AllUsers",
					}
				},
				new()
				{
					Permission = S3Permission.WRITE,
					Grantee = new()
					{
						URI = "http://acs.amazonaws.com/groups/global/AllUsers",
					}
				},
			},
			GetGrants);
		}

		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "Object")]
		[Trait(MainData.Explanation, "[object_acl : public-read-write] " +
									 "권한을 public-read-write로 생성한 오브젝트의 acl정보가 올바른지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_object_acl_canned_authenticatedread()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			var key = "foo";
			client.PutObject(bucketName, key, body: "bar", acl: S3CannedACL.AuthenticatedRead);
			var response = client.GetObjectACL(bucketName, key);


			var displayName = Config.MainUser.DisplayName;
			var userId = Config.MainUser.UserId;

			var GetGrants = response.AccessControlList.Grants;

			CheckGrants(new List<S3Grant>()
			{
				new()
				{
					Permission = S3Permission.FULL_CONTROL,
					Grantee = new()
					{
						CanonicalUser = userId,
						DisplayName = displayName,
					}
				},
				new()
				{
					Permission = S3Permission.READ,
					Grantee = new()
					{
						URI = "http://acs.amazonaws.com/groups/global/AuthenticatedUsers",
					}
				},
			},
			GetGrants);
		}


		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "Object")]
		[Trait(MainData.Explanation, "[bucket_acl: public-read-write]" +
									 "[object_acl: public-read-write => object_acl : bucket-owner-read]" +
									 "메인 유저가 권한을 public-read-write로 생성한 버켓에서 서브유저가 업로드한 오브젝트를" +
									 "서브 유저가 권한을 bucket-owner-read로 변경하였을때 올바르게 적용되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_object_acl_canned_bucketownerread()
		{
			var bucketName = GetNewBucketName();
			var MainClient = GetClient();
			var AltClient = GetAltClient();
			var key = "foo";

			MainClient.PutBucket(bucketName, acl: S3CannedACL.PublicReadWrite);
			AltClient.PutObject(bucketName, key, body: "bar");

			var BucketACLResponse = MainClient.GetBucketACL(bucketName);
			var BucketOwnerId = BucketACLResponse.AccessControlList.Owner.Id;
			var BucketOwnerDisplayName = BucketACLResponse.AccessControlList.Owner.DisplayName;

			AltClient.PutObject(bucketName, key, acl: S3CannedACL.BucketOwnerRead);
			var response = AltClient.GetObjectACL(bucketName, key);

			var AltDisplayName = Config.AltUser.DisplayName;
			var AltUserId = Config.AltUser.UserId;

			var GetGrants = response.AccessControlList.Grants;

			CheckGrants(new List<S3Grant>()
			{
				new()
				{
					Permission = S3Permission.FULL_CONTROL,
					Grantee = new()
					{
						CanonicalUser = AltUserId,
						DisplayName = AltDisplayName,
					}
				},
				new()
				{
					Permission = S3Permission.READ,
					Grantee = new()
					{
						CanonicalUser = BucketOwnerId,
						DisplayName = BucketOwnerDisplayName,
					}
				},
			},
			GetGrants);
		}

		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "Object")]
		[Trait(MainData.Explanation, "[bucket_acl: public-read-write]" +
									 "[object_acl: public-read-write => object_acl : bucket-owner-full-control]" +
									 "메인 유저가 권한을 public-read-write로 생성한 버켓에서 서브유저가 업로드한 오브젝트를" +
									 "서브 유저가 권한을 bucket-owner-full-control로 변경하였을때 올바르게 적용되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_object_acl_canned_bucketownerfullcontrol()
		{
			var bucketName = GetNewBucketName();
			var MainClient = GetClient();
			var AltClient = GetAltClient();
			var key = "foo";

			MainClient.PutBucket(bucketName, acl: S3CannedACL.PublicReadWrite);
			AltClient.PutObject(bucketName, key, body: "bar");

			var BucketACLResponse = MainClient.GetBucketACL(bucketName);
			var BucketOwnerId = BucketACLResponse.AccessControlList.Owner.Id;
			var BucketOwnerDisplayName = BucketACLResponse.AccessControlList.Owner.DisplayName;

			AltClient.PutObject(bucketName, key, acl: S3CannedACL.BucketOwnerFullControl);
			var response = AltClient.GetObjectACL(bucketName, key);

			var AltDisplayName = Config.AltUser.DisplayName;
			var AltUserId = Config.AltUser.UserId;

			var GetGrants = response.AccessControlList.Grants;

			CheckGrants(new List<S3Grant>()
			{
				new()
				{
					Permission = S3Permission.FULL_CONTROL,
					Grantee = new()
					{
						CanonicalUser = AltUserId,
						DisplayName = AltDisplayName,
					}
				},
				new()
				{
					Permission = S3Permission.FULL_CONTROL,
					Grantee = new()
					{
						CanonicalUser = BucketOwnerId,
						DisplayName = BucketOwnerDisplayName,
					}
				},
			},
			GetGrants);
		}

		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "Object")]
		[Trait(MainData.Explanation, "[bucket_acl: public-read-write]" +
			"메인 유저가 권한을 public-read-write로 생성한 버켓에서 메인유저가 생성한 오브젝트를 권한을" +
			"서브유저에게 FULL_CONTROL, 소유주를 메인유저로 설정한뒤 서브 유저가 권한을 READ_ACP, 소유주를 메인유저로 설정하였을때" +
			"오브젝트의 소유자가 유지되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_object_acl_full_control_verify_owner()
		{
			var bucketName = GetNewBucketName();
			var MainClient = GetClient();
			var AltClient = GetAltClient();
			var key = "foo";

			MainClient.PutBucket(bucketName, acl: S3CannedACL.PublicReadWrite);
			MainClient.PutObject(bucketName, key, body: "bar");

			var MainUserId = Config.MainUser.UserId;
			var MainDisplayName = Config.MainUser.DisplayName;

			var AltUserId = Config.AltUser.UserId;
			var AltDisplayName = Config.AltUser.DisplayName;

			var Grant = new S3AccessControlList()
			{
				Owner = new Owner() { DisplayName = MainDisplayName, Id = MainUserId },
				Grants = new List<S3Grant>()
				{
					new()
					{
						Permission = S3Permission.FULL_CONTROL,
						Grantee = new()
						{
							CanonicalUser = AltUserId,
							DisplayName = AltDisplayName,
						}
					},
				}
			};

			MainClient.PutObjectACL(bucketName, key, accessControlPolicy: Grant);

			Grant = new S3AccessControlList()
			{
				Owner = new Owner() { DisplayName = MainDisplayName, Id = MainUserId },
				Grants = new List<S3Grant>()
				{
					new()
					{
						Permission = S3Permission.READ_ACP,
						Grantee = new()
						{
							CanonicalUser = AltUserId,
							DisplayName = AltDisplayName,
						}
					},
				}
			};

			AltClient.PutObjectACL(bucketName, key, accessControlPolicy: Grant);

			var response = AltClient.GetObjectACL(bucketName, key);
			Assert.Equal(MainUserId, response.AccessControlList.Owner.Id);
		}


		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "ETag")]
		[Trait(MainData.Explanation, "[bucket_acl: public-read-write] 권한정보를 추가한 오브젝트의 eTag값이 변경되지 않는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_object_acl_full_control_verify_attributes()
		{
			var bucketName = GetNewBucketName();
			var MainClient = GetClient();
			var key = "foo";

			MainClient.PutBucket(bucketName, acl: S3CannedACL.PublicReadWrite);

			var Headers = new List<KeyValuePair<string, string>>() { new("x-amz-meta-foo", "bar") };
			MainClient.PutObject(bucketName, key, body: "bar", metadataList: Headers);

			var response = MainClient.GetObject(bucketName, key);
			var ContentType = response.Headers.ContentType;
			var ETag = response.ETag;

			var AltUserId = Config.AltUser.UserId;

			var Grant = new S3Grant()
			{
				Permission = S3Permission.FULL_CONTROL,
				Grantee = new()
				{
					CanonicalUser = AltUserId,
					//DisplayName = AltDisplayName,
				}
			};

			var Grants = AddObjectUserGrant(bucketName, key, Grant);

			MainClient.PutObjectACL(bucketName, key, accessControlPolicy: Grants);

			response = MainClient.GetObject(bucketName, key);
			Assert.Equal(ContentType, response.Headers.ContentType);
			Assert.Equal(ETag, response.ETag);
		}

		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "Permission")]
		[Trait(MainData.Explanation, "[bucket_acl:private] " +
									 "기본생성한 버킷에 priavte 설정이 가능한지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_bucket_acl_canned_private_to_private()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			var response = client.PutBucketACL(bucketName, acl: S3CannedACL.Private);
			Assert.Equal(HttpStatusCode.OK, response.HttpStatusCode);
		}


		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "Permission")]
		[Trait(MainData.Explanation, "오브젝트에 설정한 acl정보가 올바르게 적용되었는지 확인 : FULL_CONTROL")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_object_acl()
		{
			CheckObjectACL(S3Permission.FULL_CONTROL);
		}

		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "Permission")]
		[Trait(MainData.Explanation, "오브젝트에 설정한 acl정보가 올바르게 적용되었는지 확인 : WRITE")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_object_acl_write()
		{
			CheckObjectACL(S3Permission.WRITE);
		}

		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "Permission")]
		[Trait(MainData.Explanation, "오브젝트에 설정한 acl정보가 올바르게 적용되었는지 확인 : WRITE_ACP")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_object_acl_writeacp()
		{
			CheckObjectACL(S3Permission.WRITE_ACP);
		}

		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "Permission")]
		[Trait(MainData.Explanation, "오브젝트에 설정한 acl정보가 올바르게 적용되었는지 확인 : READ")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_object_acl_read()
		{
			CheckObjectACL(S3Permission.READ);
		}

		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "Permission")]
		[Trait(MainData.Explanation, "오브젝트에 설정한 acl정보가 올바르게 적용되었는지 확인 : READ_ACP")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_object_acl_readacp()
		{
			CheckObjectACL(S3Permission.READ_ACP);
		}

		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "Permission")]
		[Trait(MainData.Explanation, "메인 유저가 버킷에 설정한 acl정보대로 서브유저가 해당 버킷에 접근 가능한지 확인 : FULL_CONTROL")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_bucket_acl_grant_userid_fullcontrol()
		{
			var bucketName = SetupBucketACLGrantUserid(S3Permission.FULL_CONTROL);

			CheckBucketACLGrantCanRead(bucketName);
			CheckBucketACLGrantCanReadACP(bucketName);
			CheckBucketACLGrantCanWrite(bucketName);
			CheckBucketACLGrantCanWriteACP(bucketName);

			var client = GetClient();

			var BucketACLResponse = client.GetBucketACL(bucketName);
			var OwnerId = BucketACLResponse.AccessControlList.Owner.Id;
			var OwnerDisplayName = BucketACLResponse.AccessControlList.Owner.DisplayName;

			var MainUserId = Config.MainUser.UserId;
			var MainDisplayName = Config.MainUser.DisplayName;

			Assert.Equal(MainUserId, OwnerId);
			if (!Config.S3.IsAWS) Assert.Equal(MainDisplayName, OwnerDisplayName);
		}

		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "Permission")]
		[Trait(MainData.Explanation, "메인 유저가 버킷에 설정한 acl정보대로 서브유저가 해당 버킷에 접근 가능한지 확인 : READ")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_bucket_acl_grant_userid_read()
		{
			var bucketName = SetupBucketACLGrantUserid(S3Permission.READ);

			CheckBucketACLGrantCanRead(bucketName);
			CheckBucketACLGrantCantReadACP(bucketName);
			CheckBucketACLGrantCantWrite(bucketName);
			CheckBucketACLGrantCantWriteACP(bucketName);
		}

		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "Permission")]
		[Trait(MainData.Explanation, "메인 유저가 버킷에 설정한 acl정보대로 서브유저가 해당 버킷에 접근 가능한지 확인 : READ_ACP")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_bucket_acl_grant_userid_readacp()
		{
			var bucketName = SetupBucketACLGrantUserid(S3Permission.READ_ACP);

			CheckBucketACLGrantCantRead(bucketName);
			CheckBucketACLGrantCanReadACP(bucketName);
			CheckBucketACLGrantCantWrite(bucketName);
			CheckBucketACLGrantCantWriteACP(bucketName);
		}

		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "Permission")]
		[Trait(MainData.Explanation, "메인 유저가 버킷에 설정한 acl정보대로 서브유저가 해당 버킷에 접근 가능한지 확인 : WRITE")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_bucket_acl_grant_userid_write()
		{
			var bucketName = SetupBucketACLGrantUserid(S3Permission.WRITE);

			CheckBucketACLGrantCantRead(bucketName);
			CheckBucketACLGrantCantReadACP(bucketName);
			CheckBucketACLGrantCanWrite(bucketName);
			CheckBucketACLGrantCantWriteACP(bucketName);
		}

		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "Permission")]
		[Trait(MainData.Explanation, "메인 유저가 버킷에 설정한 acl정보대로 서브유저가 해당 버킷에 접근 가능한지 확인 : WRITE_ACP")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_bucket_acl_grant_userid_writeacp()
		{
			var bucketName = SetupBucketACLGrantUserid(S3Permission.WRITE_ACP);

			CheckBucketACLGrantCantRead(bucketName);
			CheckBucketACLGrantCantReadACP(bucketName);
			CheckBucketACLGrantCantWrite(bucketName);
			CheckBucketACLGrantCanWriteACP(bucketName);
		}

		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "버킷에 존재하지 않는 유저를 추가하려고 하면 에러 발생 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_bucket_acl_grant_nonexist_user()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			var BadUserId = "_foo";

			var Grant = new S3Grant() { Permission = S3Permission.FULL_CONTROL, Grantee = new() { CanonicalUser = BadUserId } };

			var Grants = AddBucketUserGrant(bucketName, Grant);

			var e = Assert.Throws<AggregateException>(() => client.PutBucketACL(bucketName, accessControlPolicy: Grants));
			Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
			Assert.Equal(MainData.INVALID_ARGUMENT, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "버킷에 권한정보를 모두 제거했을때 오브젝트를 업데이트 하면 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_bucket_acl_no_grants()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "foo";

			client.PutObject(bucketName, key, body: "bar");
			var response = client.GetBucketACL(bucketName);
			var OldGrants = response.AccessControlList.Grants;
			var Policy = new S3AccessControlList()
			{
				Owner = response.AccessControlList.Owner,
				Grants = new List<S3Grant>()
			};

			client.PutBucketACL(bucketName, accessControlPolicy: Policy);

			client.GetObject(bucketName, key);

			Assert.Throws<AggregateException>(() => client.PutObject(bucketName, key, body: "A"));

			var Client2 = GetClient();
			Client2.GetBucketACL(bucketName);
			Client2.PutBucketACL(bucketName, acl: S3CannedACL.Private);

			Policy.Grants = OldGrants;
			Client2.PutBucketACL(bucketName, accessControlPolicy: Policy);

		}

		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "Header")]
		[Trait(MainData.Explanation, "오브젝트를 생성하면서 권한정보를 여러개보낼때 모두 올바르게 적용되었는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_object_header_acl_grants()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "foo_key";

			var AltUserId = Config.AltUser.UserId;
			var AltDisplayName = Config.AltUser.DisplayName;

			var Grants = GetGrantList();

			client.PutObject(bucketName, key, body: "bar", grants: Grants);
			var response = client.GetObjectACL(bucketName, key);

			var GetGrants = response.AccessControlList.Grants;
			CheckGrants(new List<S3Grant>()
			{
				new()
				{
					Permission = S3Permission.FULL_CONTROL,
					Grantee = new()
					{
						CanonicalUser = AltUserId,
						DisplayName = AltDisplayName,
					}
				},
				new()
				{
					Permission = S3Permission.READ,
					Grantee = new()
					{
						CanonicalUser = AltUserId,
						DisplayName = AltDisplayName,
					}
				},
				new()
				{
					Permission = S3Permission.READ_ACP,
					Grantee = new()
					{
						CanonicalUser = AltUserId,
						DisplayName = AltDisplayName,
					}
				},
				new()
				{
					Permission = S3Permission.WRITE,
					Grantee = new()
					{
						CanonicalUser = AltUserId,
						DisplayName = AltDisplayName,
					}
				},
				new()
				{
					Permission = S3Permission.WRITE_ACP,
					Grantee = new()
					{
						CanonicalUser = AltUserId,
						DisplayName = AltDisplayName,
					}
				},
			},
			GetGrants);
		}

		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "Header")]
		[Trait(MainData.Explanation, "버킷 생성하면서 권한정보를 여러개 보낼때 모두 올바르게 적용되었는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_bucket_header_acl_grants()
		{
			var bucketName = GetNewBucketName();
			var client = GetClient();

			var AltUserId = Config.AltUser.UserId;
			var AltDisplayName = Config.AltUser.DisplayName;

			var Headers = GetACLHeader();

			client.PutBucket(bucketName, headerList: Headers);
			var response = client.GetBucketACL(bucketName);

			var GetGrants = response.AccessControlList.Grants;
			CheckGrants(new List<S3Grant>()
			{
				new()
				{
					Permission = S3Permission.FULL_CONTROL,
					Grantee = new()
					{
						CanonicalUser = AltUserId,
						DisplayName = AltDisplayName,
					}
				},
				new()
				{
					Permission = S3Permission.READ,
					Grantee = new()
					{
						CanonicalUser = AltUserId,
						DisplayName = AltDisplayName,
					}
				},
				new()
				{
					Permission = S3Permission.READ_ACP,
					Grantee = new()
					{
						CanonicalUser = AltUserId,
						DisplayName = AltDisplayName,
					}
				},
				new()
				{
					Permission = S3Permission.WRITE,
					Grantee = new()
					{
						CanonicalUser = AltUserId,
						DisplayName = AltDisplayName,
					}
				},
				new()
				{
					Permission = S3Permission.WRITE_ACP,
					Grantee = new()
					{
						CanonicalUser = AltUserId,
						DisplayName = AltDisplayName,
					}
				},
			},
			GetGrants);
		}

		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "Delete")]
		[Trait(MainData.Explanation, "버킷의 acl 설정이 누락될 경우 실패함을 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_bucket_acl_revoke_all()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			var response = client.GetBucketACL(bucketName);

			Assert.Throws<AggregateException>(()
				=> client.PutBucketACL(bucketName, accessControlPolicy: new() { Owner = null, Grants = response.AccessControlList.Grants }));
			Assert.Throws<AggregateException>(()
				=> client.PutBucketACL(bucketName, accessControlPolicy: new() { Owner = new(), Grants = response.AccessControlList.Grants }));
			Assert.Throws<AggregateException>(()
				=> client.PutBucketACL(bucketName, accessControlPolicy: new() { Owner = response.AccessControlList.Owner, Grants = null }));
			Assert.Throws<AggregateException>(()
				=> client.PutBucketACL(bucketName, accessControlPolicy: new() { Owner = response.AccessControlList.Owner, Grants = new() }));
			Assert.Throws<AggregateException>(()
				=> client.PutBucketACL(bucketName, accessControlPolicy: new() { Owner = null, Grants = null }));
			Assert.Throws<AggregateException>(()
				=> client.PutBucketACL(bucketName, accessControlPolicy: new() { Owner = new(), Grants = new() }));
		}

		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "Delete")]
		[Trait(MainData.Explanation, "오브젝트의 acl 설정이 누락될 경우 실패함을 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void test_object_acl_revoke_all()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var key = "foo";

			client.PutObject(bucketName, key: key, body: "bar");
			var response = client.GetObjectACL(bucketName, key);

			Assert.Throws<AggregateException>(()
				=> client.PutObjectACL(bucketName, key, accessControlPolicy: new() { Owner = null, Grants = response.AccessControlList.Grants }));
			Assert.Throws<AggregateException>(()
				=> client.PutObjectACL(bucketName, key, accessControlPolicy: new() { Owner = new(), Grants = response.AccessControlList.Grants }));
			Assert.Throws<AggregateException>(()
				=> client.PutObjectACL(bucketName, key, accessControlPolicy: new() { Owner = response.AccessControlList.Owner, Grants = null }));
			Assert.Throws<AggregateException>(()
				=> client.PutObjectACL(bucketName, key, accessControlPolicy: new() { Owner = response.AccessControlList.Owner, Grants = new() }));
			Assert.Throws<AggregateException>(()
				=> client.PutObjectACL(bucketName, key, accessControlPolicy: new() { Owner = null, Grants = null }));
			Assert.Throws<AggregateException>(()
				=> client.PutObjectACL(bucketName, key, accessControlPolicy: new() { Owner = new(), Grants = new() }));
		}

		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "Access")]
		[Trait(MainData.Explanation, "[bucket_acl:private, object_acl:private]" +
			"메인유저가 pirvate권한으로 생성한 버킷과 오브젝트를 서브유저가 오브젝트 목록을 보거나 다운로드 할 수 없음을 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_access_bucket_private_object_private()
		{
			var bucketName = SetupBucketAndObjectsACL(out string Key1, out string Key2, out string NewKey, S3CannedACL.Private, S3CannedACL.Private);

			var AltClient = GetAltClient();

			Assert.Throws<AggregateException>(() => AltClient.GetObject(bucketName, Key1));
			Assert.Throws<AggregateException>(() => AltClient.GetObject(bucketName, Key2));
			Assert.Throws<AggregateException>(() => AltClient.ListObjects(bucketName));
			Assert.Throws<AggregateException>(() => AltClient.PutObject(bucketName, Key1, body: Key1));

			var AltClient2 = GetAltClient();
			Assert.Throws<AggregateException>(() => AltClient2.PutObject(bucketName, Key2, body: Key2));
			Assert.Throws<AggregateException>(() => AltClient2.PutObject(bucketName, NewKey, body: NewKey));
		}

		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "Access")]
		[Trait(MainData.Explanation, "[bucket_acl:private, object_acl:private]" +
			"메인유저가 pirvate권한으로 생성한 버킷과 오브젝트를 서브유저가 오브젝트 목록을 보거나 다운로드 할 수 없음을 확인(ListObjects_v2)")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_access_bucket_private_objectv2_private()
		{
			var bucketName = SetupBucketAndObjectsACL(out string Key1, out string Key2, out string NewKey, S3CannedACL.Private, S3CannedACL.Private);

			var AltClient = GetAltClient();

			Assert.Throws<AggregateException>(() => AltClient.GetObject(bucketName, Key1));
			Assert.Throws<AggregateException>(() => AltClient.GetObject(bucketName, Key2));
			Assert.Throws<AggregateException>(() => AltClient.ListObjectsV2(bucketName));
			Assert.Throws<AggregateException>(() => AltClient.PutObject(bucketName, Key1, body: Key1));

			var AltClient2 = GetAltClient();

			Assert.Throws<AggregateException>(() => AltClient2.PutObject(bucketName, Key2, body: Key2));
			Assert.Throws<AggregateException>(() => AltClient2.PutObject(bucketName, NewKey, body: NewKey));
		}

		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "Access")]
		[Trait(MainData.Explanation, "[bucket_acl:private, object_acl:private, public-read] " +
									 "메인유저가 pirvate권한으로 생성한 버킷과 오브젝트는 서브유저가 목록을 보거나 다운로드할 수 없지만 " +
									 "public-read로 설정한 오브젝트는 다운로드 할 수 있음을 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_access_bucket_private_object_publicread()
		{
			var bucketName = SetupBucketAndObjectsACL(out string Key1, out string Key2, out string NewKey, S3CannedACL.Private, S3CannedACL.PublicRead);
			var AltClient = GetAltClient();
			var response = AltClient.GetObject(bucketName, Key1);

			var body = GetBody(response);
			Assert.Equal("foocontent", body);

			Assert.Throws<AggregateException>(() => AltClient.PutObject(bucketName, Key1, body: Key1));

			var AltClient2 = GetAltClient();
			Assert.Throws<AggregateException>(() => AltClient2.GetObject(bucketName, Key2));
			Assert.Throws<AggregateException>(() => AltClient2.PutObject(bucketName, Key2, body: Key2));

			var AltClient3 = GetAltClient();
			Assert.Throws<AggregateException>(() => AltClient3.ListObjects(bucketName));
			Assert.Throws<AggregateException>(() => AltClient3.PutObject(bucketName, NewKey, body: NewKey));
		}

		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "Access")]
		[Trait(MainData.Explanation, "[bucket_acl:private, object_acl:private, public-read] " +
									 "메인유저가 pirvate권한으로 생성한 버킷과 오브젝트는 서브유저가 목록을 보거나 다운로드할 수 없지만 " +
									 "public-read로 설정한 오브젝트는 다운로드 할 수 있음을 확인(ListObjectsV2)")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_access_bucket_private_objectv2_publicread()
		{
			var bucketName = SetupBucketAndObjectsACL(out string Key1, out string Key2, out string NewKey, S3CannedACL.Private, S3CannedACL.PublicRead);
			var AltClient = GetAltClient();
			var response = AltClient.GetObject(bucketName, Key1);

			var body = GetBody(response);
			Assert.Equal("foocontent", body);

			Assert.Throws<AggregateException>(() => AltClient.PutObject(bucketName, Key1, body: Key1));

			var AltClient2 = GetAltClient();
			Assert.Throws<AggregateException>(() => AltClient2.GetObject(bucketName, Key2));
			Assert.Throws<AggregateException>(() => AltClient2.PutObject(bucketName, Key2, body: Key2));

			var AltClient3 = GetAltClient();
			Assert.Throws<AggregateException>(() => AltClient3.ListObjectsV2(bucketName));
			Assert.Throws<AggregateException>(() => AltClient3.PutObject(bucketName, NewKey, body: NewKey));
		}

		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "Access")]
		[Trait(MainData.Explanation, "[bucket_acl:private, object_acl:private, public-read-write]" +
			"메인유저가 pirvate권한으로 생성한 버킷과 오브젝트는 서브유저가 목록을 보거나 다운로드할 수 없지만" +
			"public-read-write로 설정한 오브젝트는 다운로드만 할 수 있음을 확인 " +
			"(버킷의 권한이 private이기 때문에 오브젝트의 권한이 public-read-write로 설정되어있어도 업로드불가)")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_access_bucket_private_object_publicreadwrite()
		{
			var bucketName = SetupBucketAndObjectsACL(out string Key1, out string Key2, out string NewKey, S3CannedACL.Private, S3CannedACL.PublicReadWrite);
			var AltClient = GetAltClient();
			var response = AltClient.GetObject(bucketName, Key1);

			var body = GetBody(response);
			Assert.Equal("foocontent", body);

			Assert.Throws<AggregateException>(() => AltClient.PutObject(bucketName, Key1, body: Key1));

			var AltClient2 = GetAltClient();
			Assert.Throws<AggregateException>(() => AltClient2.GetObject(bucketName, Key2));
			Assert.Throws<AggregateException>(() => AltClient2.PutObject(bucketName, Key2, body: Key2));

			var AltClient3 = GetAltClient();
			var ObjList = GetKeys(AltClient3.ListObjects(bucketName));
			Assert.Equal(new List<string>() { Key2, Key1 }, ObjList);
			Assert.Throws<AggregateException>(() => AltClient3.PutObject(bucketName, NewKey, body: NewKey));
		}

		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "Access")]
		[Trait(MainData.Explanation, "[bucket_acl:private, object_acl:private, public-read-write]" +
			"메인유저가 pirvate권한으로 생성한 버킷과 오브젝트는 서브유저가 목록을 보거나 다운로드할 수 없지만" +
			"public-read-write로 설정한 오브젝트는 다운로드만 할 수 있음을 확인 (ListObjectsV2)" +
			"(버킷의 권한이 private이기 때문에 오브젝트의 권한이 public-read-write로 설정되어있어도 업로드불가)")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_access_bucket_private_objectv2_publicreadwrite()
		{
			var bucketName = SetupBucketAndObjectsACL(out string Key1, out string Key2, out string NewKey, S3CannedACL.Private, S3CannedACL.PublicReadWrite);
			var AltClient = GetAltClient();
			var response = AltClient.GetObject(bucketName, Key1);

			var body = GetBody(response);
			Assert.Equal("foocontent", body);

			Assert.Throws<AggregateException>(() => AltClient.PutObject(bucketName, Key1, body: Key1));

			var AltClient2 = GetAltClient();
			Assert.Throws<AggregateException>(() => AltClient2.GetObject(bucketName, Key2));
			Assert.Throws<AggregateException>(() => AltClient2.PutObject(bucketName, Key2, body: Key2));

			var AltClient3 = GetAltClient();
			Assert.Throws<AggregateException>(() => AltClient3.ListObjectsV2(bucketName));
			Assert.Throws<AggregateException>(() => AltClient3.PutObject(bucketName, NewKey, body: NewKey));
		}

		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "Access")]
		[Trait(MainData.Explanation, "[bucket_acl:public-read, object_acl:private] " +
									 "메인유저가 public-read권한으로 생성한 버킷에서 private권한으로 생성한 오브젝트에 대해 " +
									 "서브유저는 오브젝트 목록만 볼 수 있음을 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_access_bucket_publicread_object_private()
		{
			var bucketName = SetupBucketAndObjectsACL(out string Key1, out string Key2, out string NewKey, S3CannedACL.PublicRead, S3CannedACL.Private);
			var AltClient = GetAltClient();

			Assert.Throws<AggregateException>(() => AltClient.GetObject(bucketName, Key1));
			Assert.Throws<AggregateException>(() => AltClient.PutObject(bucketName, Key1, body: Key1));

			var AltClient2 = GetAltClient();
			Assert.Throws<AggregateException>(() => AltClient2.GetObject(bucketName, Key2));
			Assert.Throws<AggregateException>(() => AltClient2.PutObject(bucketName, Key2, body: Key2));

			var AltClient3 = GetAltClient();
			var ObjList = GetKeys(AltClient3.ListObjects(bucketName));
			Assert.Equal(new List<string>() { Key2, Key1 }, ObjList);
			Assert.Throws<AggregateException>(() => AltClient3.PutObject(bucketName, NewKey, body: NewKey));
		}

		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "Access")]
		[Trait(MainData.Explanation, "[bucket_acl:public-read, object_acl:public-read, private] " +
									 "메인유저가 public-read권한으로 생성한 버킷에서 private권한으로 생성한 오브젝트에 대해 " +
									 "서브유저는 오브젝트 목록만 볼 수 있음을 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_access_bucket_publicread_object_publicread()
		{
			var bucketName = SetupBucketAndObjectsACL(out string Key1, out string Key2, out string NewKey, S3CannedACL.PublicRead, S3CannedACL.PublicRead);
			var AltClient = GetAltClient();

			var response = AltClient.GetObject(bucketName, Key1);
			var body = GetBody(response);
			Assert.Equal("foocontent", body);

			Assert.Throws<AggregateException>(() => AltClient.PutObject(bucketName, Key1, body: Key1));

			var AltClient2 = GetAltClient();
			Assert.Throws<AggregateException>(() => AltClient2.GetObject(bucketName, Key2));
			Assert.Throws<AggregateException>(() => AltClient2.PutObject(bucketName, Key2, body: Key2));

			var AltClient3 = GetAltClient();
			var ObjList = GetKeys(AltClient3.ListObjects(bucketName));
			Assert.Equal(new List<string>() { Key2, Key1 }, ObjList);
			Assert.Throws<AggregateException>(() => AltClient3.PutObject(bucketName, NewKey, body: NewKey));
		}

		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "Access")]
		[Trait(MainData.Explanation, "[bucket_acl:public-read, object_acl:public-read-wirte, private]" +
			"메인유저가 public-read권한으로 생성한 버킷에서 public-read-write권한으로 생성한 오브젝트에 대해" +
			"서브유저는 오브젝트 목록을 보거나 다운로드 할 수 있음을 확인" +
			"(버킷의 권한이 public-read이기 때문에 오브젝트의 권한이 public-read-write로 설정되어있어도 수정불가)")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_access_bucket_publicread_object_publicreadwrite()
		{
			var bucketName = SetupBucketAndObjectsACL(out string Key1, out string Key2, out string NewKey, S3CannedACL.PublicRead, S3CannedACL.PublicReadWrite);
			var AltClient = GetAltClient();

			var response = AltClient.GetObject(bucketName, Key1);
			var body = GetBody(response);
			Assert.Equal("foocontent", body);

			Assert.Throws<AggregateException>(() => AltClient.PutObject(bucketName, Key1, body: Key1));

			var AltClient2 = GetAltClient();
			Assert.Throws<AggregateException>(() => AltClient2.GetObject(bucketName, Key2));
			Assert.Throws<AggregateException>(() => AltClient2.PutObject(bucketName, Key2, body: Key2));

			var AltClient3 = GetAltClient();
			var ObjList = GetKeys(AltClient3.ListObjects(bucketName));
			Assert.Equal(new List<string>() { Key2, Key1 }, ObjList);
			Assert.Throws<AggregateException>(() => AltClient3.PutObject(bucketName, NewKey, body: NewKey));
		}

		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "Access")]
		[Trait(MainData.Explanation, "[bucket_acl:public-read-write, object_acl:private] " +
			"메인유저가 public-read-write권한으로 생성한 버킷에서 private권한으로 생성한 오브젝트에 대해 " +
			"서브유저는 오브젝트 목록조회는 가능하지만 다운로드 할 수 없음을 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_access_bucket_publicreadwrite_object_private()
		{
			var bucketName = SetupBucketAndObjectsACL(out string Key1, out string Key2, out string NewKey, S3CannedACL.PublicReadWrite, S3CannedACL.Private);
			var AltClient = GetAltClient();

			Assert.Throws<AggregateException>(() => AltClient.GetObject(bucketName, Key1));
			Assert.Throws<AggregateException>(() => AltClient.PutObject(bucketName, Key1, body: Key1));

			Assert.Throws<AggregateException>(() => AltClient.GetObject(bucketName, Key2));
			Assert.Throws<AggregateException>(() => AltClient.PutObject(bucketName, Key2, body: Key2));

			var ObjList = GetKeys(AltClient.ListObjects(bucketName));
			Assert.Equal(new List<string>() { Key2, Key1 }, ObjList);
			AltClient.PutObject(bucketName, NewKey, body: NewKey);
		}

		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "Access")]
		[Trait(MainData.Explanation, "[bucket_acl:public-read-write, object_acl:public-read, private] " +
									 "메인유저가 public-read-write권한으로 생성한 버킷에서 public-read권한으로 생성한 오브젝트에 대해 " +
									 "서브유저는 오브젝트 목록을 읽거나 다운로드 가능함을 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_access_bucket_publicreadwrite_object_publicread()
		{
			var bucketName = SetupBucketAndObjectsACL(out string Key1, out string Key2, out string NewKey, S3CannedACL.PublicReadWrite, S3CannedACL.PublicRead);
			var AltClient = GetAltClient();

			var response = AltClient.GetObject(bucketName, Key1);
			var body = GetBody(response);
			Assert.Equal("foocontent", body);
			Assert.Throws<AggregateException>(() => AltClient.PutObject(bucketName, Key1, body: Key1));

			Assert.Throws<AggregateException>(() => AltClient.GetObject(bucketName, Key2));
			Assert.Throws<AggregateException>(() => AltClient.PutObject(bucketName, Key2, body: Key2));

			var ObjList = GetKeys(AltClient.ListObjects(bucketName));
			Assert.Equal(new List<string>() { Key2, Key1 }, ObjList);
			AltClient.PutObject(bucketName, NewKey, body: NewKey);
		}

		[Fact]
		[Trait(MainData.Major, "Grants")]
		[Trait(MainData.Minor, "Access")]
		[Trait(MainData.Explanation, "[bucket_acl:public-read-write, object_acl:public-read-write, private] " +
									 "메인유저가 public-read-write권한으로 생성한 버킷에서 public-read-write권한으로 생성한 오브젝트에 대해 " +
									 "서브유저는 오브젝트 목록을 읽거나 다운로드 가능함을 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void test_access_bucket_publicreadwrite_object_publicreadwrite()
		{
			var bucketName = SetupBucketAndObjectsACL(out string Key1, out string Key2, out string NewKey, S3CannedACL.PublicReadWrite, S3CannedACL.PublicReadWrite);
			var AltClient = GetAltClient();

			var response = AltClient.GetObject(bucketName, Key1);
			var body = GetBody(response);
			Assert.Equal(Key1, body);
			Assert.Throws<AggregateException>(() => AltClient.PutObject(bucketName, Key1, body: Key1));

			Assert.Throws<AggregateException>(() => AltClient.GetObject(bucketName, Key2));
			Assert.Throws<AggregateException>(() => AltClient.PutObject(bucketName, Key2, body: Key2));

			var ObjList = GetKeys(AltClient.ListObjects(bucketName));
			Assert.Equal(new List<string>() { Key2, Key1 }, ObjList);
			AltClient.PutObject(bucketName, NewKey, body: NewKey);
		}
	}

}
