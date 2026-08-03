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
using System;
using System.Collections.Generic;
using System.Net;
using s3tests.Utils;
using Xunit;

namespace s3tests.Test
{
	public class PutBucket : TestBase
	{
		public PutBucket(Xunit.Abstractions.ITestOutputHelper Output) => this.Output = Output;

		[Fact]
		[Trait(MainData.Major, "PutBucket")]
		[Trait(MainData.Minor, "PUT")]
		[Trait(MainData.Explanation, "생성한 버킷이 비어있는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketListEmpty()
		{
			TestId = 1;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var Response = client.ListObjects(bucketName);
			Assert.Empty(Response.S3Objects);
		}

		[Fact]
		[Trait(MainData.Major, "PutBucket")]
		[Trait(MainData.Minor, "Creation rules")]
		[Trait(MainData.Explanation, "생성할 버킷이름의 맨앞에 [_]가 있을 경우 버킷 생성 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestBucketCreateNamingBadStartsNonAlpha()
		{
			TestId = 2;
			var bucketName = GetNewBucketName();
			CheckBadBucketName("_" + bucketName);
		}

		[Fact]
		[Trait(MainData.Major, "PutBucket")]
		[Trait(MainData.Minor, "Creation rules")]
		[Trait(MainData.Explanation, "생성할 버킷이름이 한글자인 경우 버킷 생성 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestBucketCreateNamingBadShortOne()
		{
			TestId = 3;
			CheckBadBucketName("a");
		}

		[Fact]
		[Trait(MainData.Major, "PutBucket")]
		[Trait(MainData.Minor, "Creation rules")]
		[Trait(MainData.Explanation, "생성할 버킷이름이 두글자인 경우 버킷 생성 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestBucketCreateNamingBadShortTwo()
		{
			TestId = 4;
			CheckBadBucketName("aa");
		}

		[Fact]
		[Trait(MainData.Major, "PutBucket")]
		[Trait(MainData.Minor, "Creation rules")]
		[Trait(MainData.Explanation, "생성할 버킷이름이 60자인 경우 버킷 생성 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketCreateNamingGoodLong60()
		{
			TestId = 5;
			TestBucketCreateNamingGoodLong(60);
		}

		[Fact]
		[Trait(MainData.Major, "PutBucket")]
		[Trait(MainData.Minor, "Creation rules")]
		[Trait(MainData.Explanation, "생성할 버킷이름이 61자인 경우 버킷 생성 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketCreateNamingGoodLong61()
		{
			TestId = 6;
			TestBucketCreateNamingGoodLong(61);
		}

		[Fact]
		[Trait(MainData.Major, "PutBucket")]
		[Trait(MainData.Minor, "Creation rules")]
		[Trait(MainData.Explanation, "생성할 버킷이름이 62자인 경우 버킷 생성 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketCreateNamingGoodLong62()
		{
			TestId = 7;
			TestBucketCreateNamingGoodLong(62);
		}

		[Fact]
		[Trait(MainData.Major, "PutBucket")]
		[Trait(MainData.Minor, "Creation rules")]
		[Trait(MainData.Explanation, "생성할 버킷이름이 63자인 경우 버킷 생성 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketCreateNamingGoodLong63()
		{
			TestId = 8;
			TestBucketCreateNamingGoodLong(63);
		}

		[Fact]
		[Trait(MainData.Major, "PutBucket")]
		[Trait(MainData.Minor, "Creation rules")]
		[Trait(MainData.Explanation, "생성할 버킷이름이 64자인 경우 버킷 생성 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestBucketCreateNamingGoodLong64()
		{
			TestId = 9;
			TestBucketCreateNamingBadLong(64);
		}

		[Fact]
		[Trait(MainData.Major, "PutBucket")]
		[Trait(MainData.Minor, "Creation rules")]
		[Trait(MainData.Explanation, "생성할 버킷이름이 IP 주소로 되어 있을 경우 버킷 생성 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestBucketCreateNamingBadIp()
		{
			TestId = 10;
			CheckBadBucketName("192.168.11.123");
		}

		[Fact]
		[Trait(MainData.Major, "PutBucket")]
		[Trait(MainData.Minor, "Creation rules")]
		[Trait(MainData.Explanation, "생성할 버킷이름에 문자와 [_]가 포함되어 있을 경우 버킷 생성 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestBucketCreateNamingDnsUnderscore()
		{
			TestId = 11;
			CheckBadBucketName("foo_bar");
		}

		[Fact]
		[Trait(MainData.Major, "PutBucket")]
		[Trait(MainData.Minor, "Creation rules")]
		[Trait(MainData.Explanation, "생성할 버킷이름이 랜덤 알파벳 63자로 구성된 경우 버킷 생성 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketCreateNamingDnsLong()
		{
			TestId = 12;
			var prefix = GetPrefix();
			var AddLength = 63 - prefix.Length;
			prefix = S3Utils.RandomText(AddLength);
			CheckGoodBucketName(prefix);
		}

		[Fact]
		[Trait(MainData.Major, "PutBucket")]
		[Trait(MainData.Minor, "Creation rules")]
		[Trait(MainData.Explanation, "생성할 버킷이름의 끝이 [-]로 끝날 경우 버킷 생성 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestBucketCreateNamingDnsDashAtEnd()
		{
			TestId = 13;
			CheckBadBucketName("foo-");
		}

		[Fact]
		[Trait(MainData.Major, "PutBucket")]
		[Trait(MainData.Minor, "Creation rules")]
		[Trait(MainData.Explanation, "생성할 버킷이름에 문자와 [..]가 포함되어 있을 경우 버킷 생성 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestBucketCreateNamingDnsDotDot()
		{
			TestId = 14;
			CheckBadBucketName("foo..bar");
		}

		[Fact]
		[Trait(MainData.Major, "PutBucket")]
		[Trait(MainData.Minor, "Creation rules")]
		[Trait(MainData.Explanation, "생성할 버킷이름의 사이에 [.-]가 포함되어 있을 경우 버킷 생성 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestBucketCreateNamingDnsDotDash()
		{
			TestId = 15;
			CheckBadBucketName("foo.-bar");
		}

		[Fact]
		[Trait(MainData.Major, "PutBucket")]
		[Trait(MainData.Minor, "Creation rules")]
		[Trait(MainData.Explanation, "생성할 버킷이름의 사이에 [-.]가 포함되어 있을 경우 버킷 생성 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestBucketCreateNamingDnsDashDot()
		{
			TestId = 16;
			CheckBadBucketName("foo-.bar");
		}

		[Fact]
		[Trait(MainData.Major, "PutBucket")]
		[Trait(MainData.Minor, "Duplicate")]
		[Trait(MainData.Explanation, "버킷 중복 생성시 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestBucketCreateExists()
		{
			TestId = 17;
			var bucketName = GetNewBucketName();
			var client = GetClient();

			client.PutBucket(bucketName);

			var e = Assert.Throws<AggregateException>(() => client.PutBucket(bucketName));
			Assert.Equal(HttpStatusCode.Conflict, GetStatus(e));
			Assert.Equal(MainData.BUCKET_ALREADY_OWNED_BY_YOU, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "PutBucket")]
		[Trait(MainData.Minor, "Duplicate")]
		[Trait(MainData.Explanation, "[다른 2명의 사용자가 버킷 생성하려고 할 경우] " +
									 "메인유저가 버킷을 생성하고 서브유저가가 같은 이름으로 버킷 생성하려고 할 경우 실패 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestBucketCreateExistsNonowner()
		{
			TestId = 18;
			var bucketName = GetNewBucketName();
			var client = GetClient();
			var AltClient = GetAltClient();

			client.PutBucket(bucketName);

			var e = Assert.Throws<AggregateException>(() => AltClient.PutBucket(bucketName));
			Assert.Equal(HttpStatusCode.Conflict, GetStatus(e));
			Assert.Equal(MainData.BUCKET_ALREADY_EXISTS, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "PutBucket")]
		[Trait(MainData.Minor, "Creation rules")]
		[Trait(MainData.Explanation, "생성할 버킷의 이름이 알파벳으로 시작할 경우 생성되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketCreateNamingGoodStartsAlpha()
		{
			TestId = 19;
			CheckGoodBucketName("foo", prefix: "a" + GetPrefix());
		}

		[Fact]
		[Trait(MainData.Major, "PutBucket")]
		[Trait(MainData.Minor, "Creation rules")]
		[Trait(MainData.Explanation, "생성할 버킷의 이름이 알파벳으로 시작할 경우 생성되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketCreateNamingGoodStartsDigit()
		{
			TestId = 20;
			CheckGoodBucketName("foo", prefix: "0" + GetPrefix());
		}

		[Fact]
		[Trait(MainData.Major, "PutBucket")]
		[Trait(MainData.Minor, "Creation rules")]
		[Trait(MainData.Explanation, "생성할 버킷의 이름 중간에 [.]이 포함된 이름일 경우 생성되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketCreateNamingGoodContainsPeriod()
		{
			TestId = 21;
			CheckGoodBucketName("aaa.111");
		}

		[Fact]
		[Trait(MainData.Major, "PutBucket")]
		[Trait(MainData.Minor, "Creation rules")]
		[Trait(MainData.Explanation, "생성할 버킷의 이름 중간에 [-]이 포함된 이름일 경우 생성되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestBucketCreateNamingGoodContainsHyphen()
		{
			TestId = 22;
			CheckGoodBucketName("aaa-111");
		}

		[Fact]
		[Trait(MainData.Major, "PutBucket")]
		[Trait(MainData.Minor, "Duplicate")]
		[Trait(MainData.Explanation, "버킷 생성하고 오브젝트를 업로드한뒤 같은 이름의 버킷 생성하면 기존정보가 그대로 유지되는지 확인" +
									 "(버킷은 중복 생성 할 수 없음을 확인)")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestBucketRecreateNotOverriding()
		{
			TestId = 23;
			var KeyNames = new List<string>() { "mykey1", "mykey2" };
			var bucketName = SetupObjects(KeyNames);

			var ObjectList = GetObjectList(bucketName);
			Assert.Equal(KeyNames, ObjectList);

			var client = GetClient();
			Assert.Throws<AggregateException>(() => client.PutBucket(bucketName));

			ObjectList = GetObjectList(bucketName);
			Assert.Equal(KeyNames, ObjectList);
		}

		[Fact]
		[Trait(MainData.Major, "PutBucket")]
		[Trait(MainData.Minor, "location")]
		[Trait(MainData.Explanation, "버킷의 location 정보 조회")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestGetBucketLocation()
		{
			TestId = 24;
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			client.GetBucketLocation(bucketName);
		}
	}
}
