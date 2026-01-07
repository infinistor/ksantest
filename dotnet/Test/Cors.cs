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
using System.Collections.Generic;
using System.Net;
using Xunit;

namespace s3tests.Test
{
	public class Cors : TestBase
	{
		public Cors(Xunit.Abstractions.ITestOutputHelper Output) => this.Output = Output;

		[Fact]
		[Trait(MainData.Major, "Cors")]
		[Trait(MainData.Minor, "Check")]
		[Trait(MainData.Explanation, "버킷의 cors정보 세팅 성공 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestSetCors()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			var allowedMethods = new List<string>() { "GET", "PUT" };
			var allowedOrigins = new List<string>() { "*.get", "*.put" };

			var corsConfig = new CORSConfiguration()
			{
				Rules = [
					new(){
						AllowedMethods = allowedMethods,
						AllowedOrigins = allowedOrigins
					},
				]
			};

			var response = client.GetCORSConfiguration(bucketName);
			Assert.Null(response.Configuration);

			client.PutCORSConfiguration(bucketName, corsConfig);
			response = client.GetCORSConfiguration(bucketName);
			Assert.Equal(allowedMethods, response.Configuration.Rules[0].AllowedMethods);
			Assert.Equal(allowedOrigins, response.Configuration.Rules[0].AllowedOrigins);

			client.DeleteCORSConfiguration(bucketName);
			response = client.GetCORSConfiguration(bucketName);
			Assert.Null(response.Configuration);
		}

		[Fact]
		[Trait(MainData.Major, "Cors")]
		[Trait(MainData.Minor, "Post")]
		[Trait(MainData.Explanation, "버킷의 cors정보를 URL로 읽고 쓰기 성공/실패 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestCorsOriginResponse()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			client.PutBucketACL(bucketName, S3CannedACL.PublicRead);

			var corsConfig = new CORSConfiguration()
			{
				Rules = [
					new(){
						AllowedMethods =  ["GET"],
						AllowedOrigins =  ["*suffix"],
					},
					new(){
						AllowedMethods =  ["GET"],
						AllowedOrigins =  ["start*end"],
					},
					new(){
						AllowedMethods =  ["GET"],
						AllowedOrigins =  ["prefix*"],
					},
					new(){
						AllowedMethods =  ["PUT"],
						AllowedOrigins =  ["*.put"],
					},
				]
			};

			var response = client.GetCORSConfiguration(bucketName);
			Assert.Null(response.Configuration);

			client.PutCORSConfiguration(bucketName, corsConfig);

			CorsRequestAndCheck("Get", bucketName, [], HttpStatusCode.OK, null, null);
			CorsRequestAndCheck("Get", bucketName, [new("Origin", "foo.suffix")], HttpStatusCode.OK, "foo.suffix", "GET");
			CorsRequestAndCheck("Get", bucketName, [new("Origin", "foo.bar")], HttpStatusCode.OK, null, null);
			CorsRequestAndCheck("Get", bucketName, [new("Origin", "foo.suffix.get")], HttpStatusCode.OK, null, null);
			CorsRequestAndCheck("Get", bucketName, [new("Origin", "startend")], HttpStatusCode.OK, "startend", "GET");
			CorsRequestAndCheck("Get", bucketName, [new("Origin", "start1end")], HttpStatusCode.OK, "start1end", "GET");
			CorsRequestAndCheck("Get", bucketName, [new("Origin", "start12end")], HttpStatusCode.OK, "start12end", "GET");
			CorsRequestAndCheck("Get", bucketName, [new("Origin", "0start12end")], HttpStatusCode.OK, null, null);
			CorsRequestAndCheck("Get", bucketName, [new("Origin", "prefix")], HttpStatusCode.OK, "prefix", "GET");
			CorsRequestAndCheck("Get", bucketName, [new("Origin", "prefix.suffix")], HttpStatusCode.OK, "prefix.suffix", "GET");
			CorsRequestAndCheck("Get", bucketName, [new("Origin", "bla.prefix")], HttpStatusCode.OK, null, null);

			CorsRequestAndCheck("Get", bucketName, [new("Origin", "foo.suffix")], HttpStatusCode.NotFound, "foo.suffix", "GET", key: "bar");
			CorsRequestAndCheck("Put", bucketName, [new("Origin", "foo.suffix"), new("Access-Control-Request-Method", "GET"), new("content-length", "0")], HttpStatusCode.Forbidden, "foo.suffix", "GET", key: "bar");
			CorsRequestAndCheck("Put", bucketName, [new("Origin", "foo.suffix"), new("Access-Control-Request-Method", "PUT"), new("content-length", "0")], HttpStatusCode.Forbidden, null, null, key: "bar");

			CorsRequestAndCheck("Put", bucketName, [new("Origin", "foo.suffix"), new("Access-Control-Request-Method", "DELETE"), new("content-length", "0")], HttpStatusCode.Forbidden, null, null, key: "bar");
			CorsRequestAndCheck("Put", bucketName, [new("Origin", "foo.suffix"), new("content-length", "0")], HttpStatusCode.Forbidden, null, null, key: "bar");

			CorsRequestAndCheck("Put", bucketName, [new("Origin", "foo.put"), new("content-length", "0")], HttpStatusCode.Forbidden, "foo.put", "PUT", key: "bar");

			CorsRequestAndCheck("Get", bucketName, [new("Origin", "foo.suffix")], HttpStatusCode.NotFound, "foo.suffix", "GET", key: "bar");

			CorsRequestAndCheck("OPTIONS", bucketName, [], HttpStatusCode.BadRequest, null, null);
			CorsRequestAndCheck("OPTIONS", bucketName, [new("Origin", "foo.suffix")], HttpStatusCode.Forbidden, null, null);
			CorsRequestAndCheck("OPTIONS", bucketName, [new("Origin", "foo.bla")], HttpStatusCode.Forbidden, null, null);
			CorsRequestAndCheck("OPTIONS", bucketName, [new("Origin", "foo.suffix"), new("Access-Control-Request-Method", "GET"), new("content-length", "0")], HttpStatusCode.OK, "foo.suffix", "GET", key: "bar");
			CorsRequestAndCheck("OPTIONS", bucketName, [new("Origin", "foo.bar"), new("Access-Control-Request-Method", "GET")], HttpStatusCode.Forbidden, null, null);
			CorsRequestAndCheck("OPTIONS", bucketName, [new("Origin", "foo.suffix.get"), new("Access-Control-Request-Method", "GET")], HttpStatusCode.Forbidden, null, null);
			CorsRequestAndCheck("OPTIONS", bucketName, [new("Origin", "startend"), new("Access-Control-Request-Method", "GET")], HttpStatusCode.OK, "startend", "GET");
			CorsRequestAndCheck("OPTIONS", bucketName, [new("Origin", "start1end"), new("Access-Control-Request-Method", "GET")], HttpStatusCode.OK, "start1end", "GET");
			CorsRequestAndCheck("OPTIONS", bucketName, [new("Origin", "start12end"), new("Access-Control-Request-Method", "GET")], HttpStatusCode.OK, "start12end", "GET");
			CorsRequestAndCheck("OPTIONS", bucketName, [new("Origin", "0start12end"), new("Access-Control-Request-Method", "GET")], HttpStatusCode.Forbidden, null, null);
			CorsRequestAndCheck("OPTIONS", bucketName, [new("Origin", "prefix"), new("Access-Control-Request-Method", "GET")], HttpStatusCode.OK, "prefix", "GET");
			CorsRequestAndCheck("OPTIONS", bucketName, [new("Origin", "prefix.suffix"), new("Access-Control-Request-Method", "GET")], HttpStatusCode.OK, "prefix.suffix", "GET");
			CorsRequestAndCheck("OPTIONS", bucketName, [new("Origin", "bla.prefix"), new("Access-Control-Request-Method", "GET")], HttpStatusCode.Forbidden, null, null);
			CorsRequestAndCheck("OPTIONS", bucketName, [new("Origin", "foo.put"), new("Access-Control-Request-Method", "GET")], HttpStatusCode.Forbidden, null, null);
			CorsRequestAndCheck("OPTIONS", bucketName, [new("Origin", "foo.put"), new("Access-Control-Request-Method", "PUT")], HttpStatusCode.OK, "foo.put", "PUT");
		}

		[Fact]
		[Trait(MainData.Major, "Cors")]
		[Trait(MainData.Minor, "Post")]
		[Trait(MainData.Explanation, "와일드카드 문자만 입력하여 cors설정을 하였을때 정상적으로 동작하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestCorsOriginWildcard()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			client.PutBucketACL(bucketName, S3CannedACL.PublicRead);

			var corsConfig = new CORSConfiguration()
			{
				Rules = [
					new(){
						AllowedMethods =  ["GET"],
						AllowedOrigins =  ["*"],
					},
				]
			};

			var response = client.GetCORSConfiguration(bucketName);
			Assert.Null(response.Configuration);

			client.PutCORSConfiguration(bucketName, corsConfig);

			CorsRequestAndCheck("Get", bucketName, [], HttpStatusCode.OK, null, null);
			CorsRequestAndCheck("Get", bucketName, [new("Origin", "example.origin")], HttpStatusCode.OK, "*", "GET");
		}

		[Fact]
		[Trait(MainData.Major, "Cors")]
		[Trait(MainData.Minor, "Post")]
		[Trait(MainData.Explanation, "cors옵션에서 사용자 추가 헤더를 설정하고 존재하지 않는 헤더를 request 설정한 채로 curs호출하면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestCorsHeaderOption()
		{
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			client.PutBucketACL(bucketName, S3CannedACL.PublicRead);

			var corsConfig = new CORSConfiguration()
			{
				Rules = [
					new(){
						AllowedMethods = ["GET"],
						AllowedOrigins = ["*"],
						ExposeHeaders  = ["x-amz-meta-header1"],
					},
				]
			};

			var response = client.GetCORSConfiguration(bucketName);
			Assert.Null(response.Configuration);

			client.PutCORSConfiguration(bucketName, corsConfig);

			CorsRequestAndCheck("OPTIONS", bucketName, [new("Origin", "example.origin"), new("Access-Control-Request-Headers", "x-amz-meta-header2"), new("Access-Control-Request-Method", "GET")], HttpStatusCode.Forbidden, null, null, key: "bar");
		}
	}
}
