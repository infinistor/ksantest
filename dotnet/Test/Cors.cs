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
			TestId = 1;
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

			AssertNoCors(client, bucketName);

			client.PutCORSConfiguration(bucketName, corsConfig);
			var response = client.GetCORSConfiguration(bucketName);
			Assert.Equal(allowedMethods, response.Configuration.Rules[0].AllowedMethods);
			Assert.Equal(allowedOrigins, response.Configuration.Rules[0].AllowedOrigins);

			client.DeleteCORSConfiguration(bucketName);
			AssertNoCors(client, bucketName);
		}

		/// <summary>
		/// CORS 설정이 없는 상태를 확인한다.
		/// AWS는 빈 응답이 아니라 404 NoSuchCORSConfiguration을 반환한다.
		/// </summary>
		private static void AssertNoCors(S3Client client, string bucketName)
		{
			try
			{
				var response = client.GetCORSConfiguration(bucketName);
				Assert.Null(response.Configuration);
			}
			catch (AggregateException e)
			{
				Assert.Equal(HttpStatusCode.NotFound, GetStatus(e));
				Assert.Equal(MainData.NO_SUCH_CORS_CONFIGURATION, GetErrorCode(e));
			}
		}

		[Fact]
		[Trait(MainData.Major, "Cors")]
		[Trait(MainData.Minor, "Post")]
		[Trait(MainData.Explanation, "버킷의 cors정보를 URL로 읽고 쓰기 성공/실패 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		// KSAN 알려진 실패(일부러 다르게 동작, 고치지 않음): SCP 연동 때문에 CORS 설정이 있는 버킷의
		// OPTIONS 는 Origin 이 없거나 규칙에 맞지 않아도 200 을 준다(2026-04 InfiniStor 와 같게 맞춤).
		// AWS 는 Origin 이 없으면 400, 규칙에 맞지 않으면 403 이다.
		public void TestCorsOriginResponse()
		{
			TestId = 2;
			var client = GetClient();
			var bucketName = GetNewBucketCannedAcl(client, S3CannedACL.PublicRead);

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

			AssertNoCors(client, bucketName);

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
			TestId = 3;
			var client = GetClient();
			var bucketName = GetNewBucketCannedAcl(client, S3CannedACL.PublicRead);

			var corsConfig = new CORSConfiguration()
			{
				Rules = [
					new(){
						AllowedMethods =  ["GET"],
						AllowedOrigins =  ["*"],
					},
				]
			};

			AssertNoCors(client, bucketName);

			client.PutCORSConfiguration(bucketName, corsConfig);

			CorsRequestAndCheck("Get", bucketName, [], HttpStatusCode.OK, null, null);
			CorsRequestAndCheck("Get", bucketName, [new("Origin", "example.origin")], HttpStatusCode.OK, "*", "GET");
		}

		[Fact]
		[Trait(MainData.Major, "Cors")]
		[Trait(MainData.Minor, "Post")]
		[Trait(MainData.Explanation, "cors옵션에서 사용자 추가 헤더를 설정하고 존재하지 않는 헤더를 request 설정한 채로 curs호출하면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		// KSAN 알려진 실패(일부러 다르게 동작, 고치지 않음): SCP 연동 때문에 허용하지 않은
		// Access-Control-Request-Headers 로 preflight 해도 200 을 준다(InfiniStor 와 같음). AWS 는 403 이다.
		public void TestCorsHeaderOption()
		{
			TestId = 4;
			var client = GetClient();
			var bucketName = GetNewBucketCannedAcl(client, S3CannedACL.PublicRead);

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

			AssertNoCors(client, bucketName);

			client.PutCORSConfiguration(bucketName, corsConfig);

			CorsRequestAndCheck("OPTIONS", bucketName, [new("Origin", "example.origin"), new("Access-Control-Request-Headers", "x-amz-meta-header2"), new("Access-Control-Request-Method", "GET")], HttpStatusCode.Forbidden, null, null, key: "bar");
		}

		private static CORSRule CorsRule(List<string> methods, List<string> origins, string id = null,
			List<string> allowedHeaders = null, List<string> exposeHeaders = null, int? maxAgeSeconds = null)
		{
			var rule = new CORSRule { AllowedMethods = methods, AllowedOrigins = origins };
			if (id != null) rule.Id = id;
			if (allowedHeaders != null) rule.AllowedHeaders = allowedHeaders;
			if (exposeHeaders != null) rule.ExposeHeaders = exposeHeaders;
			if (maxAgeSeconds != null) rule.MaxAgeSeconds = maxAgeSeconds.Value;
			return rule;
		}

		private static void PutCors(S3Client client, string bucketName, List<CORSRule> rules)
			=> client.PutCORSConfiguration(bucketName, new CORSConfiguration { Rules = rules });

		private static void AssertInvalidCors(S3Client client, string bucketName, List<CORSRule> rules)
		{
			var e = Assert.Throws<AggregateException>(() => PutCors(client, bucketName, rules));
			Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
			Assert.Equal(MainData.INVALID_REQUEST, GetErrorCode(e));
		}

		private static string Header(Dictionary<string, string> headers, string name)
			=> headers.TryGetValue(name, out var value) ? value : null;

		[Fact]
		[Trait(MainData.Major, "Cors")]
		[Trait(MainData.Minor, "Check")]
		[Trait(MainData.Explanation, "cors 규칙의 모든 필드(ID, AllowedHeaders, ExposeHeaders, MaxAgeSeconds)가 저장되고 조회되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestCorsAllFields()
		{
			TestId = 5;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			List<CORSRule> rules =
			[
				CorsRule(["GET", "PUT"], ["http://a.example.com"], id: "rule-1",
					allowedHeaders: ["x-amz-*", "content-type"],
					exposeHeaders: ["ETag", "x-amz-meta-foo"],
					maxAgeSeconds: 600),
				CorsRule(["GET"], ["*"], id: "rule-2", maxAgeSeconds: 20),
			];
			PutCors(client, bucketName, rules);

			var response = client.GetCORSConfiguration(bucketName);
			Assert.Equal(2, response.Configuration.Rules.Count);
			var first = response.Configuration.Rules[0];
			var second = response.Configuration.Rules[1];
			Assert.Equal("rule-1", first.Id);
			Assert.Equal(["GET", "PUT"], first.AllowedMethods);
			Assert.Equal(["http://a.example.com"], first.AllowedOrigins);
			Assert.Equal(["x-amz-*", "content-type"], first.AllowedHeaders);
			Assert.Equal(["ETag", "x-amz-meta-foo"], first.ExposeHeaders);
			Assert.Equal(600, first.MaxAgeSeconds);
			Assert.Equal("rule-2", second.Id);
			Assert.Equal(["GET"], second.AllowedMethods);
			Assert.Equal(["*"], second.AllowedOrigins);
			Assert.Equal(20, second.MaxAgeSeconds);
		}

		[Fact]
		[Trait(MainData.Major, "Cors")]
		[Trait(MainData.Minor, "Preflight")]
		[Trait(MainData.Explanation, "preflight 응답에 허용 헤더, 노출 헤더, max-age, credentials 헤더가 내려오고 허용되지 않은 헤더는 거부되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		// KSAN 알려진 실패(일부러 다르게 동작, 고치지 않음): SCP 연동 때문에 OPTIONS 는 Origin 만 보고 규칙을 고르고 항상 200 을 준다.
		// Access-Control-Allow-Methods 도 GET,PUT 처럼 공백 없이 잇는다. AWS 는 GET, PUT 형식이고 허용하지 않은 헤더는 403 이다.
		public void TestCorsPreflightHeaders()
		{
			TestId = 6;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			PutCors(client, bucketName,
			[
				CorsRule(["GET", "PUT"], ["http://a.example.com"],
					allowedHeaders: ["x-amz-*", "content-type"],
					exposeHeaders: ["ETag", "x-amz-meta-foo"],
					maxAgeSeconds: 600),
			]);

			// 허용된 헤더(와일드카드, 대소문자 무시)로 preflight 요청
			var (code, headers) = CorsRequest("OPTIONS", bucketName,
			[
				new("Origin", "http://a.example.com"),
				new("Access-Control-Request-Method", "PUT"),
				new("Access-Control-Request-Headers", "X-Amz-Meta-A, Content-Type"),
			]);
			Assert.Equal(HttpStatusCode.OK, code);
			Assert.Equal("http://a.example.com", Header(headers, "access-control-allow-origin"));
			Assert.Equal("GET, PUT", Header(headers, "access-control-allow-methods"));
			Assert.Equal("x-amz-meta-a, content-type", Header(headers, "access-control-allow-headers"));
			Assert.Equal("ETag, x-amz-meta-foo", Header(headers, "access-control-expose-headers"));
			Assert.Equal("600", Header(headers, "access-control-max-age"));
			Assert.Equal("true", Header(headers, "access-control-allow-credentials"));

			// 허용되지 않은 헤더를 요청하면 거부된다
			(code, headers) = CorsRequest("OPTIONS", bucketName,
			[
				new("Origin", "http://a.example.com"),
				new("Access-Control-Request-Method", "PUT"),
				new("Access-Control-Request-Headers", "authorization"),
			]);
			Assert.Equal(HttpStatusCode.Forbidden, code);
			Assert.Null(Header(headers, "access-control-allow-origin"));
		}

		[Fact]
		[Trait(MainData.Major, "Cors")]
		[Trait(MainData.Minor, "Preflight")]
		[Trait(MainData.Explanation, "여러 cors 규칙 중 먼저 선언된 규칙부터 순서대로 적용되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		// KSAN 알려진 실패(일부러 다르게 동작, 고치지 않음): SCP 연동 때문에 OPTIONS 는 Origin 만 보고 첫 규칙을 고르고 항상 200 을 준다.
		// AWS 는 메서드까지 맞는 규칙을 순서대로 찾고, 맞는 규칙이 없으면 403 이다.
		public void TestCorsRuleOrder()
		{
			TestId = 7;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			PutCors(client, bucketName,
			[
				CorsRule(["GET", "PUT"], ["http://a.example.com"], maxAgeSeconds: 600),
				CorsRule(["GET"], ["*"], maxAgeSeconds: 20),
			]);

			// 두 규칙 모두에 맞으면 먼저 선언된 규칙이 적용된다
			var (code, headers) = CorsRequest("OPTIONS", bucketName,
				[new("Origin", "http://a.example.com"), new("Access-Control-Request-Method", "GET")]);
			Assert.Equal(HttpStatusCode.OK, code);
			Assert.Equal("http://a.example.com", Header(headers, "access-control-allow-origin"));
			Assert.Equal("GET, PUT", Header(headers, "access-control-allow-methods"));
			Assert.Equal("600", Header(headers, "access-control-max-age"));

			// 첫 규칙에 맞지 않으면 다음 규칙이 적용된다
			(code, headers) = CorsRequest("OPTIONS", bucketName,
				[new("Origin", "http://b.example.com"), new("Access-Control-Request-Method", "GET")]);
			Assert.Equal(HttpStatusCode.OK, code);
			Assert.Equal("*", Header(headers, "access-control-allow-origin"));
			Assert.Equal("GET", Header(headers, "access-control-allow-methods"));
			Assert.Equal("20", Header(headers, "access-control-max-age"));
			Assert.Null(Header(headers, "access-control-allow-credentials"));

			// 어느 규칙에도 맞지 않으면 거부된다
			(code, headers) = CorsRequest("OPTIONS", bucketName,
				[new("Origin", "http://b.example.com"), new("Access-Control-Request-Method", "PUT")]);
			Assert.Equal(HttpStatusCode.Forbidden, code);
			Assert.Null(Header(headers, "access-control-allow-origin"));
		}

		[Fact]
		[Trait(MainData.Major, "Cors")]
		[Trait(MainData.Minor, "Object")]
		[Trait(MainData.Explanation, "오브젝트에 대한 GET/HEAD/preflight 요청에 cors 헤더가 규칙대로 내려오는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestCorsObjectRequest()
		{
			TestId = 8;
			var client = GetClient();
			var bucketName = GetNewBucketCannedAcl(client, S3CannedACL.PublicRead);
			var key = "test_cors_object_request";
			client.PutObject(bucketName, key, body: "cors", acl: S3CannedACL.PublicRead);
			PutCors(client, bucketName, [CorsRule(["GET", "HEAD"], ["http://a.example.com"], exposeHeaders: ["ETag"])]);

			HttpStatusCode code;
			Dictionary<string, string> headers;
			foreach (var method in new[] { "GET", "HEAD" })
			{
				(code, headers) = CorsRequest(method, bucketName, [new("Origin", "http://a.example.com")], key);
				Assert.Equal(HttpStatusCode.OK, code);
				Assert.Equal("http://a.example.com", Header(headers, "access-control-allow-origin"));
				Assert.Equal("GET, HEAD", Header(headers, "access-control-allow-methods"));
				Assert.Equal("ETag", Header(headers, "access-control-expose-headers"));

				// 규칙에 맞지 않는 Origin 이면 요청은 성공하지만 CORS 헤더가 없다
				(code, headers) = CorsRequest(method, bucketName, [new("Origin", "http://b.example.com")], key);
				Assert.Equal(HttpStatusCode.OK, code);
				Assert.Null(Header(headers, "access-control-allow-origin"));
			}

			// 오브젝트 키에 대한 preflight
			(code, headers) = CorsRequest("OPTIONS", bucketName,
				[new("Origin", "http://a.example.com"), new("Access-Control-Request-Method", "GET")], key);
			Assert.Equal(HttpStatusCode.OK, code);
			Assert.Equal("http://a.example.com", Header(headers, "access-control-allow-origin"));
		}

		[Fact]
		[Trait(MainData.Major, "Cors")]
		[Trait(MainData.Minor, "Error")]
		[Trait(MainData.Explanation, "지원하지 않는 메서드나 소문자 메서드로 cors 설정 시 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		// KSAN 알려진 실패(일부러 다르게 동작, 고치지 않음): PutBucketCors 는 SCP 연동 때문에 규칙 내용을 검사하지 않고 그대로 저장한다.
		// AWS 는 AllowedMethod 가 GET·PUT·POST·DELETE·HEAD(대문자)가 아니면 400 InvalidRequest 다.
		public void TestCorsInvalidMethod()
		{
			TestId = 9;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			AssertInvalidCors(client, bucketName, [CorsRule(["PATCH"], ["*"])]);
			// 메서드 이름은 대문자만 허용된다
			AssertInvalidCors(client, bucketName, [CorsRule(["get"], ["*"])]);
		}

		[Fact]
		[Trait(MainData.Major, "Cors")]
		[Trait(MainData.Minor, "Error")]
		[Trait(MainData.Explanation, "AllowedOrigin 에 와일드카드를 두 개 이상 쓰면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		// KSAN 알려진 실패(일부러 다르게 동작, 고치지 않음): PutBucketCors 는 SCP 연동 때문에 규칙 내용을 검사하지 않고 그대로 저장한다.
		// AWS 는 AllowedOrigin 에 * 가 둘 이상이면 400 InvalidRequest 다.
		public void TestCorsInvalidOriginWildcard()
		{
			TestId = 10;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			AssertInvalidCors(client, bucketName, [CorsRule(["GET"], ["*a*"])]);
		}

		[Fact]
		[Trait(MainData.Major, "Cors")]
		[Trait(MainData.Minor, "Error")]
		[Trait(MainData.Explanation, "AllowedHeader 에 와일드카드를 두 개 이상 쓰면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		// KSAN 알려진 실패(일부러 다르게 동작, 고치지 않음): PutBucketCors 는 SCP 연동 때문에 규칙 내용을 검사하지 않고 그대로 저장한다.
		// AWS 는 AllowedHeader 에 * 가 둘 이상이면 400 InvalidRequest 다.
		public void TestCorsInvalidHeaderWildcard()
		{
			TestId = 11;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			AssertInvalidCors(client, bucketName, [CorsRule(["GET"], ["*"], allowedHeaders: ["*a*"])]);
		}

		[Fact]
		[Trait(MainData.Major, "Cors")]
		[Trait(MainData.Minor, "Error")]
		[Trait(MainData.Explanation, "ExposeHeader 에 와일드카드를 쓰면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		// KSAN 알려진 실패(일부러 다르게 동작, 고치지 않음): PutBucketCors 는 SCP 연동 때문에 규칙 내용을 검사하지 않고 그대로 저장한다.
		// AWS 는 ExposeHeader 에 * 가 있으면 400 InvalidRequest 다.
		public void TestCorsExposeHeaderWildcard()
		{
			TestId = 12;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			AssertInvalidCors(client, bucketName, [CorsRule(["GET"], ["*"], exposeHeaders: ["x-amz-*"])]);
		}

		[Fact]
		[Trait(MainData.Major, "Cors")]
		[Trait(MainData.Minor, "Error")]
		[Trait(MainData.Explanation, "cors 규칙은 100개까지 저장되고 101개는 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		// KSAN 알려진 실패(일부러 다르게 동작, 고치지 않음): PutBucketCors 는 SCP 연동 때문에 규칙 내용을 검사하지 않고 그대로 저장한다.
		// AWS 는 규칙이 101개 이상이면 400 InvalidRequest 다.
		public void TestCorsRuleLimit()
		{
			TestId = 13;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var rules = new List<CORSRule>();
			for (int i = 0; i < 100; i++) rules.Add(CorsRule(["GET"], [$"http://{i}.example.com"]));
			PutCors(client, bucketName, rules);
			Assert.Equal(100, client.GetCORSConfiguration(bucketName).Configuration.Rules.Count);

			rules.Add(CorsRule(["GET"], ["http://100.example.com"]));
			AssertInvalidCors(client, bucketName, rules);
		}

		[Fact]
		[Trait(MainData.Major, "Cors")]
		[Trait(MainData.Minor, "Check")]
		[Trait(MainData.Explanation, "cors 설정을 다시 저장하면 기존 규칙이 모두 교체되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestCorsOverwrite()
		{
			TestId = 14;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			PutCors(client, bucketName,
			[
				CorsRule(["GET"], ["http://a.example.com"]),
				CorsRule(["PUT"], ["http://b.example.com"]),
			]);
			PutCors(client, bucketName, [CorsRule(["HEAD"], ["http://z.example.com"])]);

			var response = client.GetCORSConfiguration(bucketName);
			Assert.Single(response.Configuration.Rules);
			Assert.Equal(["HEAD"], response.Configuration.Rules[0].AllowedMethods);
			Assert.Equal(["http://z.example.com"], response.Configuration.Rules[0].AllowedOrigins);
		}

		[Fact]
		[Trait(MainData.Major, "Cors")]
		[Trait(MainData.Minor, "Delete")]
		[Trait(MainData.Explanation, "cors 설정이 없는 버킷의 cors 삭제가 성공하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestCorsDeleteNotExist()
		{
			TestId = 15;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			// 설정이 없어도 삭제는 성공한다
			client.DeleteCORSConfiguration(bucketName);
			AssertNoCors(client, bucketName);
		}

		[Fact]
		[Trait(MainData.Major, "Cors")]
		[Trait(MainData.Minor, "Error")]
		[Trait(MainData.Explanation, "존재하지 않는 버킷의 cors 조회, 저장, 삭제가 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestCorsBucketNotExist()
		{
			TestId = 16;
			var client = GetClient();
			var bucketName = GetNewBucketName(false);

			var e = Assert.Throws<AggregateException>(() => client.GetCORSConfiguration(bucketName));
			Assert.Equal(HttpStatusCode.NotFound, GetStatus(e));
			Assert.Equal(MainData.NO_SUCH_BUCKET, GetErrorCode(e));

			e = Assert.Throws<AggregateException>(() => PutCors(client, bucketName, [CorsRule(["GET"], ["*"])]));
			Assert.Equal(HttpStatusCode.NotFound, GetStatus(e));
			Assert.Equal(MainData.NO_SUCH_BUCKET, GetErrorCode(e));

			e = Assert.Throws<AggregateException>(() => client.DeleteCORSConfiguration(bucketName));
			Assert.Equal(HttpStatusCode.NotFound, GetStatus(e));
			Assert.Equal(MainData.NO_SUCH_BUCKET, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "Cors")]
		[Trait(MainData.Minor, "Permission")]
		[Trait(MainData.Explanation, "권한이 없는 사용자의 cors 조회, 저장, 삭제가 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestCorsAltUser()
		{
			TestId = 17;
			var client = GetClient();
			var altClient = GetAltClient();
			var bucketName = GetNewBucket(client);
			PutCors(client, bucketName, [CorsRule(["GET"], ["*"])]);

			var e = Assert.Throws<AggregateException>(() => altClient.GetCORSConfiguration(bucketName));
			Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));
			Assert.Equal(MainData.ACCESS_DENIED, GetErrorCode(e));

			e = Assert.Throws<AggregateException>(() => PutCors(altClient, bucketName, [CorsRule(["GET"], ["*"])]));
			Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));
			Assert.Equal(MainData.ACCESS_DENIED, GetErrorCode(e));

			e = Assert.Throws<AggregateException>(() => altClient.DeleteCORSConfiguration(bucketName));
			Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));
			Assert.Equal(MainData.ACCESS_DENIED, GetErrorCode(e));
		}
	}
}
