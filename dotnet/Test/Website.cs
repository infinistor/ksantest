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
using Amazon.S3.Model;
using System.Collections.Generic;
using System.Net;
using Xunit;

namespace s3tests.Test
{
	public class Website : TestBase
	{
		public Website(Xunit.Abstractions.ITestOutputHelper Output) => this.Output = Output;

		[Fact]
		[Trait(MainData.Major, "Website")]
		[Trait(MainData.Minor, "Check")]
		[Trait(MainData.Explanation, "버킷의 Websize 설정 조회 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestWebsiteGetBuckets()
		{
			TestId = 1;
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			var Response = client.GetBucketWebsite(bucketName);
			Assert.Equal(HttpStatusCode.NotFound, Response.HttpStatusCode);
		}

		[Fact]
		[Trait(MainData.Major, "Website")]
		[Trait(MainData.Minor, "Check")]
		[Trait(MainData.Explanation, "버킷의 Websize 설정이 가능한지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestWebsitePutBuckets()
		{
			TestId = 2;
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			var WebConfig = new WebsiteConfiguration()
			{
				ErrorDocument = "400",
				IndexDocumentSuffix = "a"
			};

			var Response = client.PutBucketWebsite(bucketName, WebConfig);
			Assert.Equal(HttpStatusCode.OK, Response.HttpStatusCode);

			var GetResponse = client.GetBucketWebsite(bucketName);
			Assert.Equal(WebConfig.ErrorDocument, GetResponse.WebsiteConfiguration.ErrorDocument);
		}

		[Fact]
		[Trait(MainData.Major, "Website")]
		[Trait(MainData.Minor, "Delete")]
		[Trait(MainData.Explanation, "버킷의 Websize 설정이 삭제가능한지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestWebsiteDeleteBuckets()
		{
			TestId = 3;
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			var WebConfig = new WebsiteConfiguration()
			{
				ErrorDocument = "400",
				IndexDocumentSuffix = "a"
			};

			var Response = client.PutBucketWebsite(bucketName, WebConfig);
			Assert.Equal(HttpStatusCode.OK, Response.HttpStatusCode);

			var DelResponse = client.DeleteBucketWebsite(bucketName);
			Assert.Equal(HttpStatusCode.NoContent, DelResponse.HttpStatusCode);
		}

		[Fact]
		[Trait(MainData.Major, "Website")]
		[Trait(MainData.Minor, "Delete")]
		[Trait(MainData.Explanation, "Website 설정 삭제 후 조회하면 NoSuchWebsiteConfiguration 에러가 발생하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestWebsiteGetAfterDelete()
		{
			TestId = 4;
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			client.PutBucketWebsite(bucketName, new WebsiteConfiguration() { ErrorDocument = "error.html", IndexDocumentSuffix = "index.html" });
			client.GetBucketWebsite(bucketName);
			client.DeleteBucketWebsite(bucketName);

			CheckErrorResponse(HttpStatusCode.NotFound, () => client.GetBucketWebsite(bucketName), MainData.NO_SUCH_WEBSITE_CONFIGURATION);
		}

		[Fact]
		[Trait(MainData.Major, "Website")]
		[Trait(MainData.Minor, "Delete")]
		[Trait(MainData.Explanation, "Website 설정이 없는 버킷에서 삭제해도 성공하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestWebsiteDeleteNotConfigured()
		{
			TestId = 5;
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			var Response = client.DeleteBucketWebsite(bucketName);
			Assert.Equal(HttpStatusCode.NoContent, Response.HttpStatusCode);
		}

		[Fact]
		[Trait(MainData.Major, "Website")]
		[Trait(MainData.Minor, "Check")]
		[Trait(MainData.Explanation, "IndexDocument만으로 Website 설정이 가능한지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestWebsiteIndexDocumentOnly()
		{
			TestId = 6;
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			client.PutBucketWebsite(bucketName, new WebsiteConfiguration() { IndexDocumentSuffix = "index.html" });

			var Response = client.GetBucketWebsite(bucketName).WebsiteConfiguration;
			Assert.Equal("index.html", Response.IndexDocumentSuffix);
			Assert.Null(Response.ErrorDocument);
			Assert.Null(Response.RedirectAllRequestsTo);
			Assert.Empty(Response.RoutingRules);
		}

		[Fact]
		[Trait(MainData.Major, "Website")]
		[Trait(MainData.Minor, "Overwrite")]
		[Trait(MainData.Explanation, "Website 설정을 덮어쓰면 기존 설정이 완전히 대체되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestWebsiteOverwrite()
		{
			TestId = 7;
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			client.PutBucketWebsite(bucketName, new WebsiteConfiguration() { IndexDocumentSuffix = "index.html", ErrorDocument = "error.html" });
			client.PutBucketWebsite(bucketName, new WebsiteConfiguration() { IndexDocumentSuffix = "main.html" });

			var Response = client.GetBucketWebsite(bucketName).WebsiteConfiguration;
			Assert.Equal("main.html", Response.IndexDocumentSuffix);
			Assert.Null(Response.ErrorDocument);
		}

		[Fact]
		[Trait(MainData.Major, "Website")]
		[Trait(MainData.Minor, "Redirect")]
		[Trait(MainData.Explanation, "RedirectAllRequestsTo(호스트, 프로토콜) 설정이 가능한지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestWebsiteRedirectAllRequests()
		{
			TestId = 8;
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			client.PutBucketWebsite(bucketName, new WebsiteConfiguration()
			{
				RedirectAllRequestsTo = new RoutingRuleRedirect() { HostName = "example.com", Protocol = "https" }
			});

			var Response = client.GetBucketWebsite(bucketName).WebsiteConfiguration;
			Assert.Equal("example.com", Response.RedirectAllRequestsTo.HostName);
			Assert.Equal("https", Response.RedirectAllRequestsTo.Protocol);
			Assert.Null(Response.IndexDocumentSuffix);
			Assert.Null(Response.ErrorDocument);
		}

		[Fact]
		[Trait(MainData.Major, "Website")]
		[Trait(MainData.Minor, "Redirect")]
		[Trait(MainData.Explanation, "RedirectAllRequestsTo에 호스트만 설정이 가능한지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestWebsiteRedirectAllRequestsHostOnly()
		{
			TestId = 9;
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			client.PutBucketWebsite(bucketName, new WebsiteConfiguration()
			{
				RedirectAllRequestsTo = new RoutingRuleRedirect() { HostName = "example.com" }
			});

			var Response = client.GetBucketWebsite(bucketName).WebsiteConfiguration;
			Assert.Equal("example.com", Response.RedirectAllRequestsTo.HostName);
			Assert.Null(Response.RedirectAllRequestsTo.Protocol);
		}

		[Fact]
		[Trait(MainData.Major, "Website")]
		[Trait(MainData.Minor, "RoutingRule")]
		[Trait(MainData.Explanation, "RoutingRules 설정이 가능하고 조회 결과가 일치하는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestWebsiteRoutingRules()
		{
			TestId = 10;
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			var Rules = new List<RoutingRule>()
			{
				new()
				{
					Condition = new RoutingRuleCondition() { KeyPrefixEquals = "docs/" },
					Redirect = new RoutingRuleRedirect() { ReplaceKeyPrefixWith = "documents/" },
				},
				new()
				{
					Condition = new RoutingRuleCondition() { HttpErrorCodeReturnedEquals = "404" },
					Redirect = new RoutingRuleRedirect() { HostName = "example.com", Protocol = "https", HttpRedirectCode = "301", ReplaceKeyWith = "error.html" },
				},
				new()
				{
					Redirect = new RoutingRuleRedirect() { HostName = "example.com" },
				},
			};

			client.PutBucketWebsite(bucketName, new WebsiteConfiguration()
			{
				IndexDocumentSuffix = "index.html",
				ErrorDocument = "error.html",
				RoutingRules = Rules,
			});

			var Response = client.GetBucketWebsite(bucketName).WebsiteConfiguration;
			Assert.Equal("index.html", Response.IndexDocumentSuffix);
			Assert.Equal("error.html", Response.ErrorDocument);
			CheckRoutingRules(Rules, Response.RoutingRules);
		}

		[Fact]
		[Trait(MainData.Major, "Website")]
		[Trait(MainData.Minor, "RoutingRule")]
		[Trait(MainData.Explanation, "RoutingRules를 최대 개수(50개)까지 설정 가능한지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestWebsiteRoutingRulesMax()
		{
			TestId = 11;
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			var Rules = CreateRoutingRules(50);
			client.PutBucketWebsite(bucketName, new WebsiteConfiguration() { IndexDocumentSuffix = "index.html", RoutingRules = Rules });

			CheckRoutingRules(Rules, client.GetBucketWebsite(bucketName).WebsiteConfiguration.RoutingRules);
		}

		[Fact]
		[Trait(MainData.Major, "Website")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "RoutingRules가 최대 개수(50개)를 넘으면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestWebsiteRoutingRulesOverMax()
		{
			TestId = 12;
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			CheckPutWebsiteFailed(client, bucketName,
				new WebsiteConfiguration() { IndexDocumentSuffix = "index.html", RoutingRules = CreateRoutingRules(51) },
				MainData.INVALID_REQUEST);
		}

		[Fact]
		[Trait(MainData.Major, "Website")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "RedirectAllRequestsTo 없이 IndexDocument를 빠뜨리면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestWebsiteNoIndexDocument()
		{
			TestId = 13;
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			CheckPutWebsiteFailed(client, bucketName, new WebsiteConfiguration() { ErrorDocument = "error.html" }, MainData.INVALID_ARGUMENT);
		}

		[Fact]
		[Trait(MainData.Major, "Website")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "IndexDocument Suffix가 비어있으면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestWebsiteEmptyIndexSuffix()
		{
			TestId = 14;
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			CheckPutWebsiteFailed(client, bucketName, new WebsiteConfiguration() { IndexDocumentSuffix = "" }, MainData.INVALID_ARGUMENT);
		}

		[Fact]
		[Trait(MainData.Major, "Website")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "IndexDocument Suffix에 '/'가 포함되면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestWebsiteIndexSuffixWithSlash()
		{
			TestId = 15;
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			CheckPutWebsiteFailed(client, bucketName, new WebsiteConfiguration() { IndexDocumentSuffix = "dir/index.html" }, MainData.INVALID_ARGUMENT);
		}

		[Fact]
		[Trait(MainData.Major, "Website")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "RedirectAllRequestsTo와 IndexDocument를 함께 설정하면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestWebsiteRedirectAllWithIndexDocument()
		{
			TestId = 16;
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			CheckPutWebsiteFailed(client, bucketName, new WebsiteConfiguration()
			{
				RedirectAllRequestsTo = new RoutingRuleRedirect() { HostName = "example.com" },
				IndexDocumentSuffix = "index.html",
			}, MainData.INVALID_ARGUMENT);
		}

		[Fact]
		[Trait(MainData.Major, "Website")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "RedirectAllRequestsTo와 RoutingRules를 함께 설정하면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestWebsiteRedirectAllWithRoutingRules()
		{
			TestId = 17;
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			CheckPutWebsiteFailed(client, bucketName, new WebsiteConfiguration()
			{
				RedirectAllRequestsTo = new RoutingRuleRedirect() { HostName = "example.com" },
				RoutingRules = CreateRoutingRules(1),
			}, MainData.INVALID_ARGUMENT);
		}

		[Fact]
		[Trait(MainData.Major, "Website")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "RedirectAllRequestsTo의 Protocol이 http/https가 아니면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestWebsiteRedirectAllInvalidProtocol()
		{
			TestId = 18;
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			CheckPutWebsiteFailed(client, bucketName, new WebsiteConfiguration()
			{
				RedirectAllRequestsTo = new RoutingRuleRedirect() { HostName = "example.com", Protocol = "ftp" },
			}, MainData.INVALID_REQUEST);
		}

		[Fact]
		[Trait(MainData.Major, "Website")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "RoutingRule에 ReplaceKeyWith와 ReplaceKeyPrefixWith를 함께 설정하면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestWebsiteRoutingRuleReplaceKeyAndPrefix()
		{
			TestId = 19;
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			CheckPutWebsiteFailed(client, bucketName, new WebsiteConfiguration()
			{
				IndexDocumentSuffix = "index.html",
				RoutingRules =
				[
					new()
					{
						Condition = new RoutingRuleCondition() { KeyPrefixEquals = "docs/" },
						Redirect = new RoutingRuleRedirect() { ReplaceKeyWith = "index.html", ReplaceKeyPrefixWith = "documents/" },
					},
				],
			}, MainData.INVALID_REQUEST);
		}

		[Fact]
		[Trait(MainData.Major, "Website")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "RoutingRule의 HttpRedirectCode가 3XX가 아니면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestWebsiteRoutingRuleInvalidRedirectCode()
		{
			TestId = 20;
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			CheckPutWebsiteFailed(client, bucketName, new WebsiteConfiguration()
			{
				IndexDocumentSuffix = "index.html",
				RoutingRules =
				[
					new() { Redirect = new RoutingRuleRedirect() { HostName = "example.com", HttpRedirectCode = "200" } },
				],
			}, MainData.INVALID_REQUEST);
		}

		[Fact]
		[Trait(MainData.Major, "Website")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "RoutingRule 조건의 HttpErrorCodeReturnedEquals가 4XX/5XX가 아니면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestWebsiteRoutingRuleInvalidErrorCode()
		{
			TestId = 21;
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			CheckPutWebsiteFailed(client, bucketName, new WebsiteConfiguration()
			{
				IndexDocumentSuffix = "index.html",
				RoutingRules =
				[
					new()
					{
						Condition = new RoutingRuleCondition() { HttpErrorCodeReturnedEquals = "200" },
						Redirect = new RoutingRuleRedirect() { HostName = "example.com" },
					},
				],
			}, MainData.INVALID_REQUEST);
		}

		[Fact]
		[Trait(MainData.Major, "Website")]
		[Trait(MainData.Minor, "RoutingRule")]
		[Trait(MainData.Explanation, "RoutingRule의 Redirect가 비어있어도 설정이 가능한지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestWebsiteRoutingRuleEmptyRedirect()
		{
			TestId = 22;
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			var Rules = new List<RoutingRule>()
			{
				new()
				{
					Condition = new RoutingRuleCondition() { KeyPrefixEquals = "docs/" },
					Redirect = new RoutingRuleRedirect(),
				},
			};
			client.PutBucketWebsite(bucketName, new WebsiteConfiguration() { IndexDocumentSuffix = "index.html", RoutingRules = Rules });

			CheckRoutingRules(Rules, client.GetBucketWebsite(bucketName).WebsiteConfiguration.RoutingRules);
		}

		[Fact]
		[Trait(MainData.Major, "Website")]
		[Trait(MainData.Minor, "ERROR")]
		[Trait(MainData.Explanation, "존재하지 않는 버킷에 Website 설정/조회/삭제 시 NoSuchBucket 에러가 발생하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestWebsiteBucketNotFound()
		{
			TestId = 23;
			var client = GetClient();
			var bucketName = GetNewBucketName(false);

			var WebConfig = new WebsiteConfiguration() { IndexDocumentSuffix = "index.html" };
			CheckErrorResponse(HttpStatusCode.NotFound, () => client.PutBucketWebsite(bucketName, WebConfig), MainData.NO_SUCH_BUCKET);
			CheckErrorResponse(HttpStatusCode.NotFound, () => client.GetBucketWebsite(bucketName), MainData.NO_SUCH_BUCKET);
			CheckErrorResponse(HttpStatusCode.NotFound, () => client.DeleteBucketWebsite(bucketName), MainData.NO_SUCH_BUCKET);
		}

		[Fact]
		[Trait(MainData.Major, "Website")]
		[Trait(MainData.Minor, "Access")]
		[Trait(MainData.Explanation, "다른 사용자가 Website 설정/조회/삭제할 수 없는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestWebsiteAccessDenied()
		{
			TestId = 24;
			var client = GetClient();
			var altClient = GetAltClient();
			var bucketName = GetNewBucket(client);

			var WebConfig = new WebsiteConfiguration() { IndexDocumentSuffix = "index.html" };
			CheckErrorResponse(HttpStatusCode.Forbidden, () => altClient.PutBucketWebsite(bucketName, WebConfig), MainData.ACCESS_DENIED);

			client.PutBucketWebsite(bucketName, WebConfig);

			CheckErrorResponse(HttpStatusCode.Forbidden, () => altClient.GetBucketWebsite(bucketName), MainData.ACCESS_DENIED);
			CheckErrorResponse(HttpStatusCode.Forbidden, () => altClient.DeleteBucketWebsite(bucketName), MainData.ACCESS_DENIED);

			Assert.Equal("index.html", client.GetBucketWebsite(bucketName).WebsiteConfiguration.IndexDocumentSuffix);
		}

		private static List<RoutingRule> CreateRoutingRules(int count)
		{
			var rules = new List<RoutingRule>();
			for (int i = 0; i < count; i++)
			{
				rules.Add(new RoutingRule()
				{
					Condition = new RoutingRuleCondition() { KeyPrefixEquals = $"prefix{i}/" },
					Redirect = new RoutingRuleRedirect() { ReplaceKeyPrefixWith = $"target{i}/" },
				});
			}
			return rules;
		}

		private static void CheckRoutingRules(List<RoutingRule> expected, List<RoutingRule> actual)
		{
			Assert.Equal(expected.Count, actual.Count);
			for (int i = 0; i < expected.Count; i++)
			{
				var expectedCondition = expected[i].Condition;
				var actualCondition = actual[i].Condition;
				if (expectedCondition == null) Assert.Null(actualCondition);
				else
				{
					Assert.Equal(expectedCondition.KeyPrefixEquals, actualCondition.KeyPrefixEquals);
					Assert.Equal(expectedCondition.HttpErrorCodeReturnedEquals, actualCondition.HttpErrorCodeReturnedEquals);
				}

				var expectedRedirect = expected[i].Redirect;
				var actualRedirect = actual[i].Redirect;
				Assert.Equal(expectedRedirect.HostName, actualRedirect.HostName);
				Assert.Equal(expectedRedirect.Protocol, actualRedirect.Protocol);
				Assert.Equal(expectedRedirect.HttpRedirectCode, actualRedirect.HttpRedirectCode);
				Assert.Equal(expectedRedirect.ReplaceKeyWith, actualRedirect.ReplaceKeyWith);
				Assert.Equal(expectedRedirect.ReplaceKeyPrefixWith, actualRedirect.ReplaceKeyPrefixWith);
			}
		}

		// PutBucketWebsite가 400으로 실패하고 설정이 남지 않았는지 확인
		private static void CheckPutWebsiteFailed(S3Client client, string bucketName, WebsiteConfiguration webConfig, string errorCode)
		{
			CheckErrorResponse(HttpStatusCode.BadRequest, () => client.PutBucketWebsite(bucketName, webConfig), errorCode);
			CheckErrorResponse(HttpStatusCode.NotFound, () => client.GetBucketWebsite(bucketName), MainData.NO_SUCH_WEBSITE_CONFIGURATION);
		}
	}
}
