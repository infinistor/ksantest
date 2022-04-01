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
using System.Collections.Generic;
using System.Net;
using Xunit;

namespace s3tests
{
    public class Cors : TestBase
    {
        public Cors(Xunit.Abstractions.ITestOutputHelper Output) => this.Output = Output;

        [Fact(DisplayName = "test_set_cors")]
        [Trait(MainData.Major, "Cors")]
        [Trait(MainData.Minor, "Check")]
        [Trait(MainData.Explanation, "버킷의 cors정보 세팅 성공 확인")]
        [Trait(MainData.Result, MainData.ResultSuccess)]
        public void test_set_cors()
        {
            var BucketName = GetNewBucket();
            var Client = GetClient();

            var AllowedMethods = new List<string>(){ "GET", "PUT"};
            var AllowedOrigins = new List<string>() { "*.get", "*.put" };

            var CORSConfig = new CORSConfiguration(){ 
                Rules = new List<CORSRule>() {
                    new CORSRule(){
                        AllowedMethods = AllowedMethods,
                        AllowedOrigins = AllowedOrigins
                    },
                }
            };

            var Response = Client.GetCORSConfiguration(BucketName);
            Assert.Null(Response.Configuration);

            Client.PutCORSConfiguration(BucketName, CORSConfig);
            Response = Client.GetCORSConfiguration(BucketName);
            Assert.Equal(AllowedMethods, Response.Configuration.Rules[0].AllowedMethods);
            Assert.Equal(AllowedOrigins, Response.Configuration.Rules[0].AllowedOrigins);

            Client.DeleteCORSConfiguration(BucketName);
            Response = Client.GetCORSConfiguration(BucketName);
            Assert.Null(Response.Configuration);
        }

        [Fact(DisplayName = "test_cors_origin_response")]
        [Trait(MainData.Major, "Cors")]
        [Trait(MainData.Minor, "Post")]
        [Trait(MainData.Explanation, "버킷의 cors정보를 URL로 읽고 쓰기 성공/실패 확인")]
        [Trait(MainData.Result, MainData.ResultSuccess)]
        public void test_cors_origin_response()
        {
            var BucketName = GetNewBucket();
            var Client = GetClient();
            Client.PutBucketACL(BucketName, S3CannedACL.PublicRead);

            var CORSConfig = new CORSConfiguration()
            {
                Rules = new List<CORSRule>() {
                    new CORSRule(){
                        AllowedMethods =  new List<string>() { "GET" },
                        AllowedOrigins =  new List<string>() { "*suffix" },
                    },
                    new CORSRule(){
                        AllowedMethods =  new List<string>() { "GET" },
                        AllowedOrigins =  new List<string>() { "start*end" },
                    },
                    new CORSRule(){
                        AllowedMethods =  new List<string>() { "GET" },
                        AllowedOrigins =  new List<string>() { "prefix*" },
                    },
                    new CORSRule(){
                        AllowedMethods =  new List<string>() { "PUT" },
                        AllowedOrigins =  new List<string>() { "*.put" },
                    },
                }
            };

            var Response = Client.GetCORSConfiguration(BucketName);
            Assert.Null(Response.Configuration);

            Client.PutCORSConfiguration(BucketName, CORSConfig);

            CorsRequestAndCheck("Get", BucketName, new List<KeyValuePair<string, string>>(), HttpStatusCode.OK, null, null);
            CorsRequestAndCheck("Get", BucketName, new List<KeyValuePair<string, string>>() { new KeyValuePair<string, string>("Origin", "foo.suffix") }, HttpStatusCode.OK, "foo.suffix", "GET");
            CorsRequestAndCheck("Get", BucketName, new List<KeyValuePair<string, string>>() { new KeyValuePair<string, string>("Origin", "foo.bar") }, HttpStatusCode.OK, null, null);
            CorsRequestAndCheck("Get", BucketName, new List<KeyValuePair<string, string>>() { new KeyValuePair<string, string>("Origin", "foo.suffix.get") }, HttpStatusCode.OK, null, null);
            CorsRequestAndCheck("Get", BucketName, new List<KeyValuePair<string, string>>() { new KeyValuePair<string, string>("Origin", "startend") }, HttpStatusCode.OK, "startend", "GET");
            CorsRequestAndCheck("Get", BucketName, new List<KeyValuePair<string, string>>() { new KeyValuePair<string, string>("Origin", "start1end") }, HttpStatusCode.OK, "start1end", "GET");
            CorsRequestAndCheck("Get", BucketName, new List<KeyValuePair<string, string>>() { new KeyValuePair<string, string>("Origin", "start12end") }, HttpStatusCode.OK, "start12end", "GET");
            CorsRequestAndCheck("Get", BucketName, new List<KeyValuePair<string, string>>() { new KeyValuePair<string, string>("Origin", "0start12end") }, HttpStatusCode.OK, null, null);
            CorsRequestAndCheck("Get", BucketName, new List<KeyValuePair<string, string>>() { new KeyValuePair<string, string>("Origin", "prefix") }, HttpStatusCode.OK, "prefix", "GET");
            CorsRequestAndCheck("Get", BucketName, new List<KeyValuePair<string, string>>() { new KeyValuePair<string, string>("Origin", "prefix.suffix") }, HttpStatusCode.OK, "prefix.suffix", "GET");
            CorsRequestAndCheck("Get", BucketName, new List<KeyValuePair<string, string>>() { new KeyValuePair<string, string>("Origin", "bla.prefix") }, HttpStatusCode.OK, null, null);

            CorsRequestAndCheck("Get", BucketName, new List<KeyValuePair<string, string>>() { new KeyValuePair<string, string>("Origin", "foo.suffix") }, HttpStatusCode.NotFound, "foo.suffix", "GET", Key: "bar");
            CorsRequestAndCheck("Put", BucketName, new List<KeyValuePair<string, string>>() { new KeyValuePair<string, string>("Origin", "foo.suffix"), new KeyValuePair<string, string>("Access-Control-Request-Method", "GET"), new KeyValuePair<string, string>("content-length", "0") }, HttpStatusCode.Forbidden, "foo.suffix", "GET", Key: "bar");
            CorsRequestAndCheck("Put", BucketName, new List<KeyValuePair<string, string>>() { new KeyValuePair<string, string>("Origin", "foo.suffix"), new KeyValuePair<string, string>("Access-Control-Request-Method", "PUT"), new KeyValuePair<string, string>("content-length", "0") }, HttpStatusCode.Forbidden, null, null, Key: "bar");

            CorsRequestAndCheck("Put", BucketName, new List<KeyValuePair<string, string>>() { new KeyValuePair<string, string>("Origin", "foo.suffix"), new KeyValuePair<string, string>("Access-Control-Request-Method", "DELETE"), new KeyValuePair<string, string>("content-length", "0") }, HttpStatusCode.Forbidden, null, null, Key: "bar");
            CorsRequestAndCheck("Put", BucketName, new List<KeyValuePair<string, string>>() { new KeyValuePair<string, string>("Origin", "foo.suffix"), new KeyValuePair<string, string>("content-length", "0") }, HttpStatusCode.Forbidden, null, null, Key: "bar");

            CorsRequestAndCheck("Put", BucketName, new List<KeyValuePair<string, string>>() { new KeyValuePair<string, string>("Origin", "foo.put"), new KeyValuePair<string, string>("content-length", "0") }, HttpStatusCode.Forbidden, "foo.put", "PUT", Key: "bar");

            CorsRequestAndCheck("Get", BucketName, new List<KeyValuePair<string, string>>() { new KeyValuePair<string, string>("Origin", "foo.suffix") }, HttpStatusCode.NotFound, "foo.suffix", "GET", Key: "bar");

            CorsRequestAndCheck("OPTIONS", BucketName, new List<KeyValuePair<string, string>>(), HttpStatusCode.BadRequest, null, null);
            CorsRequestAndCheck("OPTIONS", BucketName, new List<KeyValuePair<string, string>>() { new KeyValuePair<string, string>("Origin", "foo.suffix") }, HttpStatusCode.Forbidden, null, null);
            CorsRequestAndCheck("OPTIONS", BucketName, new List<KeyValuePair<string, string>>() { new KeyValuePair<string, string>("Origin", "foo.bla") }, HttpStatusCode.Forbidden, null, null);
            CorsRequestAndCheck("OPTIONS", BucketName, new List<KeyValuePair<string, string>>() { new KeyValuePair<string, string>("Origin", "foo.suffix"), new KeyValuePair<string, string>("Access-Control-Request-Method", "GET"), new KeyValuePair<string, string>("content-length", "0") }, HttpStatusCode.OK, "foo.suffix", "GET", Key: "bar");
            CorsRequestAndCheck("OPTIONS", BucketName, new List<KeyValuePair<string, string>>() { new KeyValuePair<string, string>("Origin", "foo.bar"), new KeyValuePair<string, string>("Access-Control-Request-Method", "GET") }, HttpStatusCode.Forbidden, null, null);
            CorsRequestAndCheck("OPTIONS", BucketName, new List<KeyValuePair<string, string>>() { new KeyValuePair<string, string>("Origin", "foo.suffix.get"), new KeyValuePair<string, string>("Access-Control-Request-Method", "GET") }, HttpStatusCode.Forbidden, null, null);
            CorsRequestAndCheck("OPTIONS", BucketName, new List<KeyValuePair<string, string>>() { new KeyValuePair<string, string>("Origin", "startend"), new KeyValuePair<string, string>("Access-Control-Request-Method", "GET") }, HttpStatusCode.OK, "startend", "GET");
            CorsRequestAndCheck("OPTIONS", BucketName, new List<KeyValuePair<string, string>>() { new KeyValuePair<string, string>("Origin", "start1end"), new KeyValuePair<string, string>("Access-Control-Request-Method", "GET") }, HttpStatusCode.OK, "start1end", "GET");
            CorsRequestAndCheck("OPTIONS", BucketName, new List<KeyValuePair<string, string>>() { new KeyValuePair<string, string>("Origin", "start12end"), new KeyValuePair<string, string>("Access-Control-Request-Method", "GET") }, HttpStatusCode.OK, "start12end", "GET");
            CorsRequestAndCheck("OPTIONS", BucketName, new List<KeyValuePair<string, string>>() { new KeyValuePair<string, string>("Origin", "0start12end"), new KeyValuePair<string, string>("Access-Control-Request-Method", "GET") }, HttpStatusCode.Forbidden, null, null);
            CorsRequestAndCheck("OPTIONS", BucketName, new List<KeyValuePair<string, string>>() { new KeyValuePair<string, string>("Origin", "prefix"), new KeyValuePair<string, string>("Access-Control-Request-Method", "GET") }, HttpStatusCode.OK, "prefix", "GET");
            CorsRequestAndCheck("OPTIONS", BucketName, new List<KeyValuePair<string, string>>() { new KeyValuePair<string, string>("Origin", "prefix.suffix"), new KeyValuePair<string, string>("Access-Control-Request-Method", "GET") }, HttpStatusCode.OK, "prefix.suffix", "GET");
            CorsRequestAndCheck("OPTIONS", BucketName, new List<KeyValuePair<string, string>>() { new KeyValuePair<string, string>("Origin", "bla.prefix"), new KeyValuePair<string, string>("Access-Control-Request-Method", "GET") }, HttpStatusCode.Forbidden, null, null);
            CorsRequestAndCheck("OPTIONS", BucketName, new List<KeyValuePair<string, string>>() { new KeyValuePair<string, string>("Origin", "foo.put"), new KeyValuePair<string, string>("Access-Control-Request-Method", "GET") }, HttpStatusCode.Forbidden, null, null);
            CorsRequestAndCheck("OPTIONS", BucketName, new List<KeyValuePair<string, string>>() { new KeyValuePair<string, string>("Origin", "foo.put"), new KeyValuePair<string, string>("Access-Control-Request-Method", "PUT") }, HttpStatusCode.OK, "foo.put", "PUT");
        }

        [Fact(DisplayName = "test_cors_origin_wildcard")]
        [Trait(MainData.Major, "Cors")]
        [Trait(MainData.Minor, "Post")]
        [Trait(MainData.Explanation, "와일드카드 문자만 입력하여 cors설정을 하였을때 정상적으로 동작하는지 확인")]
        [Trait(MainData.Result, MainData.ResultSuccess)]
        public void test_cors_origin_wildcard()
        {
            var BucketName = GetNewBucket();
            var Client = GetClient();
            Client.PutBucketACL(BucketName, S3CannedACL.PublicRead);

            var CORSConfig = new CORSConfiguration()
            {
                Rules = new List<CORSRule>() {
                    new CORSRule(){
                        AllowedMethods =  new List<string>() { "GET" },
                        AllowedOrigins =  new List<string>() { "*" },
                    },
                }
            };

            var Response = Client.GetCORSConfiguration(BucketName);
            Assert.Null(Response.Configuration);

            Client.PutCORSConfiguration(BucketName, CORSConfig);

            CorsRequestAndCheck("Get", BucketName, new List<KeyValuePair<string, string>>(), HttpStatusCode.OK, null, null);
            CorsRequestAndCheck("Get", BucketName, new List<KeyValuePair<string, string>>() { new KeyValuePair<string, string>("Origin", "example.origin") }, HttpStatusCode.OK, "*", "GET");
        }

        [Fact(DisplayName = "test_cors_header_option")]
        [Trait(MainData.Major, "Cors")]
        [Trait(MainData.Minor, "Post")]
        [Trait(MainData.Explanation, "cors옵션에서 사용자 추가 헤더를 설정하고 존재하지 않는 헤더를 request 설정한 채로 curs호출하면 실패하는지 확인")]
        [Trait(MainData.Result, MainData.ResultSuccess)]
        public void test_cors_header_option()
        {
            var BucketName = GetNewBucket();
            var Client = GetClient();
            Client.PutBucketACL(BucketName, S3CannedACL.PublicRead);

            var CORSConfig = new CORSConfiguration()
            {
                Rules = new List<CORSRule>() {
                    new CORSRule(){
                        AllowedMethods = new List<string>() { "GET" },
                        AllowedOrigins = new List<string>() { "*" },
                        ExposeHeaders  = new List<string>() { "x-amz-meta-header1" },
                    },
                }
            };

            var Response = Client.GetCORSConfiguration(BucketName);
            Assert.Null(Response.Configuration);

            Client.PutCORSConfiguration(BucketName, CORSConfig);

            CorsRequestAndCheck("OPTIONS", BucketName, new List<KeyValuePair<string, string>>() { new KeyValuePair<string, string>("Origin", "example.origin"), new KeyValuePair<string, string>("Access-Control-Request-Headers", "x-amz-meta-header2"), new KeyValuePair<string, string>("Access-Control-Request-Method", "GET") }, HttpStatusCode.Forbidden, null, null, Key:"bar");
        }
    }
}
