"""Cors tests ported from Java testV2/Cors.java."""

from __future__ import annotations

import pytest

from s3tests.data import main_data as md
from s3tests.test_base import S3TestBase
from s3tests.utils import net_utils


class TestCors(S3TestBase):
    def _create_public_cors_bucket(self, client, test_id: int):
        bucket_name = self.create_bucket_canned_acl(client, test_id)
        client.put_bucket_acl(Bucket=bucket_name, ACL="public-read")
        return bucket_name

    def _assert_no_cors_configuration(self, client, bucket_name):
        self.assert_client_error(
            lambda: client.get_bucket_cors(Bucket=bucket_name),
            404,
            md.NO_SUCH_CORS_CONFIGURATION,
        )

    @pytest.mark.tag("Check")
    def test_set_cors(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 1)

        allowed_methods = ["GET", "PUT"]
        allowed_origins = ["*.get", "*.put"]
        cors_config = {
            "CORSRules": [
                {
                    "AllowedMethods": allowed_methods,
                    "AllowedOrigins": allowed_origins,
                }
            ]
        }

        self.assert_client_error(
            lambda: client.get_bucket_cors(Bucket=bucket_name),
            404,
            md.NO_SUCH_CORS_CONFIGURATION,
        )

        client.put_bucket_cors(Bucket=bucket_name, CORSConfiguration=cors_config)

        response = client.get_bucket_cors(Bucket=bucket_name)
        assert response["CORSRules"][0]["AllowedMethods"] == allowed_methods
        assert response["CORSRules"][0]["AllowedOrigins"] == allowed_origins

        client.delete_bucket_cors(Bucket=bucket_name)
        self.assert_client_error(
            lambda: client.get_bucket_cors(Bucket=bucket_name),
            404,
            md.NO_SUCH_CORS_CONFIGURATION,
        )

    @pytest.mark.tag("Post")
    # KSAN 알려진 실패(일부러 다르게 동작, 고치지 않음): SCP 연동 때문에 CORS 설정이 있는 버킷의
    # OPTIONS 는 Origin 이 없거나 규칙에 맞지 않아도 200 을 준다(2026-04 InfiniStor 와 같게 맞춤).
    # AWS 는 Origin 이 없으면 400, 규칙에 맞지 않으면 403 이다.
    def test_cors_origin_response(self):
        client = self.get_client()
        bucket_name = self._create_public_cors_bucket(client, 2)
        cors_config = {
            "CORSRules": [
                {"AllowedMethods": ["GET"], "AllowedOrigins": ["*suffix"]},
                {"AllowedMethods": ["GET"], "AllowedOrigins": ["start*end"]},
                {"AllowedMethods": ["GET"], "AllowedOrigins": ["prefix*"]},
                {"AllowedMethods": ["PUT"], "AllowedOrigins": ["*.put"]},
            ]
        }

        self._assert_no_cors_configuration(client, bucket_name)
        client.put_bucket_cors(Bucket=bucket_name, CORSConfiguration=cors_config)

        checks = [
            ("GET", {}, 200, None, None, None),
            ("GET", {"Origin": "foo.suffix"}, 200, "foo.suffix", "GET", None),
            ("GET", {"Origin": "foo.bar"}, 200, None, None, None),
            ("GET", {"Origin": "foo.suffix.get"}, 200, None, None, None),
            ("GET", {"Origin": "start_end"}, 200, "start_end", "GET", None),
            ("GET", {"Origin": "start1end"}, 200, "start1end", "GET", None),
            ("GET", {"Origin": "start12end"}, 200, "start12end", "GET", None),
            ("GET", {"Origin": "0start12end"}, 200, None, None, None),
            ("GET", {"Origin": "prefix"}, 200, "prefix", "GET", None),
            ("GET", {"Origin": "prefix.suffix"}, 200, "prefix.suffix", "GET", None),
            ("GET", {"Origin": "bla.prefix"}, 200, None, None, None),
            ("GET", {"Origin": "foo.suffix"}, 404, "foo.suffix", "GET", "bar"),
            ("PUT", {"Origin": "foo.suffix", "Access-Control-Request-Method": "GET", "content-length": "0"}, 403, "foo.suffix", "GET", "bar"),
            ("PUT", {"Origin": "foo.suffix", "Access-Control-Request-Method": "PUT", "content-length": "0"}, 403, None, None, "bar"),
            ("PUT", {"Origin": "foo.suffix", "Access-Control-Request-Method": "DELETE", "content-length": "0"}, 403, None, None, "bar"),
            ("PUT", {"Origin": "foo.suffix", "content-length": "0"}, 403, None, None, "bar"),
            ("PUT", {"Origin": "foo.put", "content-length": "0"}, 403, "foo.put", "PUT", "bar"),
            ("OPTIONS", {}, 400, None, None, None),
            ("OPTIONS", {"Origin": "foo.suffix"}, 403, None, None, None),
            ("OPTIONS", {"Origin": "foo.bla"}, 403, None, None, None),
            ("OPTIONS", {"Origin": "foo.suffix", "Access-Control-Request-Method": "GET", "content-length": "0"}, 200, "foo.suffix", "GET", "bar"),
            ("OPTIONS", {"Origin": "foo.bar", "Access-Control-Request-Method": "GET"}, 403, None, None, None),
            ("OPTIONS", {"Origin": "foo.suffix.get", "Access-Control-Request-Method": "GET"}, 403, None, None, None),
            ("OPTIONS", {"Origin": "start_end", "Access-Control-Request-Method": "GET"}, 200, "start_end", "GET", None),
            ("OPTIONS", {"Origin": "start1end", "Access-Control-Request-Method": "GET"}, 200, "start1end", "GET", None),
            ("OPTIONS", {"Origin": "start12end", "Access-Control-Request-Method": "GET"}, 200, "start12end", "GET", None),
            ("OPTIONS", {"Origin": "0start12end", "Access-Control-Request-Method": "GET"}, 403, None, None, None),
            ("OPTIONS", {"Origin": "prefix", "Access-Control-Request-Method": "GET"}, 200, "prefix", "GET", None),
            ("OPTIONS", {"Origin": "prefix.suffix", "Access-Control-Request-Method": "GET"}, 200, "prefix.suffix", "GET", None),
            ("OPTIONS", {"Origin": "bla.prefix", "Access-Control-Request-Method": "GET"}, 403, None, None, None),
            ("OPTIONS", {"Origin": "foo.put", "Access-Control-Request-Method": "GET"}, 403, None, None, None),
            ("OPTIONS", {"Origin": "foo.put", "Access-Control-Request-Method": "PUT"}, 200, "foo.put", "PUT", None),
        ]
        for method, headers, status, allow_origin, allow_methods, key in checks:
            self.cors_request_and_check(
                method,
                bucket_name,
                headers,
                status,
                allow_origin,
                allow_methods,
                key,
            )

    @pytest.mark.tag("Post")
    def test_cors_origin_wildcard(self):
        client = self.get_client()
        bucket_name = self._create_public_cors_bucket(client, 3)
        cors_config = {"CORSRules": [{"AllowedMethods": ["GET"], "AllowedOrigins": ["*"]}]}

        self._assert_no_cors_configuration(client, bucket_name)
        client.put_bucket_cors(Bucket=bucket_name, CORSConfiguration=cors_config)

        self.cors_request_and_check("GET", bucket_name, {}, 200, None, None)
        self.cors_request_and_check(
            "GET", bucket_name, {"Origin": "example.origin"}, 200, "*", "GET"
        )

    @pytest.mark.tag("Post")
    # KSAN 알려진 실패(일부러 다르게 동작, 고치지 않음): SCP 연동 때문에 허용하지 않은
    # Access-Control-Request-Headers 로 preflight 해도 200 을 준다(InfiniStor 와 같음). AWS 는 403 이다.
    def test_cors_header_option(self):
        client = self.get_client()
        bucket_name = self._create_public_cors_bucket(client, 4)
        cors_config = {
            "CORSRules": [
                {
                    "AllowedMethods": ["GET"],
                    "AllowedOrigins": ["*"],
                    "ExposeHeaders": ["x-amz-meta-header1"],
                }
            ]
        }

        self._assert_no_cors_configuration(client, bucket_name)
        client.put_bucket_cors(Bucket=bucket_name, CORSConfiguration=cors_config)

        headers = {
            "Origin": "example.origin",
            "Access-Control-Request-headers": "x-amz-meta-header2",
            "Access-Control-Request-Method": "GET",
        }
        self.cors_request_and_check("OPTIONS", bucket_name, headers, 403, None, None, "bar")

    @staticmethod
    def _cors_rule(methods, origins, **extra) -> dict:
        rule: dict = {"AllowedMethods": list(methods), "AllowedOrigins": list(origins)}
        rule.update(extra)
        return rule

    def _cors_request(self, method: str, bucket_name: str, headers: dict, key=None):
        return net_utils.cors_request(self.create_url(bucket_name, key), method, headers)

    def _assert_invalid_cors(self, client, bucket_name: str, rules: list) -> None:
        self.assert_client_error(
            lambda: client.put_bucket_cors(Bucket=bucket_name, CORSConfiguration={"CORSRules": rules}),
            400,
            md.INVALID_REQUEST,
        )

    @pytest.mark.tag("Check")
    def test_cors_all_fields(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 5)
        rules = [
            self._cors_rule(
                ["GET", "PUT"],
                ["http://a.example.com"],
                ID="rule-1",
                AllowedHeaders=["x-amz-*", "content-type"],
                ExposeHeaders=["ETag", "x-amz-meta-foo"],
                MaxAgeSeconds=600,
            ),
            self._cors_rule(["GET"], ["*"], ID="rule-2", MaxAgeSeconds=20),
        ]
        client.put_bucket_cors(Bucket=bucket_name, CORSConfiguration={"CORSRules": rules})

        response = client.get_bucket_cors(Bucket=bucket_name)
        assert len(response["CORSRules"]) == 2
        first, second = response["CORSRules"]
        assert first["ID"] == "rule-1"
        assert first["AllowedMethods"] == ["GET", "PUT"]
        assert first["AllowedOrigins"] == ["http://a.example.com"]
        assert first["AllowedHeaders"] == ["x-amz-*", "content-type"]
        assert first["ExposeHeaders"] == ["ETag", "x-amz-meta-foo"]
        assert first["MaxAgeSeconds"] == 600
        assert second["ID"] == "rule-2"
        assert second["AllowedMethods"] == ["GET"]
        assert second["AllowedOrigins"] == ["*"]
        assert second["MaxAgeSeconds"] == 20

    @pytest.mark.tag("Preflight")
    # KSAN 알려진 실패(일부러 다르게 동작, 고치지 않음): SCP 연동 때문에 OPTIONS 는 Origin 만 보고 규칙을 고르고 항상 200 을 준다.
    # Access-Control-Allow-Methods 도 GET,PUT 처럼 공백 없이 잇는다. AWS 는 GET, PUT 형식이고 허용하지 않은 헤더는 403 이다.
    def test_cors_preflight_headers(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 6)
        rules = [
            self._cors_rule(
                ["GET", "PUT"],
                ["http://a.example.com"],
                AllowedHeaders=["x-amz-*", "content-type"],
                ExposeHeaders=["ETag", "x-amz-meta-foo"],
                MaxAgeSeconds=600,
            )
        ]
        client.put_bucket_cors(Bucket=bucket_name, CORSConfiguration={"CORSRules": rules})

        # 허용된 헤더(와일드카드, 대소문자 무시)로 preflight 요청
        code, headers = self._cors_request(
            "OPTIONS",
            bucket_name,
            {
                "Origin": "http://a.example.com",
                "Access-Control-Request-Method": "PUT",
                "Access-Control-Request-Headers": "X-Amz-Meta-A, Content-Type",
            },
        )
        assert code == 200
        assert headers.get("access-control-allow-origin") == "http://a.example.com"
        assert headers.get("access-control-allow-methods") == "GET, PUT"
        assert headers.get("access-control-allow-headers") == "x-amz-meta-a, content-type"
        assert headers.get("access-control-expose-headers") == "ETag, x-amz-meta-foo"
        assert headers.get("access-control-max-age") == "600"
        assert headers.get("access-control-allow-credentials") == "true"

        # 허용되지 않은 헤더를 요청하면 거부된다
        code, headers = self._cors_request(
            "OPTIONS",
            bucket_name,
            {
                "Origin": "http://a.example.com",
                "Access-Control-Request-Method": "PUT",
                "Access-Control-Request-Headers": "authorization",
            },
        )
        assert code == 403
        assert headers.get("access-control-allow-origin") is None

    @pytest.mark.tag("Preflight")
    # KSAN 알려진 실패(일부러 다르게 동작, 고치지 않음): SCP 연동 때문에 OPTIONS 는 Origin 만 보고 첫 규칙을 고르고 항상 200 을 준다.
    # AWS 는 메서드까지 맞는 규칙을 순서대로 찾고, 맞는 규칙이 없으면 403 이다.
    def test_cors_rule_order(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 7)
        rules = [
            self._cors_rule(["GET", "PUT"], ["http://a.example.com"], MaxAgeSeconds=600),
            self._cors_rule(["GET"], ["*"], MaxAgeSeconds=20),
        ]
        client.put_bucket_cors(Bucket=bucket_name, CORSConfiguration={"CORSRules": rules})

        # 두 규칙 모두에 맞으면 먼저 선언된 규칙이 적용된다
        code, headers = self._cors_request(
            "OPTIONS", bucket_name, {"Origin": "http://a.example.com", "Access-Control-Request-Method": "GET"}
        )
        assert code == 200
        assert headers.get("access-control-allow-origin") == "http://a.example.com"
        assert headers.get("access-control-allow-methods") == "GET, PUT"
        assert headers.get("access-control-max-age") == "600"

        # 첫 규칙에 맞지 않으면 다음 규칙이 적용된다
        code, headers = self._cors_request(
            "OPTIONS", bucket_name, {"Origin": "http://b.example.com", "Access-Control-Request-Method": "GET"}
        )
        assert code == 200
        assert headers.get("access-control-allow-origin") == "*"
        assert headers.get("access-control-allow-methods") == "GET"
        assert headers.get("access-control-max-age") == "20"
        assert headers.get("access-control-allow-credentials") is None

        # 어느 규칙에도 맞지 않으면 거부된다
        code, headers = self._cors_request(
            "OPTIONS", bucket_name, {"Origin": "http://b.example.com", "Access-Control-Request-Method": "PUT"}
        )
        assert code == 403
        assert headers.get("access-control-allow-origin") is None

    @pytest.mark.tag("Object")
    # KSAN 알려진 실패(ifs32, 일부러 다르게 동작, 고치지 않음): SCP 연동 때문에 일반 요청의 CORS 헤더를 s3gw 와 같게 낸다.
    # Access-Control-Allow-Methods 는 "GET,HEAD"(쉼표만)다. AWS 는 "GET, HEAD" 다. 확인은 test_cors_scp_compat.
    def test_cors_object_request(self):
        client = self.get_client()
        bucket_name = self._create_public_cors_bucket(client, 8)
        key = "test_cors_object_request"
        client.put_object(Bucket=bucket_name, Key=key, Body=b"cors", ACL="public-read")
        rules = [self._cors_rule(["GET", "HEAD"], ["http://a.example.com"], ExposeHeaders=["ETag"])]
        client.put_bucket_cors(Bucket=bucket_name, CORSConfiguration={"CORSRules": rules})

        for method in ("GET", "HEAD"):
            code, headers = self._cors_request(method, bucket_name, {"Origin": "http://a.example.com"}, key)
            assert code == 200
            assert headers.get("access-control-allow-origin") == "http://a.example.com"
            assert headers.get("access-control-allow-methods") == "GET, HEAD"
            assert headers.get("access-control-expose-headers") == "ETag"

            # 규칙에 맞지 않는 Origin 이면 요청은 성공하지만 CORS 헤더가 없다
            code, headers = self._cors_request(method, bucket_name, {"Origin": "http://b.example.com"}, key)
            assert code == 200
            assert headers.get("access-control-allow-origin") is None

        # 오브젝트 키에 대한 preflight
        code, headers = self._cors_request(
            "OPTIONS",
            bucket_name,
            {"Origin": "http://a.example.com", "Access-Control-Request-Method": "GET"},
            key,
        )
        assert code == 200
        assert headers.get("access-control-allow-origin") == "http://a.example.com"

    @pytest.mark.tag("Object")
    # KSAN(ifs32) SCP 호환 케이스: 일반 요청의 CORS 헤더가 s3gw(운영계)와 같은지 확인한다.
    # Origin 이 맞는 첫 규칙의 메서드·허용 헤더·노출 헤더를 요청 메서드와 상관없이 붙이고 vary 에 Origin 이 있다.
    # 규칙에 HEAD 가 없어도 HeadBucket·HeadObject 에 헤더가 붙어야 브라우저 콘솔의 버킷 연결 확인이 된다.
    # AWS 는 규칙에 없는 메서드(HEAD)에는 CORS 헤더를 주지 않고 일반 요청에는 Allow-Headers 를 주지 않아 AWS 에서는 실패한다.
    def test_cors_scp_compat(self):
        client = self.get_client()
        bucket_name = self._create_public_cors_bucket(client, 18)
        key = "test_cors_scp_compat"
        client.put_object(Bucket=bucket_name, Key=key, Body=b"cors", ACL="public-read")
        methods = ["PUT", "POST", "GET"]
        allow_headers = ["x-amz-content-sha256", "x-amz-user-agent", "x-amz-date", "authorization",
                         "Content-Type", "Content-MD5", "x-amz-acl"]
        expose_headers = ["x-amz-meta-custom-header", "x-amz-request-id", "x-amz-id-2", "ETag", "date",
                          "x-amz-server-side-encryption"]
        rules = [self._cors_rule(methods, ["*"], AllowedHeaders=allow_headers, ExposeHeaders=expose_headers)]
        client.put_bucket_cors(Bucket=bucket_name, CORSConfiguration={"CORSRules": rules})

        # HEAD·GET 버킷(연결 확인·목록), HEAD·GET 오브젝트
        for method, target in (("HEAD", None), ("GET", None), ("HEAD", key), ("GET", key)):
            what = f"{method} {'bucket' if target is None else 'object'}"
            code, headers = self._cors_request(method, bucket_name, {"Origin": "http://console.example.com"}, target)
            assert code == 200, what
            assert headers.get("access-control-allow-origin") == "*", what
            assert self._cors_set(headers.get("access-control-allow-methods")) == self._cors_set(methods), what
            assert self._cors_set(headers.get("access-control-allow-headers")) == self._cors_set(allow_headers), what
            assert self._cors_set(headers.get("access-control-expose-headers")) == self._cors_set(expose_headers), what
            assert "origin" in self._cors_set(headers.get("vary")), what

    @staticmethod
    def _cors_set(value) -> set:
        """쉼표로 나눈 CORS 헤더 값을 소문자 집합으로 (구분자 "," 와 ", " 를 같게 본다)"""
        if value is None:
            return set()
        if isinstance(value, (list, tuple)):
            value = ",".join(value)
        return {v.strip().lower() for v in value.split(",") if v.strip()}

    @pytest.mark.tag("Error")
    # KSAN 알려진 실패(일부러 다르게 동작, 고치지 않음): PutBucketCors 는 SCP 연동 때문에 규칙 내용을 검사하지 않고 그대로 저장한다.
    # AWS 는 AllowedMethod 가 GET·PUT·POST·DELETE·HEAD(대문자)가 아니면 400 InvalidRequest 다.
    def test_cors_invalid_method(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 9)
        self._assert_invalid_cors(client, bucket_name, [self._cors_rule(["PATCH"], ["*"])])
        # 메서드 이름은 대문자만 허용된다
        self._assert_invalid_cors(client, bucket_name, [self._cors_rule(["get"], ["*"])])

    @pytest.mark.tag("Error")
    # KSAN 알려진 실패(일부러 다르게 동작, 고치지 않음): PutBucketCors 는 SCP 연동 때문에 규칙 내용을 검사하지 않고 그대로 저장한다.
    # AWS 는 AllowedOrigin 에 * 가 둘 이상이면 400 InvalidRequest 다.
    def test_cors_invalid_origin_wildcard(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 10)
        self._assert_invalid_cors(client, bucket_name, [self._cors_rule(["GET"], ["*a*"])])

    @pytest.mark.tag("Error")
    # KSAN 알려진 실패(일부러 다르게 동작, 고치지 않음): PutBucketCors 는 SCP 연동 때문에 규칙 내용을 검사하지 않고 그대로 저장한다.
    # AWS 는 AllowedHeader 에 * 가 둘 이상이면 400 InvalidRequest 다.
    def test_cors_invalid_header_wildcard(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 11)
        self._assert_invalid_cors(
            client, bucket_name, [self._cors_rule(["GET"], ["*"], AllowedHeaders=["*a*"])]
        )

    @pytest.mark.tag("Error")
    # KSAN 알려진 실패(일부러 다르게 동작, 고치지 않음): PutBucketCors 는 SCP 연동 때문에 규칙 내용을 검사하지 않고 그대로 저장한다.
    # AWS 는 ExposeHeader 에 * 가 있으면 400 InvalidRequest 다.
    def test_cors_expose_header_wildcard(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 12)
        self._assert_invalid_cors(
            client, bucket_name, [self._cors_rule(["GET"], ["*"], ExposeHeaders=["x-amz-*"])]
        )

    @pytest.mark.tag("Error")
    # KSAN 알려진 실패(일부러 다르게 동작, 고치지 않음): PutBucketCors 는 SCP 연동 때문에 규칙 내용을 검사하지 않고 그대로 저장한다.
    # AWS 는 규칙이 101개 이상이면 400 InvalidRequest 다.
    def test_cors_rule_limit(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 13)
        rules = [self._cors_rule(["GET"], [f"http://{i}.example.com"]) for i in range(100)]
        client.put_bucket_cors(Bucket=bucket_name, CORSConfiguration={"CORSRules": rules})
        assert len(client.get_bucket_cors(Bucket=bucket_name)["CORSRules"]) == 100

        rules.append(self._cors_rule(["GET"], ["http://100.example.com"]))
        self._assert_invalid_cors(client, bucket_name, rules)

    @pytest.mark.tag("Check")
    def test_cors_overwrite(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 14)
        client.put_bucket_cors(
            Bucket=bucket_name,
            CORSConfiguration={
                "CORSRules": [
                    self._cors_rule(["GET"], ["http://a.example.com"]),
                    self._cors_rule(["PUT"], ["http://b.example.com"]),
                ]
            },
        )
        client.put_bucket_cors(
            Bucket=bucket_name,
            CORSConfiguration={"CORSRules": [self._cors_rule(["HEAD"], ["http://z.example.com"])]},
        )
        response = client.get_bucket_cors(Bucket=bucket_name)
        assert len(response["CORSRules"]) == 1
        assert response["CORSRules"][0]["AllowedMethods"] == ["HEAD"]
        assert response["CORSRules"][0]["AllowedOrigins"] == ["http://z.example.com"]

    @pytest.mark.tag("Delete")
    def test_cors_delete_not_exist(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 15)
        # 설정이 없어도 삭제는 성공한다
        client.delete_bucket_cors(Bucket=bucket_name)
        self._assert_no_cors_configuration(client, bucket_name)

    @pytest.mark.tag("Error")
    def test_cors_bucket_not_exist(self):
        client = self.get_client()
        bucket_name = self.get_new_bucket_name_only(16)
        config = {"CORSRules": [self._cors_rule(["GET"], ["*"])]}
        self.assert_client_error(lambda: client.get_bucket_cors(Bucket=bucket_name), 404, md.NO_SUCH_BUCKET)
        self.assert_client_error(
            lambda: client.put_bucket_cors(Bucket=bucket_name, CORSConfiguration=config),
            404,
            md.NO_SUCH_BUCKET,
        )
        self.assert_client_error(lambda: client.delete_bucket_cors(Bucket=bucket_name), 404, md.NO_SUCH_BUCKET)

    @pytest.mark.tag("Permission")
    def test_cors_alt_user(self):
        client = self.get_client()
        alt_client = self.get_alt_client()
        bucket_name = self.create_bucket(client, 17)
        config = {"CORSRules": [self._cors_rule(["GET"], ["*"])]}
        client.put_bucket_cors(Bucket=bucket_name, CORSConfiguration=config)
        self.assert_client_error(lambda: alt_client.get_bucket_cors(Bucket=bucket_name), 403, md.ACCESS_DENIED)
        self.assert_client_error(
            lambda: alt_client.put_bucket_cors(Bucket=bucket_name, CORSConfiguration=config),
            403,
            md.ACCESS_DENIED,
        )
        self.assert_client_error(lambda: alt_client.delete_bucket_cors(Bucket=bucket_name), 403, md.ACCESS_DENIED)
