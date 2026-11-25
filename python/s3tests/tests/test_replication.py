"""Replication tests ported from Java testV2/Replication.java."""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError

from s3tests.data import main_data as md
from s3tests.test_base import S3TestBase


class TestReplication(S3TestBase):
    def _replication_role(self) -> str:
        """복제 역할 ARN. 계정 아이디는 설정의 AccountId(없으면 UserID)를 쓴다."""
        return f"arn:aws:iam::{self.config.main_user.expected_owner_id}:role/replication"

    def _replication_config(self, target_bucket_arn: str, prefix: str | None = None) -> dict:
        rule: dict = {
            "Status": "Enabled",
            "Destination": {"Bucket": target_bucket_arn},
        }
        if prefix is not None:
            rule["Priority"] = 1
            rule["Filter"] = {"Prefix": prefix}
            rule["DeleteMarkerReplication"] = {"Status": "Disabled"}
        return {"Role": self._replication_role(), "Rules": [rule]}

    @pytest.mark.tag("Check")
    def test_replication_set(self):
        prefix = "test/"
        client = self.get_client()
        source_bucket_name = self.create_bucket(client, 1)
        target_bucket_name = self.create_bucket(client, 1)
        self.check_configure_versioning_retry(source_bucket_name, "Enabled")
        self.check_configure_versioning_retry(target_bucket_name, "Enabled")

        target_bucket_arn = f"arn:aws:s3:::{target_bucket_name}"
        config = self._replication_config(target_bucket_arn, prefix)
        client.put_bucket_replication(
            Bucket=source_bucket_name, ReplicationConfiguration=config
        )
        get_config = client.get_bucket_replication(Bucket=source_bucket_name)
        self.replication_config_compare(config, get_config["ReplicationConfiguration"])

        client.delete_bucket_replication(Bucket=source_bucket_name)
        with pytest.raises(ClientError) as exc_info:
            client.get_bucket_replication(Bucket=source_bucket_name)
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404

    @pytest.mark.tag("ERROR")
    def test_replication_invalid_source_bucket_name(self):
        client = self.get_client()
        source_bucket_name = self.get_new_bucket_name_only(2)
        target_bucket_name = self.get_new_bucket_name_only(2)
        target_bucket_arn = f"arn:aws:s3:::{target_bucket_name}"
        config = self._replication_config(target_bucket_arn)
        self.assert_client_error(
            lambda: client.put_bucket_replication(
                Bucket=source_bucket_name, ReplicationConfiguration=config
            ),
            404,
            md.NO_SUCH_BUCKET,
        )

    @pytest.mark.tag("ERROR")
    def test_replication_invalid_source_bucket_versioning(self):
        client = self.get_client()
        source_bucket_name = self.create_bucket(client, 3)
        target_bucket_name = self.create_bucket(client, 3)
        target_bucket_arn = f"arn:aws:s3:::{target_bucket_name}"
        config = self._replication_config(target_bucket_arn)
        self.assert_client_error(
            lambda: client.put_bucket_replication(
                Bucket=source_bucket_name, ReplicationConfiguration=config
            ),
            400,
            md.INVALID_REQUEST,
        )

    @pytest.mark.tag("ERROR")
    def test_replication_invalid_target_bucket_name(self):
        prefix = "test/"
        client = self.get_client()
        source_bucket_name = self.create_bucket(client, 4)
        target_bucket_name = self.get_new_bucket_name_only(4)
        self.check_configure_versioning_retry(source_bucket_name, "Enabled")

        target_bucket_arn = f"arn:aws:s3:::{target_bucket_name}"
        config = self._replication_config(target_bucket_arn, prefix)
        self.assert_client_error(
            lambda: client.put_bucket_replication(
                Bucket=source_bucket_name, ReplicationConfiguration=config
            ),
            400,
            md.INVALID_REQUEST,
        )

    @pytest.mark.tag("ERROR")
    def test_replication_invalid_target_bucket_versioning(self):
        prefix = "test/"
        client = self.get_client()
        source_bucket_name = self.create_bucket(client, 5)
        target_bucket_name = self.create_bucket(client, 5)
        self.check_configure_versioning_retry(source_bucket_name, "Enabled")

        target_bucket_arn = f"arn:aws:s3:::{target_bucket_name}"
        config = self._replication_config(target_bucket_arn, prefix)
        self.assert_client_error(
            lambda: client.put_bucket_replication(
                Bucket=source_bucket_name, ReplicationConfiguration=config
            ),
            400,
            md.INVALID_REQUEST,
        )

    @pytest.mark.tag("ERROR")
    def test_replication_bucket_versioning_suspend(self):
        prefix = "test/"
        client = self.get_client()
        source_bucket_name = self.create_bucket(client, 6)
        target_bucket_name = self.create_bucket(client, 6)
        self.check_configure_versioning_retry(source_bucket_name, "Enabled")
        self.check_configure_versioning_retry(target_bucket_name, "Enabled")

        target_bucket_arn = f"arn:aws:s3:::{target_bucket_name}"
        config = self._replication_config(target_bucket_arn, prefix)
        client.put_bucket_replication(
            Bucket=source_bucket_name, ReplicationConfiguration=config
        )

        self.assert_client_error(
            lambda: client.put_bucket_versioning(
                Bucket=source_bucket_name,
                VersioningConfiguration={"Status": "Suspended"},
            ),
            409,
            md.INVALID_BUCKET_STATE,
        )

    def _setup_replication_buckets(self, client, test_id: int) -> tuple[str, str]:
        """버저닝이 켜진 원본·대상 버킷을 만들고 (원본 버킷명, 대상 버킷 ARN)을 돌려준다."""
        source_bucket_name = self.create_bucket(client, test_id)
        target_bucket_name = self.create_bucket(client, test_id)
        self.check_configure_versioning_retry(source_bucket_name, "Enabled")
        self.check_configure_versioning_retry(target_bucket_name, "Enabled")
        return source_bucket_name, f"arn:aws:s3:::{target_bucket_name}"

    def _setup_target_bucket(self, client, test_id: int) -> str:
        """버저닝이 켜진 대상 버킷을 하나 더 만들고 ARN 을 돌려준다."""
        target_bucket_name = self.create_bucket(client, test_id)
        self.check_configure_versioning_retry(target_bucket_name, "Enabled")
        return f"arn:aws:s3:::{target_bucket_name}"

    @staticmethod
    def _v1_rule(rule_id: str, prefix: str, target_bucket_arn: str) -> dict:
        """구버전(V1) 스키마 규칙: Filter·Priority·DeleteMarkerReplication 없이 규칙 직속 Prefix 를 쓴다."""
        return {"ID": rule_id, "Status": "Enabled", "Prefix": prefix, "Destination": {"Bucket": target_bucket_arn}}

    @staticmethod
    def _replication_rule(target_bucket_arn: str, **overrides) -> dict:
        rule: dict = {
            "Status": "Enabled",
            "Priority": 1,
            "Filter": {"Prefix": "test/"},
            "Destination": {"Bucket": target_bucket_arn},
            "DeleteMarkerReplication": {"Status": "Disabled"},
        }
        rule.update(overrides)
        return {key: value for key, value in rule.items() if value is not None}

    def _put_replication(self, client, source_bucket_name: str, rules: list) -> None:
        client.put_bucket_replication(
            Bucket=source_bucket_name,
            ReplicationConfiguration={"Role": self._replication_role(), "Rules": rules},
        )

    @staticmethod
    def _get_replication_rules(client, source_bucket_name: str) -> list:
        return client.get_bucket_replication(Bucket=source_bucket_name)["ReplicationConfiguration"]["Rules"]

    @pytest.mark.tag("Check")
    def test_replication_get_not_exist(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 7)
        self.assert_client_error(
            lambda: client.get_bucket_replication(Bucket=bucket_name),
            404,
            "ReplicationConfigurationNotFoundError",
        )
        # 설정이 없어도 삭제는 성공한다
        client.delete_bucket_replication(Bucket=bucket_name)

    @pytest.mark.tag("Filter")
    def test_replication_tag_filter(self):
        client = self.get_client()
        source_bucket_name, target_bucket_arn = self._setup_replication_buckets(client, 8)
        filter_ = {"Tag": {"Key": "key1", "Value": "value1"}}
        self._put_replication(client, source_bucket_name, [self._replication_rule(target_bucket_arn, Filter=filter_)])
        rules = self._get_replication_rules(client, source_bucket_name)
        assert rules[0]["Filter"] == filter_

    @pytest.mark.tag("Filter")
    def test_replication_and_filter(self):
        client = self.get_client()
        source_bucket_name, target_bucket_arn = self._setup_replication_buckets(client, 9)
        filter_ = {
            "And": {
                "Prefix": "test/",
                "Tags": [{"Key": "key1", "Value": "value1"}, {"Key": "key2", "Value": "value2"}],
            }
        }
        self._put_replication(client, source_bucket_name, [self._replication_rule(target_bucket_arn, Filter=filter_)])
        rules = self._get_replication_rules(client, source_bucket_name)
        assert rules[0]["Filter"] == filter_

    @pytest.mark.tag("Filter")
    def test_replication_empty_filter(self):
        client = self.get_client()
        source_bucket_name, target_bucket_arn = self._setup_replication_buckets(client, 10)
        # 빈 접두어는 버킷 전체를 대상으로 한다
        self._put_replication(
            client, source_bucket_name, [self._replication_rule(target_bucket_arn, Filter={"Prefix": ""})]
        )
        rules = self._get_replication_rules(client, source_bucket_name)
        assert rules[0]["Filter"].get("Prefix", "") == ""
        assert "Tag" not in rules[0]["Filter"]
        assert "And" not in rules[0]["Filter"]

    @pytest.mark.tag("Filter")
    def test_replication_v1_schema(self):
        client = self.get_client()
        source_bucket_name, target_bucket_arn = self._setup_replication_buckets(client, 11)
        # 구버전(V1) 스키마: Filter/Priority/DeleteMarkerReplication 없이 규칙 직속 Prefix 를 쓴다
        rule = {"ID": "v1-rule", "Status": "Enabled", "Prefix": "v1/", "Destination": {"Bucket": target_bucket_arn}}
        self._put_replication(client, source_bucket_name, [rule])
        rules = self._get_replication_rules(client, source_bucket_name)
        assert rules[0]["ID"] == "v1-rule"
        assert rules[0]["Prefix"] == "v1/"
        assert rules[0]["Status"] == "Enabled"
        assert rules[0]["Destination"]["Bucket"] == target_bucket_arn
        assert "Filter" not in rules[0]

    @pytest.mark.tag("Check")
    def test_replication_multiple_rules(self):
        client = self.get_client()
        source_bucket_name, target_bucket_arn = self._setup_replication_buckets(client, 12)
        self._put_replication(
            client,
            source_bucket_name,
            [
                self._replication_rule(target_bucket_arn, ID="rule-1", Priority=1, Filter={"Prefix": "a/"}),
                self._replication_rule(target_bucket_arn, ID="rule-2", Priority=2, Filter={"Prefix": "b/"}),
            ],
        )
        rules = self._get_replication_rules(client, source_bucket_name)
        assert [(r["ID"], r["Priority"], r["Filter"]["Prefix"]) for r in rules] == [
            ("rule-1", 1, "a/"),
            ("rule-2", 2, "b/"),
        ]

    @pytest.mark.tag("ERROR")
    def test_replication_duplicate_priority(self):
        client = self.get_client()
        source_bucket_name, target_bucket_arn = self._setup_replication_buckets(client, 13)
        self.assert_client_error(
            lambda: self._put_replication(
                client,
                source_bucket_name,
                [
                    self._replication_rule(target_bucket_arn, ID="rule-1", Priority=1, Filter={"Prefix": "a/"}),
                    self._replication_rule(target_bucket_arn, ID="rule-2", Priority=1, Filter={"Prefix": "b/"}),
                ],
            ),
            400,
            md.INVALID_REQUEST,
        )

    @pytest.mark.tag("ERROR")
    def test_replication_duplicate_id(self):
        client = self.get_client()
        source_bucket_name, target_bucket_arn = self._setup_replication_buckets(client, 14)
        self.assert_client_error(
            lambda: self._put_replication(
                client,
                source_bucket_name,
                [
                    self._replication_rule(target_bucket_arn, ID="rule-1", Priority=1, Filter={"Prefix": "a/"}),
                    self._replication_rule(target_bucket_arn, ID="rule-1", Priority=2, Filter={"Prefix": "b/"}),
                ],
            ),
            400,
            md.INVALID_ARGUMENT,
        )

    @pytest.mark.tag("ERROR")
    # KSAN 알려진 실패(일부러 다르게 동작, 당분간 유지): Filter 가 있는데 Priority 나 DeleteMarkerReplication 이 없어도 받는다.
    # 이전부터 받아 온 형태라 검증을 넣으면 기존 설정이 거부될 수 있어 일단 넣지 않는다. AWS 는 400 InvalidRequest 다.
    def test_replication_filter_without_priority(self):
        client = self.get_client()
        source_bucket_name, target_bucket_arn = self._setup_replication_buckets(client, 15)
        # Filter 를 쓰는 V2 스키마에서는 Priority 와 DeleteMarkerReplication 이 필수다
        self.assert_client_error(
            lambda: self._put_replication(
                client, source_bucket_name, [self._replication_rule(target_bucket_arn, Priority=None)]
            ),
            400,
            md.INVALID_REQUEST,
        )
        self.assert_client_error(
            lambda: self._put_replication(
                client, source_bucket_name, [self._replication_rule(target_bucket_arn, DeleteMarkerReplication=None)]
            ),
            400,
            md.INVALID_REQUEST,
        )

    @pytest.mark.tag("Check")
    def test_replication_multiple_destinations(self):
        client = self.get_client()
        source_bucket_name, target_bucket_arn1 = self._setup_replication_buckets(client, 24)
        target_bucket_arn2 = self._setup_target_bucket(client, 24)
        target_bucket_arn3 = self._setup_target_bucket(client, 24)

        # 빈 Filter 로 버킷 전체를 규칙마다 다른 대상 버킷으로 보낸다. Priority 는 서로 달라야 한다
        targets = [target_bucket_arn1, target_bucket_arn2, target_bucket_arn3]
        self._put_replication(
            client,
            source_bucket_name,
            [
                self._replication_rule(target, ID=f"rule-{i + 1}", Priority=i + 1, Filter={})
                for i, target in enumerate(targets)
            ],
        )

        rules = self._get_replication_rules(client, source_bucket_name)
        assert len(rules) == 3
        for i, rule in enumerate(rules):
            assert rule["ID"] == f"rule-{i + 1}"
            assert rule["Priority"] == i + 1
            assert rule["Destination"]["Bucket"] == targets[i]
            assert rule["DeleteMarkerReplication"]["Status"] == "Disabled"
            assert "Filter" in rule
            assert rule["Filter"].get("Prefix", "") == ""

    @pytest.mark.tag("ERROR")
    def test_replication_priority_without_filter(self):
        client = self.get_client()
        source_bucket_name, target_bucket_arn = self._setup_replication_buckets(client, 25)
        # Filter 없이 규칙 직속 Prefix 를 쓰면 구버전(V1) 스키마라 Priority 를 쓸 수 없다.
        # 빈 Prefix(버킷 전체)여도 같다. Filter 와 Prefix 가 둘 다 없으면 AWS 는 MalformedXML 이다
        rule = {
            "ID": "rule-1",
            "Status": "Enabled",
            "Priority": 1,
            "Prefix": "",
            "Destination": {"Bucket": target_bucket_arn},
        }
        self.assert_client_error(
            lambda: self._put_replication(client, source_bucket_name, [rule]),
            400,
            md.INVALID_REQUEST,
        )

    @pytest.mark.tag("ERROR")
    def test_replication_v1_multiple_destinations(self):
        client = self.get_client()
        source_bucket_name, target_bucket_arn1 = self._setup_replication_buckets(client, 26)
        target_bucket_arn2 = self._setup_target_bucket(client, 26)

        # V1 스키마는 대상 버킷이 하나뿐이다. 접두어가 겹치지 않으면 규칙 여러 개는 같은 대상으로만 둘 수 있다
        self._put_replication(
            client,
            source_bucket_name,
            [self._v1_rule("rule-1", "a/", target_bucket_arn1), self._v1_rule("rule-2", "b/", target_bucket_arn1)],
        )
        assert len(self._get_replication_rules(client, source_bucket_name)) == 2

        self.assert_client_error(
            lambda: self._put_replication(
                client,
                source_bucket_name,
                [self._v1_rule("rule-1", "a/", target_bucket_arn1), self._v1_rule("rule-2", "b/", target_bucket_arn2)],
            ),
            400,
            md.INVALID_REQUEST,
        )

    @pytest.mark.tag("ERROR")
    def test_replication_v1_overlapping_prefix(self):
        client = self.get_client()
        source_bucket_name, target_bucket_arn = self._setup_replication_buckets(client, 27)
        # V1 스키마에는 Priority 가 없어 접두어가 겹치는 규칙을 둘 수 없다
        self.assert_client_error(
            lambda: self._put_replication(
                client,
                source_bucket_name,
                [self._v1_rule("rule-1", "a/", target_bucket_arn), self._v1_rule("rule-2", "a/b/", target_bucket_arn)],
            ),
            400,
            md.INVALID_REQUEST,
        )

    @pytest.mark.tag("DeleteMarker")
    def test_replication_delete_marker_enabled(self):
        client = self.get_client()
        source_bucket_name, target_bucket_arn = self._setup_replication_buckets(client, 16)
        self._put_replication(
            client,
            source_bucket_name,
            [self._replication_rule(target_bucket_arn, DeleteMarkerReplication={"Status": "Enabled"})],
        )
        rules = self._get_replication_rules(client, source_bucket_name)
        assert rules[0]["DeleteMarkerReplication"]["Status"] == "Enabled"

    @pytest.mark.tag("ERROR")
    def test_replication_delete_marker_with_tag_filter(self):
        client = self.get_client()
        source_bucket_name, target_bucket_arn = self._setup_replication_buckets(client, 17)
        # 태그 필터가 있으면 삭제 마커 복제를 켤 수 없다
        self.assert_client_error(
            lambda: self._put_replication(
                client,
                source_bucket_name,
                [
                    self._replication_rule(
                        target_bucket_arn,
                        Filter={"Tag": {"Key": "key1", "Value": "value1"}},
                        DeleteMarkerReplication={"Status": "Enabled"},
                    )
                ],
            ),
            400,
            md.INVALID_REQUEST,
        )

    @pytest.mark.tag("Check")
    def test_replication_disabled_rule(self):
        client = self.get_client()
        source_bucket_name, target_bucket_arn = self._setup_replication_buckets(client, 18)
        self._put_replication(client, source_bucket_name, [self._replication_rule(target_bucket_arn, Status="Disabled")])
        rules = self._get_replication_rules(client, source_bucket_name)
        assert rules[0]["Status"] == "Disabled"

    @pytest.mark.tag("ERROR")
    def test_replication_same_bucket(self):
        client = self.get_client()
        source_bucket_name = self.create_bucket(client, 20)
        self.check_configure_versioning_retry(source_bucket_name, "Enabled")
        self.assert_client_error(
            lambda: self._put_replication(
                client, source_bucket_name, [self._replication_rule(f"arn:aws:s3:::{source_bucket_name}")]
            ),
            400,
            md.INVALID_REQUEST,
        )

    @pytest.mark.tag("ERROR")
    def test_replication_invalid_destination_arn(self):
        client = self.get_client()
        source_bucket_name = self.create_bucket(client, 21)
        target_bucket_name = self.create_bucket(client, 21)
        self.check_configure_versioning_retry(source_bucket_name, "Enabled")
        self.check_configure_versioning_retry(target_bucket_name, "Enabled")
        # 대상 버킷은 ARN 형식이어야 한다
        self.assert_client_error(
            lambda: self._put_replication(client, source_bucket_name, [self._replication_rule(target_bucket_name)]),
            400,
            md.INVALID_ARGUMENT,
        )

    @pytest.mark.tag("ERROR")
    def test_replication_bucket_not_exist(self):
        client = self.get_client()
        bucket_name = self.get_new_bucket_name_only(22)
        self.assert_client_error(lambda: client.get_bucket_replication(Bucket=bucket_name), 404, md.NO_SUCH_BUCKET)
        self.assert_client_error(
            lambda: client.delete_bucket_replication(Bucket=bucket_name), 404, md.NO_SUCH_BUCKET
        )

    @pytest.mark.tag("Permission")
    def test_replication_alt_user(self):
        client = self.get_client()
        alt_client = self.get_alt_client()
        source_bucket_name, target_bucket_arn = self._setup_replication_buckets(client, 23)
        rules = [self._replication_rule(target_bucket_arn)]
        self._put_replication(client, source_bucket_name, rules)
        self.assert_client_error(
            lambda: alt_client.get_bucket_replication(Bucket=source_bucket_name), 403, md.ACCESS_DENIED
        )
        self.assert_client_error(
            lambda: self._put_replication(alt_client, source_bucket_name, rules), 403, md.ACCESS_DENIED
        )
        self.assert_client_error(
            lambda: alt_client.delete_bucket_replication(Bucket=source_bucket_name), 403, md.ACCESS_DENIED
        )
