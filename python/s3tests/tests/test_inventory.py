"""Inventory tests ported from Java testV2/Inventory.java."""

from __future__ import annotations

import pytest

from s3tests.data import main_data as md
from s3tests.test_base import S3TestBase


class TestInventory(S3TestBase):
    def _inventory_config(
        self,
        inventory_id: str,
        target_bucket_name: str,
        prefix: str | None = None,
        optional_fields: list[str] | None = None,
        included_object_versions: str = "Current",
        frequency: str = "Daily",
        output_format: str = "CSV",
    ) -> dict:
        destination: dict = {
            "Bucket": f"arn:aws:s3:::{target_bucket_name}",
            "Format": output_format,
        }
        if prefix is not None:
            destination["Prefix"] = prefix
        config: dict = {
            "Id": inventory_id,
            "Destination": {"S3BucketDestination": destination},
            "IsEnabled": True,
            "IncludedObjectVersions": included_object_versions,
            "Schedule": {"Frequency": frequency},
        }
        if optional_fields is not None:
            config["OptionalFields"] = optional_fields
        return config

    @pytest.mark.tag("List")
    def test_list_bucket_inventory(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 1)
        response = client.list_bucket_inventory_configurations(Bucket=bucket_name)
        assert not response.get("InventoryConfigurationList")

    @pytest.mark.tag("Put")
    def test_put_bucket_inventory(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 2)
        target_bucket_name = self.create_bucket(client, 2)
        inventory_id = "my-inventory-v2"
        client.put_bucket_inventory_configuration(
            Bucket=bucket_name,
            Id=inventory_id,
            InventoryConfiguration=self._inventory_config(inventory_id, target_bucket_name),
        )

    @pytest.mark.tag("Check")
    def test_check_bucket_inventory(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 3)
        target_bucket_name = self.create_bucket(client, 3)
        inventory_id = "my-inventory"
        client.put_bucket_inventory_configuration(
            Bucket=bucket_name,
            Id=inventory_id,
            InventoryConfiguration=self._inventory_config(inventory_id, target_bucket_name),
        )
        response = client.list_bucket_inventory_configurations(Bucket=bucket_name)
        assert len(response.get("InventoryConfigurationList", [])) == 1

    @pytest.mark.tag("Get")
    def test_get_bucket_inventory(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 4)
        target_bucket_name = self.create_bucket(client, 4)
        inventory_id = "my-inventory"
        client.put_bucket_inventory_configuration(
            Bucket=bucket_name,
            Id=inventory_id,
            InventoryConfiguration=self._inventory_config(inventory_id, target_bucket_name, prefix="a/"),
        )
        response = client.get_bucket_inventory_configuration(Bucket=bucket_name, Id=inventory_id)
        assert response["InventoryConfiguration"]["Id"] == inventory_id

    @pytest.mark.tag("Delete")
    def test_delete_bucket_inventory(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 5)
        target_bucket_name = self.create_bucket(client, 5)
        inventory_id = "my-inventory"
        client.put_bucket_inventory_configuration(
            Bucket=bucket_name,
            Id=inventory_id,
            InventoryConfiguration=self._inventory_config(inventory_id, target_bucket_name),
        )
        client.delete_bucket_inventory_configuration(Bucket=bucket_name, Id=inventory_id)
        response = client.list_bucket_inventory_configurations(Bucket=bucket_name)
        assert not response.get("InventoryConfigurationList")

    @pytest.mark.tag("Error")
    def test_get_bucket_inventory_not_exist(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 6)
        inventory_id = "my-inventory"
        self.assert_client_error(
            lambda: client.get_bucket_inventory_configuration(Bucket=bucket_name, Id=inventory_id),
            404,
            md.NO_SUCH_CONFIGURATION,
        )

    @pytest.mark.tag("Error")
    def test_delete_bucket_inventory_not_exist(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 7)
        inventory_id = "my-inventory"
        self.assert_client_error(
            lambda: client.delete_bucket_inventory_configuration(Bucket=bucket_name, Id=inventory_id),
            404,
            md.NO_SUCH_CONFIGURATION,
        )

    @pytest.mark.tag("Error")
    def test_put_bucket_inventory_not_exist(self):
        client = self.get_client()
        bucket_name = self.get_new_bucket_name(8)
        target_bucket_name = self.create_bucket(client, 8)
        inventory_id = "my-inventory"
        self.assert_client_error(
            lambda: client.put_bucket_inventory_configuration(
                Bucket=bucket_name,
                Id=inventory_id,
                InventoryConfiguration=self._inventory_config(inventory_id, target_bucket_name),
            ),
            404,
            md.NO_SUCH_BUCKET,
        )

    @pytest.mark.tag("Error")
    def test_put_bucket_inventory_id_not_exist(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 9)
        target_bucket_name = self.create_bucket(client, 9)
        inventory_id = ""
        self.assert_client_error(
            lambda: client.put_bucket_inventory_configuration(
                Bucket=bucket_name,
                Id=inventory_id,
                InventoryConfiguration=self._inventory_config(inventory_id, target_bucket_name),
            ),
            400,
            md.MALFORMED_XML,
        )

    @pytest.mark.tag("Error")
    def test_put_bucket_inventory_id_duplicate(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 10)
        target_bucket_name = self.create_bucket(client, 10)
        inventory_id = "my-inventory"
        config = self._inventory_config(inventory_id, target_bucket_name)
        client.put_bucket_inventory_configuration(Bucket=bucket_name, Id=inventory_id, InventoryConfiguration=config)
        client.put_bucket_inventory_configuration(Bucket=bucket_name, Id=inventory_id, InventoryConfiguration=config)
        response = client.list_bucket_inventory_configurations(Bucket=bucket_name)
        assert len(response.get("InventoryConfigurationList", [])) == 1

    @pytest.mark.tag("Put")
    def test_put_bucket_inventory_target_not_exist(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 11)
        target_bucket_name = self.get_new_bucket_name(11)
        inventory_id = "my-inventory"
        # AWS 는 타깃 버킷의 존재 여부를 확인하지 않고 설정을 저장한다
        client.put_bucket_inventory_configuration(
            Bucket=bucket_name,
            Id=inventory_id,
            InventoryConfiguration=self._inventory_config(inventory_id, target_bucket_name),
        )
        response = client.get_bucket_inventory_configuration(Bucket=bucket_name, Id=inventory_id)
        assert response["InventoryConfiguration"]["Id"] == inventory_id
        assert (
            response["InventoryConfiguration"]["Destination"]["S3BucketDestination"]["Bucket"]
            == f"arn:aws:s3:::{target_bucket_name}"
        )

    @pytest.mark.tag("Error")
    def test_put_bucket_inventory_invalid_format(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 12)
        target_bucket_name = self.create_bucket(client, 12)
        inventory_id = "my-inventory"
        self.assert_client_error(
            lambda: client.put_bucket_inventory_configuration(
                Bucket=bucket_name,
                Id=inventory_id,
                InventoryConfiguration=self._inventory_config(inventory_id, target_bucket_name, output_format="JSON"),
            ),
            400,
            md.MALFORMED_XML,
        )

    @pytest.mark.tag("Error")
    def test_put_bucket_inventory_invalid_frequency(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 13)
        target_bucket_name = self.create_bucket(client, 13)
        inventory_id = "my-inventory"
        self.assert_client_error(
            lambda: client.put_bucket_inventory_configuration(
                Bucket=bucket_name,
                Id=inventory_id,
                InventoryConfiguration=self._inventory_config(inventory_id, target_bucket_name, frequency="Hourly"),
            ),
            400,
            md.MALFORMED_XML,
        )

    @pytest.mark.tag("Error")
    def test_put_bucket_inventory_invalid_case(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 14)
        target_bucket_name = self.create_bucket(client, 14)
        inventory_id = "my-inventory"
        self.assert_client_error(
            lambda: client.put_bucket_inventory_configuration(
                Bucket=bucket_name,
                Id=inventory_id,
                InventoryConfiguration=self._inventory_config(inventory_id, target_bucket_name, included_object_versions="CUrrENT"),
            ),
            400,
            md.MALFORMED_XML,
        )

    @pytest.mark.tag("Put")
    def test_put_bucket_inventory_prefix(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 15)
        target_bucket_name = self.create_bucket(client, 15)
        inventory_id = "my-inventory"
        inventory_prefix = "a/"
        client.put_bucket_inventory_configuration(
            Bucket=bucket_name,
            Id=inventory_id,
            InventoryConfiguration=self._inventory_config(inventory_id, target_bucket_name, prefix=inventory_prefix),
        )
        result = client.get_bucket_inventory_configuration(Bucket=bucket_name, Id=inventory_id)
        assert result["InventoryConfiguration"]["Id"] == inventory_id
        assert result["InventoryConfiguration"]["Destination"]["S3BucketDestination"]["Prefix"] == inventory_prefix

    @pytest.mark.tag("Put")
    def test_put_bucket_inventory_optional(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 16)
        target_bucket_name = self.create_bucket(client, 16)
        inventory_id = "my-inventory"
        inventory_prefix = "a/"
        optional_fields = ["Size", "LastModifiedDate"]
        client.put_bucket_inventory_configuration(
            Bucket=bucket_name,
            Id=inventory_id,
            InventoryConfiguration=self._inventory_config(
                inventory_id,
                target_bucket_name,
                prefix=inventory_prefix,
                optional_fields=optional_fields,
            ),
        )
        result = client.get_bucket_inventory_configuration(Bucket=bucket_name, Id=inventory_id)
        assert result["InventoryConfiguration"]["Id"] == inventory_id
        assert result["InventoryConfiguration"]["Destination"]["S3BucketDestination"]["Prefix"] == inventory_prefix
        assert result["InventoryConfiguration"]["OptionalFields"] == optional_fields

    @pytest.mark.tag("Error")
    def test_put_bucket_inventory_invalid_optional(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 17)
        target_bucket_name = self.create_bucket(client, 17)
        inventory_id = "my-inventory"
        self.assert_client_error(
            lambda: client.put_bucket_inventory_configuration(
                Bucket=bucket_name,
                Id=inventory_id,
                InventoryConfiguration=self._inventory_config(
                    inventory_id,
                    target_bucket_name,
                    prefix="a/",
                    optional_fields=["SIZE", "--"],
                ),
            ),
            400,
            md.MALFORMED_XML,
        )

    def _put_inventory(self, client, bucket_name: str, inventory_id: str, config: dict):
        return client.put_bucket_inventory_configuration(
            Bucket=bucket_name, Id=inventory_id, InventoryConfiguration=config
        )

    @pytest.mark.tag("Max")
    def test_put_bucket_inventory_id_length(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 18)
        target_bucket_name = self.create_bucket(client, 18)
        # ID 는 64자까지 허용한다
        inventory_id = "a" * 64
        self._put_inventory(client, bucket_name, inventory_id, self._inventory_config(inventory_id, target_bucket_name))
        response = client.get_bucket_inventory_configuration(Bucket=bucket_name, Id=inventory_id)
        assert response["InventoryConfiguration"]["Id"] == inventory_id

        too_long = "a" * 65
        self.assert_client_error(
            lambda: self._put_inventory(
                client, bucket_name, too_long, self._inventory_config(too_long, target_bucket_name)
            ),
            400,
            md.MALFORMED_XML,
        )

    @pytest.mark.tag("Error")
    def test_put_bucket_inventory_invalid_id(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 19)
        target_bucket_name = self.create_bucket(client, 19)
        # ID 에는 영숫자와 - _ . 만 쓸 수 있다
        inventory_id = "a b/c"
        self.assert_client_error(
            lambda: self._put_inventory(
                client, bucket_name, inventory_id, self._inventory_config(inventory_id, target_bucket_name)
            ),
            400,
            md.MALFORMED_XML,
        )

    @pytest.mark.tag("Error")
    def test_put_bucket_inventory_id_mismatch(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 20)
        target_bucket_name = self.create_bucket(client, 20)
        # 쿼리의 id 와 본문의 Id 가 다르면 거부된다
        self.assert_client_error(
            lambda: self._put_inventory(
                client, bucket_name, "my-inventory", self._inventory_config("other-inventory", target_bucket_name)
            ),
            400,
            md.ID_MISMATCH,
        )
        assert not client.list_bucket_inventory_configurations(Bucket=bucket_name).get("InventoryConfigurationList")

    @pytest.mark.tag("Check")
    def test_put_bucket_inventory_disabled(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 21)
        target_bucket_name = self.create_bucket(client, 21)
        inventory_id = "my-inventory"
        config = self._inventory_config(inventory_id, target_bucket_name)
        config["IsEnabled"] = False
        self._put_inventory(client, bucket_name, inventory_id, config)
        response = client.get_bucket_inventory_configuration(Bucket=bucket_name, Id=inventory_id)
        assert response["InventoryConfiguration"]["IsEnabled"] is False

    @pytest.mark.tag("Error")
    def test_put_bucket_inventory_invalid_versions(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 22)
        target_bucket_name = self.create_bucket(client, 22)
        inventory_id = "my-inventory"
        self.assert_client_error(
            lambda: self._put_inventory(
                client,
                bucket_name,
                inventory_id,
                self._inventory_config(inventory_id, target_bucket_name, included_object_versions="Invalid"),
            ),
            400,
            md.MALFORMED_XML,
        )

    @pytest.mark.tag("Check")
    def test_put_bucket_inventory_formats(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 23)
        target_bucket_name = self.create_bucket(client, 23)
        # 형식(CSV/ORC/Parquet), 주기(Daily/Weekly), 대상 버전(All/Current)이 그대로 저장된다
        cases = [
            ("inventory-csv", "CSV", "Daily", "All"),
            ("inventory-orc", "ORC", "Weekly", "Current"),
            ("inventory-parquet", "Parquet", "Weekly", "All"),
        ]
        for inventory_id, output_format, frequency, versions in cases:
            self._put_inventory(
                client,
                bucket_name,
                inventory_id,
                self._inventory_config(
                    inventory_id,
                    target_bucket_name,
                    included_object_versions=versions,
                    frequency=frequency,
                    output_format=output_format,
                ),
            )
        for inventory_id, output_format, frequency, versions in cases:
            config = client.get_bucket_inventory_configuration(Bucket=bucket_name, Id=inventory_id)[
                "InventoryConfiguration"
            ]
            assert config["Destination"]["S3BucketDestination"]["Format"] == output_format
            assert config["Schedule"]["Frequency"] == frequency
            assert config["IncludedObjectVersions"] == versions

    @pytest.mark.tag("Check")
    def test_put_bucket_inventory_destination(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 24)
        target_bucket_name = self.create_bucket(client, 24)
        inventory_id = "my-inventory"
        # 대상의 접두어와 SSE-S3 암호화 설정이 그대로 저장된다
        config = self._inventory_config(inventory_id, target_bucket_name, prefix="inventory/")
        config["Destination"]["S3BucketDestination"]["Encryption"] = {"SSES3": {}}
        self._put_inventory(client, bucket_name, inventory_id, config)

        destination = client.get_bucket_inventory_configuration(Bucket=bucket_name, Id=inventory_id)[
            "InventoryConfiguration"
        ]["Destination"]["S3BucketDestination"]
        assert destination["Bucket"] == f"arn:aws:s3:::{target_bucket_name}"
        assert destination["Prefix"] == "inventory/"
        assert "SSES3" in destination["Encryption"]

    @pytest.mark.tag("Error")
    def test_put_bucket_inventory_invalid_destination_arn(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 25)
        target_bucket_name = self.create_bucket(client, 25)
        inventory_id = "my-inventory"
        # 대상 버킷은 ARN 형식이어야 한다
        config = self._inventory_config(inventory_id, target_bucket_name)
        config["Destination"]["S3BucketDestination"]["Bucket"] = target_bucket_name
        self.assert_client_error(
            lambda: self._put_inventory(client, bucket_name, inventory_id, config),
            400,
            md.INVALID_S3_DESTINATION_BUCKET,
        )

    @pytest.mark.tag("List")
    def test_list_bucket_inventory_pagination(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 26)
        target_bucket_name = self.create_bucket(client, 26)
        ids = [f"inventory-{i:03d}" for i in range(101)]
        for inventory_id in ids:
            self._put_inventory(
                client, bucket_name, inventory_id, self._inventory_config(inventory_id, target_bucket_name)
            )

        # 한 번에 100개까지만 돌려주고 나머지는 토큰으로 이어서 받는다
        response = client.list_bucket_inventory_configurations(Bucket=bucket_name)
        assert len(response["InventoryConfigurationList"]) == 100
        assert response["IsTruncated"] is True
        listed = [config["Id"] for config in response["InventoryConfigurationList"]]

        response = client.list_bucket_inventory_configurations(
            Bucket=bucket_name, ContinuationToken=response["NextContinuationToken"]
        )
        assert len(response["InventoryConfigurationList"]) == 1
        assert response["IsTruncated"] is False
        listed += [config["Id"] for config in response["InventoryConfigurationList"]]
        assert sorted(listed) == ids

    @pytest.mark.tag("List")
    def test_list_bucket_inventory_invalid_token(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 27)
        self.assert_client_error(
            lambda: client.list_bucket_inventory_configurations(Bucket=bucket_name, ContinuationToken="invalid"),
            400,
            md.MALFORMED_CONTINUATION_TOKEN,
        )

    @pytest.mark.tag("Error")
    def test_bucket_inventory_bucket_not_exist(self):
        client = self.get_client()
        bucket_name = self.get_new_bucket_name_only(28)
        self.assert_client_error(
            lambda: client.list_bucket_inventory_configurations(Bucket=bucket_name), 404, md.NO_SUCH_BUCKET
        )
        self.assert_client_error(
            lambda: client.get_bucket_inventory_configuration(Bucket=bucket_name, Id="my-inventory"),
            404,
            md.NO_SUCH_BUCKET,
        )
        self.assert_client_error(
            lambda: client.delete_bucket_inventory_configuration(Bucket=bucket_name, Id="my-inventory"),
            404,
            md.NO_SUCH_BUCKET,
        )
