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

    @pytest.mark.tag("Error")
    def test_put_bucket_inventory_target_not_exist(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 11)
        target_bucket_name = self.get_new_bucket_name(11)
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
