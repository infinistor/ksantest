"""Policy tests ported from Java testV2/Policy.java."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest
from botocore.exceptions import ClientError

from s3tests.data import main_data as md
from s3tests.test_base import S3TestBase
from s3tests.utils import utils


class TestPolicy(S3TestBase):
    @pytest.mark.tag("Check")
    def test_bucket_policy(self):
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client)
        key = "asdf"
        client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8"))

        resource1 = f"arn:aws:s3:::{bucket_name}"
        resource2 = f"arn:aws:s3:::{bucket_name}/*"
        policy_document = {
            md.POLICY_VERSION: md.POLICY_VERSION_DATE,
            md.POLICY_STATEMENT: [
                {
                    md.POLICY_EFFECT: md.POLICY_EFFECT_ALLOW,
                    md.POLICY_PRINCIPAL: "*",
                    md.POLICY_ACTION: "s3:ListBucket",
                    md.POLICY_RESOURCE: [resource1, resource2],
                }
            ],
        }
        policy_str = json.dumps(policy_document, separators=(",", ":"))
        client.put_bucket_policy(Bucket=bucket_name, Policy=policy_str)

        alt_client = self.get_alt_client()
        response = alt_client.list_objects(Bucket=bucket_name)
        assert len(response.get("Contents", [])) == 1

        get_response = client.get_bucket_policy(Bucket=bucket_name)
        assert get_response["Policy"] == policy_str

    @pytest.mark.tag("Check")
    def test_bucket_v2_policy(self):
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client)
        key = "asdf"
        client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8"))

        resource1 = f"arn:aws:s3:::{bucket_name}"
        resource2 = f"arn:aws:s3:::{bucket_name}/*"
        policy_document = {
            md.POLICY_VERSION: md.POLICY_VERSION_DATE,
            md.POLICY_STATEMENT: [
                {
                    md.POLICY_EFFECT: md.POLICY_EFFECT_ALLOW,
                    md.POLICY_PRINCIPAL: "*",
                    md.POLICY_ACTION: "s3:ListBucket",
                    md.POLICY_RESOURCE: [resource1, resource2],
                }
            ],
        }
        policy_str = json.dumps(policy_document, separators=(",", ":"))
        client.put_bucket_policy(Bucket=bucket_name, Policy=policy_str)

        alt_client = self.get_alt_client()
        response = alt_client.list_objects_v2(Bucket=bucket_name)
        assert len(response.get("Contents", [])) == 1

        get_response = client.get_bucket_policy(Bucket=bucket_name)
        assert get_response["Policy"] == policy_str

    @pytest.mark.tag("Priority")
    def test_bucket_policy_acl(self):
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client)
        key = "asdf"
        client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8"))

        resource1 = f"arn:aws:s3:::{bucket_name}"
        resource2 = f"arn:aws:s3:::{bucket_name}/*"
        policy_document = {
            md.POLICY_VERSION: md.POLICY_VERSION_DATE,
            md.POLICY_STATEMENT: [
                {
                    md.POLICY_EFFECT: md.POLICY_EFFECT_DENY,
                    md.POLICY_PRINCIPAL: {"AWS": "*"},
                    md.POLICY_ACTION: "s3:ListBucket",
                    md.POLICY_RESOURCE: [resource1, resource2],
                }
            ],
        }
        policy_str = json.dumps(policy_document, separators=(",", ":"))

        client.put_bucket_acl(Bucket=bucket_name, ACL="authenticated-read")
        client.put_bucket_policy(Bucket=bucket_name, Policy=policy_str)

        alt_client = self.get_alt_client()
        self.assert_client_error(
            lambda: alt_client.get_object(Bucket=bucket_name, Key=key),
            403,
            md.ACCESS_DENIED,
        )

        client.delete_bucket_policy(Bucket=bucket_name)
        client.put_bucket_acl(Bucket=bucket_name, ACL="public-read")

    @pytest.mark.tag("Priority")
    def test_bucket_v2_policy_acl(self):
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client)
        key = "asdf"
        client.put_object(Bucket=bucket_name, Key=key, Body=key.encode("utf-8"))

        resource1 = f"arn:aws:s3:::{bucket_name}"
        resource2 = f"arn:aws:s3:::{bucket_name}/*"
        policy_document = {
            md.POLICY_VERSION: md.POLICY_VERSION_DATE,
            md.POLICY_STATEMENT: [
                {
                    md.POLICY_EFFECT: md.POLICY_EFFECT_DENY,
                    md.POLICY_PRINCIPAL: {"AWS": "*"},
                    md.POLICY_ACTION: "s3:ListBucket",
                    md.POLICY_RESOURCE: [resource1, resource2],
                }
            ],
        }
        policy_str = json.dumps(policy_document, separators=(",", ":"))

        client.put_bucket_acl(Bucket=bucket_name, ACL="authenticated-read")
        client.put_bucket_policy(Bucket=bucket_name, Policy=policy_str)

        alt_client = self.get_alt_client()
        self.assert_client_error(
            lambda: alt_client.list_objects_v2(Bucket=bucket_name),
            403,
            md.ACCESS_DENIED,
        )

        client.delete_bucket_policy(Bucket=bucket_name)
        client.put_bucket_acl(Bucket=bucket_name, ACL="public-read")

    @pytest.mark.tag("Tagging")
    def test_get_tags_acl_public(self):
        key = "acl"
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client)
        self.create_key_with_random_content(client, key, 0, bucket_name)

        resource = utils.make_arn_resource(f"{bucket_name}/{key}")
        policy_document = self.make_json_policy("s3:GetObjectTagging", resource)
        client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)

        input_tag_set = self.make_simple_tag_set(10)
        client.put_object_tagging(
            Bucket=bucket_name,
            Key=key,
            Tagging={"TagSet": input_tag_set},
        )

        alt_client = self.get_alt_client()
        get_response = alt_client.get_object_tagging(Bucket=bucket_name, Key=key)
        self.tag_compare(input_tag_set, get_response["TagSet"])

    @pytest.mark.tag("Tagging")
    def test_put_tags_acl_public(self):
        key = "acl"
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client)
        self.create_key_with_random_content(client, key, 0, bucket_name)

        resource = utils.make_arn_resource(f"{bucket_name}/{key}")
        policy_document = self.make_json_policy("s3:PutObjectTagging", resource)
        client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)

        input_tag_set = self.make_simple_tag_set(10)
        alt_client = self.get_alt_client()
        alt_client.put_object_tagging(
            Bucket=bucket_name,
            Key=key,
            Tagging={"TagSet": input_tag_set},
        )

        get_response = client.get_object_tagging(Bucket=bucket_name, Key=key)
        self.tag_compare(input_tag_set, get_response["TagSet"])

    @pytest.mark.tag("Tagging")
    def test_delete_tags_obj_public(self):
        key = "acl"
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client)
        self.create_key_with_random_content(client, key, 0, bucket_name)

        resource = utils.make_arn_resource(f"{bucket_name}/{key}")
        policy_document = self.make_json_policy("s3:DeleteObjectTagging", resource)
        client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)

        input_tag_set = self.make_simple_tag_set(10)
        client.put_object_tagging(
            Bucket=bucket_name,
            Key=key,
            Tagging={"TagSet": input_tag_set},
        )

        alt_client = self.get_alt_client()
        alt_client.delete_object_tagging(Bucket=bucket_name, Key=key)

        get_response = client.get_object_tagging(Bucket=bucket_name, Key=key)
        assert len(get_response["TagSet"]) == 0

    @pytest.mark.tag("TagOptions")
    def test_bucket_policy_get_obj_existing_tag(self):
        public_tag = "publicTag"
        private_tag = "privateTag"
        invalid_tag = "invalidTag"
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client)
        self.create_objects(client, bucket_name, [public_tag, private_tag, invalid_tag])

        tag_conditional = {
            "StringEquals": {"s3:ExistingObjectTag/security": "public"}
        }
        resource = utils.make_arn_resource(f"{bucket_name}/*")
        policy_document = self.make_json_policy("s3:GetObject", resource, None, tag_conditional)
        client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)

        client.put_object_tagging(
            Bucket=bucket_name,
            Key=public_tag,
            Tagging={
                "TagSet": [
                    {"Key": "security", "Value": "public"},
                    {"Key": "foo", "Value": "bar"},
                ]
            },
        )
        client.put_object_tagging(
            Bucket=bucket_name,
            Key=private_tag,
            Tagging={"TagSet": [{"Key": "security", "Value": "private"}]},
        )
        client.put_object_tagging(
            Bucket=bucket_name,
            Key=invalid_tag,
            Tagging={"TagSet": [{"Key": "security1", "Value": "public"}]},
        )

        alt_client = self.get_alt_client()
        alt_client.get_object(Bucket=bucket_name, Key=public_tag)

        self.assert_client_error(
            lambda: alt_client.get_object(Bucket=bucket_name, Key=private_tag),
            403,
            md.ACCESS_DENIED,
        )
        self.assert_client_error(
            lambda: alt_client.get_object(Bucket=bucket_name, Key=invalid_tag),
            403,
            md.ACCESS_DENIED,
        )

    @pytest.mark.tag("TagOptions")
    def test_bucket_policy_get_obj_tagging_existing_tag(self):
        public_tag = "publicTag"
        private_tag = "privateTag"
        invalid_tag = "invalidTag"
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client)
        self.create_objects(client, bucket_name, [public_tag, private_tag, invalid_tag])

        tag_conditional = {
            "StringEquals": {"s3:ExistingObjectTag/security": "public"}
        }
        resource = utils.make_arn_resource(f"{bucket_name}/*")
        policy_document = self.make_json_policy("s3:GetObjectTagging", resource, None, tag_conditional)
        client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)

        client.put_object_tagging(
            Bucket=bucket_name,
            Key=public_tag,
            Tagging={
                "TagSet": [
                    {"Key": "security", "Value": "public"},
                    {"Key": "foo", "Value": "bar"},
                ]
            },
        )
        client.put_object_tagging(
            Bucket=bucket_name,
            Key=private_tag,
            Tagging={"TagSet": [{"Key": "security", "Value": "private"}]},
        )
        client.put_object_tagging(
            Bucket=bucket_name,
            Key=invalid_tag,
            Tagging={"TagSet": [{"Key": "security1", "Value": "public"}]},
        )

        alt_client = self.get_alt_client()
        alt_client.get_object_tagging(Bucket=bucket_name, Key=public_tag)

        self.assert_client_error(
            lambda: alt_client.get_object(Bucket=bucket_name, Key=public_tag),
            403,
            md.ACCESS_DENIED,
        )
        self.assert_client_error(
            lambda: alt_client.get_object_tagging(Bucket=bucket_name, Key=private_tag),
            403,
            md.ACCESS_DENIED,
        )
        self.assert_client_error(
            lambda: alt_client.get_object_tagging(Bucket=bucket_name, Key=invalid_tag),
            403,
            md.ACCESS_DENIED,
        )

    @pytest.mark.tag("TagOptions")
    def test_bucket_policy_put_obj_tagging_existing_tag(self):
        public_tag = "publicTag"
        private_tag = "privateTag"
        invalid_tag = "invalidTag"
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client)
        self.create_objects(client, bucket_name, [public_tag, private_tag, invalid_tag])

        tag_conditional = {
            "StringEquals": {"s3:ExistingObjectTag/security": "public"}
        }
        resource = utils.make_arn_resource(f"{bucket_name}/*")
        policy_document = self.make_json_policy("s3:PutObjectTagging", resource, None, tag_conditional)
        client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)

        client.put_object_tagging(
            Bucket=bucket_name,
            Key=public_tag,
            Tagging={
                "TagSet": [
                    {"Key": "security", "Value": "public"},
                    {"Key": "foo", "Value": "bar"},
                ]
            },
        )
        client.put_object_tagging(
            Bucket=bucket_name,
            Key=private_tag,
            Tagging={"TagSet": [{"Key": "security", "Value": "private"}]},
        )
        client.put_object_tagging(
            Bucket=bucket_name,
            Key=invalid_tag,
            Tagging={"TagSet": [{"Key": "security1", "Value": "public"}]},
        )

        test_tag_set = [
            {"Key": "security", "Value": "public"},
            {"Key": "foo", "Value": "bar"},
        ]
        alt_client = self.get_alt_client()
        alt_client.put_object_tagging(
            Bucket=bucket_name,
            Key=public_tag,
            Tagging={"TagSet": test_tag_set},
        )

        self.assert_client_error(
            lambda: alt_client.put_object_tagging(
                Bucket=bucket_name,
                Key=private_tag,
                Tagging={"TagSet": test_tag_set},
            ),
            403,
            md.ACCESS_DENIED,
        )

        alt_client.put_object_tagging(
            Bucket=bucket_name,
            Key=public_tag,
            Tagging={"TagSet": [{"Key": "security", "Value": "private"}]},
        )
        self.assert_client_error(
            lambda: alt_client.put_object_tagging(
                Bucket=bucket_name,
                Key=invalid_tag,
                Tagging={"TagSet": test_tag_set},
            ),
            403,
            md.ACCESS_DENIED,
        )

    @pytest.mark.tag("PathOptions")
    def test_bucket_policy_put_obj_copy_source(self):
        public_foo = "public/foo"
        public_bar = "public/bar"
        private_foo = "private/foo"
        client = self.get_client()
        alt_client = self.get_alt_client()
        source_bucket_name = self.create_bucket_canned_acl(client)
        target_bucket_name = self.create_bucket_canned_acl(client)

        self.create_objects(client, source_bucket_name, [public_foo, public_bar, private_foo])

        source_resource = utils.make_arn_resource(f"{source_bucket_name}/*")
        policy_document = self.make_json_policy("s3:GetObject", source_resource)
        client.put_bucket_policy(Bucket=source_bucket_name, Policy=policy_document)

        tag_conditional = {
            "StringLike": {"s3:x-amz-copy-source": f"{source_bucket_name}/public/*"}
        }
        resource = utils.make_arn_resource(f"{target_bucket_name}/*")
        policy_document2 = self.make_json_policy("s3:PutObject", resource, None, tag_conditional)
        client.put_bucket_policy(Bucket=target_bucket_name, Policy=policy_document2)

        new_foo = "newFoo"
        alt_client.copy_object(
            CopySource={"Bucket": source_bucket_name, "Key": public_foo},
            Bucket=target_bucket_name,
            Key=new_foo,
        )
        response = alt_client.get_object(Bucket=target_bucket_name, Key=new_foo)
        assert self.get_body(response) == public_foo

        new_foo2 = "newFoo2"
        alt_client.copy_object(
            CopySource={"Bucket": source_bucket_name, "Key": public_bar},
            Bucket=target_bucket_name,
            Key=new_foo2,
        )
        response = alt_client.get_object(Bucket=target_bucket_name, Key=new_foo2)
        assert self.get_body(response) == public_bar

        with pytest.raises(ClientError):
            alt_client.copy_object(
                CopySource={"Bucket": source_bucket_name, "Key": private_foo},
                Bucket=target_bucket_name,
                Key=new_foo2,
            )

    @pytest.mark.tag("MetadataOptions")
    def test_bucket_policy_put_obj_copy_source_meta(self):
        public_foo = "public/foo"
        public_bar = "public/bar"
        client = self.get_client()
        source_bucket_name = self.create_bucket_canned_acl(client)
        self.create_objects(client, source_bucket_name, [public_foo, public_bar])

        source_resource = utils.make_arn_resource(f"{source_bucket_name}/*")
        policy_document = self.make_json_policy("s3:GetObject", source_resource)
        client.put_bucket_policy(Bucket=source_bucket_name, Policy=policy_document)

        target_bucket_name = self.create_bucket_canned_acl(client)
        s3_conditional = {"StringEquals": {"s3:x-amz-metadata-directive": "COPY"}}
        resource = utils.make_arn_resource(f"{target_bucket_name}/*")
        policy_document2 = self.make_json_policy("s3:PutObject", resource, None, s3_conditional)
        client.put_bucket_policy(Bucket=target_bucket_name, Policy=policy_document2)

        alt_client = self.get_alt_client()
        new_foo = "newFoo"
        alt_client.copy_object(
            CopySource={"Bucket": source_bucket_name, "Key": public_foo},
            Bucket=target_bucket_name,
            Key=new_foo,
            MetadataDirective="COPY",
        )
        response = alt_client.get_object(Bucket=target_bucket_name, Key=new_foo)
        assert self.get_body(response) == public_foo

        new_foo2 = "newFoo2"
        with pytest.raises(ClientError):
            alt_client.copy_object(
                CopySource={"Bucket": source_bucket_name, "Key": public_bar},
                Bucket=target_bucket_name,
                Key=new_foo2,
            )
        with pytest.raises(ClientError):
            alt_client.copy_object(
                CopySource={"Bucket": source_bucket_name, "Key": public_bar},
                Bucket=target_bucket_name,
                Key=new_foo2,
                MetadataDirective="REPLACE",
            )

    @pytest.mark.tag("ACLOptions")
    def test_bucket_policy_put_obj_acl(self):
        client = self.get_client()
        alt_client = self.get_alt_client()
        bucket_name = self.create_bucket_canned_acl(client)

        tag_conditional = {"StringLike": {"s3:x-amz-acl": "public*"}}
        resource = utils.make_arn_resource(f"{bucket_name}/*")
        s1 = self.make_json_statement("s3:PutObject", resource)
        s2 = self.make_json_statement(
            "s3:PutObject",
            resource,
            md.POLICY_EFFECT_DENY,
            None,
            tag_conditional,
        )
        policy_document = self.make_json_policy_statements(s1, s2)
        client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)

        key1 = "private-key"
        alt_client.put_object(Bucket=bucket_name, Key=key1, Body=key1.encode("utf-8"))

        key2 = "public-key"
        self.assert_client_error(
            lambda: alt_client.put_object(
                Bucket=bucket_name,
                Key=key2,
                Body=key2.encode("utf-8"),
                ACL="public-read",
            ),
            403,
            md.ACCESS_DENIED,
        )

    @pytest.mark.tag("GrantOptions")
    def test_bucket_policy_put_obj_grant(self):
        client = self.get_client()
        bucket_name1 = self.create_bucket_canned_acl(client)
        bucket_name2 = self.create_bucket_canned_acl(client)

        main_user_id = self.config.main_user.id
        alt_user_id = self.config.alt_user.id
        owner_id = f"id={main_user_id}"

        s3_conditional = {"StringEquals": {"s3:x-amz-grant-full-control": owner_id}}
        resource = utils.make_arn_resource(f"{bucket_name1}/*")
        policy_document = self.make_json_policy("s3:PutObject", resource, None, s3_conditional)

        resource2 = utils.make_arn_resource(f"{bucket_name2}/*")
        policy_document2 = self.make_json_policy("s3:PutObject", resource2)
        client.put_bucket_policy(Bucket=bucket_name1, Policy=policy_document)
        client.put_bucket_policy(Bucket=bucket_name2, Policy=policy_document2)

        alt_client = self.get_alt_client()
        key1 = "key1"
        alt_client.put_object(
            Bucket=bucket_name1,
            Key=key1,
            Body=key1.encode("utf-8"),
            GrantFullControl=owner_id,
        )

        key2 = "key2"
        alt_client.put_object(Bucket=bucket_name2, Key=key2, Body=key2.encode("utf-8"))

        acl1_response = client.get_object_acl(Bucket=bucket_name1, Key=key1)
        with pytest.raises(ClientError):
            client.get_object_acl(Bucket=bucket_name1, Key=key2)

        acl2_response = alt_client.get_object_acl(Bucket=bucket_name2, Key=key2)
        assert acl1_response["Grants"][0]["Grantee"]["ID"] == main_user_id
        assert acl2_response["Grants"][0]["Grantee"]["ID"] == alt_user_id

    @pytest.mark.tag("TagOptions")
    def test_bucket_policy_get_obj_acl_existing_tag(self):
        public_tag = "publicTag"
        private_tag = "privateTag"
        invalid_tag = "invalidTag"
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client)
        self.create_objects(client, bucket_name, [public_tag, private_tag, invalid_tag])

        tag_conditional = {
            "StringEquals": {"s3:ExistingObjectTag/security": "public"}
        }
        resource = utils.make_arn_resource(f"{bucket_name}/*")
        policy_document = self.make_json_policy("s3:GetObjectAcl", resource, None, tag_conditional)
        client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)

        client.put_object_tagging(
            Bucket=bucket_name,
            Key=public_tag,
            Tagging={
                "TagSet": [
                    {"Key": "security", "Value": "public"},
                    {"Key": "foo", "Value": "bar"},
                ]
            },
        )
        client.put_object_tagging(
            Bucket=bucket_name,
            Key=private_tag,
            Tagging={"TagSet": [{"Key": "security", "Value": "private"}]},
        )
        client.put_object_tagging(
            Bucket=bucket_name,
            Key=invalid_tag,
            Tagging={"TagSet": [{"Key": "security1", "Value": "public"}]},
        )

        alt_client = self.get_alt_client()
        alt_client.get_object_acl(Bucket=bucket_name, Key=public_tag)

        self.assert_client_error(
            lambda: alt_client.get_object(Bucket=bucket_name, Key=public_tag),
            403,
            md.ACCESS_DENIED,
        )
        self.assert_client_error(
            lambda: alt_client.get_object_tagging(Bucket=bucket_name, Key=private_tag),
            403,
            md.ACCESS_DENIED,
        )
        self.assert_client_error(
            lambda: alt_client.get_object_tagging(Bucket=bucket_name, Key=invalid_tag),
            403,
            md.ACCESS_DENIED,
        )

    @pytest.mark.tag("Status")
    def test_bucket_policy_status_with_all_user(self):
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client)

        with pytest.raises(ClientError):
            client.get_bucket_policy_status(Bucket=bucket_name)

        resource1 = f"{md.POLICY_RESOURCE_PREFIX}{bucket_name}"
        resource2 = f"{md.POLICY_RESOURCE_PREFIX}{bucket_name}/*"
        policy_document = {
            md.POLICY_VERSION: md.POLICY_VERSION_DATE,
            md.POLICY_STATEMENT: [
                {
                    md.POLICY_EFFECT: md.POLICY_EFFECT_ALLOW,
                    md.POLICY_PRINCIPAL: {"AWS": "*"},
                    md.POLICY_ACTION: "s3:ListBucket",
                    md.POLICY_RESOURCE: [resource1, resource2],
                }
            ],
        }
        policy_str = json.dumps(policy_document, separators=(",", ":"))
        client.put_bucket_policy(Bucket=bucket_name, Policy=policy_str)

        response = client.get_bucket_policy_status(Bucket=bucket_name)
        assert response["PolicyStatus"]["IsPublic"] is True

    @pytest.mark.tag("Status")
    def test_bucket_policy_status_with_specific_user_access(self):
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client)

        with pytest.raises(ClientError):
            client.get_bucket_policy_status(Bucket=bucket_name)

        resource1 = f"{md.POLICY_RESOURCE_PREFIX}{bucket_name}"
        resource2 = f"{md.POLICY_RESOURCE_PREFIX}{bucket_name}/*"
        policy_document = {
            md.POLICY_VERSION: md.POLICY_VERSION_DATE,
            md.POLICY_STATEMENT: [
                {
                    md.POLICY_EFFECT: md.POLICY_EFFECT_ALLOW,
                    md.POLICY_PRINCIPAL: {"CanonicalUser": self.config.main_user.id},
                    md.POLICY_ACTION: "s3:ListBucket",
                    md.POLICY_RESOURCE: [resource1, resource2],
                }
            ],
        }
        policy_str = json.dumps(policy_document, separators=(",", ":"))
        client.put_bucket_policy(Bucket=bucket_name, Policy=policy_str)

        response = client.get_bucket_policy_status(Bucket=bucket_name)
        assert response["PolicyStatus"]["IsPublic"] is False

    @pytest.mark.tag("Status")
    def test_bucket_policy_status_with_wide_ip_range(self):
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client)

        policy_document = {
            md.POLICY_VERSION: md.POLICY_VERSION_DATE,
            md.POLICY_STATEMENT: [
                {
                    md.POLICY_EFFECT: md.POLICY_EFFECT_ALLOW,
                    md.POLICY_PRINCIPAL: {"AWS": "*"},
                    md.POLICY_ACTION: "s3:GetObject",
                    md.POLICY_RESOURCE: utils.make_arn_resource(f"{bucket_name}/*"),
                    md.POLICY_CONDITION: {
                        "IpAddress": {"aws:SourceIp": "0.0.0.0/1"}
                    },
                }
            ],
        }
        policy_str = json.dumps(policy_document, separators=(",", ":"))
        client.put_bucket_policy(Bucket=bucket_name, Policy=policy_str)

        response = client.get_bucket_policy_status(Bucket=bucket_name)
        assert response["PolicyStatus"]["IsPublic"] is True

    @pytest.mark.tag("Status")
    def test_bucket_policy_status_with_ip_range(self):
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client)

        policy_document = {
            md.POLICY_VERSION: md.POLICY_VERSION_DATE,
            md.POLICY_STATEMENT: [
                {
                    md.POLICY_EFFECT: md.POLICY_EFFECT_ALLOW,
                    md.POLICY_PRINCIPAL: {"AWS": "*"},
                    md.POLICY_ACTION: "s3:GetObject",
                    md.POLICY_RESOURCE: [utils.make_arn_resource(f"{bucket_name}/*")],
                    md.POLICY_CONDITION: {
                        "IpAddress": {"aws:SourceIp": "192.168.1.0/24"}
                    },
                }
            ],
        }
        policy_str = json.dumps(policy_document, separators=(",", ":"))
        client.put_bucket_policy(Bucket=bucket_name, Policy=policy_str)

        response = client.get_bucket_policy_status(Bucket=bucket_name)
        assert response["PolicyStatus"]["IsPublic"] is False

    @pytest.mark.tag("Status")
    def test_bucket_policy_status_with_time_condition(self):
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client)
        resource = utils.make_arn_resource(f"{bucket_name}/*")

        now = datetime.now(timezone.utc)
        start_time = (now + timedelta(minutes=10)).isoformat()
        end_time = (now + timedelta(minutes=10, seconds=1)).isoformat()

        policy_document = {
            md.POLICY_VERSION: md.POLICY_VERSION_DATE,
            md.POLICY_STATEMENT: [
                {
                    md.POLICY_EFFECT: md.POLICY_EFFECT_ALLOW,
                    md.POLICY_PRINCIPAL: {"AWS": "*"},
                    md.POLICY_ACTION: "s3:GetObject",
                    md.POLICY_RESOURCE: resource,
                    md.POLICY_CONDITION: {
                        "DateGreaterThan": {"aws:CurrentTime": start_time},
                        "DateLessThan": {"aws:CurrentTime": end_time},
                    },
                }
            ],
        }
        policy_str = json.dumps(policy_document, separators=(",", ":"))
        client.put_bucket_policy(Bucket=bucket_name, Policy=policy_str)

        response = client.get_bucket_policy_status(Bucket=bucket_name)
        assert response["PolicyStatus"]["IsPublic"] is True

    @pytest.mark.tag("Status")
    def test_bucket_policy_status_with_tag_condition(self):
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client)
        resource = utils.make_arn_resource(f"{bucket_name}/*")

        policy_document = {
            md.POLICY_VERSION: md.POLICY_VERSION_DATE,
            md.POLICY_STATEMENT: [
                {
                    md.POLICY_EFFECT: md.POLICY_EFFECT_ALLOW,
                    md.POLICY_PRINCIPAL: {"AWS": "*"},
                    md.POLICY_ACTION: "s3:GetObject",
                    md.POLICY_RESOURCE: resource,
                    md.POLICY_CONDITION: {
                        "StringEquals": {"s3:ExistingObjectTag/access": "restricted"}
                    },
                }
            ],
        }
        policy_str = json.dumps(policy_document, separators=(",", ":"))
        client.put_bucket_policy(Bucket=bucket_name, Policy=policy_str)

        response = client.get_bucket_policy_status(Bucket=bucket_name)
        assert response["PolicyStatus"]["IsPublic"] is True
