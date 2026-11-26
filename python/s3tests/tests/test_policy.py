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
        bucket_name = self.create_bucket_canned_acl(client, 1)
        key = "test_bucket_policy_key"
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
        bucket_name = self.create_bucket_canned_acl(client, 2)
        key = "test_bucket_v2_policy_key"
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
        bucket_name = self.create_bucket_canned_acl(client, 3)
        key = "test_bucket_policy_acl_key"
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
        bucket_name = self.create_bucket_canned_acl(client, 4)
        key = "test_bucket_v2_policy_acl_key"
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
        key = "test_get_tags_acl_public_key"
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client, 5)
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
        key = "test_put_tags_acl_public_key"
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client, 6)
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
        key = "test_delete_tags_obj_public_key"
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client, 7)
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
        bucket_name = self.create_bucket_canned_acl(client, 8)
        self.create_objects(client, bucket_name, [public_tag, private_tag, invalid_tag])

        tag_conditional = {"StringEquals": {"s3:ExistingObjectTag/security": "public"}}
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
        bucket_name = self.create_bucket_canned_acl(client, 9)
        self.create_objects(client, bucket_name, [public_tag, private_tag, invalid_tag])

        tag_conditional = {"StringEquals": {"s3:ExistingObjectTag/security": "public"}}
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
        bucket_name = self.create_bucket_canned_acl(client, 10)
        self.create_objects(client, bucket_name, [public_tag, private_tag, invalid_tag])

        tag_conditional = {"StringEquals": {"s3:ExistingObjectTag/security": "public"}}
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
        source_bucket_name = self.create_bucket_canned_acl(client, 11)
        target_bucket_name = self.create_bucket_canned_acl(client, 11)
        self.create_objects(client, source_bucket_name, [public_foo, public_bar, private_foo])

        source_resource = utils.make_arn_resource(f"{source_bucket_name}/*")
        policy_document = self.make_json_policy("s3:GetObject", source_resource)
        client.put_bucket_policy(Bucket=source_bucket_name, Policy=policy_document)

        tag_conditional = {"StringLike": {"s3:x-amz-copy-source": f"{source_bucket_name}/public/*"}}
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
        source_bucket_name = self.create_bucket_canned_acl(client, 12)
        self.create_objects(client, source_bucket_name, [public_foo, public_bar])

        source_resource = utils.make_arn_resource(f"{source_bucket_name}/*")
        policy_document = self.make_json_policy("s3:GetObject", source_resource)
        client.put_bucket_policy(Bucket=source_bucket_name, Policy=policy_document)

        target_bucket_name = self.create_bucket_canned_acl(client, 12)
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
        bucket_name = self.create_bucket_canned_acl(client, 13)

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

        key1 = "test_bucket_policy_put_obj_acl_private"
        alt_client.put_object(Bucket=bucket_name, Key=key1, Body=key1.encode("utf-8"))

        key2 = "test_bucket_policy_put_obj_acl_public"
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
        bucket_name1 = self.create_bucket_canned_acl(client, 14)
        bucket_name2 = self.create_bucket_canned_acl(client, 14)

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
        key1 = "test_bucket_policy_put_obj_grant_private"
        alt_client.put_object(
            Bucket=bucket_name1,
            Key=key1,
            Body=key1.encode("utf-8"),
            GrantFullControl=owner_id,
        )

        key2 = "test_bucket_policy_put_obj_grant_public"
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
        bucket_name = self.create_bucket_canned_acl(client, 15)
        self.create_objects(client, bucket_name, [public_tag, private_tag, invalid_tag])

        tag_conditional = {"StringEquals": {"s3:ExistingObjectTag/security": "public"}}
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
        bucket_name = self.create_bucket_canned_acl(client, 16)

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
        bucket_name = self.create_bucket_canned_acl(client, 17)

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
        bucket_name = self.create_bucket_canned_acl(client, 18)

        policy_document = {
            md.POLICY_VERSION: md.POLICY_VERSION_DATE,
            md.POLICY_STATEMENT: [
                {
                    md.POLICY_EFFECT: md.POLICY_EFFECT_ALLOW,
                    md.POLICY_PRINCIPAL: {"AWS": "*"},
                    md.POLICY_ACTION: "s3:GetObject",
                    md.POLICY_RESOURCE: utils.make_arn_resource(f"{bucket_name}/*"),
                    md.POLICY_CONDITION: {"IpAddress": {"aws:SourceIp": "0.0.0.0/1"}},
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
        bucket_name = self.create_bucket_canned_acl(client, 19)

        policy_document = {
            md.POLICY_VERSION: md.POLICY_VERSION_DATE,
            md.POLICY_STATEMENT: [
                {
                    md.POLICY_EFFECT: md.POLICY_EFFECT_ALLOW,
                    md.POLICY_PRINCIPAL: {"AWS": "*"},
                    md.POLICY_ACTION: "s3:GetObject",
                    md.POLICY_RESOURCE: [utils.make_arn_resource(f"{bucket_name}/*")],
                    md.POLICY_CONDITION: {"IpAddress": {"aws:SourceIp": "192.168.1.0/24"}},
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
        bucket_name = self.create_bucket_canned_acl(client, 20)
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
        bucket_name = self.create_bucket_canned_acl(client, 21)
        resource = utils.make_arn_resource(f"{bucket_name}/*")

        policy_document = {
            md.POLICY_VERSION: md.POLICY_VERSION_DATE,
            md.POLICY_STATEMENT: [
                {
                    md.POLICY_EFFECT: md.POLICY_EFFECT_ALLOW,
                    md.POLICY_PRINCIPAL: {"AWS": "*"},
                    md.POLICY_ACTION: "s3:GetObject",
                    md.POLICY_RESOURCE: resource,
                    md.POLICY_CONDITION: {"StringEquals": {"s3:ExistingObjectTag/access": "restricted"}},
                }
            ],
        }
        policy_str = json.dumps(policy_document, separators=(",", ":"))
        client.put_bucket_policy(Bucket=bucket_name, Policy=policy_str)

        response = client.get_bucket_policy_status(Bucket=bucket_name)
        assert response["PolicyStatus"]["IsPublic"] is True

    @staticmethod
    def _deny_statement(bucket_name: str, **overrides) -> dict:
        """모든 주체의 GetObject 를 거부하는 문장에 주어진 항목을 덮어쓴다."""
        statement = {
            "Effect": "Deny",
            "Principal": "*",
            "Action": "s3:GetObject",
            "Resource": f"arn:aws:s3:::{bucket_name}/*",
        }
        statement.update(overrides)
        return statement

    @staticmethod
    def _policy_document(*statements, version: str = "2012-10-17") -> str:
        return json.dumps({"Version": version, "Statement": list(statements)})

    def _assert_malformed_policy(self, client, bucket_name: str, policy: str) -> None:
        self.assert_client_error(
            lambda: client.put_bucket_policy(Bucket=bucket_name, Policy=policy), 400, md.MALFORMED_POLICY
        )

    @pytest.mark.tag("Get")
    def test_bucket_policy_get_not_exist(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 22)
        self.assert_client_error(lambda: client.get_bucket_policy(Bucket=bucket_name), 404, md.NO_SUCH_BUCKET_POLICY)
        self.assert_client_error(
            lambda: client.get_bucket_policy_status(Bucket=bucket_name), 404, md.NO_SUCH_BUCKET_POLICY
        )

    @pytest.mark.tag("Delete")
    def test_bucket_policy_delete_not_exist(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 23)
        # 정책이 없어도 삭제는 성공한다
        response = client.delete_bucket_policy(Bucket=bucket_name)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 204

    @pytest.mark.tag("Check")
    def test_bucket_policy_put_get_delete(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 24)
        policy = self._policy_document(self._deny_statement(bucket_name, Sid="deny-get"))
        client.put_bucket_policy(Bucket=bucket_name, Policy=policy)

        assert json.loads(client.get_bucket_policy(Bucket=bucket_name)["Policy"]) == json.loads(policy)
        assert client.get_bucket_policy_status(Bucket=bucket_name)["PolicyStatus"]["IsPublic"] is False

        client.delete_bucket_policy(Bucket=bucket_name)
        self.assert_client_error(lambda: client.get_bucket_policy(Bucket=bucket_name), 404, md.NO_SUCH_BUCKET_POLICY)

    @pytest.mark.tag("ERROR")
    def test_bucket_policy_bucket_not_exist(self):
        client = self.get_client()
        bucket_name = self.get_new_bucket_name_only(25)
        policy = self._policy_document(self._deny_statement(bucket_name))
        self.assert_client_error(lambda: client.get_bucket_policy(Bucket=bucket_name), 404, md.NO_SUCH_BUCKET)
        self.assert_client_error(
            lambda: client.put_bucket_policy(Bucket=bucket_name, Policy=policy), 404, md.NO_SUCH_BUCKET
        )
        self.assert_client_error(lambda: client.delete_bucket_policy(Bucket=bucket_name), 404, md.NO_SUCH_BUCKET)

    @pytest.mark.tag("Access")
    def test_bucket_policy_access_denied(self):
        client = self.get_client()
        alt_client = self.get_alt_client()
        bucket_name = self.create_bucket(client, 26)
        policy = self._policy_document(self._deny_statement(bucket_name))
        client.put_bucket_policy(Bucket=bucket_name, Policy=policy)

        self.assert_client_error(lambda: alt_client.get_bucket_policy(Bucket=bucket_name), 403, md.ACCESS_DENIED)
        self.assert_client_error(
            lambda: alt_client.put_bucket_policy(Bucket=bucket_name, Policy=policy), 403, md.ACCESS_DENIED
        )
        self.assert_client_error(lambda: alt_client.delete_bucket_policy(Bucket=bucket_name), 403, md.ACCESS_DENIED)
        # 정책은 그대로 남아 있다
        assert json.loads(client.get_bucket_policy(Bucket=bucket_name)["Policy"]) == json.loads(policy)

    @pytest.mark.tag("ERROR")
    def test_bucket_policy_invalid_json(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 27)
        self._assert_malformed_policy(client, bucket_name, "not json")
        # Statement 가 비어 있다
        self._assert_malformed_policy(client, bucket_name, self._policy_document())
        self._assert_malformed_policy(
            client, bucket_name, self._policy_document(self._deny_statement(bucket_name), version="2020-01-01")
        )

    @pytest.mark.tag("ERROR")
    def test_bucket_policy_invalid_element(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 28)
        invalid_overrides = [
            {"Effect": "Maybe"},
            {"Action": "s3:Invalid"},
            {"Principal": {"AWS": "arn:aws:iam::000000000000:user/not-exist"}},
            {"Condition": {"StringEquals": {"s3:invalid": "value"}}},  # 없는 조건 키
            {"Condition": {"InvalidEquals": {"aws:SourceIp": "1.1.1.1"}}},  # 없는 조건 연산자
        ]
        for overrides in invalid_overrides:
            self._assert_malformed_policy(
                client, bucket_name, self._policy_document(self._deny_statement(bucket_name, **overrides))
            )
        self.assert_client_error(lambda: client.get_bucket_policy(Bucket=bucket_name), 404, md.NO_SUCH_BUCKET_POLICY)

    @pytest.mark.tag("ERROR")
    def test_bucket_policy_invalid_resource(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 29)
        # 다른 버킷의 리소스
        self._assert_malformed_policy(
            client,
            bucket_name,
            self._policy_document(self._deny_statement(bucket_name, Resource="arn:aws:s3:::other-bucket-name/*")),
        )
        # 버킷 대상 액션에 오브젝트 리소스
        self._assert_malformed_policy(
            client, bucket_name, self._policy_document(self._deny_statement(bucket_name, Action="s3:ListBucket"))
        )
        # Resource, Principal 누락
        for missing in ("Resource", "Principal"):
            statement = self._deny_statement(bucket_name)
            del statement[missing]
            self._assert_malformed_policy(client, bucket_name, self._policy_document(statement))

    @pytest.mark.tag("Max")
    def test_bucket_policy_too_large(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 30)
        # 정책 문서는 20KB 까지
        statements = [
            self._deny_statement(
                bucket_name, Sid=f"statement{i}", Condition={"StringEquals": {"aws:Referer": "x" * 180}}
            )
            for i in range(70)
        ]
        policy = self._policy_document(*statements)
        assert len(policy) > 20480
        self._assert_malformed_policy(client, bucket_name, policy)

    # region 정책 효과 (AWS 실측 기준)
    # 정책을 바꾼 뒤에는 새 클라이언트로 확인한다(같은 연결은 바뀌기 전 권한으로 답할 수 있다).
    # Deny 는 정리에 필요 없는 액션(GetObject, PutObject 등)으로만 건다.

    def _alt_principal(self) -> dict:
        return {"CanonicalUser": self.config.alt_user.id}

    def _statement(self, effect: str, action, resource, principal=None, condition=None, **overrides) -> dict:
        """문장을 만든다. overrides 값이 None 이면 그 항목을 뺀다(NotAction 등으로 바꿀 때)."""
        statement = {
            "Effect": effect,
            "Principal": principal or self._alt_principal(),
            "Action": action,
            "Resource": resource,
        }
        if condition:
            statement["Condition"] = condition
        for name, value in overrides.items():
            if value is None:
                statement.pop(name, None)
            else:
                statement[name] = value
        return statement

    def _put_policy(self, client, bucket_name: str, *statements) -> None:
        client.put_bucket_policy(Bucket=bucket_name, Policy=self._policy_document(*statements))

    def _assert_denied(self, func) -> None:
        self.assert_client_error(func, 403, md.ACCESS_DENIED)

    @pytest.mark.tag("Effect")
    def test_bucket_policy_allow_user(self):
        key = "test_bucket_policy_allow_user"
        client = self.get_client()
        bucket_name = self.create_bucket(client, 31)
        client.put_object(Bucket=bucket_name, Key=key, Body=b"content")
        self._put_policy(client, bucket_name, self._statement("Allow", "s3:GetObject", utils.make_arn_resource(f"{bucket_name}/*")))
        assert client.get_bucket_policy_status(Bucket=bucket_name)["PolicyStatus"]["IsPublic"] is False

        alt_client = self.get_alt_client()
        assert self.get_body(alt_client.get_object(Bucket=bucket_name, Key=key)) == "content"
        assert alt_client.head_object(Bucket=bucket_name, Key=key)["ContentLength"] == 7
        # 허용한 액션 말고는 그대로 거부된다
        self._assert_denied(lambda: alt_client.put_object(Bucket=bucket_name, Key="new", Body=b"x"))
        self._assert_denied(lambda: alt_client.list_objects_v2(Bucket=bucket_name))
        self._assert_denied(lambda: alt_client.get_object_acl(Bucket=bucket_name, Key=key))
        self._assert_denied(lambda: alt_client.get_object_tagging(Bucket=bucket_name, Key=key))
        self._assert_denied(lambda: alt_client.delete_object(Bucket=bucket_name, Key=key))
        # 목록 권한이 없으면 없는 키도 403 이다
        self._assert_denied(lambda: alt_client.get_object(Bucket=bucket_name, Key="not-exist"))
        # 정책에 없는 주체
        self._assert_denied(lambda: self.get_public_client().get_object(Bucket=bucket_name, Key=key))

    @pytest.mark.tag("Effect")
    def test_bucket_policy_deny_overrides_allow(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 32)
        self.create_objects(client, bucket_name, ["public/a", "private/a"])
        self._put_policy(
            client,
            bucket_name,
            self._statement("Allow", "s3:GetObject", utils.make_arn_resource(f"{bucket_name}/*")),
            self._statement("Deny", "s3:GetObject", utils.make_arn_resource(f"{bucket_name}/private/*")),
        )
        alt_client = self.get_alt_client()
        assert self.get_body(alt_client.get_object(Bucket=bucket_name, Key="public/a")) == "public/a"
        self._assert_denied(lambda: alt_client.get_object(Bucket=bucket_name, Key="private/a"))
        # Deny 의 주체가 아닌 소유자는 읽을 수 있다
        assert self.get_body(self.get_client().get_object(Bucket=bucket_name, Key="private/a")) == "private/a"

    @pytest.mark.tag("Effect")
    def test_bucket_policy_deny_owner(self):
        key = "test_bucket_policy_deny_owner"
        client = self.get_client()
        bucket_name = self.create_bucket(client, 33)
        client.put_object(Bucket=bucket_name, Key=key, Body=b"content")
        self._put_policy(
            client, bucket_name, self._statement("Deny", "s3:GetObject", utils.make_arn_resource(f"{bucket_name}/*"), principal="*")
        )
        # 명시적 Deny 는 버킷 소유자에게도 적용된다
        owner = self.get_client()
        self._assert_denied(lambda: owner.get_object(Bucket=bucket_name, Key=key))
        self.assert_client_error(lambda: owner.head_object(Bucket=bucket_name, Key=key), 403, "403")
        self._assert_denied(
            lambda: owner.copy_object(Bucket=bucket_name, Key="copy", CopySource={"Bucket": bucket_name, "Key": key})
        )
        # 거부하지 않은 액션은 그대로 된다
        owner.put_object(Bucket=bucket_name, Key="new", Body=b"x")
        assert owner.get_object_tagging(Bucket=bucket_name, Key=key)["TagSet"] == []

    @pytest.mark.tag("NotAction")
    def test_bucket_policy_not_action_deny(self):
        key = "test_bucket_policy_not_action_deny"
        client = self.get_client()
        bucket_name = self.create_bucket(client, 34)
        client.put_object(Bucket=bucket_name, Key=key, Body=b"content")
        resources = [utils.make_arn_resource(bucket_name), utils.make_arn_resource(f"{bucket_name}/*")]
        self._put_policy(
            client,
            bucket_name,
            self._statement("Allow", "s3:*", resources),
            # GetObject, ListBucket 이 아닌 모든 액션을 거부
            self._statement("Deny", None, resources, Action=None, NotAction=["s3:GetObject", "s3:ListBucket"]),
        )
        alt_client = self.get_alt_client()
        assert self.get_body(alt_client.get_object(Bucket=bucket_name, Key=key)) == "content"
        assert alt_client.list_objects_v2(Bucket=bucket_name)["KeyCount"] == 1
        self._assert_denied(lambda: alt_client.put_object(Bucket=bucket_name, Key="new", Body=b"x"))
        self._assert_denied(lambda: alt_client.delete_object(Bucket=bucket_name, Key=key))
        self._assert_denied(lambda: alt_client.get_object_tagging(Bucket=bucket_name, Key=key))

    @pytest.mark.tag("NotAction")
    def test_bucket_policy_not_action_allow(self):
        key = "test_bucket_policy_not_action_allow"
        client = self.get_client()
        bucket_name = self.create_bucket(client, 35)
        client.put_object(Bucket=bucket_name, Key=key, Body=b"content")
        # PutObject, DeleteObject 가 아닌 모든 오브젝트 액션을 허용
        self._put_policy(
            client,
            bucket_name,
            self._statement(
                "Allow",
                None,
                utils.make_arn_resource(f"{bucket_name}/*"),
                Action=None,
                NotAction=["s3:PutObject", "s3:DeleteObject"],
            ),
        )
        alt_client = self.get_alt_client()
        assert self.get_body(alt_client.get_object(Bucket=bucket_name, Key=key)) == "content"
        assert alt_client.get_object_tagging(Bucket=bucket_name, Key=key)["TagSet"] == []
        self._assert_denied(lambda: alt_client.put_object(Bucket=bucket_name, Key="new", Body=b"x"))
        self._assert_denied(lambda: alt_client.delete_object(Bucket=bucket_name, Key=key))

    @pytest.mark.tag("NotResource")
    def test_bucket_policy_not_resource_allow(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 36)
        self.create_objects(client, bucket_name, ["public/a", "private/a"])
        self._put_policy(
            client,
            bucket_name,
            self._statement(
                "Allow", "s3:GetObject", None, Resource=None, NotResource=utils.make_arn_resource(f"{bucket_name}/private/*")
            ),
        )
        alt_client = self.get_alt_client()
        assert self.get_body(alt_client.get_object(Bucket=bucket_name, Key="public/a")) == "public/a"
        self._assert_denied(lambda: alt_client.get_object(Bucket=bucket_name, Key="private/a"))

    @pytest.mark.tag("NotResource")
    def test_bucket_policy_not_resource_deny(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 37)
        self.create_objects(client, bucket_name, ["public/a", "private/a"])
        self._put_policy(
            client,
            bucket_name,
            self._statement("Allow", "s3:GetObject", utils.make_arn_resource(f"{bucket_name}/*")),
            # public/ 아래가 아닌 것은 모두 거부
            self._statement(
                "Deny", "s3:GetObject", None, Resource=None, NotResource=utils.make_arn_resource(f"{bucket_name}/public/*")
            ),
        )
        alt_client = self.get_alt_client()
        assert self.get_body(alt_client.get_object(Bucket=bucket_name, Key="public/a")) == "public/a"
        self._assert_denied(lambda: alt_client.get_object(Bucket=bucket_name, Key="private/a"))

    @pytest.mark.tag("NotPrincipal")
    def test_bucket_policy_not_principal(self):
        key = "test_bucket_policy_not_principal"
        client = self.get_client()
        bucket_name = self.create_bucket(client, 38)
        client.put_object(Bucket=bucket_name, Key=key, Body=b"content")
        resource = utils.make_arn_resource(f"{bucket_name}/*")
        # Allow 에는 NotPrincipal 을 쓸 수 없다
        self._assert_malformed_policy(
            client,
            bucket_name,
            self._policy_document(
                self._statement("Allow", "s3:GetObject", resource, Principal=None, NotPrincipal=self._alt_principal())
            ),
        )
        self._put_policy(
            client,
            bucket_name,
            self._statement("Allow", "s3:GetObject", resource),
            # 서브 사용자가 아닌 모든 주체의 GetObject 를 거부(소유자 포함)
            self._statement("Deny", "s3:GetObject", resource, Principal=None, NotPrincipal=self._alt_principal()),
        )
        assert self.get_body(self.get_alt_client().get_object(Bucket=bucket_name, Key=key)) == "content"
        self._assert_denied(lambda: self.get_client().get_object(Bucket=bucket_name, Key=key))

    @pytest.mark.tag("Wildcard")
    def test_bucket_policy_resource_wildcard(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 39)
        keys = ["a1/file1.txt", "a1/file10.txt", "b1/file1.txt", "a1/sub/file2.txt", "a1/file1.TXT"]
        self.create_objects(client, bucket_name, keys)
        # * 는 0자 이상(/ 포함), ? 는 정확히 한 글자, 대소문자를 구분한다
        self._put_policy(
            client, bucket_name, self._statement("Allow", "s3:GetObject", utils.make_arn_resource(f"{bucket_name}/a*/file?.txt"))
        )
        alt_client = self.get_alt_client()
        for key in ("a1/file1.txt", "a1/sub/file2.txt"):
            assert self.get_body(alt_client.get_object(Bucket=bucket_name, Key=key)) == key
        for key in ("a1/file10.txt", "b1/file1.txt", "a1/file1.TXT"):
            self._assert_denied(lambda key=key: alt_client.get_object(Bucket=bucket_name, Key=key))

    @pytest.mark.tag("Wildcard")
    def test_bucket_policy_action_wildcard(self):
        key = "test_bucket_policy_action_wildcard"
        client = self.get_client()
        bucket_name = self.create_bucket(client, 40)
        client.put_object(Bucket=bucket_name, Key=key, Body=b"content")
        self._put_policy(
            client,
            bucket_name,
            self._statement(
                "Allow", "s3:Get*", [utils.make_arn_resource(bucket_name), utils.make_arn_resource(f"{bucket_name}/*")]
            ),
        )
        alt_client = self.get_alt_client()
        assert self.get_body(alt_client.get_object(Bucket=bucket_name, Key=key)) == "content"
        assert len(alt_client.get_object_acl(Bucket=bucket_name, Key=key)["Grants"]) == 1
        assert alt_client.get_object_tagging(Bucket=bucket_name, Key=key)["TagSet"] == []
        assert "Status" not in alt_client.get_bucket_versioning(Bucket=bucket_name)
        alt_client.get_bucket_location(Bucket=bucket_name)
        alt_client.get_bucket_acl(Bucket=bucket_name)
        # Get 으로 시작하지 않는 액션
        self._assert_denied(lambda: alt_client.list_objects_v2(Bucket=bucket_name))
        self._assert_denied(lambda: alt_client.put_object(Bucket=bucket_name, Key="new", Body=b"x"))

    @pytest.mark.tag("Wildcard")
    def test_bucket_policy_action_case_insensitive(self):
        key = "test_bucket_policy_action_case_insensitive"
        client = self.get_client()
        bucket_name = self.create_bucket(client, 41)
        client.put_object(Bucket=bucket_name, Key=key, Body=b"content")
        # 액션 이름은 대소문자를 구분하지 않는다
        self._put_policy(client, bucket_name, self._statement("Allow", "s3:getobject", utils.make_arn_resource(f"{bucket_name}/*")))
        assert self.get_body(self.get_alt_client().get_object(Bucket=bucket_name, Key=key)) == "content"

    @pytest.mark.tag("Resource")
    def test_bucket_policy_list_bucket_resource(self):
        key = "test_bucket_policy_list_bucket_resource"
        client = self.get_client()
        bucket_name = self.create_bucket(client, 42)
        client.put_object(Bucket=bucket_name, Key=key, Body=b"content")
        self._put_policy(client, bucket_name, self._statement("Allow", "s3:ListBucket", utils.make_arn_resource(bucket_name)))
        alt_client = self.get_alt_client()
        assert alt_client.list_objects_v2(Bucket=bucket_name)["KeyCount"] == 1
        assert len(alt_client.list_objects(Bucket=bucket_name)["Contents"]) == 1
        alt_client.head_bucket(Bucket=bucket_name)
        # ListBucket 만으로는 읽을 수 없고, 버전·멀티파트 목록은 다른 액션이다
        self._assert_denied(lambda: alt_client.get_object(Bucket=bucket_name, Key=key))
        self._assert_denied(lambda: alt_client.list_object_versions(Bucket=bucket_name))
        self._assert_denied(lambda: alt_client.list_multipart_uploads(Bucket=bucket_name))
        # 목록 권한이 있으면 없는 키는 404 다
        self.assert_client_error(lambda: alt_client.get_object(Bucket=bucket_name, Key="not-exist"), 404, md.NO_SUCH_KEY)

    @pytest.mark.tag("Condition")
    def test_bucket_policy_list_prefix_condition(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 43)
        self.create_objects(client, bucket_name, ["public/a", "private/a"])
        self._put_policy(
            client,
            bucket_name,
            self._statement(
                "Allow",
                "s3:ListBucket",
                utils.make_arn_resource(bucket_name),
                condition={"StringEquals": {"s3:prefix": "public/"}},
            ),
        )
        alt_client = self.get_alt_client()
        assert alt_client.list_objects_v2(Bucket=bucket_name, Prefix="public/")["KeyCount"] == 1
        assert len(alt_client.list_objects(Bucket=bucket_name, Prefix="public/")["Contents"]) == 1
        self._assert_denied(lambda: alt_client.list_objects_v2(Bucket=bucket_name))
        self._assert_denied(lambda: alt_client.list_objects_v2(Bucket=bucket_name, Prefix="public/a"))
        self._assert_denied(lambda: alt_client.list_objects_v2(Bucket=bucket_name, Prefix="private/"))

    @pytest.mark.tag("Condition")
    def test_bucket_policy_list_condition_keys(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 44)
        self.create_objects(client, bucket_name, ["public/a", "private/a"])
        # 조건 연산자가 여럿이면 모두 맞아야 한다
        self._put_policy(
            client,
            bucket_name,
            self._statement(
                "Allow",
                "s3:ListBucket",
                utils.make_arn_resource(bucket_name),
                condition={
                    "StringLike": {"s3:prefix": "public/*"},
                    "StringEquals": {"s3:delimiter": "/"},
                    "NumericLessThanEquals": {"s3:max-keys": "10"},
                },
            ),
        )
        alt_client = self.get_alt_client()
        response = alt_client.list_objects_v2(Bucket=bucket_name, Prefix="public/", Delimiter="/", MaxKeys=10)
        assert response["KeyCount"] == 1
        self._assert_denied(lambda: alt_client.list_objects_v2(Bucket=bucket_name, Prefix="public/", MaxKeys=10))
        self._assert_denied(
            lambda: alt_client.list_objects_v2(Bucket=bucket_name, Prefix="public/", Delimiter="/", MaxKeys=11)
        )
        self._assert_denied(lambda: alt_client.list_objects_v2(Bucket=bucket_name, Prefix="public/", Delimiter="/"))
        self._assert_denied(
            lambda: alt_client.list_objects_v2(Bucket=bucket_name, Prefix="private/", Delimiter="/", MaxKeys=10)
        )

    @pytest.mark.tag("Condition")
    def test_bucket_policy_list_deny_prefix(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 45)
        self.create_objects(client, bucket_name, ["public/a", "private/a"])
        resource = utils.make_arn_resource(bucket_name)
        self._put_policy(
            client,
            bucket_name,
            self._statement("Allow", "s3:ListBucket", resource),
            self._statement("Deny", "s3:ListBucket", resource, condition={"StringLike": {"s3:prefix": "private/*"}}),
        )
        alt_client = self.get_alt_client()
        self._assert_denied(lambda: alt_client.list_objects_v2(Bucket=bucket_name, Prefix="private/"))
        # 조건은 요청의 prefix 값만 본다. 응답에 private/ 아래 키가 들어 있어도 막지 않는다
        assert alt_client.list_objects_v2(Bucket=bucket_name)["KeyCount"] == 2
        assert alt_client.list_objects_v2(Bucket=bucket_name, Prefix="priv")["KeyCount"] == 1

    @pytest.mark.tag("Condition")
    def test_bucket_policy_list_max_keys(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 46)
        self.create_objects(client, bucket_name, ["a"])
        self._put_policy(
            client,
            bucket_name,
            self._statement(
                "Allow",
                "s3:ListBucket",
                utils.make_arn_resource(bucket_name),
                condition={"NumericLessThanEquals": {"s3:max-keys": "10"}},
            ),
        )
        alt_client = self.get_alt_client()
        assert alt_client.list_objects_v2(Bucket=bucket_name, MaxKeys=10)["KeyCount"] == 1
        assert len(alt_client.list_objects(Bucket=bucket_name, MaxKeys=5)["Contents"]) == 1
        self._assert_denied(lambda: alt_client.list_objects_v2(Bucket=bucket_name, MaxKeys=11))
        # max-keys 를 보내지 않으면 조건 키가 없어 맞지 않는다
        self._assert_denied(lambda: alt_client.list_objects_v2(Bucket=bucket_name))

    @pytest.mark.tag("Condition")
    def test_bucket_policy_secure_transport(self):
        key = "test_bucket_policy_secure_transport"
        client = self.get_client()
        bucket_name = self.create_bucket(client, 47)
        client.put_object(Bucket=bucket_name, Key=key, Body=b"content")
        self._put_policy(
            client,
            bucket_name,
            self._statement(
                "Deny",
                "s3:GetObject",
                utils.make_arn_resource(f"{bucket_name}/*"),
                principal="*",
                condition={"Bool": {"aws:SecureTransport": "false"}},
            ),
        )
        http_client = self.create_client(False, self.config.main_user, False)
        https_client = self.create_client(True, self.config.main_user, False)
        assert self.get_body(https_client.get_object(Bucket=bucket_name, Key=key)) == "content"
        self._assert_denied(lambda: http_client.get_object(Bucket=bucket_name, Key=key))
        # 거부한 액션이 아니면 http 로도 된다
        http_client.put_object(Bucket=bucket_name, Key="new", Body=b"x")

    @pytest.mark.tag("Condition")
    def test_bucket_policy_source_ip(self):
        key = "test_bucket_policy_source_ip"
        client = self.get_client()
        # (조건, 서브 사용자의 GetObject 가 되는지). 192.0.2.0/24 는 문서용 대역이라 요청 주소가 될 수 없다
        cases = [
            ({"IpAddress": {"aws:SourceIp": "0.0.0.0/0"}}, True),
            ({"IpAddress": {"aws:SourceIp": "192.0.2.0/24"}}, False),
            ({"NotIpAddress": {"aws:SourceIp": "192.0.2.0/24"}}, True),
            ({"NotIpAddress": {"aws:SourceIp": "0.0.0.0/0"}}, False),
            ({"IpAddress": {"aws:SourceIp": ["192.0.2.0/24", "0.0.0.0/0"]}}, True),
        ]
        for condition, allowed in cases:
            bucket_name = self.create_bucket(client, 48)
            client.put_object(Bucket=bucket_name, Key=key, Body=b"content")
            self._put_policy(
                client,
                bucket_name,
                self._statement("Allow", "s3:GetObject", utils.make_arn_resource(f"{bucket_name}/*"), condition=condition),
            )
            alt_client = self.get_alt_client()
            if allowed:
                assert self.get_body(alt_client.get_object(Bucket=bucket_name, Key=key)) == "content"
            else:
                self._assert_denied(lambda: alt_client.get_object(Bucket=bucket_name, Key=key))

        bucket_name = self.create_bucket(client, 48)
        self._assert_malformed_policy(
            client,
            bucket_name,
            self._policy_document(
                self._statement(
                    "Allow",
                    "s3:GetObject",
                    utils.make_arn_resource(f"{bucket_name}/*"),
                    condition={"IpAddress": {"aws:SourceIp": "not-an-ip"}},
                )
            ),
        )

    @pytest.mark.tag("Condition")
    def test_bucket_policy_deny_unencrypted_put(self):
        key = "test_bucket_policy_deny_unencrypted_put"
        client = self.get_client()
        bucket_name = self.create_bucket(client, 49)
        self._put_policy(
            client,
            bucket_name,
            self._statement(
                "Deny",
                "s3:PutObject",
                utils.make_arn_resource(f"{bucket_name}/*"),
                principal="*",
                condition={"StringNotEquals": {"s3:x-amz-server-side-encryption": "AES256"}},
            ),
        )
        owner = self.get_client()
        source = {"Bucket": bucket_name, "Key": key}
        # 헤더가 없으면 값이 AES256 이 아닌 것으로 본다
        self._assert_denied(lambda: owner.put_object(Bucket=bucket_name, Key=key, Body=b"content"))
        owner.put_object(Bucket=bucket_name, Key=key, Body=b"content", ServerSideEncryption="AES256")
        # 멀티파트 시작과 복사도 PutObject 액션이다
        self._assert_denied(lambda: owner.create_multipart_upload(Bucket=bucket_name, Key="multi"))
        self._assert_denied(lambda: owner.copy_object(Bucket=bucket_name, Key="copy", CopySource=source))
        owner.copy_object(Bucket=bucket_name, Key="copy", CopySource=source, ServerSideEncryption="AES256")
        assert self.get_body(owner.get_object(Bucket=bucket_name, Key="copy")) == "content"

    @pytest.mark.tag("Condition")
    def test_bucket_policy_null_condition(self):
        key = "test_bucket_policy_null_condition"
        client = self.get_client()
        bucket_name = self.create_bucket(client, 50)
        # 암호화 헤더가 없는(Null) 업로드를 거부
        self._put_policy(
            client,
            bucket_name,
            self._statement(
                "Deny",
                "s3:PutObject",
                utils.make_arn_resource(f"{bucket_name}/*"),
                principal="*",
                condition={"Null": {"s3:x-amz-server-side-encryption": "true"}},
            ),
        )
        owner = self.get_client()
        self._assert_denied(lambda: owner.put_object(Bucket=bucket_name, Key=key, Body=b"content"))
        owner.put_object(Bucket=bucket_name, Key=key, Body=b"content", ServerSideEncryption="AES256")
        assert owner.head_object(Bucket=bucket_name, Key=key)["ServerSideEncryption"] == "AES256"

    @pytest.mark.tag("Version")
    def test_bucket_policy_get_object_version_action(self):
        key = "test_bucket_policy_get_object_version_action"
        client = self.get_client()
        bucket_name = self.create_bucket(client, 51)
        self.check_configure_versioning_retry(bucket_name, "Enabled")
        version1 = client.put_object(Bucket=bucket_name, Key=key, Body=b"1")["VersionId"]
        version2 = client.put_object(Bucket=bucket_name, Key=key, Body=b"2")["VersionId"]
        self._put_policy(client, bucket_name, self._statement("Allow", "s3:GetObject", utils.make_arn_resource(f"{bucket_name}/*")))
        alt_client = self.get_alt_client()
        assert self.get_body(alt_client.get_object(Bucket=bucket_name, Key=key)) == "2"
        # 버전을 지정한 조회는 s3:GetObjectVersion 이 필요하다(최신 버전이어도)
        for version_id in (version1, version2):
            self._assert_denied(
                lambda version_id=version_id: alt_client.get_object(Bucket=bucket_name, Key=key, VersionId=version_id)
            )
        self.assert_client_error(
            lambda: alt_client.head_object(Bucket=bucket_name, Key=key, VersionId=version1), 403, "403"
        )

    @pytest.mark.tag("Version")
    def test_bucket_policy_version_id_condition(self):
        key = "test_bucket_policy_version_id_condition"
        client = self.get_client()
        bucket_name = self.create_bucket(client, 52)
        self.check_configure_versioning_retry(bucket_name, "Enabled")
        version1 = client.put_object(Bucket=bucket_name, Key=key, Body=b"1")["VersionId"]
        version2 = client.put_object(Bucket=bucket_name, Key=key, Body=b"2")["VersionId"]
        self._put_policy(
            client,
            bucket_name,
            self._statement(
                "Allow",
                "s3:GetObjectVersion",
                utils.make_arn_resource(f"{bucket_name}/*"),
                condition={"StringEquals": {"s3:VersionId": version1}},
            ),
        )
        alt_client = self.get_alt_client()
        assert self.get_body(alt_client.get_object(Bucket=bucket_name, Key=key, VersionId=version1)) == "1"
        self._assert_denied(lambda: alt_client.get_object(Bucket=bucket_name, Key=key, VersionId=version2))
        # 버전을 지정하지 않은 조회는 s3:GetObject 가 필요하다
        self._assert_denied(lambda: alt_client.get_object(Bucket=bucket_name, Key=key))

    @pytest.mark.tag("Version")
    def test_bucket_policy_delete_object_version_action(self):
        key = "test_bucket_policy_delete_object_version_action"
        client = self.get_client()
        bucket_name = self.create_bucket(client, 53)
        self.check_configure_versioning_retry(bucket_name, "Enabled")
        version_id = client.put_object(Bucket=bucket_name, Key=key, Body=b"1")["VersionId"]
        self._put_policy(client, bucket_name, self._statement("Allow", "s3:DeleteObject", utils.make_arn_resource(f"{bucket_name}/*")))
        alt_client = self.get_alt_client()
        # 버전을 지정한 삭제는 s3:DeleteObjectVersion 이 필요하다
        self._assert_denied(lambda: alt_client.delete_object(Bucket=bucket_name, Key=key, VersionId=version_id))
        response = alt_client.delete_objects(
            Bucket=bucket_name, Delete={"Objects": [{"Key": key, "VersionId": version_id}]}
        )
        assert not response.get("Deleted")
        assert response["Errors"][0]["Code"] == md.ACCESS_DENIED
        # 버전을 지정하지 않은 삭제(삭제 마커)는 된다
        assert alt_client.delete_object(Bucket=bucket_name, Key=key)["DeleteMarker"] is True
        assert self.get_body(client.get_object(Bucket=bucket_name, Key=key, VersionId=version_id)) == "1"

    @pytest.mark.tag("Condition")
    def test_bucket_policy_request_object_tag_keys(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 54, "BucketOwnerEnforced")
        # 요청의 태그 키가 모두 project, team 안에 있어야 한다
        self._put_policy(
            client,
            bucket_name,
            self._statement(
                "Allow",
                ["s3:PutObject", "s3:PutObjectTagging"],
                utils.make_arn_resource(f"{bucket_name}/*"),
                condition={"ForAllValues:StringEquals": {"s3:RequestObjectTagKeys": ["project", "team"]}},
            ),
        )
        alt_client = self.get_alt_client()
        alt_client.put_object(Bucket=bucket_name, Key="allowed", Body=b"x", Tagging="project=a")
        # 태그가 없으면 ForAllValues 는 참이다
        alt_client.put_object(Bucket=bucket_name, Key="no-tag", Body=b"x")
        self._assert_denied(
            lambda: alt_client.put_object(Bucket=bucket_name, Key="denied", Body=b"x", Tagging="project=a&owner=b")
        )
        assert client.get_object_tagging(Bucket=bucket_name, Key="allowed")["TagSet"] == [{"Key": "project", "Value": "a"}]

    @pytest.mark.tag("Action")
    def test_bucket_policy_put_object_action(self):
        key = "test_bucket_policy_put_object_action"
        client = self.get_client()
        bucket_name = self.create_bucket(client, 55, "BucketOwnerEnforced")
        client.put_object(Bucket=bucket_name, Key=key, Body=b"content")
        self._put_policy(client, bucket_name, self._statement("Allow", "s3:PutObject", utils.make_arn_resource(f"{bucket_name}/*")))
        alt_client = self.get_alt_client()
        alt_client.put_object(Bucket=bucket_name, Key="new", Body=b"x")
        # 멀티파트 시작·파트 업로드는 PutObject 로, 시작한 사용자는 파트 목록·중단도 할 수 있다
        upload_id = alt_client.create_multipart_upload(Bucket=bucket_name, Key="multi")["UploadId"]
        alt_client.upload_part(Bucket=bucket_name, Key="multi", UploadId=upload_id, PartNumber=1, Body=b"x")
        assert len(alt_client.list_parts(Bucket=bucket_name, Key="multi", UploadId=upload_id)["Parts"]) == 1
        alt_client.abort_multipart_upload(Bucket=bucket_name, Key="multi", UploadId=upload_id)

        # 태그를 붙이려면 PutObjectTagging, 복사하려면 원본의 GetObject 가 필요하다
        self._assert_denied(lambda: alt_client.put_object(Bucket=bucket_name, Key="tagged", Body=b"x", Tagging="a=b"))
        self._assert_denied(
            lambda: alt_client.copy_object(Bucket=bucket_name, Key="copy", CopySource={"Bucket": bucket_name, "Key": key})
        )
        self._assert_denied(lambda: alt_client.list_multipart_uploads(Bucket=bucket_name))
        self._assert_denied(lambda: alt_client.get_object(Bucket=bucket_name, Key="new"))
        self._assert_denied(lambda: alt_client.delete_object(Bucket=bucket_name, Key="new"))

    @pytest.mark.tag("Action")
    def test_bucket_policy_multipart_list_actions(self):
        key = "test_bucket_policy_multipart_list_actions"
        client = self.get_client()
        bucket_name = self.create_bucket(client, 56)
        upload_id = client.create_multipart_upload(Bucket=bucket_name, Key=key)["UploadId"]
        client.upload_part(Bucket=bucket_name, Key=key, UploadId=upload_id, PartNumber=1, Body=b"x")
        self._put_policy(
            client,
            bucket_name,
            self._statement(
                "Allow",
                ["s3:ListMultipartUploadParts", "s3:ListBucketMultipartUploads"],
                [utils.make_arn_resource(bucket_name), utils.make_arn_resource(f"{bucket_name}/*")],
            ),
        )
        alt_client = self.get_alt_client()
        assert len(alt_client.list_parts(Bucket=bucket_name, Key=key, UploadId=upload_id)["Parts"]) == 1
        assert len(alt_client.list_multipart_uploads(Bucket=bucket_name)["Uploads"]) == 1
        self._assert_denied(lambda: alt_client.abort_multipart_upload(Bucket=bucket_name, Key=key, UploadId=upload_id))
        self._assert_denied(
            lambda: alt_client.upload_part(Bucket=bucket_name, Key=key, UploadId=upload_id, PartNumber=2, Body=b"x")
        )

    @pytest.mark.tag("Action")
    def test_bucket_policy_bucket_config_actions(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 57)
        actions = [
            "s3:GetBucketVersioning",
            "s3:GetBucketTagging",
            "s3:PutBucketTagging",
            "s3:GetBucketCORS",
            "s3:PutBucketCORS",
            "s3:GetBucketAcl",
            "s3:GetBucketLocation",
            "s3:ListBucketVersions",
        ]
        self._put_policy(client, bucket_name, self._statement("Allow", actions, utils.make_arn_resource(bucket_name)))
        alt_client = self.get_alt_client()
        assert "Status" not in alt_client.get_bucket_versioning(Bucket=bucket_name)
        alt_client.get_bucket_acl(Bucket=bucket_name)
        alt_client.get_bucket_location(Bucket=bucket_name)
        assert not alt_client.list_object_versions(Bucket=bucket_name).get("Versions")
        # 권한은 있고 설정이 없으면 403 이 아니라 404 다
        self.assert_client_error(lambda: alt_client.get_bucket_tagging(Bucket=bucket_name), 404, md.NO_SUCH_TAG_SET)
        self.assert_client_error(
            lambda: alt_client.get_bucket_cors(Bucket=bucket_name), 404, md.NO_SUCH_CORS_CONFIGURATION
        )
        # 설정 삭제는 Put 액션으로 판정한다
        tag_set = [{"Key": "a", "Value": "b"}]
        alt_client.put_bucket_tagging(Bucket=bucket_name, Tagging={"TagSet": tag_set})
        assert client.get_bucket_tagging(Bucket=bucket_name)["TagSet"] == tag_set
        alt_client.delete_bucket_tagging(Bucket=bucket_name)
        alt_client.put_bucket_cors(
            Bucket=bucket_name, CORSConfiguration={"CORSRules": [{"AllowedMethods": ["GET"], "AllowedOrigins": ["*"]}]}
        )
        alt_client.delete_bucket_cors(Bucket=bucket_name)

        # 허용하지 않은 액션
        self._assert_denied(lambda: alt_client.list_objects_v2(Bucket=bucket_name))
        self._assert_denied(
            lambda: alt_client.put_bucket_versioning(Bucket=bucket_name, VersioningConfiguration={"Status": "Enabled"})
        )
        self._assert_denied(lambda: alt_client.get_bucket_lifecycle_configuration(Bucket=bucket_name))
        self._assert_denied(lambda: alt_client.delete_bucket(Bucket=bucket_name))

    @pytest.mark.tag("Form")
    def test_bucket_policy_statement_forms(self):
        key = "test_bucket_policy_statement_forms"
        client = self.get_client()
        bucket_name = self.create_bucket(client, 58)
        client.put_object(Bucket=bucket_name, Key=key, Body=b"content")
        resource = utils.make_arn_resource(f"{bucket_name}/*")
        statement = self._statement("Allow", "s3:GetObject", resource)

        # Statement 는 배열이 아니라 객체 하나여도 된다
        client.put_bucket_policy(Bucket=bucket_name, Policy=json.dumps({"Version": "2012-10-17", "Statement": statement}))
        assert self.get_body(self.get_alt_client().get_object(Bucket=bucket_name, Key=key)) == "content"

        # Version 을 빼면 2008-10-17 로 저장된다
        client.put_bucket_policy(Bucket=bucket_name, Policy=json.dumps({"Statement": [statement]}))
        assert json.loads(client.get_bucket_policy(Bucket=bucket_name)["Policy"])["Version"] == "2008-10-17"
        client.put_bucket_policy(Bucket=bucket_name, Policy=self._policy_document(statement, version="2008-10-17"))

        # Id 와 Sid 는 그대로 돌려준다. Sid 가 겹쳐도 받는다
        policy = {
            "Version": "2012-10-17",
            "Id": "policy-id",
            "Statement": [
                self._statement("Allow", "s3:GetObject", resource, Sid="same"),
                self._statement("Allow", "s3:GetObjectTagging", resource, Sid="same"),
            ],
        }
        client.put_bucket_policy(Bucket=bucket_name, Policy=json.dumps(policy))
        actual = json.loads(client.get_bucket_policy(Bucket=bucket_name)["Policy"])
        assert actual["Id"] == "policy-id"
        assert [s["Sid"] for s in actual["Statement"]] == ["same", "same"]
        assert [s["Action"] for s in actual["Statement"]] == ["s3:GetObject", "s3:GetObjectTagging"]

        # 주체를 여럿 적을 수 있다
        principal = {"CanonicalUser": [self.config.alt_user.id, self.config.main_user.id]}
        self._put_policy(client, bucket_name, self._statement("Allow", "s3:GetObject", resource, principal=principal))
        assert self.get_body(self.get_alt_client().get_object(Bucket=bucket_name, Key=key)) == "content"

    @pytest.mark.tag("ERROR")
    def test_bucket_policy_invalid_statement(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client, 59)
        resource = utils.make_arn_resource(f"{bucket_name}/*")
        invalid_statements = [
            # 같은 뜻의 항목을 함께 쓴다
            self._statement("Allow", "s3:GetObject", resource, NotAction="s3:PutObject"),
            self._statement("Allow", "s3:GetObject", resource, NotResource=utils.make_arn_resource(f"{bucket_name}/a/*")),
            self._statement("Deny", "s3:GetObject", resource, NotPrincipal=self._alt_principal()),
            # 필수 항목 누락
            self._statement("Allow", None, resource, Action=None),
            self._statement("Allow", "s3:GetObject", resource, Effect=None),
            {},
            # 값이 잘못됐다
            self._statement("Allow", "s3:GetObject", resource, Unknown="value"),
            self._statement("Allow", [], resource),
            self._statement("Allow", "ec2:RunInstances", resource),
            self._statement("Allow", "s3:GetObject", "*"),
            self._statement("Allow", "s3:GetObject", resource, principal={"CanonicalUser": "0" * 64}),
        ]
        for statement in invalid_statements:
            self._assert_malformed_policy(client, bucket_name, self._policy_document(statement))
        # 최상위가 객체가 아니다
        self._assert_malformed_policy(client, bucket_name, "[]")
        self.assert_client_error(lambda: client.get_bucket_policy(Bucket=bucket_name), 404, md.NO_SUCH_BUCKET_POLICY)

    @pytest.mark.tag("Delete")
    def test_bucket_policy_delete_revokes(self):
        key = "test_bucket_policy_delete_revokes"
        client = self.get_client()
        bucket_name = self.create_bucket(client, 60)
        client.put_object(Bucket=bucket_name, Key=key, Body=b"content")
        self._put_policy(client, bucket_name, self._statement("Allow", "s3:GetObject", utils.make_arn_resource(f"{bucket_name}/*")))
        assert self.get_body(self.get_alt_client().get_object(Bucket=bucket_name, Key=key)) == "content"

        client.delete_bucket_policy(Bucket=bucket_name)
        self._assert_denied(lambda: self.get_alt_client().get_object(Bucket=bucket_name, Key=key))

    @pytest.mark.tag("Effect")
    def test_bucket_policy_other_user_write(self):
        key = "test_bucket_policy_other_user_write"
        client = self.get_client()
        bucket_name = self.create_bucket(client, 61, "BucketOwnerEnforced")
        self._put_policy(
            client,
            bucket_name,
            self._statement(
                "Allow", ["s3:PutObject", "s3:GetObject", "s3:DeleteObject"], utils.make_arn_resource(f"{bucket_name}/*")
            ),
        )
        alt_client = self.get_alt_client()
        alt_client.put_object(Bucket=bucket_name, Key=key, Body=b"content")
        # 다른 사용자가 올린 오브젝트도 버킷 소유자의 것이다
        assert self.get_body(client.get_object(Bucket=bucket_name, Key=key)) == "content"
        assert self.get_body(alt_client.get_object(Bucket=bucket_name, Key=key)) == "content"
        response = alt_client.delete_objects(Bucket=bucket_name, Delete={"Objects": [{"Key": key}, {"Key": "not-exist"}]})
        assert sorted(d["Key"] for d in response["Deleted"]) == sorted([key, "not-exist"])
        assert not response.get("Errors")
        self.assert_client_error(lambda: client.get_object(Bucket=bucket_name, Key=key), 404, md.NO_SUCH_KEY)

    @pytest.mark.tag("Effect")
    def test_bucket_policy_anonymous_prefix(self):
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client, 62)
        self.create_objects(client, bucket_name, ["public/a", "private/a"])
        self._put_policy(
            client,
            bucket_name,
            self._statement("Allow", "s3:GetObject", utils.make_arn_resource(f"{bucket_name}/public/*"), principal="*"),
        )
        assert client.get_bucket_policy_status(Bucket=bucket_name)["PolicyStatus"]["IsPublic"] is True

        public_client = self.get_public_client()
        assert self.get_body(public_client.get_object(Bucket=bucket_name, Key="public/a")) == "public/a"
        self._assert_denied(lambda: public_client.get_object(Bucket=bucket_name, Key="private/a"))
        self._assert_denied(lambda: public_client.put_object(Bucket=bucket_name, Key="public/b", Body=b"x"))
        self._assert_denied(lambda: public_client.list_objects_v2(Bucket=bucket_name))

    # endregion
