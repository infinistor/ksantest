"""ListObjectsVersions tests ported from Java testV2/ListObjectsVersions.java."""

from __future__ import annotations

import pytest

from s3tests.data import main_data as md
from s3tests.test_base import S3TestBase


class TestListObjectsVersions(S3TestBase):
    @pytest.mark.tag("Check")
    def test_bucket_list_versions_many(self):
        client = self.get_client()
        bucket_name = self.create_objects(client, "foo", "bar", "baz")

        response = client.list_object_versions(Bucket=bucket_name, MaxKeys=2)
        assert self.get_keys2(response.get("Versions")) == ["bar", "baz"]
        assert len(response["Versions"]) == 2
        assert response["IsTruncated"] is True

        response = client.list_object_versions(
            Bucket=bucket_name, KeyMarker="baz", MaxKeys=2
        )
        assert self.get_keys2(response.get("Versions")) == ["foo"]
        assert len(response["Versions"]) == 1
        assert response["IsTruncated"] is False

    @pytest.mark.tag("Delimiter")
    def test_bucket_list_versions_delimiter_basic(self):
        client = self.get_client()
        bucket_name = self.create_objects(client, "foo/bar", "foo/bars/xyzzy", "quux/thud", "asdf")
        delimiter = "/"

        response = client.list_object_versions(Bucket=bucket_name, Delimiter=delimiter)
        assert response["Delimiter"] == delimiter
        assert self.get_keys2(response.get("Versions")) == ["asdf"]
        prefixes = self.get_prefix_list(response.get("CommonPrefixes"))
        assert len(prefixes) == 2
        assert prefixes == ["foo/", "quux/"]

    @pytest.mark.tag("Encoding")
    def test_bucket_list_versions_encoding_basic(self):
        client = self.get_client()
        bucket_name = self.create_objects(
            client, "foo+1/bar", "foo/bar/xyzzy", "quux ab/thud", "asdf+b"
        )
        delimiter = "/"

        response = client.list_object_versions(
            Bucket=bucket_name, Delimiter=delimiter, EncodingType="url"
        )
        encoding = response.get("EncodingType")
        assert response["Delimiter"] == delimiter
        assert self.get_keys2(response.get("Versions"), encoding) == ["asdf+b"]
        prefixes = self.get_prefix_list(response.get("CommonPrefixes"), encoding)
        assert len(prefixes) == 3
        assert sorted(prefixes) == sorted(["foo+1/", "foo/", "quux ab/"])

    @pytest.mark.tag("Filtering")
    def test_bucket_list_versions_delimiter_prefix(self):
        bucket_name = self.create_objects_keys(
            ["asdf", "boo/bar", "boo/baz/xyzzy", "cquux/thud", "cquux/bla"]
        )
        delimiter = "/"
        prefix = ""

        marker = self.validate_list_object(
            bucket_name, prefix, delimiter, "", 1, True, ["asdf"], [], "asdf"
        )
        marker = self.validate_list_object(
            bucket_name, prefix, delimiter, marker, 1, True, [], ["boo/"], "boo/"
        )
        self.validate_list_object(
            bucket_name, prefix, delimiter, marker, 1, False, [], ["cquux/"], None
        )

        marker = self.validate_list_object(
            bucket_name, prefix, delimiter, "", 2, True, ["asdf"], ["boo/"], "boo/"
        )
        self.validate_list_object(
            bucket_name, prefix, delimiter, marker, 2, False, [], ["cquux/"], None
        )

        prefix = "boo/"
        marker = self.validate_list_object(
            bucket_name, prefix, delimiter, "", 1, True, ["boo/bar"], [], "boo/bar"
        )
        self.validate_list_object(
            bucket_name, prefix, delimiter, marker, 1, False, [], ["boo/baz/"], None
        )
        self.validate_list_object(
            bucket_name, prefix, delimiter, "", 2, False, ["boo/bar"], ["boo/baz/"], None
        )

    @pytest.mark.tag("Filtering")
    def test_bucket_list_versions_delimiter_prefix_ends_with_delimiter(self):
        client = self.get_client()
        bucket_name = self.create_objects(client, "asdf/")
        self.validate_list_object(
            bucket_name, "asdf/", "/", "", 1000, False, ["asdf/"], [], None
        )

    @pytest.mark.tag("Delimiter")
    def test_bucket_list_versions_delimiter_alt(self):
        client = self.get_client()
        bucket_name = self.create_objects(client, "bar", "baz", "cab", "foo")
        delimiter = "a"

        response = client.list_object_versions(Bucket=bucket_name, Delimiter=delimiter)
        assert response["Delimiter"] == delimiter
        assert self.get_keys2(response.get("Versions")) == ["foo"]
        prefixes = self.get_prefix_list(response.get("CommonPrefixes"))
        assert len(prefixes) == 2
        assert prefixes == ["ba", "ca"]

    @pytest.mark.tag("Filtering")
    def test_bucket_list_versions_delimiter_prefix_underscore(self):
        bucket_name = self.create_objects_keys(
            ["Obj1_", "Under1/bar", "Under1/baz/xyzzy", "Under2/thud", "Under2/bla"]
        )
        delimiter = "/"
        prefix = ""

        marker = self.validate_list_object(
            bucket_name, prefix, delimiter, "", 1, True, ["Obj1_"], [], "Obj1_"
        )
        marker = self.validate_list_object(
            bucket_name, prefix, delimiter, marker, 1, True, [], ["Under1/"], "Under1/"
        )
        self.validate_list_object(
            bucket_name, prefix, delimiter, marker, 1, False, [], ["Under2/"], None
        )

        marker = self.validate_list_object(
            bucket_name, prefix, delimiter, "", 2, True, ["Obj1_"], ["Under1/"], "Under1/"
        )
        self.validate_list_object(
            bucket_name, prefix, delimiter, marker, 2, False, [], ["Under2/"], None
        )

        prefix = "Under1/"
        marker = self.validate_list_object(
            bucket_name, prefix, delimiter, "", 1, True, ["Under1/bar"], [], "Under1/bar"
        )
        self.validate_list_object(
            bucket_name, prefix, delimiter, marker, 1, False, [], ["Under1/baz/"], None
        )
        self.validate_list_object(
            bucket_name, prefix, delimiter, "", 2, False, ["Under1/bar"], ["Under1/baz/"], None
        )

    @pytest.mark.tag("Delimiter")
    def test_bucket_list_versions_delimiter_percentage(self):
        client = self.get_client()
        bucket_name = self.create_objects(client, "b%ar", "b%az", "c%ab", "foo")
        delimiter = "%"

        response = client.list_object_versions(Bucket=bucket_name, Delimiter=delimiter)
        assert response["Delimiter"] == delimiter
        assert self.get_keys2(response.get("Versions")) == ["foo"]
        prefixes = self.get_prefix_list(response.get("CommonPrefixes"))
        assert len(prefixes) == 2
        assert prefixes == ["b%", "c%"]

    @pytest.mark.tag("Delimiter")
    def test_bucket_list_versions_delimiter_whitespace(self):
        client = self.get_client()
        bucket_name = self.create_objects(client, "b ar", "b az", "c ab", "foo")
        delimiter = " "

        response = client.list_object_versions(Bucket=bucket_name, Delimiter=delimiter)
        assert response["Delimiter"] == delimiter
        assert self.get_keys2(response.get("Versions")) == ["foo"]
        prefixes = self.get_prefix_list(response.get("CommonPrefixes"))
        assert len(prefixes) == 2
        assert prefixes == ["b ", "c "]

    @pytest.mark.tag("Delimiter")
    def test_bucket_list_versions_delimiter_dot(self):
        client = self.get_client()
        bucket_name = self.create_objects(client, "b.ar", "b.az", "c.ab", "foo")
        delimiter = "."

        response = client.list_object_versions(Bucket=bucket_name, Delimiter=delimiter)
        assert response["Delimiter"] == delimiter
        assert self.get_keys2(response.get("Versions")) == ["foo"]
        prefixes = self.get_prefix_list(response.get("CommonPrefixes"))
        assert len(prefixes) == 2
        assert prefixes == ["b.", "c."]

    @pytest.mark.tag("Delimiter")
    def test_bucket_list_versions_delimiter_unreadable(self):
        client = self.get_client()
        key_names = ["bar", "baz", "cab", "foo"]
        bucket_name = self.create_objects(client, *key_names)
        delimiter = "\n"

        response = client.list_object_versions(Bucket=bucket_name, Delimiter=delimiter)
        assert response["Delimiter"] == delimiter
        assert self.get_keys2(response.get("Versions")) == key_names
        assert len(self.get_prefix_list(response.get("CommonPrefixes"))) == 0

    @pytest.mark.tag("Delimiter")
    def test_bucket_list_versions_delimiter_empty(self):
        client = self.get_client()
        key_names = ["bar", "baz", "cab", "foo"]
        bucket_name = self.create_objects(client, *key_names)
        delimiter = ""

        response = client.list_object_versions(Bucket=bucket_name, Delimiter=delimiter)
        assert response.get("Delimiter", "") == ""
        assert self.get_keys2(response.get("Versions")) == key_names
        assert len(self.get_prefix_list(response.get("CommonPrefixes"))) == 0

    @pytest.mark.tag("Delimiter")
    def test_bucket_list_versions_delimiter_none(self):
        client = self.get_client()
        key_names = ["bar", "baz", "cab", "foo"]
        bucket_name = self.create_objects(client, *key_names)

        response = client.list_object_versions(Bucket=bucket_name)
        assert response.get("Delimiter") is None
        assert self.get_keys2(response.get("Versions")) == key_names
        assert len(self.get_prefix_list(response.get("CommonPrefixes"))) == 0

    @pytest.mark.tag("Delimiter")
    def test_bucket_list_versions_delimiter_not_exist(self):
        key_names = ["bar", "baz", "cab", "foo"]
        client = self.get_client()
        bucket_name = self.create_objects(client, *key_names)
        delimiter = "/"

        response = client.list_object_versions(Bucket=bucket_name, Delimiter=delimiter)
        assert response["Delimiter"] == delimiter
        assert self.get_keys2(response.get("Versions")) == key_names
        assert len(self.get_prefix_list(response.get("CommonPrefixes"))) == 0

    @pytest.mark.tag("Delimiter")
    def test_bucket_list_versions_delimiter_not_skip_special(self):
        key_names = [f"0/{i}" for i in range(1000, 1999)]
        key_names2 = ["1999", "1999#", "1999+", "2000"]
        key_names.extend(key_names2)
        client = self.get_client()
        bucket_name = self.create_objects_list(client, key_names)
        delimiter = "/"

        response = client.list_object_versions(Bucket=bucket_name, Delimiter=delimiter)
        assert response["Delimiter"] == delimiter
        assert self.get_keys2(response.get("Versions")) == key_names2
        assert self.get_prefix_list(response.get("CommonPrefixes")) == ["0/"]

    @pytest.mark.tag("prefix")
    def test_bucket_list_versions_prefix_basic(self):
        client = self.get_client()
        bucket_name = self.create_objects(client, "foo/bar", "foo/baz", "quux")
        prefix = "foo/"

        response = client.list_object_versions(Bucket=bucket_name, Prefix=prefix)
        assert response["Prefix"] == prefix
        assert self.get_keys2(response.get("Versions")) == ["foo/bar", "foo/baz"]
        assert len(self.get_prefix_list(response.get("CommonPrefixes"))) == 0

    @pytest.mark.tag("prefix")
    def test_bucket_list_versions_prefix_alt(self):
        client = self.get_client()
        bucket_name = self.create_objects(client, "bar", "baz", "foo")
        prefix = "ba"

        response = client.list_object_versions(Bucket=bucket_name, Prefix=prefix)
        assert response["Prefix"] == prefix
        assert self.get_keys2(response.get("Versions")) == ["bar", "baz"]
        assert len(self.get_prefix_list(response.get("CommonPrefixes"))) == 0

    @pytest.mark.tag("prefix")
    def test_bucket_list_versions_prefix_empty(self):
        key_names = ["foo/bar", "foo/baz", "quux"]
        client = self.get_client()
        bucket_name = self.create_objects(client, *key_names)
        prefix = ""

        response = client.list_object_versions(Bucket=bucket_name, Prefix=prefix)
        assert response.get("Prefix", "") == ""
        assert self.get_keys2(response.get("Versions")) == key_names
        assert len(self.get_prefix_list(response.get("CommonPrefixes"))) == 0

    @pytest.mark.tag("prefix")
    def test_bucket_list_versions_prefix_none(self):
        key_names = ["foo/bar", "foo/baz", "quux"]
        client = self.get_client()
        bucket_name = self.create_objects(client, *key_names)

        response = client.list_object_versions(Bucket=bucket_name)
        assert response.get("Prefix", "") == ""
        assert self.get_keys2(response.get("Versions")) == key_names
        assert len(self.get_prefix_list(response.get("CommonPrefixes"))) == 0

    @pytest.mark.tag("prefix")
    def test_bucket_list_versions_prefix_not_exist(self):
        key_names = ["foo/bar", "foo/baz", "quux"]
        client = self.get_client()
        bucket_name = self.create_objects(client, *key_names)
        prefix = "d"

        response = client.list_object_versions(Bucket=bucket_name, Prefix=prefix)
        assert response["Prefix"] == prefix
        assert self.get_keys2(response.get("Versions")) == []
        assert len(self.get_prefix_list(response.get("CommonPrefixes"))) == 0

    @pytest.mark.tag("prefix")
    def test_bucket_list_versions_prefix_unreadable(self):
        key_names = ["foo/bar", "foo/baz", "quux"]
        client = self.get_client()
        bucket_name = self.create_objects(client, *key_names)
        prefix = "\n"

        response = client.list_object_versions(Bucket=bucket_name, Prefix=prefix)
        assert self.get_response_prefix(response) == prefix
        assert self.get_keys2(response.get("Versions")) == []
        assert len(self.get_prefix_list(response.get("CommonPrefixes"))) == 0

    @pytest.mark.tag("PrefixAndDelimiter")
    def test_bucket_list_versions_prefix_delimiter_basic(self):
        client = self.get_client()
        bucket_name = self.create_objects(client, "foo/bar", "foo/baz/xyzzy", "quux/thud", "asdf")
        prefix = "foo/"
        delimiter = "/"

        response = client.list_object_versions(
            Bucket=bucket_name, Delimiter=delimiter, Prefix=prefix
        )
        assert response["Prefix"] == prefix
        assert response["Delimiter"] == delimiter
        assert self.get_keys2(response.get("Versions")) == ["foo/bar"]
        assert self.get_prefix_list(response.get("CommonPrefixes")) == ["foo/baz/"]

    @pytest.mark.tag("PrefixAndDelimiter")
    def test_bucket_list_versions_prefix_delimiter_alt(self):
        client = self.get_client()
        bucket_name = self.create_objects(client, "bar", "bazar", "cab", "foo")
        delimiter = "a"
        prefix = "ba"

        response = client.list_object_versions(
            Bucket=bucket_name, Delimiter=delimiter, Prefix=prefix
        )
        assert response["Prefix"] == prefix
        assert response["Delimiter"] == delimiter
        assert self.get_keys2(response.get("Versions")) == ["bar"]
        assert self.get_prefix_list(response.get("CommonPrefixes")) == ["baza"]

    @pytest.mark.tag("PrefixAndDelimiter")
    def test_bucket_list_versions_prefix_delimiter_prefix_not_exist(self):
        client = self.get_client()
        bucket_name = self.create_objects(client, "b/a/r", "b/a/c", "b/a/g", "g")

        response = client.list_object_versions(Bucket=bucket_name, Delimiter="d", Prefix="/")
        assert self.get_keys2(response.get("Versions")) == []
        assert len(self.get_prefix_list(response.get("CommonPrefixes"))) == 0

    @pytest.mark.tag("PrefixAndDelimiter")
    def test_bucket_list_versions_prefix_delimiter_delimiter_not_exist(self):
        client = self.get_client()
        bucket_name = self.create_objects(client, "b/a/c", "b/a/g", "b/a/r", "g")

        response = client.list_object_versions(Bucket=bucket_name, Delimiter="z", Prefix="b")
        assert self.get_keys2(response.get("Versions")) == ["b/a/c", "b/a/g", "b/a/r"]
        assert len(self.get_prefix_list(response.get("CommonPrefixes"))) == 0

    @pytest.mark.tag("PrefixAndDelimiter")
    def test_bucket_list_versions_prefix_delimiter_prefix_delimiter_not_exist(self):
        client = self.get_client()
        bucket_name = self.create_objects(client, "b/a/r", "b/a/c", "b/a/g", "g")

        response = client.list_object_versions(Bucket=bucket_name, Delimiter="z", Prefix="y")
        assert self.get_keys2(response.get("Versions")) == []
        assert len(self.get_prefix_list(response.get("CommonPrefixes"))) == 0

    @pytest.mark.tag("MaxKeys")
    def test_bucket_list_versions_max_keys_one(self):
        key_names = ["bar", "baz", "foo", "quxx"]
        client = self.get_client()
        bucket_name = self.create_objects(client, *key_names)

        response = client.list_object_versions(Bucket=bucket_name, MaxKeys=1)
        assert response["IsTruncated"] is True
        assert self.get_keys2(response.get("Versions")) == key_names[:1]

        response = client.list_object_versions(Bucket=bucket_name, KeyMarker=key_names[0])
        assert response["IsTruncated"] is False
        assert self.get_keys2(response.get("Versions")) == key_names[1:]

    @pytest.mark.tag("MaxKeys")
    def test_bucket_list_versions_max_keys_zero(self):
        key_names = ["bar", "baz", "foo", "quxx"]
        client = self.get_client()
        bucket_name = self.create_objects(client, *key_names)

        response = client.list_object_versions(Bucket=bucket_name, MaxKeys=0)
        assert response["IsTruncated"] is False
        assert len(self.get_keys2(response.get("Versions"))) == 0

    @pytest.mark.tag("MaxKeys")
    def test_bucket_list_versions_max_keys_none(self):
        key_names = ["bar", "baz", "foo", "quxx"]
        client = self.get_client()
        bucket_name = self.create_objects(client, *key_names)

        response = client.list_object_versions(Bucket=bucket_name)
        assert response["IsTruncated"] is False
        assert self.get_keys2(response.get("Versions")) == key_names
        assert response["MaxKeys"] == 1000

    @pytest.mark.tag("marker")
    def test_bucket_list_versions_marker_none(self):
        key_names = ["bar", "baz", "foo", "quxx"]
        client = self.get_client()
        bucket_name = self.create_objects(client, *key_names)

        response = client.list_object_versions(Bucket=bucket_name, KeyMarker="")
        assert response.get("NextKeyMarker") is None

    @pytest.mark.tag("marker")
    def test_bucket_list_versions_marker_empty(self):
        key_names = ["bar", "baz", "foo", "quxx"]
        client = self.get_client()
        bucket_name = self.create_objects(client, *key_names)

        response = client.list_object_versions(Bucket=bucket_name, KeyMarker="")
        assert response.get("NextKeyMarker") is None
        assert response["IsTruncated"] is False
        assert self.get_keys2(response.get("Versions")) == key_names

    @pytest.mark.tag("marker")
    def test_bucket_list_versions_marker_unreadable(self):
        key_names = ["bar", "baz", "foo", "quxx"]
        client = self.get_client()
        bucket_name = self.create_objects(client, *key_names)
        marker = "\n"

        response = client.list_object_versions(Bucket=bucket_name, KeyMarker=marker)
        assert response.get("NextKeyMarker") is None
        assert response["IsTruncated"] is False
        assert self.get_keys2(response.get("Versions")) == key_names

    @pytest.mark.tag("marker")
    def test_bucket_list_versions_marker_not_in_list(self):
        key_names = ["bar", "baz", "foo", "quxx"]
        client = self.get_client()
        bucket_name = self.create_objects(client, *key_names)
        marker = "blah"

        response = client.list_object_versions(Bucket=bucket_name, KeyMarker=marker)
        assert response.get("KeyMarker") == marker
        assert self.get_keys2(response.get("Versions")) == ["foo", "quxx"]

    @pytest.mark.tag("marker")
    def test_bucket_list_versions_marker_after_list(self):
        key_names = ["bar", "baz", "foo", "quxx"]
        client = self.get_client()
        bucket_name = self.create_objects(client, *key_names)
        marker = "zzz"

        response = client.list_object_versions(Bucket=bucket_name, KeyMarker=marker)
        assert response.get("KeyMarker") == marker
        assert response["IsTruncated"] is False
        assert len(self.get_keys2(response.get("Versions"))) == 0

    @pytest.mark.tag("Metadata")
    def test_bucket_list_versions_return_data(self):
        keys = ["bar", "baz", "foo"]
        client = self.get_client()
        bucket_name = self.create_bucket(client)

        self.check_configure_versioning_retry(bucket_name, "Enabled")
        self.create_objects_list(client, keys, bucket_name)

        data_list = []
        for key in keys:
            obj_response = client.head_object(Bucket=bucket_name, Key=key)
            acl_response = client.get_object_acl(Bucket=bucket_name, Key=key)
            data_list.append(
                {
                    "key": key,
                    "display_name": acl_response["Owner"].get("DisplayName"),
                    "id": acl_response["Owner"]["ID"],
                    "e_tag": obj_response["ETag"],
                    "last_modified": obj_response["LastModified"],
                    "content_length": obj_response["ContentLength"],
                    "version_id": obj_response.get("VersionId"),
                }
            )

        response = client.list_object_versions(Bucket=bucket_name)
        for obj in response.get("Versions", []):
            key = obj["Key"]
            data = self.get_object_to_key(key, data_list)
            assert data is not None
            self.assert_list_object_return_data(obj, data)
            if data.get("version_id") is not None:
                assert data["version_id"] == obj["VersionId"]
            else:
                head = client.head_object(Bucket=bucket_name, Key=key)
                assert head.get("VersionId") == obj["VersionId"]

    @pytest.mark.tag("ACL")
    def test_bucket_list_versions_objects_anonymous(self):
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client, "public-read")
        public_client = self.get_public_client()
        public_client.list_object_versions(Bucket=bucket_name)

    @pytest.mark.tag("ACL")
    def test_bucket_list_versions_objects_anonymous_fail(self):
        bucket_name = self.create_bucket()
        public_client = self.get_public_client()
        self.assert_client_error(
            lambda: public_client.list_object_versions(Bucket=bucket_name),
            403,
            md.ACCESS_DENIED,
        )

    @pytest.mark.tag("ERROR")
    def test_bucket_list_versions_not_exist(self):
        bucket_name = self.get_new_bucket_name_only()
        client = self.get_client()
        self.assert_client_error(
            lambda: client.list_object_versions(Bucket=bucket_name),
            404,
            md.NO_SUCH_BUCKET,
        )

    @pytest.mark.tag("Filtering")
    def test_versioning_bucket_list_filtering_all(self):
        key_names = ["test1/f1", "test2/f2", "test3", "test4/f3", "testF4"]
        client = self.get_client()
        bucket_name = self.create_objects_list(client, key_names)

        marker = "test3"
        delimiter = "/"
        max_keys = 3

        response = client.list_object_versions(
            Bucket=bucket_name, Delimiter=delimiter, MaxKeys=max_keys
        )
        assert response["Delimiter"] == delimiter
        assert response["MaxKeys"] == max_keys
        assert response.get("NextKeyMarker") == marker
        assert response["IsTruncated"] is True
        assert self.get_keys2(response.get("Versions")) == ["test3"]
        assert self.get_prefix_list(response.get("CommonPrefixes")) == ["test1/", "test2/"]

        response = client.list_object_versions(
            Bucket=bucket_name,
            Delimiter=delimiter,
            MaxKeys=max_keys,
            KeyMarker=marker,
        )
        assert response["Delimiter"] == delimiter
        assert response["MaxKeys"] == max_keys
        assert response["IsTruncated"] is False

    @pytest.mark.tag("Object")
    def test_versioning_obj_list_marker(self):
        client = self.get_client()
        bucket_name = self.create_bucket(client)
        key_name = "testVersioningObjListMarker"
        objects = []

        self.check_configure_versioning_retry(bucket_name, "Enabled")

        for index in range(10):
            response = client.put_object(
                Bucket=bucket_name,
                Key=key_name,
                Body=f"{key_name}{index}".encode("utf-8"),
            )
            objects.append(response["VersionId"])

        objects.reverse()

        response = client.list_object_versions(Bucket=bucket_name)
        assert len(response.get("Versions", [])) == len(objects)

        for index, version_id in enumerate(objects):
            assert version_id == response["Versions"][index]["VersionId"]
