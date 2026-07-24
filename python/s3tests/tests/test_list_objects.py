"""ListObjects tests ported from Java testV2/ListObjects.java."""

from __future__ import annotations

import pytest

from s3tests.data import main_data as md
from s3tests.test_base import S3TestBase


class TestListObjects(S3TestBase):
    @pytest.mark.tag("Check")
    def test_bucket_list_many(self):
        client = self.get_client()
        bucket_name = self.create_objects(client, 1, "foo", "bar", "baz")

        response = client.list_objects(Bucket=bucket_name, MaxKeys=2)
        assert self.get_keys(response.get("Contents")) == ["bar", "baz"]
        assert len(response["Contents"]) == 2
        assert response["IsTruncated"] is True

        response = client.list_objects(Bucket=bucket_name, Marker="baz", MaxKeys=2)
        assert self.get_keys(response.get("Contents")) == ["foo"]
        assert len(response["Contents"]) == 1
        assert response["IsTruncated"] is False

    @pytest.mark.tag("delimiter")
    def test_bucket_list_delimiter_basic(self):
        client = self.get_client()
        bucket_name = self.create_objects(client, 2, "foo/bar", "foo/bars/xyzzy", "quux/thud", "asdf")
        delimiter = "/"

        response = client.list_objects(Bucket=bucket_name, Delimiter=delimiter)
        assert response["Delimiter"] == delimiter
        assert self.get_keys(response.get("Contents")) == ["asdf"]
        prefixes = self.get_prefix_list(response.get("CommonPrefixes"))
        assert len(prefixes) == 2
        assert prefixes == ["foo/", "quux/"]

    @pytest.mark.tag("Encoding")
    def test_bucket_list_encoding_basic(self):
        client = self.get_client()
        bucket_name = self.create_objects(client, 3, "foo+1/bar", "foo/bar/xyzzy", "quux ab/thud", "asdf+b")
        delimiter = "/"

        response = client.list_objects(Bucket=bucket_name, Delimiter=delimiter, EncodingType="url")
        encoding = response.get("EncodingType")
        assert response["Delimiter"] == delimiter
        assert self.get_keys(response.get("Contents"), encoding) == ["asdf+b"]
        prefixes = self.get_prefix_list(response.get("CommonPrefixes"), encoding)
        assert len(prefixes) == 3
        assert sorted(prefixes) == sorted(["foo+1/", "foo/", "quux ab/"])

    @pytest.mark.tag("Filtering")
    def test_bucket_list_delimiter_prefix(self):
        delimiter = "/"
        prefix = ""
        client = self.get_client()
        bucket_name = self.create_objects_keys(client, 4, ["asdf", "boo/bar", "boo/baz/xyzzy", "cquux/thud", "cquux/bla"])

        marker = self.validate_list_object(bucket_name, prefix, delimiter, "", 1, True, ["asdf"], [], "asdf")
        marker = self.validate_list_object(bucket_name, prefix, delimiter, marker, 1, True, [], ["boo/"], "boo/")
        self.validate_list_object(bucket_name, prefix, delimiter, marker, 1, False, [], ["cquux/"], None)

        marker = self.validate_list_object(bucket_name, prefix, delimiter, "", 2, True, ["asdf"], ["boo/"], "boo/")
        self.validate_list_object(bucket_name, prefix, delimiter, marker, 2, False, [], ["cquux/"], None)

        prefix = "boo/"
        marker = self.validate_list_object(bucket_name, prefix, delimiter, "", 1, True, ["boo/bar"], [], "boo/bar")
        self.validate_list_object(bucket_name, prefix, delimiter, marker, 1, False, [], ["boo/baz/"], None)
        self.validate_list_object(bucket_name, prefix, delimiter, "", 2, False, ["boo/bar"], ["boo/baz/"], None)

    @pytest.mark.tag("Filtering")
    def test_bucket_list_delimiter_prefix_ends_with_delimiter(self):
        client = self.get_client()
        bucket_name = self.create_objects(client, 5, "asdf/")
        self.validate_list_object(bucket_name, "asdf/", "/", "", 1000, False, ["asdf/"], [], None)

    @pytest.mark.tag("delimiter")
    def test_bucket_list_delimiter_alt(self):
        client = self.get_client()
        bucket_name = self.create_objects(client, 6, "bar", "baz", "cab", "foo")
        delimiter = "a"

        response = client.list_objects(Bucket=bucket_name, Delimiter=delimiter)
        assert response["Delimiter"] == delimiter
        assert self.get_keys(response.get("Contents")) == ["foo"]
        prefixes = self.get_prefix_list(response.get("CommonPrefixes"))
        assert len(prefixes) == 2
        assert prefixes == ["ba", "ca"]

    @pytest.mark.tag("Filtering")
    def test_bucket_list_delimiter_prefix_underscore(self):
        delimiter = "/"
        prefix = ""
        client = self.get_client()
        bucket_name = self.create_objects_keys(client, 7, ["Obj1_", "Under1/bar", "Under1/baz/xyzzy", "Under2/thud", "Under2/bla"])

        marker = self.validate_list_object(bucket_name, prefix, delimiter, "", 1, True, ["Obj1_"], [], "Obj1_")
        marker = self.validate_list_object(bucket_name, prefix, delimiter, marker, 1, True, [], ["Under1/"], "Under1/")
        self.validate_list_object(bucket_name, prefix, delimiter, marker, 1, False, [], ["Under2/"], None)

        marker = self.validate_list_object(bucket_name, prefix, delimiter, "", 2, True, ["Obj1_"], ["Under1/"], "Under1/")
        self.validate_list_object(bucket_name, prefix, delimiter, marker, 2, False, [], ["Under2/"], None)

        prefix = "Under1/"
        marker = self.validate_list_object(bucket_name, prefix, delimiter, "", 1, True, ["Under1/bar"], [], "Under1/bar")
        self.validate_list_object(bucket_name, prefix, delimiter, marker, 1, False, [], ["Under1/baz/"], None)
        self.validate_list_object(bucket_name, prefix, delimiter, "", 2, False, ["Under1/bar"], ["Under1/baz/"], None)

    @pytest.mark.tag("delimiter")
    def test_bucket_list_delimiter_percentage(self):
        client = self.get_client()
        bucket_name = self.create_objects(client, 8, "b%ar", "b%az", "c%ab", "foo")
        delimiter = "%"

        response = client.list_objects(Bucket=bucket_name, Delimiter=delimiter)
        assert response["Delimiter"] == delimiter
        assert self.get_keys(response.get("Contents")) == ["foo"]
        prefixes = self.get_prefix_list(response.get("CommonPrefixes"))
        assert len(prefixes) == 2
        assert prefixes == ["b%", "c%"]

    @pytest.mark.tag("delimiter")
    def test_bucket_list_delimiter_whitespace(self):
        client = self.get_client()
        bucket_name = self.create_objects(client, 9, "b ar", "b az", "c ab", "foo")
        delimiter = " "

        response = client.list_objects(Bucket=bucket_name, Delimiter=delimiter)
        assert response["Delimiter"] == delimiter
        assert self.get_keys(response.get("Contents")) == ["foo"]
        prefixes = self.get_prefix_list(response.get("CommonPrefixes"))
        assert len(prefixes) == 2
        assert prefixes == ["b ", "c "]

    @pytest.mark.tag("delimiter")
    def test_bucket_list_delimiter_dot(self):
        client = self.get_client()
        bucket_name = self.create_objects(client, 10, "b.ar", "b.az", "c.ab", "foo")
        delimiter = "."

        response = client.list_objects(Bucket=bucket_name, Delimiter=delimiter)
        assert response["Delimiter"] == delimiter
        assert self.get_keys(response.get("Contents")) == ["foo"]
        prefixes = self.get_prefix_list(response.get("CommonPrefixes"))
        assert len(prefixes) == 2
        assert prefixes == ["b.", "c."]

    @pytest.mark.tag("delimiter")
    def test_bucket_list_delimiter_unreadable(self):
        key_names = ["bar", "baz", "cab", "foo"]
        client = self.get_client()
        bucket_name = self.create_objects(client, 11, *key_names)
        delimiter = "\n"

        response = client.list_objects(Bucket=bucket_name, Delimiter=delimiter)
        assert response["Delimiter"] == delimiter
        assert self.get_keys(response.get("Contents")) == key_names
        assert len(self.get_prefix_list(response.get("CommonPrefixes"))) == 0

    @pytest.mark.tag("delimiter")
    def test_bucket_list_delimiter_empty(self):
        key_names = ["bar", "baz", "cab", "foo"]
        client = self.get_client()
        bucket_name = self.create_objects(client, 12, *key_names)
        delimiter = ""

        response = client.list_objects(Bucket=bucket_name, Delimiter=delimiter)
        assert response.get("Delimiter") in (None, "")
        assert self.get_keys(response.get("Contents")) == key_names
        assert len(self.get_prefix_list(response.get("CommonPrefixes"))) == 0

    @pytest.mark.tag("delimiter")
    def test_bucket_list_delimiter_none(self):
        key_names = ["bar", "baz", "cab", "foo"]
        client = self.get_client()
        bucket_name = self.create_objects(client, 13, *key_names)

        response = client.list_objects(Bucket=bucket_name)
        assert response.get("Delimiter") in (None, "")
        assert self.get_keys(response.get("Contents")) == key_names
        assert len(self.get_prefix_list(response.get("CommonPrefixes"))) == 0

    @pytest.mark.tag("delimiter")
    def test_bucket_list_delimiter_not_exist(self):
        key_names = ["bar", "baz", "cab", "foo"]
        client = self.get_client()
        bucket_name = self.create_objects(client, 14, *key_names)
        delimiter = "/"

        response = client.list_objects(Bucket=bucket_name, Delimiter=delimiter)
        assert response["Delimiter"] == delimiter
        assert self.get_keys(response.get("Contents")) == key_names
        assert len(self.get_prefix_list(response.get("CommonPrefixes"))) == 0

    @pytest.mark.tag("delimiter")
    def test_bucket_list_delimiter_not_skip_special(self):
        key_names = [f"0/{i}" for i in range(1000, 1999)]
        key_names2 = ["1999", "1999#", "1999+", "2000"]
        key_names.extend(key_names2)
        client = self.get_client()
        bucket_name = self.create_objects(client, 15, key_names)
        delimiter = "/"

        response = client.list_objects(Bucket=bucket_name, Delimiter=delimiter)
        assert response["Delimiter"] == delimiter
        assert self.get_keys(response.get("Contents")) == key_names2
        assert self.get_prefix_list(response.get("CommonPrefixes")) == ["0/"]

    @pytest.mark.tag("prefix")
    def test_bucket_list_prefix_basic(self):
        client = self.get_client()
        bucket_name = self.create_objects(client, 16, "foo/bar", "foo/baz", "quux")
        prefix = "foo/"

        response = client.list_objects(Bucket=bucket_name, Prefix=prefix)
        assert response["Prefix"] == prefix
        assert self.get_keys(response.get("Contents")) == ["foo/bar", "foo/baz"]
        assert len(self.get_prefix_list(response.get("CommonPrefixes"))) == 0

    @pytest.mark.tag("prefix")
    def test_bucket_list_prefix_alt(self):
        client = self.get_client()
        bucket_name = self.create_objects(client, 17, "bar", "baz", "foo")
        prefix = "ba"

        response = client.list_objects(Bucket=bucket_name, Prefix=prefix)
        assert response["Prefix"] == prefix
        assert self.get_keys(response.get("Contents")) == ["bar", "baz"]
        assert len(self.get_prefix_list(response.get("CommonPrefixes"))) == 0

    @pytest.mark.tag("prefix")
    def test_bucket_list_prefix_empty(self):
        key_names = ["foo/bar", "foo/baz", "quux"]
        client = self.get_client()
        bucket_name = self.create_objects(client, 18, *key_names)
        prefix = ""

        response = client.list_objects(Bucket=bucket_name, Prefix=prefix)
        assert response.get("Prefix", "") == ""
        assert self.get_keys(response.get("Contents")) == key_names
        assert len(self.get_prefix_list(response.get("CommonPrefixes"))) == 0

    @pytest.mark.tag("prefix")
    def test_bucket_list_prefix_none(self):
        key_names = ["foo/bar", "foo/baz", "quux"]
        client = self.get_client()
        bucket_name = self.create_objects(client, 19, *key_names)

        response = client.list_objects(Bucket=bucket_name)
        assert response.get("Prefix", "") == ""
        assert self.get_keys(response.get("Contents")) == key_names
        assert len(self.get_prefix_list(response.get("CommonPrefixes"))) == 0

    @pytest.mark.tag("prefix")
    def test_bucket_list_prefix_not_exist(self):
        key_names = ["foo/bar", "foo/baz", "quux"]
        client = self.get_client()
        bucket_name = self.create_objects(client, 20, *key_names)
        prefix = "d"

        response = client.list_objects(Bucket=bucket_name, Prefix=prefix)
        assert response["Prefix"] == prefix
        assert self.get_keys(response.get("Contents")) == []
        assert len(self.get_prefix_list(response.get("CommonPrefixes"))) == 0

    @pytest.mark.tag("prefix")
    def test_bucket_list_prefix_unreadable(self):
        key_names = ["foo/bar", "foo/baz", "quux"]
        client = self.get_client()
        bucket_name = self.create_objects(client, 21, *key_names)
        prefix = "\n"

        response = client.list_objects(Bucket=bucket_name, Prefix=prefix)
        assert self.get_response_prefix(response) == prefix
        assert self.get_keys(response.get("Contents")) == []
        assert len(self.get_prefix_list(response.get("CommonPrefixes"))) == 0

    @pytest.mark.tag("PrefixAndDelimiter")
    def test_bucket_list_prefix_delimiter_basic(self):
        client = self.get_client()
        bucket_name = self.create_objects(client, 22, "foo/bar", "foo/baz/xyzzy", "quux/thud", "asdf")
        prefix = "foo/"
        delimiter = "/"

        response = client.list_objects(Bucket=bucket_name, Delimiter=delimiter, Prefix=prefix)
        assert response["Prefix"] == prefix
        assert response["Delimiter"] == delimiter
        assert self.get_keys(response.get("Contents")) == ["foo/bar"]
        assert self.get_prefix_list(response.get("CommonPrefixes")) == ["foo/baz/"]

    @pytest.mark.tag("PrefixAndDelimiter")
    def test_bucket_list_prefix_delimiter_alt(self):
        client = self.get_client()
        bucket_name = self.create_objects(client, 23, "bar", "bazar", "cab", "foo")
        delimiter = "a"
        prefix = "ba"

        response = client.list_objects(Bucket=bucket_name, Delimiter=delimiter, Prefix=prefix)
        assert response["Prefix"] == prefix
        assert response["Delimiter"] == delimiter
        assert self.get_keys(response.get("Contents")) == ["bar"]
        assert self.get_prefix_list(response.get("CommonPrefixes")) == ["baza"]

    @pytest.mark.tag("PrefixAndDelimiter")
    def test_bucket_list_prefix_delimiter_prefix_not_exist(self):
        client = self.get_client()
        bucket_name = self.create_objects(client, 24, "b/a/r", "b/a/c", "b/a/g", "g")

        response = client.list_objects(Bucket=bucket_name, Delimiter="d", Prefix="/")
        assert self.get_keys(response.get("Contents")) == []
        assert len(self.get_prefix_list(response.get("CommonPrefixes"))) == 0

    @pytest.mark.tag("PrefixAndDelimiter")
    def test_bucket_list_prefix_delimiter_delimiter_not_exist(self):
        client = self.get_client()
        bucket_name = self.create_objects(client, 25, "b/a/c", "b/a/g", "b/a/r", "g")

        response = client.list_objects(Bucket=bucket_name, Delimiter="z", Prefix="b")
        assert self.get_keys(response.get("Contents")) == ["b/a/c", "b/a/g", "b/a/r"]
        assert len(self.get_prefix_list(response.get("CommonPrefixes"))) == 0

    @pytest.mark.tag("PrefixAndDelimiter")
    def test_bucket_list_prefix_delimiter_prefix_delimiter_not_exist(self):
        client = self.get_client()
        bucket_name = self.create_objects(client, 26, "b/a/r", "b/a/c", "b/a/g", "g")

        response = client.list_objects(Bucket=bucket_name, Delimiter="z", Prefix="y")
        assert self.get_keys(response.get("Contents")) == []
        assert len(self.get_prefix_list(response.get("CommonPrefixes"))) == 0

    @pytest.mark.tag("maxKeys")
    def test_bucket_list_max_keys_one(self):
        key_names = ["bar", "baz", "foo", "quxx"]
        client = self.get_client()
        bucket_name = self.create_objects(client, 27, *key_names)

        response = client.list_objects(Bucket=bucket_name, MaxKeys=1)
        assert response["IsTruncated"] is True
        assert self.get_keys(response.get("Contents")) == key_names[:1]

        response = client.list_objects(Bucket=bucket_name, Marker=key_names[0])
        assert response["IsTruncated"] is False
        assert self.get_keys(response.get("Contents")) == key_names[1:]

    @pytest.mark.tag("maxKeys")
    def test_bucket_list_max_keys_zero(self):
        key_names = ["bar", "baz", "foo", "quxx"]
        client = self.get_client()
        bucket_name = self.create_objects(client, 28, *key_names)

        response = client.list_objects(Bucket=bucket_name, MaxKeys=0)
        assert response["IsTruncated"] is False
        assert len(self.get_keys(response.get("Contents"))) == 0

    @pytest.mark.tag("maxKeys")
    def test_bucket_list_max_keys_none(self):
        key_names = ["bar", "baz", "foo", "quxx"]
        client = self.get_client()
        bucket_name = self.create_objects(client, 29, *key_names)

        response = client.list_objects(Bucket=bucket_name)
        assert response["IsTruncated"] is False
        assert self.get_keys(response.get("Contents")) == key_names
        assert response["MaxKeys"] == 1000

    @pytest.mark.tag("marker")
    def test_bucket_list_marker_none(self):
        key_names = ["bar", "baz", "foo", "quxx"]
        client = self.get_client()
        bucket_name = self.create_objects(client, 30, *key_names)

        response = client.list_objects(Bucket=bucket_name, Marker="")
        assert response.get("NextMarker") is None

    @pytest.mark.tag("marker")
    def test_bucket_list_marker_empty(self):
        key_names = ["bar", "baz", "foo", "quxx"]
        client = self.get_client()
        bucket_name = self.create_objects(client, 31, *key_names)

        response = client.list_objects(Bucket=bucket_name, Marker="")
        assert response.get("NextMarker") is None
        assert response["IsTruncated"] is False
        assert self.get_keys(response.get("Contents")) == key_names

    @pytest.mark.tag("marker")
    def test_bucket_list_marker_unreadable(self):
        key_names = ["bar", "baz", "foo", "quxx"]
        client = self.get_client()
        bucket_name = self.create_objects(client, 32, *key_names)
        marker = "\n"

        response = client.list_objects(Bucket=bucket_name, Marker=marker)
        assert response.get("NextMarker") is None
        assert response["IsTruncated"] is False
        assert self.get_keys(response.get("Contents")) == key_names

    @pytest.mark.tag("marker")
    def test_bucket_list_marker_not_in_list(self):
        key_names = ["bar", "baz", "foo", "quxx"]
        client = self.get_client()
        bucket_name = self.create_objects(client, 33, *key_names)
        marker = "blah"

        response = client.list_objects(Bucket=bucket_name, Marker=marker)
        assert response.get("Marker") == marker
        assert self.get_keys(response.get("Contents")) == ["foo", "quxx"]

    @pytest.mark.tag("marker")
    def test_bucket_list_marker_after_list(self):
        key_names = ["bar", "baz", "foo", "quxx"]
        client = self.get_client()
        bucket_name = self.create_objects(client, 34, *key_names)
        marker = "zzz"

        response = client.list_objects(Bucket=bucket_name, Marker=marker)
        assert response.get("Marker") == marker
        assert response["IsTruncated"] is False
        assert len(self.get_keys(response.get("Contents"))) == 0

    @pytest.mark.tag("Metadata")
    def test_bucket_list_return_data(self):
        key_names = ["bar", "baz", "foo"]
        client = self.get_client()
        bucket_name = self.create_objects(client, 35, *key_names)

        data = []
        for key_name in key_names:
            head_response = client.head_object(Bucket=bucket_name, Key=key_name)
            acl_response = client.get_object_acl(Bucket=bucket_name, Key=key_name)
            data.append(
                {
                    "key": key_name,
                    "display_name": acl_response["Owner"].get("DisplayName"),
                    "id": acl_response["Owner"]["ID"],
                    "e_tag": head_response["ETag"],
                    "last_modified": head_response["LastModified"],
                    "content_length": head_response["ContentLength"],
                }
            )

        response = client.list_objects(Bucket=bucket_name)
        for obj in response.get("Contents", []):
            key_name = obj["Key"]
            key_data = self.get_object_to_key(key_name, data)
            assert key_data is not None
            self.assert_list_object_return_data(obj, key_data)

    @pytest.mark.tag("ACL")
    def test_bucket_list_objects_anonymous(self):
        client = self.get_client()
        bucket_name = self.create_bucket_canned_acl(client, 36, "public-read")
        public_client = self.get_public_client()
        public_client.list_objects(Bucket=bucket_name)

    @pytest.mark.tag("ACL")
    def test_bucket_list_objects_anonymous_fail(self):
        bucket_name = self.create_bucket(37)
        public_client = self.get_public_client()
        self.assert_client_error(
            lambda: public_client.list_objects(Bucket=bucket_name),
            403,
            md.ACCESS_DENIED,
        )

    @pytest.mark.tag("ERROR")
    def test_bucket_not_exist(self):
        bucket_name = self.get_new_bucket_name_only(38)
        client = self.get_client()
        self.assert_client_error(
            lambda: client.list_objects(Bucket=bucket_name),
            404,
            md.NO_SUCH_BUCKET,
        )

    @pytest.mark.tag("Filtering")
    def test_bucket_list_filtering_all(self):
        key_names = ["test1/f1", "test2/f2", "test3", "test4/f3", "testF4"]
        client = self.get_client()
        bucket_name = self.create_objects(client, 39, key_names)

        marker = "test3"
        delimiter = "/"
        max_keys = 3

        response = client.list_objects(Bucket=bucket_name, Delimiter=delimiter, MaxKeys=max_keys)
        assert response["Delimiter"] == delimiter
        assert response["MaxKeys"] == max_keys
        assert response.get("NextMarker") == marker
        assert response["IsTruncated"] is True
        assert self.get_keys(response.get("Contents")) == ["test3"]
        assert self.get_prefix_list(response.get("CommonPrefixes")) == ["test1/", "test2/"]

        response = client.list_objects(Bucket=bucket_name, Delimiter=delimiter, MaxKeys=max_keys, Marker=marker)
        assert response["Delimiter"] == delimiter
        assert response["MaxKeys"] == max_keys
        assert response["IsTruncated"] is False

    @pytest.mark.tag("Versioning")
    def test_bucket_list_versioning(self):
        key_names = ["aaa", "bbb", "ccc"]
        client = self.get_client()
        bucket_name = self.create_bucket(client, 40)

        self.check_configure_versioning_retry(bucket_name, "Enabled")

        for key in key_names:
            for index in range(3):
                client.put_object(Bucket=bucket_name, Key=key, Body=f"{key}{index}".encode("utf-8"))

        response = client.list_objects(Bucket=bucket_name)
        assert len(response.get("Contents", [])) == 3
        assert self.get_keys(response.get("Contents")) == key_names
