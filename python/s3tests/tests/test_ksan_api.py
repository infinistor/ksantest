"""KsanApi tests ported from Java test/KsanApiTest.java."""

from __future__ import annotations

import pytest

from s3tests.test_base import S3TestBase


class TestKsanApi(S3TestBase):
    @pytest.mark.tag("Get")
    def test_get_bucket_enabled_tagging_index(self):
        client = self.get_client()
        self.create_bucket(client, 1)

    @pytest.mark.tag("Put")
    def test_put_bucket_enabled_tagging_index(self):
        client = self.get_client()
        self.create_bucket(client, 2)
