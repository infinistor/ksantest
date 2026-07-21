"""KMS tests ported from Java testV2/KMS.java."""

from __future__ import annotations

import pytest

from s3tests.test_base import S3TestBase


class TestKms(S3TestBase):
    @pytest.mark.tag("PutGet")
    def test_sse_kms_encrypted_transfer_1b(self):
        client = self.get_client()
        self.create_bucket(client)
