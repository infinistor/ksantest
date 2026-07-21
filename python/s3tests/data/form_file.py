"""Multipart form file payload for POST uploads."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class FormFile:
    name: str
    content_type: str
    body: str
