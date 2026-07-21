"""User credential and grant data."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class UserData:
    display_name: str = ""
    id: str = ""
    email: str = ""
    access_key: str = ""
    secret_key: str = ""
    kms: str = ""
    x_auth_token: str = ""

    def to_grantee(self) -> dict:
        return {"ID": self.id, "Type": "CanonicalUser"}

    def to_grant(self, permission: str) -> dict:
        return {"Grantee": self.to_grantee(), "Permission": permission}

    def to_owner(self) -> dict:
        return {"ID": self.id, "DisplayName": self.display_name}
