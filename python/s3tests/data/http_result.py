"""HTTP upload result ported from Java MyResult."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class HttpResult:
    status_code: int = -1
    message: str = ""
    url: str = field(default="")

    def get_error_code(self) -> str:
        start = self.message.find("<Message>")
        end = self.message.find("</Message>")
        if start != -1 and end != -1:
            return self.message[start:end]
        return self.message

    def get_content(self) -> str:
        if not self.message:
            return ""
        return self.message[:-1] if self.message.endswith("\r") else self.message
