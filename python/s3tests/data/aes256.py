"""AES256 encryption helpers ported from Java AES256."""

from __future__ import annotations

import base64
import os

from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC


def encrypt(msg: str, key: str) -> str:
    salt_bytes = os.urandom(20)
    secret = _derive_key(key, salt_bytes)
    iv = os.urandom(16)
    cipher = Cipher(algorithms.AES(secret), modes.CBC(iv))
    encryptor = cipher.encryptor()
    padded = _pkcs5_pad(msg.encode("utf-8"), 16)
    encrypted = encryptor.update(padded) + encryptor.finalize()
    buffer = salt_bytes + iv + encrypted
    return base64.b64encode(buffer).decode("ascii")


def decrypt(msg: str, key: str) -> str:
    buffer = base64.b64decode(msg)
    salt_bytes = buffer[:20]
    iv_bytes = buffer[20:36]
    encrypted_text_bytes = buffer[36:]
    secret = _derive_key(key, salt_bytes)
    cipher = Cipher(algorithms.AES(secret), modes.CBC(iv_bytes))
    decryptor = cipher.decryptor()
    decrypted = decryptor.update(encrypted_text_bytes) + decryptor.finalize()
    return _pkcs5_unpad(decrypted).decode("utf-8")


def _derive_key(key: str, salt_bytes: bytes) -> bytes:
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA1(),
        length=32,
        salt=salt_bytes,
        iterations=70000,
    )
    return kdf.derive(key.encode("utf-8"))


def _pkcs5_pad(data: bytes, block_size: int) -> bytes:
    pad_len = block_size - (len(data) % block_size)
    return data + bytes([pad_len]) * pad_len


def _pkcs5_unpad(data: bytes) -> bytes:
    pad_len = data[-1]
    return data[:-pad_len]
