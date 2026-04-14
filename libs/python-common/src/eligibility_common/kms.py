"""Envelope encryption for PHI fields. Local fake; real KMS adapter in prod.

Format:
    ciphertext = b64(wrapped_dek) || "." || b64(nonce) || "." || b64(ct)
"""
from __future__ import annotations

import base64
import os
from dataclasses import dataclass

from cryptography.hazmat.primitives.ciphers.aead import AESGCM


@dataclass
class LocalKMS:
    """Pretends to be Cloud KMS. DEKs are just stored alongside — OK for local dev."""

    master_key: bytes

    @classmethod
    def from_env(cls) -> "LocalKMS":
        raw = os.environ.get("KMS_MASTER_KEY", "0" * 32).encode()[:32].ljust(32, b"0")
        return cls(master_key=raw)

    def _aead(self) -> AESGCM:
        return AESGCM(self.master_key)

    def encrypt(self, plaintext: bytes) -> str:
        dek = os.urandom(32)
        nonce = os.urandom(12)
        ct = AESGCM(dek).encrypt(nonce, plaintext, None)
        wrapped = self._aead().encrypt(nonce, dek, None)
        return ".".join(base64.urlsafe_b64encode(p).decode() for p in (wrapped, nonce, ct))

    def decrypt(self, token: str) -> bytes:
        wrapped_b64, nonce_b64, ct_b64 = token.split(".")
        wrapped = base64.urlsafe_b64decode(wrapped_b64)
        nonce = base64.urlsafe_b64decode(nonce_b64)
        ct = base64.urlsafe_b64decode(ct_b64)
        dek = self._aead().decrypt(nonce, wrapped, None)
        return AESGCM(dek).decrypt(nonce, ct, None)
