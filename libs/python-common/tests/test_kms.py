from eligibility_common.kms import LocalKMS


def test_roundtrip() -> None:
    kms = LocalKMS(master_key=b"0" * 32)
    token = kms.encrypt(b"sensitive-ssn")
    assert "." in token
    assert kms.decrypt(token) == b"sensitive-ssn"


def test_different_keys_produce_different_tokens() -> None:
    kms = LocalKMS(master_key=b"0" * 32)
    a = kms.encrypt(b"same")
    b = kms.encrypt(b"same")
    assert a != b  # random nonce + DEK
