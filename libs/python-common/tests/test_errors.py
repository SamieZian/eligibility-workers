from eligibility_common.errors import AppError, ConflictError, DomainError, InfraError, ValidationError


def test_domain_error_defaults() -> None:
    e = DomainError("X", "y")
    assert e.code == "X"
    assert e.http_status == 409
    assert e.retryable is False


def test_validation_error_422() -> None:
    e = ValidationError("BAD", "nope")
    assert e.http_status == 422


def test_conflict_retryable_412() -> None:
    e = ConflictError()
    assert e.http_status == 412
    assert e.retryable is True


def test_infra_error_5xx_retryable() -> None:
    e = InfraError("DOWN", "oops")
    assert e.http_status == 503
    assert e.retryable is True


def test_app_error_is_exception() -> None:
    with_exc = AppError("X", "y")
    assert isinstance(with_exc, Exception)
    assert str(with_exc) == "X: y"
