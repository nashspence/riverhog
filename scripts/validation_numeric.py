"""Qualification owns numeric resources; evidence only records their exact identity."""

from __future__ import annotations

import hashlib
import json
import re
import sys
from collections.abc import Mapping
from dataclasses import asdict
from pathlib import Path

from riverhog_growth_contracts import NumericWorkPolicy, NumericWorkRefused, NumericWorkUsage

ROOT = Path(__file__).resolve().parents[1]
PROFILE_PATH = Path(__file__).with_name("qualification_numeric.json")
PROFILE_FORMAT = "riverhog-numeric-qualification-profile/v1"
PROFILE_NAME = "qualification"
_FIELDS = ("profile", "max_work", "max_integer_bits", "reader_sha256")
_ENVIRONMENT_PREFIX = "RIVERHOG_VALIDATION_NUMERIC_"
_ENVIRONMENT = {name: _ENVIRONMENT_PREFIX + name.upper() for name in _FIELDS}


class NumericConfigurationError(ValueError):
    """The owning qualification resource configuration is absent or contradictory."""


def numeric_policy_snapshot(policy: NumericWorkPolicy) -> NumericWorkPolicy:
    if type(policy) is not NumericWorkPolicy:
        raise NumericConfigurationError("qualification requires an exact numeric work policy")
    try:
        return NumericWorkPolicy(policy.max_work, policy.max_integer_bits)
    except ValueError as error:
        raise NumericConfigurationError(
            "qualification numeric resource values are invalid"
        ) from error


def numeric_reader_sha256() -> str:
    """Bind the imported implementation, not only a file expected in the checkout."""
    import riverhog_growth_contracts

    loaded = Path(str(riverhog_growth_contracts.__file__)).resolve()
    expected = ROOT / "packages/riverhog-growth-contracts/src/riverhog_growth_contracts/__init__.py"
    try:
        actual = hashlib.sha256(loaded.read_bytes()).hexdigest()
        expected_digest = hashlib.sha256(expected.read_bytes()).hexdigest()
    except OSError as error:
        raise NumericConfigurationError(
            "qualification numeric reader source is unavailable"
        ) from error
    if actual != expected_digest:
        raise NumericConfigurationError(
            "loaded numeric reader differs from the qualification checkout"
        )
    return actual


def qualification_numeric_policy() -> NumericWorkPolicy:
    """Read the one explicit source-owned profile; missing configuration is an error."""

    def unique_fields(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result = dict(pairs)
        if len(result) != len(pairs):
            raise NumericConfigurationError("qualification numeric resources have duplicate fields")
        return result

    try:
        value = json.loads(PROFILE_PATH.read_bytes(), object_pairs_hook=unique_fields)
    except (OSError, ValueError) as error:
        raise NumericConfigurationError(
            "qualification numeric resource configuration is unavailable"
        ) from error
    if (
        type(value) is not dict
        or set(value) != {"format", "profile", "max_work", "max_integer_bits"}
        or value["format"] != PROFILE_FORMAT
        or value["profile"] != PROFILE_NAME
    ):
        raise NumericConfigurationError("qualification numeric resource configuration is malformed")
    try:
        return NumericWorkPolicy(value["max_work"], value["max_integer_bits"])
    except ValueError as error:
        raise NumericConfigurationError(
            "qualification numeric resource values are invalid"
        ) from error


def numeric_policy_identity(policy: NumericWorkPolicy) -> dict[str, str]:
    """The fixed profile name and reader identity accompany explicit resource values."""
    selected = numeric_policy_snapshot(policy)
    return {
        "profile": PROFILE_NAME,
        "max_work": str(selected.max_work),
        "max_integer_bits": str(selected.max_integer_bits),
        "reader_sha256": numeric_reader_sha256(),
    }


def _from_identity(identity: Mapping[str, object]) -> NumericWorkPolicy:
    if set(identity) != set(_FIELDS) or identity["profile"] != PROFILE_NAME:
        raise NumericConfigurationError(
            "missing or malformed qualification numeric resource identity"
        )
    values = []
    for name in ("max_work", "max_integer_bits"):
        value = identity[name]
        if type(value) is not str or re.fullmatch(r"[1-9][0-9]*", value) is None:
            raise NumericConfigurationError(
                "qualification numeric resources require positive exact decimals"
            )
        try:
            values.append(int(value))
        except ValueError as error:
            raise NumericConfigurationError(
                "qualification numeric resource encoding is invalid"
            ) from error
    if identity["reader_sha256"] != numeric_reader_sha256():
        raise NumericConfigurationError(
            "qualification numeric reader identity is stale or mismatched"
        )
    return NumericWorkPolicy(*values)


def numeric_policy_conditions(policy: NumericWorkPolicy) -> tuple[tuple[str, str], ...]:
    return tuple(
        ("growth_numeric_" + key, value) for key, value in numeric_policy_identity(policy).items()
    )


def numeric_policy_from_conditions(conditions: Mapping[str, str]) -> NumericWorkPolicy:
    return _from_identity(
        {
            key.removeprefix("growth_numeric_"): value
            for key, value in conditions.items()
            if key.startswith("growth_numeric_")
        }
    )


def numeric_environment(policy: NumericWorkPolicy) -> dict[str, str]:
    return {_ENVIRONMENT[key]: value for key, value in numeric_policy_identity(policy).items()}


def numeric_policy_from_environment(environment: Mapping[str, str]) -> NumericWorkPolicy:
    if any(
        name.startswith(_ENVIRONMENT_PREFIX) and name not in _ENVIRONMENT.values()
        for name in environment
    ):
        raise NumericConfigurationError(
            "qualification numeric environment has unknown resource fields"
        )
    return _from_identity(
        {key: environment[value] for key, value in _ENVIRONMENT.items() if value in environment}
    )


def bind_numeric_environment(environment: dict[str, str], policy: NumericWorkPolicy) -> None:
    """Install owner-selected conditions without permitting inherited escalation."""
    selected = numeric_environment(policy)
    if any(name.startswith(_ENVIRONMENT_PREFIX) for name in environment):
        inherited = numeric_policy_from_environment(environment)
        if numeric_policy_identity(inherited) != numeric_policy_identity(policy):
            raise NumericConfigurationError(
                "inherited numeric resources differ from the owning validation work"
            )
    environment.update(selected)


def numeric_validation_receipt(
    policy: NumericWorkPolicy, source: Mapping[str, object], usage: NumericWorkUsage
) -> dict[str, object]:
    if type(usage) is not NumericWorkUsage:
        raise ValueError("numeric validation requires its original completed usage")
    return {
        "format": "riverhog-numeric-validation/v1",
        "source": dict(source),
        "numeric_policy": numeric_policy_identity(policy),
        "usage": {name: str(value) for name, value in asdict(usage).items()},
    }


def numeric_refusal_receipt(
    error: NumericWorkRefused, *, numeric_policy: NumericWorkPolicy
) -> dict[str, str]:
    """Retain finite refusal facts without serializing an original traceback."""
    if (
        type(error.reason) is not str
        or error.reason
        not in {
            "work_allowance_exhausted",
            "operand_width",
            "operation_width",
            "unsupported_numeric_operand",
            "unsupported_source_record",
            "unsupported_changed_source_record",
        }
        or type(error.operation) is not str
        or not error.operation
        or len(error.operation) > 128
    ):
        raise ValueError("numeric refusal has no bounded diagnostic identity")
    values = {
        "max_work": error.limit,
        "max_integer_bits": error.max_integer_bits,
        "consumed": error.consumed,
        "requested": error.requested,
    }
    if any(type(value) is not int or value < 0 for value in values.values()):
        raise ValueError("numeric refusal has invalid work diagnostics")
    selected = numeric_policy_snapshot(numeric_policy)
    if error.limit != selected.max_work or error.max_integer_bits != selected.max_integer_bits:
        raise ValueError("numeric refusal contradicts the owning resource policy")
    # Callback exceptions are data too. Bound diagnostic conversion before str();
    # this transport check never authorizes numeric work or changes its outcome.
    if any(
        value.bit_length() > sys.int_info.str_digits_check_threshold for value in values.values()
    ):
        raise ValueError("numeric refusal diagnostics exceed the bounded encoding")
    return {
        "reason": error.reason,
        "operation": error.operation,
        **{name: str(value) for name, value in values.items()},
    }
