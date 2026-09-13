# stove0_target_support.TargetExecutionEvidence

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetexecutionevidence:18a625fb6c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-28ade4e82b"></a>
| Field | Shape |
|---|---|
| <a id="s-d77b25774c"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-f90bb8cbbd"></a>`distribution` | "stove0-target-support" |
| <a id="s-3ddba0acd9"></a>`module` | "stove0_target_support" |
| <a id="s-07f7bd1438"></a>`name` | "TargetExecutionEvidence" |
| <a id="s-fc09de4a00"></a>`unit` | "export" |

## Governing policies

- <a id="pa-199ca6d1d8"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.TargetExecutionEvidence`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 162ea081cdde2ae61d6e0262abe960cf43c572cf1b7ebef7043a43aa8dd274d8 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "f76617128b8960e01b2b657a4031d97d1632c188c092473b906be914186ef931",
    "signature": "\"(*, target_contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], operation_contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], plan_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], execution_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], runtime: dict[str, JsonValue] = <factory>) -> None\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "TargetExecutionEvidence",
  "unit": "export"
}
```
