# stove0_target_support.TargetFailure

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetfailure:09ce8d2403 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-dce584700f"></a>
| Field | Shape |
|---|---|
| <a id="s-cb48ca51e4"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-f66d8a8288"></a>`distribution` | "stove0-target-support" |
| <a id="s-7184615570"></a>`module` | "stove0_target_support" |
| <a id="s-ded8318de1"></a>`name` | "TargetFailure" |
| <a id="s-c5ab76ee59"></a>`unit` | "export" |

## Governing policies

- <a id="pa-46bcc9ff56"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.TargetFailure`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 845b2cc11290f5639995a3076dd376c060d177f797c48eedd4249a0e5ea817da -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "1f3471f4274fa0dc57b46bac02964b1e6b6f07bbd8a9a19f618b2031ccb0c065",
    "signature": "\"(*, code: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], message: Annotated[str, MinLen(min_length=1), MaxLen(max_length=1000)], retryable: bool) -> None\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "TargetFailure",
  "unit": "export"
}
```
